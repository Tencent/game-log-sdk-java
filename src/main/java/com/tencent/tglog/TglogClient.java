package com.tencent.tglog;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * High-performance TGLog client for game servers.
 *
 * Features:
 * - Lock-free multi-producer architecture
 * - Multiple worker threads with NIO
 * - Automatic batching and retry
 * - DNS resolution and endpoint management
 * - Rich performance metrics
 *
 * Example usage:
 * <pre>
 * TglogConfig config = TglogConfig.createTcp("logs.game.com", 8080)
 *     .setWorkerNum(8)
 *     .setBatchingMaxMessages(50);
 *
 * try (TglogClient client = new TglogClient(config)) {
 *     client.send("battle|winner=1|loser=2\n", new TglogCallback() {
 *         public void onSuccess(String payload) {
 *             System.out.println("Sent: " + payload);
 *         }
 *         public void onError(String payload, int code, String msg) {
 *             System.err.println("Failed: " + msg);
 *         }
 *     });
 * }
 * </pre>
 */
public class TglogClient implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(TglogClient.class);

    private final TglogConfig config;
    private final TglogMetrics metrics;

    // Components
    private final EndpointManager endpointManager;
    private final List<TglogWorker> workers;
    private final AtomicInteger roundRobinIndex = new AtomicInteger(0);

    // State
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicBoolean closed = new AtomicBoolean(false);

    /**
     * Create a new TGLog client with the specified configuration.
     *
     * @param config Client configuration
     * @throws TglogException if initialization fails
     */
    public TglogClient(TglogConfig config) throws TglogException {
        if (config == null) {
            throw new TglogException("Config cannot be null");
        }

        this.config = config;
        this.metrics = new TglogMetrics();

        // Auto-adjust batching max size based on protocol limits
        int maxRequestSize = config.getMaxRequestSize();
        if (config.getBatchingMaxSize() > maxRequestSize) {
            logger.warn("Batching max size ({}) exceeds protocol limit ({}), adjusting to {}",
                    config.getBatchingMaxSize(), maxRequestSize, maxRequestSize);
            config.setBatchingMaxSize(maxRequestSize);
        }

        // Initialize endpoint manager
        this.endpointManager = new EndpointManager(
                config.getHost(),
                config.getPort(),
                config
        );

        // Initialize workers
        int workerNum = config.getWorkerNum();
        this.workers = new ArrayList<>(workerNum);

        for (int i = 0; i < workerNum; i++) {
            TglogWorker worker = new TglogWorker(i, config, metrics);
            workers.add(worker);
        }

        // Start the client
        start();

        logger.info("TglogClient created: {}://{}:{} (workers={})",
                config.getProtocol(), config.getHost(), config.getPort(), workerNum);
    }

    /**
     * Start the client.
     */
    private void start() throws TglogException {
        if (running.compareAndSet(false, true)) {
            boolean endpointManagerStarted = false;
            List<TglogWorker> startedWorkers = new ArrayList<>();

            try {
                // Start endpoint manager
                endpointManager.start();
                endpointManagerStarted = true;

                // Register endpoint change listener
                endpointManager.addChangeListener(this::onEndpointsChanged);

                // Get initial endpoints
                List<InetSocketAddress> endpoints = endpointManager.getCurrentEndpoints();
                if (endpoints.isEmpty()) {
                    throw new TglogException("No endpoints resolved for: " + config.getHost());
                }

                // Update metrics
                metrics.setEndpointCount(endpoints.size());

                // Set initial endpoints and start all workers
                for (TglogWorker worker : workers) {
                    worker.setInitialEndpoints(endpoints);
                    worker.start();
                    startedWorkers.add(worker);
                }

                logger.info("TglogClient started with {} endpoints", endpoints.size());

            } catch (Exception e) {
                // Cleanup on failure
                logger.error("Failed to start client, cleaning up resources", e);

                // Stop all started workers
                for (TglogWorker worker : startedWorkers) {
                    try {
                        worker.stop();
                    } catch (Exception ex) {
                        logger.error("Error stopping worker during cleanup", ex);
                    }
                }

                // Stop endpoint manager if it was started
                if (endpointManagerStarted) {
                    try {
                        endpointManager.stop();
                    } catch (Exception ex) {
                        logger.error("Error stopping endpoint manager during cleanup", ex);
                    }
                }

                running.set(false);
                throw new TglogException("Failed to start client", e);
            }
        }
    }

    /**
     * Send a log message asynchronously.
     *
     * This method is thread-safe and non-blocking. It uses lock-free queues
     * to distribute messages to worker threads.
     *
     * This method both accepts a callback and returns
     * a CompletableFuture, allowing flexible async programming styles.
     *
     * @param logPayload Log message (must end with \n)
     * @param callback Callback for send result (can be null)
     * @return CompletableFuture that completes when the message is sent
     * @throws TglogException if client is closed or queue is full
     */
    public CompletableFuture<Void> send(String logPayload, TglogCallback callback) throws TglogException {
        // Validate client state - synchronous validation errors only throw exception
        // Check both closed and running to avoid race condition
        if (closed.get() || !running.get()) {
            throw new TglogException(TglogErrorCode.PRODUCER_CLOSED, "Client is closed or not running");
        }

        // Validate payload - synchronous validation errors only throw exception
        if (logPayload == null || logPayload.isEmpty()) {
            throw new TglogException(TglogErrorCode.BAD_LOG, "Log payload cannot be null or empty");
        }

        // Validate message ends with newline (required by protocol)
        if (!logPayload.endsWith("\n")) {
            throw new TglogException(TglogErrorCode.BAD_LOG, "Log payload must end with '\\n'");
        }

        // Create CompletableFuture for this send operation
        CompletableFuture<Void> future = new CompletableFuture<>();

        // Wrap user callback to also complete the future
        TglogCallback wrappedCallback = (payload, exception) -> {
            // Call user callback if provided
            if (callback != null) {
                try {
                    callback.onCompletion(payload, exception);
                } catch (Exception e) {
                    logger.warn("Exception in user callback", e);
                }
            }

            // Complete the future
            if (exception == null) {
                future.complete(null);
            } else {
                future.completeExceptionally(exception);
            }
        };

        // Create log message (which will cache the payload size)
        LogMessage message = new LogMessage(logPayload, wrappedCallback);

        // Check log payload length against protocol limits - synchronous validation
        int payloadLength = message.getPayloadSize();
        int maxRequestSize = config.getMaxRequestSize();
        if (payloadLength > maxRequestSize) {
            String errorMsg = "Log payload too long: " + payloadLength + " bytes exceeds " +
                    config.getProtocol() + " limit of " + maxRequestSize + " bytes";
            throw new TglogException(TglogErrorCode.LOG_TOO_LONG, errorMsg);
        }

        // Update metrics
        metrics.incrementRequestMsg();

        // Select a worker (round-robin)
        TglogWorker worker = selectWorker();

        // Enqueue message - synchronous enqueue failure throws exception
        // Asynchronous send failures will be reported via callback and future
        if (!worker.enqueue(message)) {
            metrics.incrementDroppedMsg();
            throw new TglogException(TglogErrorCode.SEND_QUEUE_IS_FULL, "Message queue is full");
        }

        return future;
    }

    /**
     * Send a log message without callback.
     *
     * @param logPayload Log message
     * @return CompletableFuture that completes when the message is sent
     * @throws TglogException if send fails
     */
    public CompletableFuture<Void> send(String logPayload) throws TglogException {
        return send(logPayload, null);
    }

    /**
     * Select a worker using round-robin.
     * Uses bitwise AND to handle integer overflow safely.
     */
    private TglogWorker selectWorker() {
        // Use bitwise AND with Integer.MAX_VALUE to ensure positive value
        // This handles Integer.MIN_VALUE overflow correctly (unlike Math.abs)
        int index = (roundRobinIndex.getAndIncrement() & Integer.MAX_VALUE) % workers.size();
        return workers.get(index);
    }

    /**
     * Handle endpoint changes from DNS resolution.
     */
    private void onEndpointsChanged(List<InetSocketAddress> newEndpoints) {
        logger.info("Endpoints changed: {} endpoints", newEndpoints.size());

        metrics.setEndpointCount(newEndpoints.size());

        // Notify all workers via lock-free queue (like C version)
        for (TglogWorker worker : workers) {
            worker.notifyEndpointUpdate(newEndpoints);
        }
    }

    /**
     * Get performance metrics.
     *
     * @return Current metrics snapshot
     */
    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings(
        value = "EI_EXPOSE_REP",
        justification = "Intentionally exposing metrics for application access and monitoring")
    public TglogMetrics getMetrics() {
        return metrics;
    }

    /**
     * Get metrics as string.
     *
     * @param json true for JSON format, false for human-readable format
     * @return Metrics string
     */
    public String getMetricsString(boolean json) {
        return json ? metrics.toJson() : metrics.toString();
    }


    /**
     * Flush all pending messages.
     * Waits for all messages in queues, batches, and connection buffers to be sent.
     *
     * @param timeoutMs Maximum time to wait in milliseconds
     * @return true if all messages were flushed, false if timeout
     */
    public boolean flush(long timeoutMs) {
        long startTime = System.currentTimeMillis();

        while (System.currentTimeMillis() - startTime < timeoutMs) {
            // Check if all workers have no pending work
            boolean allIdle = true;
            for (TglogWorker worker : workers) {
                if (worker.hasPendingWork()) {
                    allIdle = false;
                    break;
                }
            }

            if (allIdle) {
                return true;
            }

            // Wait a bit
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }

        return false;
    }

    /**
     * Close the client and release all resources.
     * This will flush pending messages and wait for workers to finish.
     */
    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            logger.info("Closing TglogClient...");

            // Try to flush pending messages
            flush(config.getStopTimeout());

            // Stop endpoint manager
            endpointManager.stop();

            // Stop all workers
            for (TglogWorker worker : workers) {
                worker.stop();
            }

            // Wait for workers to stop
            long deadline = System.currentTimeMillis() + config.getStopTimeout();
            for (TglogWorker worker : workers) {
                long remaining = deadline - System.currentTimeMillis();
                if (remaining > 0) {
                    while (!worker.isStopped() && System.currentTimeMillis() < deadline) {
                        try {
                            Thread.sleep(10);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            break;
                        }
                    }
                }
            }

            running.set(false);

            logger.info("TglogClient closed");
        }
    }

    /**
     * Check if client is running.
     *
     * @return true if client is running
     */
    public boolean isRunning() {
        return running.get() && !closed.get();
    }

    /**
     * Check if client is closed.
     *
     * @return true if client is closed
     */
    public boolean isClosed() {
        return closed.get();
    }

    /**
     * Get configuration.
     *
     * @return Client configuration
     */
    public TglogConfig getConfig() {
        return config;
    }

    @Override
    public String toString() {
        return String.format("TglogClient{protocol=%s, host='%s', port=%d, workers=%d, running=%s}",
                config.getProtocol(), config.getHost(), config.getPort(),
                workers.size(), isRunning());
    }
}
