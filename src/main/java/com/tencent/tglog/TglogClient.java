package com.tencent.tglog;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
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

    private final TglogConfig config;
    private final TglogLogger logger;
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
        this.logger = config.getLogger() != null ? config.getLogger() : new TglogLogger.NoOpLogger();
        this.metrics = new TglogMetrics();

        // Auto-adjust batching max size based on protocol limits
        int maxRequestSize = config.getMaxRequestSize();
        if (config.getBatchingMaxSize() > maxRequestSize) {
            logger.log(TglogLogger.Level.WARN,
                    "Batching max size (" + config.getBatchingMaxSize() +
                    ") exceeds protocol limit (" + maxRequestSize +
                    "), adjusting to " + maxRequestSize);
            config.setBatchingMaxSize(maxRequestSize);
        }

        // Initialize endpoint manager
        this.endpointManager = new EndpointManager(
                config.getHost(),
                config.getPort(),
                config,
                logger
        );

        // Initialize workers
        int workerNum = config.getWorkerNum();
        this.workers = new ArrayList<>(workerNum);

        for (int i = 0; i < workerNum; i++) {
            TglogWorker worker = new TglogWorker(i, config, logger, metrics);
            workers.add(worker);
        }

        // Start the client
        start();

        logger.log(TglogLogger.Level.INFO,
                "TglogClient created: " + config.getProtocol() + "://" +
                        config.getHost() + ":" + config.getPort() +
                        " (workers=" + workerNum + ")");
    }

    /**
     * Start the client.
     */
    private void start() throws TglogException {
        if (running.compareAndSet(false, true)) {
            try {
                // Start endpoint manager
                endpointManager.start();

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
                }

                logger.log(TglogLogger.Level.INFO,
                        "TglogClient started with " + endpoints.size() + " endpoints");

            } catch (Exception e) {
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
     * @param logPayload Log message (must end with \n)
     * @param callback Callback for send result (can be null)
     * @throws TglogException if client is closed or queue is full
     */
    public void send(String logPayload, TglogCallback callback) throws TglogException {
        // Validate client state (like C version: check client first)
        if (closed.get()) {
            if (callback != null) {
                callback.onError(logPayload, TglogException.ERR_PRODUCER_CLOSED, "Client is closed");
            }
            throw new TglogException("Client is closed", TglogException.ERR_PRODUCER_CLOSED);
        }

        // Validate payload (like C version: invoke callback before throwing)
        if (logPayload == null || logPayload.isEmpty()) {
            if (callback != null) {
                callback.onError(logPayload, TglogException.ERR_BAD_LOG, "Log payload cannot be null or empty");
            }
            throw new TglogException("Log payload cannot be null or empty", TglogException.ERR_BAD_LOG);
        }

        // Ensure message ends with newline (like C version: check TGLOG_DELIMITER)
        if (!logPayload.endsWith("\n")) {
            logPayload += "\n";
        }

        // Create log message (which will cache the payload size)
        LogMessage message = new LogMessage(logPayload, callback);

        // Check log payload length against protocol limits
        int payloadLength = message.getPayloadSize();
        int maxRequestSize = config.getMaxRequestSize();
        if (payloadLength > maxRequestSize) {
            String errorMsg = "Log payload too long: " + payloadLength + " bytes exceeds " +
                    config.getProtocol() + " limit of " + maxRequestSize + " bytes";
            if (callback != null) {
                callback.onError(logPayload, TglogException.ERR_LOG_TOO_LONG, errorMsg);
            }
            throw new TglogException(errorMsg, TglogException.ERR_LOG_TOO_LONG);
        }

        // Update metrics
        metrics.incrementRequestMsg();

        // Select a worker (round-robin)
        TglogWorker worker = selectWorker();

        // Enqueue message
        if (!worker.enqueue(message)) {
            metrics.incrementDroppedMsg();
            if (callback != null) {
                callback.onError(logPayload, TglogException.ERR_SEND_QUEUE_IS_FULL, "Message queue is full");
            }
            throw new TglogException("Message queue is full", TglogException.ERR_SEND_QUEUE_IS_FULL);
        }
    }

    /**
     * Send a log message without callback (fire and forget).
     *
     * @param logPayload Log message
     * @throws TglogException if send fails
     */
    public void send(String logPayload) throws TglogException {
        send(logPayload, null);
    }

    /**
     * Select a worker using round-robin.
     */
    private TglogWorker selectWorker() {
        int index = Math.abs(roundRobinIndex.getAndIncrement() % workers.size());
        return workers.get(index);
    }

    /**
     * Handle endpoint changes from DNS resolution.
     */
    private void onEndpointsChanged(List<InetSocketAddress> newEndpoints) {
        logger.log(TglogLogger.Level.INFO,
                "Endpoints changed: " + newEndpoints.size() + " endpoints");

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
     * Get metrics as JSON string.
     *
     * @return JSON representation of metrics
     */
    public String getMetricsJson() {
        return metrics.toJson();
    }

    /**
     * Print metrics to string.
     *
     * @return Human-readable metrics
     */
    public String printMetrics() {
        StringBuilder sb = new StringBuilder();
        sb.append("TGLog Client Metrics:").append(System.lineSeparator());
        sb.append("====================").append(System.lineSeparator());
        sb.append(String.format("Requests:     %,d%n", metrics.getRequestMsgCount()));
        sb.append(String.format("Enqueued:     %,d%n", metrics.getEnqueuedMsgCount()));
        sb.append(String.format("Succeeded:    %,d%n", metrics.getSucceededMsgCount()));
        sb.append(String.format("Failed:       %,d%n", metrics.getFailedMsgCount()));
        sb.append(String.format("Timeout:      %,d%n", metrics.getTimeoutMsgCount()));
        sb.append(String.format("Dropped:      %,d%n", metrics.getDroppedMsgCount()));
        sb.append(String.format("Success Rate: %.2f%%%n", metrics.getSuccessRate() * 100));
        sb.append(String.format("Queue Size:   %d / %d (%.1f%%)%n",
                metrics.getMsgQueueSize(),
                metrics.getMsgQueueCapacity(),
                metrics.getQueueUtilization() * 100));
        sb.append(String.format("Endpoints:    %d%n", metrics.getEndpointCount()));
        sb.append(String.format("Connections:  %d%n", metrics.getActiveConnCount()));
        return sb.toString();
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
            logger.log(TglogLogger.Level.INFO, "Closing TglogClient...");

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

            logger.log(TglogLogger.Level.INFO, "TglogClient closed");
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
