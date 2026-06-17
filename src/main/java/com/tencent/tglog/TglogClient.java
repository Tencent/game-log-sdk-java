package com.tencent.tglog;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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
 *     client.send("battle|winner=1|loser=2\n", (payload, exception) -> {
 *         if (exception == null) {
 *             System.out.println("Sent: " + payload);
 *         } else {
 *             System.err.println("Failed: " + exception.getMessage());
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

        // Auto-adjust batching max size based on protocol limits
        int maxRequestSize = config.getMaxRequestSize();
        if (config.getBatchingMaxSize() > maxRequestSize) {
            logger.warn("Batching max size ({}) exceeds protocol limit ({}), adjusting to {}",
                    config.getBatchingMaxSize(), maxRequestSize, maxRequestSize);
            config.setBatchingMaxSize(maxRequestSize);
        }

        this.config = config;

        // Use custom registry if provided, otherwise use default
        io.prometheus.client.CollectorRegistry registry = config.getCollectorRegistry();
        if (registry == null) {
            registry = io.prometheus.client.CollectorRegistry.defaultRegistry;
        }
        this.metrics = new TglogMetrics(registry);

        // From this point on, any failure must unregister metrics to keep
        // the shared CollectorRegistry clean for retry / recreation.
        boolean initSucceeded = false;
        try {

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

            initSucceeded = true;
        } finally {
            if (!initSucceeded) {
                // start() already stops any workers/endpointManager it started,
                // but nothing releases metrics. Do that here so a failed
                // construction doesn't leak collectors in the shared registry.
                try {
                    metrics.unregister();
                } catch (Exception ex) {
                    logger.warn("Failed to unregister metrics during construction cleanup", ex);
                }
            }
        }
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

                // Set initial endpoints and start all workers. Spawning all worker
                // threads first (start) and only then waiting on each (awaitReady)
                // makes connection initialization run in parallel across workers,
                // rather than serializing it one worker at a time.
                for (TglogWorker worker : workers) {
                    worker.setInitialEndpoints(endpoints);
                    startedWorkers.add(worker);
                    worker.start();
                }

                // Wait for every worker to finish establishing its initial connections.
                for (TglogWorker worker : workers) {
                    worker.awaitReady();
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
     * @throws TglogException with {@link TglogErrorCode#PRODUCER_CLOSED} if the client
     *         or selected worker is not running, or {@link TglogErrorCode#SEND_QUEUE_IS_FULL}
     *         if the worker queue is full (also for {@code BAD_LOG}/{@code LOG_TOO_LONG} on
     *         payload validation failure)
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
            // Call user callback if provided. Catch Throwable (not just Exception) so a
            // user callback throwing an Error cannot escape and skip future completion.
            try {
                if (callback != null) {
                    callback.onCompletion(payload, exception);
                }
            } catch (Throwable t) {
                logger.warn("Exception/Error in user callback", t);
            } finally {
                // Always complete the future, even if the user callback threw,
                // so callers blocking on the CompletableFuture never hang.
                if (exception == null) {
                    future.complete(null);
                } else {
                    future.completeExceptionally(exception);
                }
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
        // Asynchronous send failures will be reported via callback and future.
        // enqueue() distinguishes a full queue (backpressure) from a worker that
        // is not accepting messages (closing/closed, or its thread exited), so the
        // caller is not misled into retrying backpressure against a dead worker.
        TglogErrorCode enqueueResult = worker.enqueue(message);
        if (enqueueResult != TglogErrorCode.OK) {
            metrics.incrementDroppedMsg();
            if (enqueueResult == TglogErrorCode.SEND_QUEUE_IS_FULL) {
                throw new TglogException(TglogErrorCode.SEND_QUEUE_IS_FULL, "Message queue is full");
            }
            throw new TglogException(TglogErrorCode.PRODUCER_CLOSED,
                    "Worker is not running (closing, closed, or its thread exited)");
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
     * Flush all pending messages, waiting up to {@code timeoutMs} for every worker
     * to drain its queues, batches, and connection output buffers.
     * <p>
     * During normal operation you usually do not need to call this; messages are
     * batched and sent automatically by worker threads, and {@link #close()} already
     * flushes pending messages (bounded by {@link TglogConfig#getStopTimeout()})
     * before stopping workers. It is provided for cases where you want to confirm
     * pending data has been sent without closing the client.
     * <p>
     * This is non-blocking on the caller side beyond the wait: it posts a flush
     * request to each worker and waits on the returned futures, so it never touches
     * worker-internal state from the calling thread.
     *
     * @param timeoutMs Maximum time to wait in milliseconds
     * @return true if all messages were flushed, false if the timeout elapsed first
     */
    public boolean flush(long timeoutMs) {
        // Post a flush request to each worker rather than inspecting worker-internal
        // state from this (caller) thread. Each worker completes its future from
        // inside the worker thread once it has drained all pending work, so we never
        // touch the worker's non-thread-safe structures cross-thread.
        List<CompletableFuture<Boolean>> futures = new ArrayList<>(workers.size());
        for (TglogWorker worker : workers) {
            futures.add(worker.requestFlush());
        }

        long deadline = System.currentTimeMillis() + timeoutMs;
        boolean allDrained = true;
        for (CompletableFuture<Boolean> future : futures) {
            long remaining = deadline - System.currentTimeMillis();
            try {
                if (remaining <= 0) {
                    // Out of time budget; only succeed if already completed as drained.
                    if (!future.isDone() || !Boolean.TRUE.equals(future.getNow(Boolean.FALSE))) {
                        allDrained = false;
                    }
                } else {
                    Boolean drained = future.get(remaining, TimeUnit.MILLISECONDS);
                    if (drained == null || !drained) {
                        allDrained = false;
                    }
                }
            } catch (TimeoutException e) {
                allDrained = false;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            } catch (ExecutionException e) {
                logger.warn("Flush request failed for a worker", e.getCause());
                allDrained = false;
            }
        }

        return allDrained;
    }

    /**
     * Close the client and release all resources.
     * This will flush pending messages and wait for workers to finish.
     */
    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            logger.info("Closing TglogClient...");

            // Try to flush pending messages. A false result means some workers
            // still had undelivered data when the flush budget ran out; we still
            // proceed to close (AutoCloseable must release resources), but log it
            // so the timeout is observable. Per-message visibility is already
            // provided by cleanup()'s failRemainingMessages() (PRODUCER_CLOSED
            // callbacks + dropped metrics); this WARN is the aggregate signal.
            if (!flush(config.getStopTimeout())) {
                logger.warn("flush() did not drain all pending messages within {}ms during close; "
                        + "remaining messages will be failed with PRODUCER_CLOSED during worker cleanup",
                        config.getStopTimeout());
            }

            // Stop endpoint manager
            endpointManager.stop();

            // Stop all workers
            for (TglogWorker worker : workers) {
                worker.stop();
            }

            // Wait for workers to stop by joining their threads. Joining (rather
            // than polling isStopped()) guarantees the worker's cleanup() has
            // fully finished before we unregister metrics below, removing the
            // race where a still-running worker touches metrics concurrently
            // with unregistration.
            long deadline = System.currentTimeMillis() + config.getStopTimeout();
            boolean allStopped = true;
            for (TglogWorker worker : workers) {
                long remaining = deadline - System.currentTimeMillis();
                // Always give a worker at least a small window to terminate so a
                // late worker isn't skipped just because earlier ones used up the
                // budget; use 1ms as the floor (0 would mean "wait forever").
                long joinTimeout = remaining > 0 ? remaining : 1;
                if (!worker.join(joinTimeout)) {
                    allStopped = false;
                }
            }
            if (!allStopped) {
                logger.warn("One or more workers did not stop within {}ms; proceeding with "
                        + "metrics unregistration anyway to keep the registry rebuildable",
                        config.getStopTimeout());
            }

            running.set(false);

            // Unregister metrics. Normally all workers have fully stopped by now
            // (joined above), so there is no concurrent access. Even if a worker
            // overran the stop timeout, we still unregister: leaving collectors
            // registered would permanently leak them and break the next
            // TglogClient construction on a shared registry. Doing it last keeps
            // the shared CollectorRegistry clean for reuse (config hot-reload,
            // webapp redeploy, tests, etc.).
            try {
                metrics.unregister();
            } catch (Exception e) {
                logger.warn("Failed to unregister metrics during close", e);
            }

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
