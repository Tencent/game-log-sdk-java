package com.tencent.tglog;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Worker thread that handles message batching and network I/O.
 * <p>
 * Key design principles (matching C version):
 * - Lock-free message queue (MPSC - Multi-Producer Single-Consumer)
 * - Lock-free endpoint update queue
 * - All connections list access happens in worker thread only (no locks needed)
 * - Batch retry with exponential backoff
 * - Connection lifecycle management
 * - Endpoint recovery with backoff
 */
class TglogWorker implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(TglogWorker.class);

    enum State {
        INIT,       // Worker initialized
        READY,      // Worker ready to accept messages
        CLOSING,    // Worker is closing
        CLOSED      // Worker is closed
    }

    /**
     * Internal result class for send operations.
     * Used to avoid exceptions in hot path while maintaining type safety.
     */
    private static class SendResult {
        final TglogErrorCode errorCode;

        SendResult(TglogErrorCode errorCode) {
            this.errorCode = errorCode;
        }

        boolean isOk() {
            return errorCode == TglogErrorCode.OK;
        }

        static SendResult ok() {
            return new SendResult(TglogErrorCode.OK);
        }

        static SendResult error(TglogErrorCode errorCode) {
            return new SendResult(errorCode);
        }
    }

    // Worker identification
    private final int workerId;
    private final TglogConfig config;
    private final TglogMetrics metrics;

    // State management
    private volatile State state = State.INIT;
    // Reference to the worker thread, kept so that close() can join() it and
    // confirm cleanup has fully finished before unregistering shared metrics.
    private volatile Thread workerThread;
    private final Object readyLock = new Object();
    private final AtomicInteger readyConnCount = new AtomicInteger(0);
    private final AtomicInteger failedConnCount = new AtomicInteger(0);
    private final AtomicInteger initConnCount = new AtomicInteger(0);
    private volatile boolean initStarted = false;

    // Lock-free queues (MPSC pattern - multiple producers, single consumer in worker thread)
    private final LockFreeQueue<LogMessage> messageQueue;
    private final ConcurrentLinkedQueue<List<InetSocketAddress>> endpointUpdateQueue;

    // Flush requests posted by external threads (e.g. TglogClient.flush()).
    // The worker thread polls these in its event loop and decides completion by
    // calling hasPendingWork() from inside the worker thread, so external threads
    // never touch worker-internal non-thread-safe structures.
    private final ConcurrentLinkedQueue<CompletableFuture<Boolean>> flushRequestQueue;

    // Initial endpoints (set before start, read-only after that)
    private List<InetSocketAddress> initialEndpoints;

    // Endpoint and connection management (accessed only by worker thread - no locks needed!)
    private List<InetSocketAddress> currentEndpoints;
    private final List<TglogConnection> connections;
    private final List<TglogConnection> connectingConns;
    private final AtomicLong connectionIndex = new AtomicLong(0);
    private int endpointReplaceIndex = 0;
    private int connectionsPerEndpoint;

    // Unavailable endpoint tracking (accessed only by worker thread)
    private final Map<String, Long> unavailableEndpoints;
    private final Map<String, Integer> endpointRetryCount;

    // Batch management (accessed only by worker thread)
    private final BatchBuffer currentBatch;
    private LogMessage pendingMessage = null;

    // NIO components (accessed only by worker thread)
    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings(
            value = "UWF_FIELD_NOT_INITIALIZED_IN_CONSTRUCTOR",
            justification = "Selector is initialized in run() method before any use")
    private volatile Selector selector;

    // Timers and intervals
    private long lastEmergencySupplementTime = 0;

    // Constants
    private static final int MSG_PER_RUN = 3000;
    private static final long ENDPOINT_RECOVER_INTERVAL_MS = 5000; // 5 seconds
    private static final long CONNECTION_REPLACE_CHECK_INTERVAL_MS = 20000; // 20 seconds
    private static final long EMERGENCY_SUPPLEMENT_INTERVAL_MS = 5000; // 5 seconds
    private static final long EMERGENCY_SUPPLEMENT_MIN_INTERVAL_MS = 2000; // 2 seconds
    private static final long METRICS_UPDATE_INTERVAL_MS = 5000; // 5 seconds
    private static final long PENDING_CLOSE_CHECK_INTERVAL_MS = 1000; // 1 second
    private static final int DEFAULT_CONN_CLOSE_DELAY_MS = 30000; // 30 seconds (normal replacement)
    private static final int ERROR_CONN_CLOSE_DELAY_WITH_DATA_MS = 30000; // 30 seconds (error with buffered data)
    private static final int ERROR_CONN_CLOSE_DELAY_NO_DATA_MS = 5000; // 5 seconds (error without buffered data)

    // Pending delayed-close entries (processed in worker thread)
    private final List<DelayedClose> pendingCloseList;

    /**
     * Represents a connection scheduled for delayed close.
     */
    private static class DelayedClose {
        final TglogConnection conn;
        long deadline; // System.currentTimeMillis() + delayMs
        boolean isError;

        DelayedClose(TglogConnection conn, long deadline, boolean isError) {
            this.conn = conn;
            this.deadline = deadline;
            this.isError = isError;
        }
    }

    /**
     * Create a new worker.
     */
    public TglogWorker(int workerId, TglogConfig config, TglogMetrics metrics) {
        this.workerId = workerId;
        this.config = config;
        this.metrics = metrics;

        // Initialize lock-free queues
        this.messageQueue = new LockFreeQueue<>(config.getMaxPendingMessages());
        this.endpointUpdateQueue = new ConcurrentLinkedQueue<>();
        this.flushRequestQueue = new ConcurrentLinkedQueue<>();

        // Set queue capacity metric (actual capacity after rounding to power of 2)
        metrics.setMsgQueueCapacity(workerId, messageQueue.capacity());

        // Initialize collections (accessed only by worker thread)
        this.currentEndpoints = new ArrayList<>();
        this.connections = new ArrayList<>();
        this.connectingConns = new ArrayList<>();
        this.unavailableEndpoints = new HashMap<>();
        this.endpointRetryCount = new HashMap<>();

        // Initialize batch buffer
        this.currentBatch = new BatchBuffer(config);

        // Initialize pending close list (processed in worker thread)
        this.pendingCloseList = new ArrayList<>();
    }

    /**
     * Set initial endpoints (called by Client before start).
     * This is the lock-free way to pass initial data to worker.
     */
    public void setInitialEndpoints(List<InetSocketAddress> endpoints) {
        this.initialEndpoints = new ArrayList<>(endpoints);
        logger.info("Worker {} received {} initial endpoints", workerId, endpoints.size());
    }

    /**
     * Notify worker of endpoint update (called by Client).
     * This is lock-free - just enqueue the update, worker will process it.
     */
    public void notifyEndpointUpdate(List<InetSocketAddress> newEndpoints) {
        // Create a copy to avoid external modifications
        List<InetSocketAddress> copy = new ArrayList<>(newEndpoints);
        endpointUpdateQueue.offer(copy);

        // Wake up selector if it's blocked
        if (selector != null) {
            selector.wakeup();
        }

        logger.debug("Worker {} notified of endpoint update: {} endpoints", workerId, newEndpoints.size());
    }

    /**
     * Request a flush from an external thread (e.g. {@link TglogClient#flush(long)}).
     * <p>
     * This does NOT block and does NOT inspect worker-internal state on the caller
     * thread. Instead it enqueues a future that the worker thread completes once it
     * observes no pending work (via {@link #hasPendingWork()}, called only inside the
     * worker thread). If the worker is no longer running, the future is completed
     * immediately so callers never hang.
     *
     * @return a future that completes with {@code true} when the worker has drained
     *         all pending work, or {@code false} if the worker stopped before draining.
     */
    public CompletableFuture<Boolean> requestFlush() {
        CompletableFuture<Boolean> future = new CompletableFuture<>();

        // Enqueue first, then re-check state. Ordering matters: if we enqueue
        // before the worker transitions to CLOSING, the worker's event loop or
        // its shutdown path (completeFlushRequestsOnStop) will service it. If the
        // worker has already stopped, there is no loop to service it, so we must
        // complete it here. We never call hasPendingWork() on this (external)
        // thread, to avoid touching worker-internal structures cross-thread.
        flushRequestQueue.offer(future);

        if (state != State.READY && state != State.INIT) {
            // Worker is closing/closed and may never poll the queue again. Drain
            // it here ourselves and complete with false: we cannot guarantee a
            // full flush once the worker is no longer accepting/processing.
            CompletableFuture<Boolean> pendingReq;
            while ((pendingReq = flushRequestQueue.poll()) != null) {
                pendingReq.complete(false);
            }
            return future;
        }

        // Wake up selector if it's blocked so the request is serviced promptly.
        if (selector != null) {
            selector.wakeup();
        }
        return future;
    }

    /**
     * Process pending flush requests (called from the worker thread only).
     * <p>
     * For each outstanding request, completes it with {@code true} if there is no
     * more pending work; otherwise leaves it in the queue to be re-checked on the
     * next event-loop iteration. This keeps the event loop non-blocking while still
     * giving callers an accurate, in-thread view of pending work.
     */
    private void processFlushRequests() {
        if (flushRequestQueue.isEmpty()) {
            return;
        }

        // Only evaluate hasPendingWork() once per iteration; it is identical for
        // all waiting requests since they all observe the same worker state.
        boolean pending = hasPendingWork();
        if (pending) {
            return;
        }

        CompletableFuture<Boolean> future;
        while ((future = flushRequestQueue.poll()) != null) {
            future.complete(true);
        }
    }

    /**
     * Fail (complete) all outstanding flush requests on worker shutdown so that
     * callers blocked in {@link TglogClient#flush(long)} do not hang. Completes each
     * with {@code true} only if no work remains at this point, otherwise {@code false}.
     */
    private void completeFlushRequestsOnStop() {
        if (flushRequestQueue.isEmpty()) {
            return;
        }
        boolean drained = !hasPendingWork();
        CompletableFuture<Boolean> future;
        while ((future = flushRequestQueue.poll()) != null) {
            future.complete(drained);
        }
    }

    /**
     * Start the worker thread.
     * <p>
     * This only spawns the worker thread and returns immediately; it does NOT wait
     * for the worker to become ready. Callers must invoke {@link #awaitReady()}
     * afterwards to block until the worker has established its initial connections.
     * <p>
     * Splitting start (spawn) from await (wait-for-ready) lets the client start all
     * workers first and then wait on them, so every worker's connection
     * initialization runs in parallel instead of one worker at a time.
     */
    public void start() {
        logger.info("Worker {} starting thread, initialEndpoints={}", workerId, (initialEndpoints == null ? "null" : initialEndpoints.size()));

        Thread thread = new Thread(this, "TglogWorker-" + workerId);
        thread.setDaemon(false);
        this.workerThread = thread;
        thread.start();

        logger.debug("Worker {} thread started", workerId);
    }

    /**
     * Wait for the worker to become ready (initial connections established).
     * <p>
     * Must be called after {@link #start()}. Blocks until the worker has finished
     * initializing its connections (or the connection timeout elapses).
     *
     * @throws RuntimeException if the worker failed to establish any connection
     */
    public void awaitReady() {
        logger.debug("Worker {} waiting for ready...", workerId);

        // Wait for worker to be ready
        long startTime = System.currentTimeMillis();
        long timeout = config.getConnectionTimeout() * 2; // Give enough time

        synchronized (readyLock) {
            // First wait for initialization to start
            while (!initStarted && state == State.INIT) {
                long elapsed = System.currentTimeMillis() - startTime;
                if (elapsed >= timeout) {
                    logger.error("Worker {} timeout waiting for init to start", workerId);
                    break;
                }
                try {
                    readyLock.wait(100);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }

            // Then wait for connections to be ready
            while (state == State.INIT && readyConnCount.get() + failedConnCount.get() < initConnCount.get()) {
                long elapsed = System.currentTimeMillis() - startTime;
                if (elapsed >= timeout) {
                    logger.error("Worker {} timeout waiting for connections", workerId);
                    break;
                }
                try {
                    readyLock.wait(100);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }

        logger.debug("Worker {} wait complete: ready={}, failed={}, init={}", workerId, readyConnCount.get(), failedConnCount.get(), initConnCount.get());

        if (readyConnCount.get() <= 0) {
            throw new RuntimeException("Worker " + workerId + " failed to initialize connections");
        }

        logger.info("Worker {} started with {} ready connections", workerId, readyConnCount.get());
    }

    /**
     * Worker thread main loop.
     */
    @Override
    public void run() {
        try {
            logger.info("Worker {} thread started", workerId);

            // Initialize NIO selector
            logger.debug("Worker {} opening selector", workerId);
            selector = Selector.open();

            // Initialize connections from initial endpoints
            logger.debug("Worker {} initializing connections", workerId);
            initializeConnections();

            // Wait for initial connections to be ready
            logger.debug("Worker {} waiting for connections", workerId);
            waitForInitialConnections();

            // Mark as ready (only if not already stopping)
            if (state == State.INIT) {
                state = State.READY;
                logger.info("Worker {} is ready", workerId);

                // Main event loop
                runEventLoop();
            } else {
                logger.info("Worker {} skipping event loop, state={}", workerId, state);
            }

        } catch (Exception e) {
            logger.error("Worker {}{}", workerId, " error", e);
            // Notify waiting thread even on error
            synchronized (readyLock) {
                readyLock.notifyAll();
            }
        } finally {
            cleanup();
        }
    }

    /**
     * Initialize connections from initial endpoints.
     */
    private void initializeConnections() throws IOException {
        logger.info("Worker {} initializing connections, initialEndpoints={}", workerId, (initialEndpoints == null ? "null" : initialEndpoints.size()));

        if (initialEndpoints == null || initialEndpoints.isEmpty()) {
            throw new IOException("No initial endpoints provided");
        }

        // Copy initial endpoints to current endpoints
        currentEndpoints = new ArrayList<>(initialEndpoints);

        // Determine connections per endpoint
        connectionsPerEndpoint = currentEndpoints.size() <= 2 ? 3 : 2;
        logger.info("Worker {} using {} connections per endpoint (total endpoints: {})",
                workerId, connectionsPerEndpoint, currentEndpoints.size());

        // Create connections for each endpoint
        for (InetSocketAddress endpoint : currentEndpoints) {
            int successfulConns = 0;
            for (int i = 0; i < connectionsPerEndpoint; i++) {
                try {
                    TglogConnection conn = new TglogConnection(endpoint, config, metrics, workerId);
                    conn.connect(selector);
                    connectingConns.add(conn);
                    initConnCount.incrementAndGet();
                    successfulConns++;
                } catch (IOException e) {
                    logger.warn("Failed to create connection {}/{} for endpoint {}", (i + 1), connectionsPerEndpoint, endpoint, e);
                }
            }

            // Mark endpoint unavailable if all connections failed
            if (successfulConns == 0) {
                markEndpointUnavailable(endpoint);
            }
        }

        if (connectingConns.isEmpty()) {
            throw new IOException("No connections could be created");
        }

        logger.debug("Worker {} created {} initial connections", workerId, connectingConns.size());

        // Signal that initialization has started (after initConnCount is set)
        synchronized (readyLock) {
            initStarted = true;
            readyLock.notifyAll();
        }

        // For UDP, connections are immediately ready - check them now
        if (config.getProtocol() == TglogConfig.Protocol.UDP) {
            checkConnectingConnections();
        }
    }

    /**
     * Wait for initial connections to be ready.
     */
    private void waitForInitialConnections() throws IOException {
        long deadline = System.currentTimeMillis() + config.getConnectionTimeout();

        while (readyConnCount.get() + failedConnCount.get() < initConnCount.get()) {
            long timeout = deadline - System.currentTimeMillis();
            if (timeout <= 0) {
                break;
            }

            try {
                // Check connection status
                int readyKeys = selector.select(Math.min(timeout, 100));

                if (readyKeys > 0) {
                    processSelectionKeys();
                }

                // Check for completed connections
                checkConnectingConnections();

            } catch (IOException e) {
                logger.error("Error waiting for connections", e);
                break;
            }
        }

        // Notify waiting thread
        synchronized (readyLock) {
            readyLock.notifyAll();
        }
    }

    /**
     * Main event loop.
     */
    private void runEventLoop() {
        long lastEndpointRecoverTime = System.currentTimeMillis();
        long lastConnectionReplaceTime = System.currentTimeMillis();
        long lastMetricsUpdateTime = System.currentTimeMillis();
        long lastPendingCloseCheckTime = System.currentTimeMillis();
        lastEmergencySupplementTime = 0; // Allow first supplement immediately

        while (state == State.READY) {
            try {
                // Process endpoint updates (lock-free queue)
                processEndpointUpdates();

                // Handle NIO events with adaptive timeout
                // Worker thread polls message queue periodically without needing wakeup
                // 10ms when idle for low CPU, 5ms when busy for high throughput
                int readyKeys;
                if (messageQueue.isEmpty()) {
                    readyKeys = selector.select(10);
                } else {
                    readyKeys = selector.select(5);
                }

                if (readyKeys > 0) {
                    processSelectionKeys();
                }

                // Check connecting connections
                checkConnectingConnections();

                // Process messages from queue (up to MSG_PER_RUN)
                processMessages();

                // Service flush requests after processing messages, so callers
                // observe the most up-to-date pending state for this iteration.
                processFlushRequests();

                // Periodic tasks
                long now = System.currentTimeMillis();

                // Recover unavailable endpoints (every 5s)
                if (now - lastEndpointRecoverTime >= ENDPOINT_RECOVER_INTERVAL_MS) {
                    recoverUnavailableEndpoints();
                    lastEndpointRecoverTime = now;
                }

                // Replace expired connections (every 20s)
                if (config.getMaxConnectionLifetime() > 0 &&
                        now - lastConnectionReplaceTime >= CONNECTION_REPLACE_CHECK_INTERVAL_MS) {
                    replaceExpiredConnections();
                    lastConnectionReplaceTime = now;
                }

                // Emergency supplement check (every 5s)
                if (now - lastEmergencySupplementTime >= EMERGENCY_SUPPLEMENT_INTERVAL_MS) {
                    emergencySupplementConnection();
                    lastEmergencySupplementTime = now;
                }

                // Process pending delayed closes (every 1s)
                if (now - lastPendingCloseCheckTime >= PENDING_CLOSE_CHECK_INTERVAL_MS) {
                    processPendingCloses();
                    lastPendingCloseCheckTime = now;
                }

                // Update metrics (every 5s)
                if (now - lastMetricsUpdateTime >= METRICS_UPDATE_INTERVAL_MS) {
                    metrics.setMsgQueueSize(workerId, messageQueue.size());
                    metrics.setActiveConnCount(workerId, connections.size());
                    lastMetricsUpdateTime = now;
                }

            } catch (Exception e) {
                logger.error("Error in event loop", e);
            }
        }

        logger.info("Worker {} event loop ended", workerId);
    }

    /**
     * Process endpoint updates from queue (lock-free).
     */
    private void processEndpointUpdates() {
        List<InetSocketAddress> newEndpoints;
        while ((newEndpoints = endpointUpdateQueue.poll()) != null) {
            handleEndpointUpdate(newEndpoints);
        }
    }

    /**
     * Handle endpoint update (called in worker thread).
     */
    private void handleEndpointUpdate(List<InetSocketAddress> newEndpoints) {
        // Ignore empty/null updates. Applying them would wipe currentEndpoints and
        // close every connection, leaving the worker with nowhere to send. An empty
        // endpoint list almost always means a transient DNS resolution failure rather
        // than a genuine "no servers" topology, so keep the existing endpoints.
        if (newEndpoints == null || newEndpoints.isEmpty()) {
            logger.warn("Worker {} ignoring empty endpoint update, keeping {} current endpoints",
                    workerId, currentEndpoints.size());
            return;
        }

        logger.info("Worker {} processing endpoint update: {} endpoints", workerId, newEndpoints.size());

        // Find added and removed endpoints
        Set<InetSocketAddress> oldSet = new HashSet<>(currentEndpoints);
        Set<InetSocketAddress> newSet = new HashSet<>(newEndpoints);

        // Find removed endpoints
        Set<InetSocketAddress> removed = new HashSet<>(oldSet);
        removed.removeAll(newSet);

        // Find added endpoints
        Set<InetSocketAddress> added = new HashSet<>(newSet);
        added.removeAll(oldSet);

        // Update the endpoint view BEFORE adding/removing connections. markEndpointUnavailable()
        // consults currentEndpoints.size() to decide whether backoff applies, so it must already
        // reflect the new topology; otherwise a newly added but unreachable endpoint could be seen
        // against the old (smaller) endpoint list and wrongly skipped from being marked unavailable.
        currentEndpoints = new ArrayList<>(newEndpoints);
        metrics.setEndpointCount(currentEndpoints.size());

        // Add connections for new endpoints first (to ensure availability)
        for (InetSocketAddress endpoint : added) {
            addConnectionsForEndpoint(endpoint);
            metrics.incrementAddEndpoint();
        }

        // Remove connections for removed endpoints after adding new ones
        for (InetSocketAddress endpoint : removed) {
            removeConnectionsByEndpoint(endpoint);
            unavailableEndpoints.remove(endpoint.toString());
            endpointRetryCount.remove(endpoint.toString());
            metrics.incrementRemoveEndpoint();
        }

        logger.info("Worker {} endpoint update complete: added={}, removed={}, total={}", workerId, added.size(), removed.size(), currentEndpoints.size());
    }

    /**
     * Process NIO selection keys.
     */
    private void processSelectionKeys() {
        Iterator<SelectionKey> keyIterator = selector.selectedKeys().iterator();

        while (keyIterator.hasNext()) {
            SelectionKey key = keyIterator.next();
            keyIterator.remove();

            if (!key.isValid()) {
                continue;
            }

            TglogConnection conn = (TglogConnection) key.attachment();
            if (conn == null) {
                continue;
            }

            try {
                if (key.isConnectable()) {
                    // A still-connecting socket is not yet a live data channel, so
                    // connectable is handled exclusively (it has no meaningful read/write yet).
                    handleConnectable(conn);
                } else {
                    // readable and writable can both be set on the same ready key. Handle
                    // them independently (not else-if) so servicing a read does not defer
                    // a pending write flush to the next event-loop iteration.
                    if (key.isReadable()) {
                        // OP_READ triggered - check if connection is closed or discard unexpected data
                        handleReadable(conn);
                    }
                    // Re-check validity: handleReadable() may have closed/cancelled the key.
                    if (key.isValid() && key.isWritable()) {
                        handleWritable(conn);
                    }
                }
            } catch (Exception e) {
                logger.error("Error processing key for {}", conn.getEndpoint(), e);
                handleConnectionError(conn);
            }
        }
    }

    /**
     * Check connecting connections for completion.
     */
    private void checkConnectingConnections() {
        logger.debug("Worker {} checking {} connecting connections", workerId, connectingConns.size());

        for (int i = connectingConns.size() - 1; i >= 0; i--) {
            TglogConnection conn = connectingConns.get(i);
            TglogConnection.State connState = conn.getState();

            logger.debug("Worker {} connection {} state: {}, endpoint: {}", workerId, i, connState, conn.getEndpoint());

            if (connState == TglogConnection.State.CONNECTED) {
                // Move to connected list
                connectingConns.remove(i);
                connections.add(conn);

                // Clear unavailable state for this endpoint since a connection
                // has been successfully established.
                String addr = conn.getEndpoint().toString();
                if (unavailableEndpoints.remove(addr) != null) {
                    endpointRetryCount.remove(addr);
                    logger.info("Worker {} endpoint recovered: {}", workerId, addr);
                }

                // Update metrics
                metrics.incrementOpenedConn(workerId);

                // Notify if this is an init connection
                if (state == State.INIT) {
                    synchronized (readyLock) {
                        readyConnCount.incrementAndGet();
                        logger.info("Worker {} ready conn count: {}/{}", workerId, readyConnCount.get(), initConnCount.get());
                        readyLock.notifyAll();
                    }
                }

                // If this connection was created to replace an expired one, the
                // replacement is now usable: retire the old connection (delayed close
                // so its buffered data can still drain) and clear the reference.
                // If the old connection already errored out while this one was still
                // connecting, it is no longer in `connections` and remove() returns
                // false, so we simply skip retiring it.
                TglogConnection replacedConn = conn.getReplacingConn();
                if (replacedConn != null) {
                    if (connections.remove(replacedConn)) {
                        logger.info("Worker {} replacement ready, retiring expired connection: {}",
                                workerId, replacedConn.getEndpoint());
                        closeConnectionWithDelay(replacedConn);
                    }
                    conn.setReplacingConn(null);
                }

                logger.info("Worker {} connection ready: {} (total: {})",
                        workerId, conn.getEndpoint(), connections.size());

            } else if (connState == TglogConnection.State.ERROR) {
                // Remove failed connection
                connectingConns.remove(i);

                // If this was a replacement attempt, it failed: drop the reference and
                // keep the old (expired) connection serving. The next replace cycle
                // will retry; meanwhile the endpoint does not lose a usable connection.
                if (conn.getReplacingConn() != null) {
                    logger.warn("Worker {} replacement connection failed, keeping expired connection alive: {}",
                            workerId, conn.getEndpoint());
                    conn.setReplacingConn(null);
                }

                // Mark endpoint unavailable
                markEndpointUnavailable(conn.getEndpoint());

                // Notify if this is an init connection
                if (state == State.INIT) {
                    synchronized (readyLock) {
                        failedConnCount.incrementAndGet();
                        logger.warn("Worker {} failed conn count: {}/{}", workerId, failedConnCount.get(), initConnCount.get());
                        readyLock.notifyAll();
                    }
                }

                logger.warn("Worker {} connection failed: {}", workerId, conn.getEndpoint());
            }
        }
    }

    /**
     * Handle connectable event.
     */
    private void handleConnectable(TglogConnection conn) {
        if (conn.finishConnect()) {
            logger.debug("Connection established: {}", conn.getEndpoint());
        } else {
            // finishConnect failed: the connection never became established (still in connecting
            // phase). Do not supplement — supplementConnection() is for replacing a connection
            // that was already running and broke mid-flight. Retries are handled by the
            // recoverUnavailableEndpoints() timer with backoff.
            logger.warn("Failed to finish connection: {}", conn.getEndpoint());
            handleConnectionError(conn, false);
        }
    }

    /**
     * Handle readable event.
     * For TGLog v1 protocol (no responses), OP_READ typically means connection closed by peer.
     */
    private void handleReadable(TglogConnection conn) throws IOException {
        logger.info("Worker {} handleReadable triggered for connection {}", workerId, conn.getEndpoint());

        // TGLog v1 protocol has no responses, so any readable data likely means:
        // 1. Connection closed by peer (read returns -1 for TCP)
        // 2. Unexpected data (just discard it)

        if (config.getProtocol() == TglogConfig.Protocol.TCP) {
            java.nio.channels.SocketChannel channel =
                (java.nio.channels.SocketChannel) conn.getSelectionKey().channel();
            java.nio.ByteBuffer testBuf = java.nio.ByteBuffer.allocate(1024);
            int bytesRead = channel.read(testBuf);

            if (bytesRead < 0) {
                // Connection closed by peer
                logger.warn("Worker {} TCP connection closed by peer: {}", workerId, conn.getEndpoint());
                handleConnectionError(conn);
            } else if (bytesRead > 0) {
                // Unexpected data received, just discard it
                logger.info("Worker {} received {} bytes unexpected TCP data from {}, discarding",
                        workerId, bytesRead, conn.getEndpoint());
            }
            // bytesRead == 0 means no data available yet, ignore
        } else {
            // UDP
            java.nio.channels.DatagramChannel channel =
                (java.nio.channels.DatagramChannel) conn.getSelectionKey().channel();
            java.nio.ByteBuffer testBuf = java.nio.ByteBuffer.allocate(1024);
            int bytesRead = channel.read(testBuf);

            if (bytesRead > 0) {
                // Unexpected data received, just discard it
                logger.info("Worker {} received {} bytes unexpected UDP data from {}, discarding",
                        workerId, bytesRead, conn.getEndpoint());
            }
            // UDP doesn't have connection close, bytesRead won't be -1
        }
    }

    /**
     * Handle writable event: flush the connection's buffered output.
     * <p>
     * If the flush fails the connection was marked ERROR, so handle it now
     * (remove + supplement + mark endpoint) instead of waiting for a future send
     * to be routed to this connection and discover -1.
     */
    private void handleWritable(TglogConnection conn) {
        if (!conn.tryFlush()) {
            handleConnectionError(conn);
        }
    }

    /**
     * Handle connection error.
     *
     * @param supplement true when the connection was running (should be replaced immediately);
     *                   false when it never finished connecting (retry handled by recovery timer).
     */
    private void handleConnectionError(TglogConnection conn, boolean supplement) {
        // A connection already scheduled for delayed close is past judgement: a write
        // failure or peer close on it is expected, not a sign the endpoint is bad. Do
        // NOT mark the endpoint unavailable or supplement; just rescue its buffered data
        // and close it. This prevents an already-removed/expiring connection (e.g. a
        // DNS-removed endpoint's connection) from wrongly marking endpoints unavailable
        // or triggering spurious supplements.
        if (removeFromPendingClose(conn)) {
            flushOrRescueAndClose(conn);
            return;
        }

        // Remove from both lists
        connections.remove(conn);
        connectingConns.remove(conn);

        // If this connection was an in-flight replacement that just failed, drop its
        // reference to the old (expired) connection so the latter keeps serving and is
        // eligible to be replaced again on the next cycle.
        if (conn.getReplacingConn() != null) {
            logger.warn("Worker {} in-flight replacement connection errored, keeping expired connection alive: {}",
                    workerId, conn.getEndpoint());
            conn.setReplacingConn(null);
        }

        // If this connection is instead an OLD connection that some in-flight replacement
        // points at (it broke before its replacement finished connecting), no action is
        // needed: it was just removed from `connections` above, so when the replacement
        // reaches CONNECTED its connections.remove(old) returns false and retiring is skipped.

        // Immediately unregister OP_READ to avoid busy-wait during delayed close
        if (conn.getSelectionKey() != null && conn.getSelectionKey().isValid()) {
            conn.getSelectionKey().interestOps(0);
        }

        // Mark endpoint unavailable
        markEndpointUnavailable(conn.getEndpoint());

        // Close connection with data rescue (error mode)
        closeConnectionWithDelay(conn, true);

        // Update metrics
        metrics.incrementErrorConn(workerId);

        // Supplement only when replacing a mid-flight broken connection
        if (supplement) {
            supplementConnection();
        }
    }

    /**
     * Handle connection error, supplementing a replacement connection immediately.
     */
    private void handleConnectionError(TglogConnection conn) {
        handleConnectionError(conn, true);
    }

    /**
     * Process messages from queue (normal event-loop mode, with retry/backoff).
     */
    private void processMessages() {
        processMessages(false);
    }

    /**
     * Process messages from queue.
     *
     * @param shutdown when true the worker is draining during {@link #cleanup()}:
     *                 the batch is sent immediately without honoring retry backoff,
     *                 and a failed send fails the batch instead of re-arming a retry.
     *                 This guarantees forward progress on every call so the cleanup
     *                 drain loop cannot busy-spin until {@code stopTimeout} elapses
     *                 (it otherwise would, because a batch stuck in backoff makes
     *                 processMessages() return without consuming the queue).
     */
    private void processMessages(boolean shutdown) {
        int processed = 0;

        while (processed < MSG_PER_RUN && (state == State.READY || state == State.CLOSING)) {
            // sendBatch(retry) selector: during shutdown we never re-enter the
            // retry/backoff state, we attempt one send and accept its outcome.
            boolean retry = !shutdown;

            // Check if we're retrying
            if (isRetrying()) {
                // Check if retry has timed out (exceeded total backoff time)
                if (isRetryingTimeout() || isBatchSendTimeout()) {
                    logger.warn("Worker {} batch retry/send timeout", workerId);
                    failCurrentBatch(TglogErrorCode.SEND_TIMEOUT);
                    // Continue to process new messages
                } else if (shutdown || currentBatch.shouldRetryNow(Backoff.BATCH_RETRY)) {
                    // Time to retry - attempt to send the batch again.
                    // During shutdown we ignore the backoff window and retry now so
                    // the drain loop can finish fast instead of waiting out backoff.
                    logger.debug("Worker {} attempting retry {}/{}",
                            workerId, currentBatch.getRetryCount(), config.getMaxRetries());
                    if (!sendBatch(retry)) {
                        break; // Still retrying (will check again in next iteration)
                    }

                    // Retry succeed, continue to process new messages
                } else {
                    // Still in backoff period, skip processing new messages
                    break;
                }
            }

            // Check batch send timeout
            if (isBatchSendTimeout()) {
                logger.warn("Worker {} batch send timeout", workerId);
                failCurrentBatch(TglogErrorCode.SEND_TIMEOUT);
            }

            // Send current batch if ready
            if (canSendBatch()) {
                if (!sendBatch(retry)) {
                    break; // Batch is retrying
                }
            }

            // Get next message
            LogMessage msg = pendingMessage;
            if (msg == null) {
                msg = messageQueue.poll();
                if (msg == null) {
                    break; // No more messages
                }
                metrics.incrementDequeuedMsg();
            }
            pendingMessage = null;
            processed++;

            // Check if message fits in current batch
            int msgSize = msg.getPayloadSize();
            int batchSize = currentBatch.getCurrentSize();
            int maxSize = config.getMaxRequestSize();

            if (batchSize + msgSize > maxSize) {
                // Save message and send current batch first
                pendingMessage = msg;
                if (!sendBatch(retry)) {
                    break; // Batch is retrying
                }
                continue;
            }

            // Add message to batch
            if (!currentBatch.add(msg)) {
                // Batch full - save message and send batch
                pendingMessage = msg;
                if (!sendBatch(retry)) {
                    break; // Batch is retrying
                }
                continue;
            }

            // Check if batch should be sent now
            if (canSendBatchIgnoreTimeout()) {
                if (!sendBatch(retry)) {
                    break; // Batch is retrying
                }
            }
        }
    }

    /**
     * Check if batch can be sent (including timeout).
     */
    private boolean canSendBatch() {
        return currentBatch.shouldFlush();
    }

    /**
     * Check if batch can be sent (ignore timeout).
     */
    private boolean canSendBatchIgnoreTimeout() {
        if (currentBatch.isEmpty()) {
            return false;
        }

        return currentBatch.size() >= config.getBatchingMaxMessages() ||
                currentBatch.getCurrentSize() >= config.getBatchingMaxSize();
    }

    /**
     * Check if batch send has timed out.
     */
    private boolean isBatchSendTimeout() {
        return currentBatch.isSendTimeout(config.getSendTimeout());
    }

    /**
     * Check if currently retrying.
     */
    private boolean isRetrying() {
        return currentBatch.isRetrying();
    }

    /**
     * Check if retry has timed out.
     * Uses total backoff time for all retries as the deadline.
     */
    private boolean isRetryingTimeout() {
        if (!isRetrying()) {
            return false;
        }

        // Calculate total backoff time for all retries (sum of all retry intervals)
        long totalBackoff = Backoff.BATCH_RETRY.getTotalBackoffTime(config.getMaxRetries());
        return currentBatch.isRetryTimeout((int) totalBackoff);
    }

    /**
     * Send current batch.
     *
     * @param retry Whether to retry on failure
     * @return true if batch is done, false if retrying
     */
    private boolean sendBatch(boolean retry) {
        if (currentBatch.isEmpty()) {
            return true;
        }

        SendResult result = doSendBatch();

        if (result.isOk()) {
            // Success
            completeBatch(TglogErrorCode.OK);
            return true;
        }

        // Failed
        if (!retry) {
            completeBatch(result.errorCode);
            return true;
        }

        // Retry
        return retryBatch();
    }

    /**
     * Actually send the batch.
     */
    private SendResult doSendBatch() {
        // Get a connection
        TglogConnection conn = getConnection();
        if (conn == null) {
            logger.error("Worker {} no available connections", workerId);
            return SendResult.error(TglogErrorCode.NO_AVAILABLE_CONN);
        }

        // Prepare buffers
        ByteBuffer[] buffers = currentBatch.prepareForSend();

        // Send based on protocol. Both senders handle connection errors (including
        // IOException thrown mid-write) internally by switching connections, so no
        // outer catch is needed here.
        if (config.getProtocol() == TglogConfig.Protocol.TCP) {
            return sendBatchTcp(conn, buffers);
        } else {
            return sendBatchUdp(conn, buffers);
        }
    }

    /**
     * Send batch via TCP.
     * Following C version's logic:
     * - sent == 0: high watermark exceeded, try next connection
     * - sent < 0: connection error, mark connection as failed and try next
     * - sent > 0: success or partial send (buffered)
     */
    private SendResult sendBatchTcp(TglogConnection conn, ByteBuffer[] buffers) {
        int tryCount = 0;
        long totalSize = 0;
        for (ByteBuffer buf : buffers) {
            totalSize += buf.remaining();
        }

        // Fix the retry budget BEFORE the loop. handleConnectionError() removes the failed
        // connection from `connections`, shrinking its size; if the loop condition kept
        // reading connections.size() it would move toward tryCount and abandon roughly half
        // the connections (e.g. 3 connections would only try 2). Snapshot the count so we
        // attempt up to the original number of connections.
        final int maxTries = connections.size();

        while (tryCount < maxTries) {
            long sent;
            try {
                sent = conn.send(buffers);
            } catch (PartialWriteException e) {
                // The connection committed some bytes to the kernel and then died. Because
                // the protocol is request-only (no ACK) we resend at WHOLE-MESSAGE
                // boundaries: messages fully committed are treated as best-effort delivered
                // (notified success + dropped); the straddling message and everything after
                // it are resent intact on the next connection. This guarantees we never
                // write the second half of a message to a different connection (no frame
                // corruption) and never duplicate an already-committed whole message.
                int delivered = currentBatch.advancePastCommitted(e.getCommittedBytes());
                for (int i = 0; i < delivered; i++) {
                    metrics.incrementSucceededMsg();
                }
                logger.error("Worker {} TCP partial-write on connection {} ({} bytes committed, "
                                + "{} msgs best-effort delivered), resending remainder on next conn",
                        workerId, conn.getEndpoint(), e.getCommittedBytes(), delivered, e);
                handleConnectionError(conn);
                tryCount++;

                if (currentBatch.isEmpty()) {
                    // The whole batch was committed on the dead connection; nothing left.
                    return SendResult.ok();
                }

                if (tryCount < maxTries) {
                    conn = getConnection();
                    if (conn == null) {
                        return SendResult.error(TglogErrorCode.NO_AVAILABLE_CONN);
                    }
                    // Re-derive buffers: advancePastCommitted() trimmed/rewound the batch's
                    // cached buffers so only the remaining messages are sent next.
                    buffers = currentBatch.prepareForSend();
                    continue;
                }

                return SendResult.error(TglogErrorCode.SEND_FAILED);
            } catch (IOException e) {
                // A connection that fails before committing any batch bytes (e.g. the
                // initial flush of a stale outputBuffer, or a write that wrote 0 bytes then
                // threw) throws a plain IOException. No batch bytes were committed, so resend
                // the whole batch on the next connection after rewinding its buffers.
                logger.error("Worker {} TCP send error on connection {}, trying next connection",
                        workerId, conn.getEndpoint(), e);
                handleConnectionError(conn);
                tryCount++;

                if (tryCount < maxTries) {
                    conn = getConnection();
                    if (conn == null) {
                        return SendResult.error(TglogErrorCode.NO_AVAILABLE_CONN);
                    }
                    buffers = currentBatch.prepareForSend();
                    continue;
                }

                return SendResult.error(TglogErrorCode.SEND_FAILED);
            }

            // Connection error - mark as failed and try another connection
            if (sent < 0) {
                logger.error("Worker {} TCP send error on connection {}, trying next connection",
                        workerId, conn.getEndpoint());
                handleConnectionError(conn);
                tryCount++;

                if (tryCount < maxTries) {
                    conn = getConnection();
                    if (conn == null) {
                        return SendResult.error(TglogErrorCode.NO_AVAILABLE_CONN);
                    }
                    buffers = currentBatch.prepareForSend();
                    continue;
                }

                return SendResult.error(TglogErrorCode.SEND_FAILED);
            }

            // High watermark exceeded - try another connection (like C version)
            if (sent == 0) {
                logger.warn("Worker {} connection {} exceeded high watermark, trying next connection",
                        workerId, conn.getEndpoint());
                tryCount++;

                if (tryCount < maxTries) {
                    conn = getConnection();
                    if (conn == null) {
                        return SendResult.error(TglogErrorCode.NO_AVAILABLE_CONN);
                    }
                    continue;
                }

                // All connections exceeded high watermark, need to backoff and retry
                logger.warn("Worker {} all connections exceeded high watermark, will retry later", workerId);
                return SendResult.error(TglogErrorCode.SEND_FAILED);
            }

            // Check if all sent
            boolean allSent = true;
            for (ByteBuffer buf : buffers) {
                if (buf.hasRemaining()) {
                    allSent = false;
                    break;
                }
            }

            if (allSent) {
                return SendResult.ok();
            }

            // Partial send - data is buffered, consider it success
            logger.debug("Worker {} partial TCP send, data buffered", workerId);
            return SendResult.ok();
        }

        return SendResult.error(TglogErrorCode.SEND_FAILED);
    }

    /**
     * Send batch via UDP.
     */
    private SendResult sendBatchUdp(TglogConnection conn, ByteBuffer[] buffers) {
        // UDP sends the single combined buffer.
        ByteBuffer buffer = buffers[0];

        // Fix the retry budget BEFORE the loop, mirroring sendBatchTcp(): handleConnectionError()
        // removes the failed connection from `connections`, shrinking its size; reading
        // connections.size() inside the loop condition would move it toward tryCount and abandon
        // roughly half the connections. Snapshot the count so we attempt up to the original number.
        final int maxTries = connections.size();
        int tryCount = 0;

        while (tryCount < maxTries) {
            int sent;
            try {
                sent = conn.send(buffer);
            } catch (IOException e) {
                // A connection that fails mid-write throws instead of returning -1.
                // Treat it the same as sent < 0: drop this connection and try the next.
                logger.error("Worker {} UDP send error on connection {}, trying next connection",
                        workerId, conn.getEndpoint(), e);
                handleConnectionError(conn);
                tryCount++;
                if (tryCount < maxTries) {
                    conn = getConnection();
                    if (conn == null) {
                        return SendResult.error(TglogErrorCode.NO_AVAILABLE_CONN);
                    }
                    continue;
                }
                return SendResult.error(TglogErrorCode.SEND_FAILED);
            }

            // Connection error - mark as failed and try another connection (aligned with TCP:
            // drop the bad connection on every failure, not only on the last attempt).
            if (sent < 0) {
                logger.error("Worker {} UDP send error on connection {}, trying next connection",
                        workerId, conn.getEndpoint());
                handleConnectionError(conn);
                tryCount++;
                if (tryCount < maxTries) {
                    conn = getConnection();
                    if (conn == null) {
                        return SendResult.error(TglogErrorCode.NO_AVAILABLE_CONN);
                    }
                    continue;
                }
                return SendResult.error(TglogErrorCode.SEND_FAILED);
            }

            // sent == 0 means EAGAIN / would block - try another connection (not a connection
            // error, so do NOT drop the connection, mirroring TCP's high-watermark handling).
            if (sent == 0) {
                logger.warn("Worker {} UDP connection {} would block, trying next connection",
                        workerId, conn.getEndpoint());
                tryCount++;
                if (tryCount < maxTries) {
                    conn = getConnection();
                    if (conn == null) {
                        return SendResult.error(TglogErrorCode.NO_AVAILABLE_CONN);
                    }
                    continue;
                }
                logger.warn("Worker {} all UDP connections would block, will retry later", workerId);
                return SendResult.error(TglogErrorCode.SEND_FAILED);
            }

            // sent > 0: UDP sends all or nothing, so all data was sent.
            return SendResult.ok();
        }

        return SendResult.error(TglogErrorCode.SEND_FAILED);
    }

    /**
     * Retry current batch.
     *
     * @return true if batch is done, false if retrying
     */
    private boolean retryBatch() {
        // Check max retries
        if (currentBatch.getRetryCount() >= config.getMaxRetries()) {
            logger.warn("Worker {} max retries exceeded", workerId);
            completeBatch(TglogErrorCode.SEND_TIMEOUT);
            return true;
        }

        // Mark batch for retry (increments retry count and sets retry timestamps)
        currentBatch.markForRetry();
        metrics.incrementRetryMsg();

        // Calculate backoff for logging
        long backoffMs = Backoff.BATCH_RETRY.getNextInterval(currentBatch.getRetryCount());

        logger.info("Worker {} retrying batch in {}ms (attempt {}/{})",
                workerId, backoffMs, currentBatch.getRetryCount(), config.getMaxRetries());

        return false; // Still retrying
    }

    /**
     * Complete current batch (success or failure).
     */
    private void completeBatch(TglogErrorCode errorCode) {
        if (currentBatch.isEmpty()) {
            return;
        }

        // Invoke callbacks
        int msgCount = currentBatch.size();
        if (errorCode == TglogErrorCode.OK) {
            currentBatch.notifyAndRemove(msgCount);
            for (int i = 0; i < msgCount; i++) {
                metrics.incrementSucceededMsg();
            }
        } else {
            currentBatch.notifyAllFailed(errorCode);

            if (errorCode == TglogErrorCode.SEND_TIMEOUT) {
                for (int i = 0; i < msgCount; i++) {
                    metrics.incrementTimeoutMsg();
                }
            } else {
                for (int i = 0; i < msgCount; i++) {
                    metrics.incrementFailedMsg();
                }
            }
        }

        // Clear batch (this also resets createTime and retry state in BatchBuffer)
        currentBatch.clear();
    }

    /**
     * Fail current batch.
     */
    private void failCurrentBatch(TglogErrorCode errorCode) {
        completeBatch(errorCode);
    }

    /**
     * Get a connection using round-robin.
     * Like C version, attempts emergency supplement if no connections available.
     */
    private TglogConnection getConnection() {
        if (connections.isEmpty()) {
            logger.error("Worker {} no available connections", workerId);

            // Check time interval to avoid frequent emergency supplement attempts (like C version)
            long now = System.currentTimeMillis();
            long elapsed = now - lastEmergencySupplementTime;

            if (elapsed >= EMERGENCY_SUPPLEMENT_MIN_INTERVAL_MS) {
                // All connections are closed, try to supplement new connections
                logger.info("Worker {} emergency supplement connections, elapsed: {} ms",
                        workerId, elapsed);
                supplementConnection();
                lastEmergencySupplementTime = now;
            } else {
                logger.debug("Worker {} skip emergency supplement due to time interval, elapsed: {} ms",
                        workerId, elapsed);
            }

            return null;
        }

        long index = connectionIndex.getAndIncrement() & Long.MAX_VALUE;
        return connections.get((int) (index % connections.size()));
    }

    /**
     * Mark endpoint as unavailable.
     * <p>
     * If the endpoint is already marked unavailable, does not reset the timestamp or retry
     * counter, so that the existing backoff state is preserved. This prevents repeated
     * failures (e.g. from {@link #supplementConnection()}) from resetting the recovery
     * backoff and causing overly frequent retry attempts.
     */
    private void markEndpointUnavailable(InetSocketAddress endpoint) {
        String addr = endpoint.toString();

        // Don't mark if only one endpoint
        if (currentEndpoints.size() <= 1) {
            logger.warn("Worker {} skip marking {} unavailable (only one endpoint)",
                    workerId, addr);
            return;
        }

        // Don't mark if there are still connections
        for (TglogConnection conn : connections) {
            if (conn.getEndpoint().equals(endpoint)) {
                logger.warn("Worker {} skip marking {} unavailable (still has connections)",
                        workerId, addr);
                return;
            }
        }

        // If already marked unavailable, preserve existing backoff state.
        // Don't reset the timestamp or retryCount, so that recovery backoff
        // continues from the original failure time.
        if (unavailableEndpoints.containsKey(addr)) {
            logger.warn("Worker {} endpoint already marked unavailable: {}", workerId, addr);
            return;
        }

        unavailableEndpoints.put(addr, System.currentTimeMillis());
        endpointRetryCount.put(addr, 0);

        logger.warn("Worker {} marked endpoint unavailable: {}", workerId, addr);
    }

    /**
     * Recover unavailable endpoints.
     */
    private void recoverUnavailableEndpoints() {
        if (unavailableEndpoints.isEmpty()) {
            return;
        }

        long now = System.currentTimeMillis();
        List<String> toRecover = new ArrayList<>();

        for (Map.Entry<String, Long> entry : unavailableEndpoints.entrySet()) {
            String addr = entry.getKey();
            long unavailableTime = entry.getValue();
            int retries = endpointRetryCount.getOrDefault(addr, 0);

            long nextRetryInterval = Backoff.ENDPOINT_RECOVERY.getNextInterval(retries);

            if (now - unavailableTime >= nextRetryInterval) {
                toRecover.add(addr);
            }
        }

        for (String addr : toRecover) {
            // Find endpoint
            InetSocketAddress endpoint = findEndpointByAddress(addr);
            if (endpoint != null) {
                logger.info("Worker {} attempting to recover endpoint: {}", workerId, addr);

                // Try to create connections
                int successfulConns = 0;
                for (int i = 0; i < connectionsPerEndpoint; i++) {
                    try {
                        TglogConnection conn = new TglogConnection(endpoint, config, metrics, workerId);
                        conn.connect(selector);
                        connectingConns.add(conn);
                        successfulConns++;
                    } catch (IOException e) {
                        logger.warn("Failed to recover connection {}/{} for {}", (i + 1), connectionsPerEndpoint, addr, e);
                    }
                }

                // A successful conn.connect() for non-blocking TCP only *initiates*
                // the handshake; the connection is not actually established until
                // finishConnect() succeeds. So we must NOT clear the unavailable/backoff
                // state here, otherwise a host that accepts the connect attempt but then
                // fails finishConnect() would reset its retry count to 0 every cycle,
                // degrading exponential backoff into fixed-interval retries.
                //
                // The unavailable/retry state is cleared in checkConnectingConnections()
                // only once the connection reaches CONNECTED. Here we just advance the
                // backoff so we don't re-attempt before the next interval elapses.
                int retries = endpointRetryCount.getOrDefault(addr, 0);
                endpointRetryCount.put(addr, retries + 1);
                unavailableEndpoints.put(addr, now);

                if (successfulConns == 0) {
                    logger.warn("Worker {} failed to initiate any recovery connection for {}", workerId, addr);
                }
            }
        }

        // Supplement connections to maintain target count
        int targetTotal = currentEndpoints.size() * connectionsPerEndpoint;
        int currentTotal = connections.size() + connectingConns.size();

        while (currentTotal < targetTotal) {
            if (!supplementConnection()) {
                break;
            }
            currentTotal++;
        }
    }

    /**
     * Find endpoint by address string.
     */
    private InetSocketAddress findEndpointByAddress(String addr) {
        for (InetSocketAddress endpoint : currentEndpoints) {
            if (endpoint.toString().equals(addr)) {
                return endpoint;
            }
        }
        return null;
    }

    /**
     * Replace expired connections.
     */
    private void replaceExpiredConnections() {
        // long now = System.currentTimeMillis(); // Unused, kept for potential future use

        for (int i = connections.size() - 1; i >= 0; i--) {
            TglogConnection conn = connections.get(i);

            if (conn.isExpired()) {
                logger.info("Worker {} replacing expired connection: {}", workerId, conn.getEndpoint());

                // Try to create replacement
                replaceConnection(conn);
            }
        }
    }

    /**
     * Replace a connection.
     * <p>
     * The old connection is NOT removed or closed here. {@code connect()} on a
     * non-blocking TCP socket only initiates the handshake; the new connection is not
     * usable until {@link #checkConnectingConnections()} observes it as CONNECTED.
     * Removing the old connection now would shrink the usable pool for the whole
     * handshake window (and, in the worst case, leave the worker with zero usable
     * connections and trigger an unnecessary emergency supplement).
     * <p>
     * Instead the new connection records a reference to the old one it will replace
     * ({@link TglogConnection#setReplacingConn}). When the new connection reaches
     * CONNECTED, {@code checkConnectingConnections()} schedules the old one for delayed
     * close. If the new connection fails to connect, the reference is dropped and the
     * old (expired) connection keeps serving until the next replace cycle retries.
     */
    private void replaceConnection(TglogConnection oldConn) {
        // Find an available endpoint using round-robin
        for (int i = 0; i < currentEndpoints.size(); i++) {
            int index = (endpointReplaceIndex++ & Integer.MAX_VALUE) % currentEndpoints.size();
            InetSocketAddress endpoint = currentEndpoints.get(index);

            if (unavailableEndpoints.containsKey(endpoint.toString())) {
                continue;
            }

            try {
                TglogConnection newConn = new TglogConnection(endpoint, config, metrics, workerId);
                newConn.connect(selector);
                connectingConns.add(newConn);

                // Defer removing/closing the old connection until the new one is
                // actually CONNECTED (handled in checkConnectingConnections()).
                newConn.setReplacingConn(oldConn);

                return;
            } catch (IOException e) {
                logger.warn("Failed to create replacement connection for {}", endpoint, e);
                markEndpointUnavailable(endpoint);
            }
        }
    }

    /**
     * Close connection with delay (to allow pending data to be sent).
     * Implements C version's delayed close strategy:
     * - Normal replacement: 30 seconds delay
     * - Error with buffered data: 30 seconds delay
     * - Error without buffered data: 5 seconds delay
     * When timer fires, rescue pending data to other connections before closing.
     *
     * @param conn    Connection to close
     * @param isError true if closing due to error, false for normal replacement
     */
    private void closeConnectionWithDelay(TglogConnection conn, boolean isError) {
        // Determine delay based on close reason and buffered data
        int delayMs;
        if (isError) {
            if (conn.hasBufferedData()) {
                delayMs = ERROR_CONN_CLOSE_DELAY_WITH_DATA_MS;
            } else {
                delayMs = ERROR_CONN_CLOSE_DELAY_NO_DATA_MS;
            }
        } else {
            delayMs = DEFAULT_CONN_CLOSE_DELAY_MS;
        }

        long newDeadline = System.currentTimeMillis() + delayMs;

        // If already scheduled, only shorten the deadline (never extend it)
        for (DelayedClose dc : pendingCloseList) {
            if (dc.conn == conn) {
                if (newDeadline < dc.deadline) {
                    logger.info("Worker {} shortening connection close deadline from {}ms to {}ms: {}",
                            workerId, dc.deadline - System.currentTimeMillis(), delayMs, conn.getEndpoint());
                    dc.deadline = newDeadline;
                    dc.isError = isError;
                } else {
                    logger.debug("Worker {} connection close already scheduled with shorter deadline: {}",
                            workerId, conn.getEndpoint());
                }
                return;
            }
        }

        logger.info("Worker {} scheduling connection close in {}ms: {}",
                workerId, delayMs, conn.getEndpoint());
        pendingCloseList.add(new DelayedClose(conn, newDeadline, isError));
    }

    /**
     * Close connection with delay (normal replacement).
     */
    private void closeConnectionWithDelay(TglogConnection conn) {
        closeConnectionWithDelay(conn, false);
    }

    /**
     * Process pending delayed-close connections whose deadline has passed.
     * Rescues buffered data to alternative connections before closing.
     * Called from the worker thread's main event loop.
     */
    private void processPendingCloses() {
        if (pendingCloseList.isEmpty()) {
            return;
        }

        long now = System.currentTimeMillis();
        Iterator<DelayedClose> it = pendingCloseList.iterator();
        while (it.hasNext()) {
            DelayedClose dc = it.next();
            if (now < dc.deadline) {
                continue;
            }

            it.remove();
            flushOrRescueAndClose(dc.conn);
        }
    }

    /**
     * Remove a connection from the pending delayed-close list if present.
     *
     * @return true if the connection was found and removed (i.e. it was a
     *         delayed-close connection), false otherwise.
     */
    private boolean removeFromPendingClose(TglogConnection conn) {
        Iterator<DelayedClose> it = pendingCloseList.iterator();
        while (it.hasNext()) {
            if (it.next().conn == conn) {
                it.remove();
                return true;
            }
        }
        return false;
    }

    /**
     * Drain a delayed-close connection's buffered data, then close it.
     * <p>
     * Strategy (self-flush first, rescue as fallback):
     * <ol>
     *   <li>If the connection is still CONNECTED and has buffered data, try to
     *       flush it through the connection itself. This preserves data ordering
     *       and avoids needlessly moving data between connections.</li>
     *   <li>If data still remains (the connection could not flush it all, or it
     *       is already ERROR and can never self-flush), rescue the remainder onto
     *       a healthy alternative connection.</li>
     *   <li>Close the connection.</li>
     * </ol>
     * Used by {@link #processPendingCloses()} when a delayed-close deadline fires,
     * by {@link #handleConnectionError(TglogConnection, boolean)} when a
     * delayed-close connection fails a write / is closed by the peer, and by
     * {@link #cleanup()} on shutdown so data buffered on a connection that could
     * never flush is not silently dropped when the connection is force-closed.
     */
    private void flushOrRescueAndClose(TglogConnection conn) {
        try {
            // 1. Self-flush first: let the connection send its own buffered data.
            if (conn.getState() == TglogConnection.State.CONNECTED && conn.hasBufferedData()) {
                conn.tryFlush();
            }

            // 2. Rescue any remainder onto a healthy alternative connection.
            if (conn.hasBufferedData()) {
                TglogConnection altConn = getConnection();
                if (altConn != null && altConn != conn) {
                    Queue<ByteBuffer> pending = conn.removePendingData();
                    if (pending != null && !pending.isEmpty()) {
                        if (altConn.writePendingData(pending)) {
                            int rescuedBytes = 0;
                            for (ByteBuffer buf : pending) {
                                rescuedBytes += buf.remaining();
                            }
                            metrics.addRescuedMsgBytes(rescuedBytes);
                            logger.info("Worker {} rescued {} bytes from closing connection {}",
                                    workerId, rescuedBytes, conn.getEndpoint());
                        } else {
                            logger.warn("Worker {} failed to rescue data from closing connection {}",
                                    workerId, conn.getEndpoint());
                        }
                    }
                } else {
                    logger.warn("Worker {} no alternative connection to rescue data from {}",
                            workerId, conn.getEndpoint());
                }
            }

            // 3. Close.
            conn.close();
            metrics.incrementClosedConn(workerId);
            logger.info("Worker {} closed connection after delay: {}", workerId, conn.getEndpoint());

        } catch (Exception e) {
            logger.error("Worker {} error during delayed connection close", workerId, e);
            try {
                conn.close();
            } catch (Exception e2) {
                logger.error("Worker {} error closing connection", workerId, e2);
            }
        }
    }

    /**
     * Supplement a connection.
     */
    private boolean supplementConnection() {
        // Try each endpoint
        for (InetSocketAddress endpoint : currentEndpoints) {
            if (unavailableEndpoints.containsKey(endpoint.toString())) {
                continue;
            }

            // Count existing connections for this endpoint
            int existing = 0;
            for (TglogConnection conn : connections) {
                if (conn.getEndpoint().equals(endpoint)) {
                    existing++;
                }
            }
            for (TglogConnection conn : connectingConns) {
                if (conn.getEndpoint().equals(endpoint)) {
                    existing++;
                }
            }

            // Create if below target
            if (existing < connectionsPerEndpoint) {
                try {
                    TglogConnection conn = new TglogConnection(endpoint, config, metrics, workerId);
                    conn.connect(selector);
                    connectingConns.add(conn);
                    return true;
                } catch (IOException e) {
                    logger.warn("Failed to supplement connection for {}", endpoint, e);
                    markEndpointUnavailable(endpoint);
                }
            } else {
                logger.warn("Worker {} existing connections for endpoint {} is {}, which is greater than target {}",
                        workerId, endpoint, existing, connectionsPerEndpoint);
            }
        }

        // All endpoints are at or above target. Try to force supplement, but only to available
        // endpoints that are below a hard cap to prevent unbounded connection accumulation
        // (e.g. when one server is down and connections keep failing and re-trying).
        if (currentEndpoints.isEmpty()) {
            logger.warn("Worker {} no endpoints available", workerId);
            return false;
        }

        int hardCap = connectionsPerEndpoint * 2;
        int randomStart = ThreadLocalRandom.current().nextInt(currentEndpoints.size());
        for (int i = 0; i < currentEndpoints.size(); i++) {
            int idx = (randomStart + i) % currentEndpoints.size();
            InetSocketAddress endpoint = currentEndpoints.get(idx);

            // Skip unavailable endpoints
            if (unavailableEndpoints.containsKey(endpoint.toString())) {
                continue;
            }

            // Count existing + connecting connections for this endpoint
            int existing = 0;
            for (TglogConnection conn : connections) {
                if (conn.getEndpoint().equals(endpoint)) {
                    existing++;
                }
            }
            for (TglogConnection conn : connectingConns) {
                if (conn.getEndpoint().equals(endpoint)) {
                    existing++;
                }
            }

            // Hard cap: never exceed connectionsPerEndpoint * 2
            if (existing >= hardCap) {
                continue;
            }

            try {
                TglogConnection conn = new TglogConnection(endpoint, config, metrics, workerId);
                conn.connect(selector);
                connectingConns.add(conn);
                logger.info("Worker {} force supplement connection to {} (existing={}, hardCap={})",
                        workerId, endpoint, existing, hardCap);
                return true;
            } catch (IOException e) {
                logger.error("Worker {} failed to force supplement connection for {}", workerId, endpoint, e);
                markEndpointUnavailable(endpoint);
            }
        }

        logger.warn("Worker {} no available endpoint for force supplement (all unavailable or at cap)", workerId);
        return false;
    }

    /**
     * Emergency supplement connection (called by timer every 5 seconds).
     * Like C version, directly supplements if no connections available.
     * Note: lastEmergencySupplementTime is updated by the caller (runEventLoop).
     */
    private void emergencySupplementConnection() {
        if (connections.isEmpty()) {
            logger.info("Worker {} emergency supplement: no connections available, try to supplement",
                    workerId);
            supplementConnection();
        }
    }

    /**
     * Add connections for a new endpoint.
     */
    private void addConnectionsForEndpoint(InetSocketAddress endpoint) {
        logger.info("Worker {} adding connections for new endpoint: {}", workerId, endpoint);

        int successfulConns = 0;
        for (int i = 0; i < connectionsPerEndpoint; i++) {
            try {
                TglogConnection conn = new TglogConnection(endpoint, config, metrics, workerId);
                conn.connect(selector);
                connectingConns.add(conn);
                successfulConns++;
            } catch (IOException e) {
                logger.warn("Failed to create connection {}/{} for {}", (i + 1), connectionsPerEndpoint, endpoint, e);
            }
        }

        if (successfulConns == 0) {
            markEndpointUnavailable(endpoint);
        }
    }

    /**
     * Remove connections for an endpoint.
     */
    private void removeConnectionsByEndpoint(InetSocketAddress endpoint) {
        logger.info("Worker {} removing connections for endpoint: {}", workerId, endpoint);

        // Remove from connections list
        for (int i = connections.size() - 1; i >= 0; i--) {
            TglogConnection conn = connections.get(i);
            if (conn.getEndpoint().equals(endpoint)) {
                connections.remove(i);
                closeConnectionWithDelay(conn);
            }
        }

        // Remove from connecting list
        for (int i = connectingConns.size() - 1; i >= 0; i--) {
            TglogConnection conn = connectingConns.get(i);
            if (conn.getEndpoint().equals(endpoint)) {
                connectingConns.remove(i);
                // If this connecting socket was an in-flight replacement, drop its
                // reference so we don't try to retire an old connection on behalf of a
                // replacement that no longer exists.
                conn.setReplacingConn(null);
                conn.close();
                metrics.incrementClosedConn(workerId);
            }
        }

        // No extra cleanup needed for replacements targeting this endpoint's old
        // connections: those old connections were just removed/closed above, so any
        // surviving replacement that later reaches CONNECTED finds connections.remove(old)
        // returning false and skips retiring.
    }

    /**
     * Enqueue a message (called from client thread - lock-free).
     * <p>
     * Distinguishes the two failure modes so callers can react correctly:
     * <ul>
     *   <li>{@link TglogErrorCode#PRODUCER_CLOSED} — the worker is not accepting
     *       messages (still initializing, closing, closed, or its thread exited
     *       unexpectedly). The queue is irrelevant; retrying/backing off as if it
     *       were backpressure will never drain because there is no consumer.</li>
     *   <li>{@link TglogErrorCode#SEND_QUEUE_IS_FULL} — the worker is running but
     *       its bounded queue is full (genuine backpressure).</li>
     * </ul>
     *
     * @return {@link TglogErrorCode#OK} on success, otherwise the failure code.
     */
    public TglogErrorCode enqueue(LogMessage message) {
        if (state != State.READY) {
            return TglogErrorCode.PRODUCER_CLOSED;
        }

        if (!messageQueue.offer(message)) {
            return TglogErrorCode.SEND_QUEUE_IS_FULL;
        }

        metrics.incrementEnqueuedMsg();
        // No need to wake up selector - worker thread polls queue every 2ms
        return TglogErrorCode.OK;
    }

    /**
     * Get queue size.
     */
    public int getQueueSize() {
        return messageQueue.size();
    }

    /**
     * Get active connection count.
     */
    public int getActiveConnectionCount() {
        return connections.size();
    }

    /**
     * Check if worker has pending work (messages in queue, batch, or connection buffers).
     * <p>
     * Reads worker-internal, non-thread-safe structures and therefore MUST be called
     * only from the worker thread (e.g. via {@link #processFlushRequests()} or
     * {@link #completeFlushRequestsOnStop()} on the worker thread's cleanup path).
     */
    private boolean hasPendingWork() {
        // Check message queue
        if (!messageQueue.isEmpty()) {
            return true;
        }

        // Check if there's a pending message
        if (pendingMessage != null) {
            return true;
        }

        // Check current batch
        if (currentBatch != null && !currentBatch.isEmpty()) {
            return true;
        }

        // Check connection output buffers
        for (TglogConnection conn : connections) {
            if (conn.getBufferedDataSize() > 0) {
                return true;
            }
        }

        // Check pending delayed-close connections for buffered data
        for (DelayedClose dc : pendingCloseList) {
            if (dc.conn.getBufferedDataSize() > 0) {
                return true;
            }
        }

        return false;
    }

    /**
     * Stop the worker.
     */
    public void stop() {
        if (state == State.READY || state == State.INIT) {
            state = State.CLOSING;

            // Wake up selector
            if (selector != null) {
                selector.wakeup();
            }
        }
    }

    /**
     * Check if worker is stopped.
     */
    public boolean isStopped() {
        return state == State.CLOSED;
    }

    /**
     * Wait for the worker thread to terminate, up to the given timeout.
     * <p>
     * Joining the actual thread (rather than polling {@link #isStopped()}) lets
     * the caller confirm that {@code cleanup()} has fully finished before it
     * touches shared resources such as the Prometheus registry, eliminating the
     * race where metrics are unregistered while the worker is still running.
     *
     * @param timeoutMs maximum time to wait in milliseconds (0 means wait forever)
     * @return true if the worker thread has terminated, false if it is still running
     */
    public boolean join(long timeoutMs) {
        Thread thread = this.workerThread;
        if (thread == null) {
            // Never started, or already finished and reference cleared.
            return isStopped();
        }
        try {
            thread.join(timeoutMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return !thread.isAlive();
        }
        return !thread.isAlive();
    }

    /**
     * Drain connection buffers before closing (like C version's tglog_worker_drain_and_stop).
     * This ensures all buffered data in connection output buffers is sent before closing.
     */
    private void drainConnectionBuffers() {
        long stopTimeout = config.getStopTimeout();
        long startTime = System.currentTimeMillis();
        int drainAttempts = 0;
        final int maxDrainAttempts = (int) (stopTimeout / 5); // Check every 5ms

        logger.info("Worker {} draining connection buffers (timeout: {}ms)", workerId, stopTimeout);

        while (drainAttempts < maxDrainAttempts) {
            boolean allDrained = true;

            // Check all active connections
            for (TglogConnection conn : connections) {
                if (conn.hasBufferedData()) {
                    allDrained = false;
                    int bufferedSize = conn.getBufferedDataSize();
                    logger.debug("Worker {} connection {} has {} bytes buffered, flushing...",
                            workerId, conn.getEndpoint(), bufferedSize);

                    // Try to flush the connection
                    conn.tryFlush();
                }
            }

            // Check pending delayed-close connections
            for (DelayedClose dc : pendingCloseList) {
                if (dc.conn.hasBufferedData()) {
                    allDrained = false;
                    int bufferedSize = dc.conn.getBufferedDataSize();
                    logger.debug("Worker {} pending-close connection {} has {} bytes buffered, flushing...",
                            workerId, dc.conn.getEndpoint(), bufferedSize);

                    dc.conn.tryFlush();
                }
            }

            // If all buffers are drained, we're done
            if (allDrained) {
                long elapsed = System.currentTimeMillis() - startTime;
                logger.info("Worker {} all connection buffers drained in {}ms", workerId, elapsed);
                return;
            }

            // Check if we've exceeded the timeout
            long elapsed = System.currentTimeMillis() - startTime;
            if (elapsed >= stopTimeout) {
                logger.warn("Worker {} drain timeout after {}ms, some data may be lost", workerId, elapsed);
                return;
            }

            // Use selector to wait for connections to become writable (like C's event_base)
            try {
                // Short timeout to allow data to be sent
                int readyKeys = selector.select(5);

                if (readyKeys > 0) {
                    // Process writable connections
                    Iterator<SelectionKey> keyIterator = selector.selectedKeys().iterator();
                    while (keyIterator.hasNext()) {
                        SelectionKey key = keyIterator.next();
                        keyIterator.remove();

                        if (key.isValid() && key.isWritable()) {
                            TglogConnection conn = (TglogConnection) key.attachment();
                            if (conn != null) {
                                conn.tryFlush();
                            }
                        }
                    }
                }
            } catch (IOException e) {
                logger.warn("Worker {} error during drain: {}", workerId, e.getMessage());
                break;
            }

            drainAttempts++;
        }

        long elapsed = System.currentTimeMillis() - startTime;
        logger.warn("Worker {} drain completed after {} attempts ({}ms), may have unflushed data",
                workerId, drainAttempts, elapsed);
    }

    /**
     * Cleanup resources.
     */
    private void cleanup() {
        logger.info("Worker {} cleaning up", workerId);

        // Count total remaining messages (queue + batch + pending)
        int queueSize = messageQueue.size();
        int batchSize = currentBatch.size();
        int pendingSize = pendingMessage != null ? 1 : 0;
        int totalRemaining = queueSize + batchSize + pendingSize;

        logger.info("Worker {} flushing {} remaining messages (queue={}, batch={}, pending={})",
                workerId, totalRemaining, queueSize, batchSize, pendingSize);

        // Flush remaining messages with timeout protection. Run processMessages in
        // shutdown mode so a batch stuck in retry/backoff is sent immediately and
        // without re-arming another retry; otherwise this loop would busy-spin
        // (processMessages returns without draining the queue while in backoff)
        // until flushDeadline, wasting the whole stopTimeout on CPU spin.
        long flushDeadline = System.currentTimeMillis() + config.getStopTimeout();
        boolean flushTimedOut = false;
        while (!messageQueue.isEmpty() || pendingMessage != null) {
            if (System.currentTimeMillis() >= flushDeadline) {
                flushTimedOut = true;
                break;
            }

            processMessages(true);

            // Send final batch (no retry on shutdown)
            if (!currentBatch.isEmpty()) {
                sendBatch(false);
            }
        }

        // Send final batch if any
        if (!currentBatch.isEmpty()) {
            logger.info("Worker {} sending final batch with {} messages", workerId, currentBatch.size());
            sendBatch(false);
        }

        // Fail any messages that could not be flushed in time, so their
        // callbacks/Futures complete instead of hanging. These messages were
        // accepted by send() and must be reported as failed on shutdown.
        if (flushTimedOut || !messageQueue.isEmpty() || pendingMessage != null || !currentBatch.isEmpty()) {
            failRemainingMessages();
        }

        // Drain connection buffers (like C version's tglog_worker_drain_and_stop)
        drainConnectionBuffers();

        // Rescue-and-close pending delayed-close connections. A pending-close
        // connection whose endpoint is unwritable (the very reason it was being
        // closed) never drains via drainConnectionBuffers(); rescue moves its
        // buffered data onto a still-healthy connection instead of dropping it.
        // Must run while `connections` is still populated, since getConnection()
        // (used inside flushOrRescueAndClose) picks the rescue target from that list.
        logger.info("Worker {} rescue-closing {} pending delayed-close connections",
                workerId, pendingCloseList.size());
        for (DelayedClose dc : pendingCloseList) {
            flushOrRescueAndClose(dc.conn);
        }
        pendingCloseList.clear();

        // Rescue may have moved data onto active connections; drain once more so
        // those rescued bytes get flushed before we close everything.
        drainConnectionBuffers();

        // Close all connections directly (no delay needed, we've already flushed)
        for (TglogConnection conn : connections) {
            conn.close();
        }
        connections.clear();

        for (TglogConnection conn : connectingConns) {
            conn.close();
        }
        connectingConns.clear();

        // Close selector
        if (selector != null) {
            try {
                selector.close();
            } catch (IOException e) {
                logger.error("Error closing selector", e);
            }
        }

        // Complete any flush requests that were enqueued during shutdown. After
        // cleanup, completeFlushRequestsOnStop reports whether all work drained.
        // Done before flipping state to CLOSED so a concurrent requestFlush() that
        // still sees a non-CLOSED state and enqueues will be handled either here or
        // by its own post-offer state re-check.
        completeFlushRequestsOnStop();

        state = State.CLOSED;
        logger.info("Worker {} cleanup complete", workerId);

        // A requestFlush() racing with the state flip above may have enqueued after
        // our drain but before observing CLOSED. Drain once more so nothing hangs.
        completeFlushRequestsOnStop();
    }

    /**
     * Fail all messages that remain undelivered at shutdown.
     * <p>
     * Drains {@code currentBatch}, {@code pendingMessage}, and {@code messageQueue},
     * invoking each message's callback with {@link TglogErrorCode#PRODUCER_CLOSED}
     * and updating dropped-message metrics. Without this, messages accepted by
     * {@code send()} would be silently dropped on a cleanup timeout, leaving their
     * callbacks uninvoked and their CompletableFutures uncompleted.
     */
    private void failRemainingMessages() {
        TglogException exception = new TglogException(TglogErrorCode.PRODUCER_CLOSED);
        int failed = 0;

        // Batch messages that were built but never sent (e.g. flush timed out).
        if (!currentBatch.isEmpty()) {
            failed += currentBatch.size();
            currentBatch.notifyAllFailed(TglogErrorCode.PRODUCER_CLOSED);
        }

        // Message held back between batches.
        if (pendingMessage != null) {
            failMessage(pendingMessage, exception);
            pendingMessage = null;
            failed++;
        }

        // Anything still queued by producers.
        LogMessage msg;
        while ((msg = messageQueue.poll()) != null) {
            failMessage(msg, exception);
            failed++;
        }

        if (failed > 0) {
            for (int i = 0; i < failed; i++) {
                metrics.incrementDroppedMsg();
            }
            logger.warn("Worker {} failed {} undelivered messages on shutdown", workerId, failed);
        }
    }

    /**
     * Invoke a single message's callback with the given failure, isolating
     * callback exceptions so one bad callback does not block the rest.
     */
    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings(
            value = "DE_MIGHT_IGNORE",
            justification = "Intentionally ignoring callback exceptions to prevent one bad callback from affecting others")
    private void failMessage(LogMessage message, TglogException exception) {
        if (message.getCallback() != null) {
            try {
                message.getCallback().onCompletion(message.getPayload(), exception);
            } catch (Exception e) {
                // Ignore callback exceptions
            }
        }
    }

}
