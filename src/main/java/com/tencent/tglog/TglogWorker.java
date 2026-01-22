package com.tencent.tglog;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
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
    private static final Random random = new Random();

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
    private final Object readyLock = new Object();
    private final AtomicInteger readyConnCount = new AtomicInteger(0);
    private final AtomicInteger failedConnCount = new AtomicInteger(0);
    private final AtomicInteger initConnCount = new AtomicInteger(0);
    private volatile boolean initStarted = false;

    // Lock-free queues (MPSC pattern - multiple producers, single consumer in worker thread)
    private final LockFreeQueue<LogMessage> messageQueue;
    private final ConcurrentLinkedQueue<List<InetSocketAddress>> endpointUpdateQueue;

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
    private Selector selector;

    // Timers and intervals
    private long lastEmergencySupplementTime = 0;

    // Constants
    private static final int MSG_PER_RUN = 3000;
    private static final long ENDPOINT_RECOVER_INTERVAL_MS = 30000; // 30 seconds
    private static final long CONNECTION_REPLACE_CHECK_INTERVAL_MS = 20000; // 20 seconds
    private static final long EMERGENCY_SUPPLEMENT_INTERVAL_MS = 5000; // 5 seconds
    private static final long EMERGENCY_SUPPLEMENT_MIN_INTERVAL_MS = 2000; // 2 seconds
    private static final long METRICS_UPDATE_INTERVAL_MS = 5000; // 5 seconds
    private static final int DEFAULT_CONN_CLOSE_DELAY_MS = 30000; // 10 seconds (normal replacement)
    private static final int ERROR_CONN_CLOSE_DELAY_WITH_DATA_MS = 30000; // 10 seconds (error with buffered data)
    private static final int ERROR_CONN_CLOSE_DELAY_NO_DATA_MS = 5000; // 5 seconds (error without buffered data)

    // Scheduled executor for delayed connection closing
    private final ScheduledExecutorService closeScheduler;

    // Track pending close tasks so we can cancel them during cleanup
    private final Map<TglogConnection, ScheduledFuture<?>> pendingCloseTasks;

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

        // Initialize close scheduler (one thread per worker for delayed connection closing)
        this.closeScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "TglogWorker-" + workerId + "-CloseScheduler");
            t.setDaemon(true);
            return t;
        });

        // Initialize pending close tasks map
        this.pendingCloseTasks = new HashMap<>();
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
     * Start the worker thread.
     */
    public void start() {
        logger.info("Worker {} starting thread, initialEndpoints={}", workerId, (initialEndpoints == null ? "null" : initialEndpoints.size()));

        Thread workerThread = new Thread(this, "TglogWorker-" + workerId);
        workerThread.setDaemon(false);
        workerThread.start();

        logger.debug("Worker {} thread started, waiting for ready...", workerId);

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

            // Mark as ready
            state = State.READY;
            logger.info("Worker {} is ready", workerId);

            // Main event loop
            runEventLoop();

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
        connectionsPerEndpoint = currentEndpoints.size() <= 2 ? 2 : 1;
        logger.info("Worker {} using {} connections per endpoint (total endpoints: {})",
                workerId, connectionsPerEndpoint, currentEndpoints.size());

        // Create connections for each endpoint
        for (InetSocketAddress endpoint : currentEndpoints) {
            int successfulConns = 0;
            for (int i = 0; i < connectionsPerEndpoint; i++) {
                try {
                    TglogConnection conn = new TglogConnection(endpoint, config);
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
                logger.error("{}", "Error waiting for connections", e);
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

                // Periodic tasks
                long now = System.currentTimeMillis();

                // Recover unavailable endpoints (every 30s)
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

                // Update metrics (every 5s)
                if (now - lastMetricsUpdateTime >= METRICS_UPDATE_INTERVAL_MS) {
                    metrics.setMsgQueueSize(workerId, messageQueue.size());
                    metrics.setActiveConnCount(workerId, connections.size());
                    lastMetricsUpdateTime = now;
                }

            } catch (Exception e) {
                logger.error("{}", "Error in event loop", e);
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

        // Update current endpoints
        currentEndpoints = new ArrayList<>(newEndpoints);
        metrics.setEndpointCount(currentEndpoints.size());

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
                    handleConnectable(conn);
                } else if (key.isReadable()) {
                    // OP_READ triggered - check if connection is closed or discard unexpected data
                    handleReadable(conn);
                } else if (key.isWritable()) {
                    conn.tryFlush();
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

                logger.info("Worker {} connection ready: {} (total: {})",
                        workerId, conn.getEndpoint(), connections.size());

            } else if (connState == TglogConnection.State.ERROR) {
                // Remove failed connection
                connectingConns.remove(i);

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
            logger.warn("Failed to finish connection: {}", conn.getEndpoint());
            handleConnectionError(conn);
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
     * Handle connection error.
     */
    private void handleConnectionError(TglogConnection conn) {
        // Remove from both lists
        connections.remove(conn);
        connectingConns.remove(conn);

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

        // Try to supplement
        supplementConnection();
    }

    /**
     * Process messages from queue.
     */
    private void processMessages() {
        int processed = 0;

        while (processed < MSG_PER_RUN && state == State.READY) {
            // Check if we're retrying
            if (isRetrying()) {
                // Check if retry has timed out (exceeded total backoff time)
                if (isRetryingTimeout() || isBatchSendTimeout()) {
                    logger.warn("Worker {} batch retry/send timeout", workerId);
                    failCurrentBatch(TglogErrorCode.SEND_TIMEOUT);
                    // Continue to process new messages
                } else if (currentBatch.shouldRetryNow(Backoff.BATCH_RETRY)) {
                    // Time to retry - attempt to send the batch again
                    logger.debug("Worker {} attempting retry {}/{}",
                            workerId, currentBatch.getRetryCount(), config.getMaxRetries());
                    if (!sendBatch(true)) {
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
                if (!sendBatch(true)) {
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
                if (!sendBatch(true)) {
                    break; // Batch is retrying
                }
                continue;
            }

            // Add message to batch
            if (!currentBatch.add(msg)) {
                // Batch full - save message and send batch
                pendingMessage = msg;
                if (!sendBatch(true)) {
                    break; // Batch is retrying
                }
                continue;
            }

            // Check if batch should be sent now
            if (canSendBatchIgnoreTimeout()) {
                if (!sendBatch(true)) {
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

        try {
            // Prepare buffers
            ByteBuffer[] buffers = currentBatch.prepareForSend();

            // Send based on protocol
            if (config.getProtocol() == TglogConfig.Protocol.TCP) {
                return sendBatchTcp(conn, buffers);
            } else {
                return sendBatchUdp(conn, buffers);
            }

        } catch (IOException e) {
            logger.error("Worker {}{}", workerId, " send error", e);
            handleConnectionError(conn);
            return SendResult.error(TglogErrorCode.SEND_FAILED);
        }
    }

    /**
     * Send batch via TCP.
     * Following C version's logic:
     * - sent == 0: high watermark exceeded, try next connection
     * - sent < 0: connection error, mark connection as failed and try next
     * - sent > 0: success or partial send (buffered)
     */
    private SendResult sendBatchTcp(TglogConnection conn, ByteBuffer[] buffers) throws IOException {
        int tryCount = 0;
        long totalSize = 0;
        for (ByteBuffer buf : buffers) {
            totalSize += buf.remaining();
        }

        while (tryCount < connections.size()) {
            long sent = conn.send(buffers);

            // Connection error - mark as failed and try another connection
            if (sent < 0) {
                logger.error("Worker {} TCP send error on connection {}, trying next connection",
                        workerId, conn.getEndpoint());
                handleConnectionError(conn);
                tryCount++;

                if (tryCount < connections.size()) {
                    conn = getConnection();
                    if (conn == null) {
                        return SendResult.error(TglogErrorCode.NO_AVAILABLE_CONN);
                    }
                    continue;
                }

                return SendResult.error(TglogErrorCode.SEND_FAILED);
            }

            // High watermark exceeded - try another connection (like C version)
            if (sent == 0) {
                logger.warn("Worker {} connection {} exceeded high watermark, trying next connection",
                        workerId, conn.getEndpoint());
                tryCount++;

                if (tryCount < connections.size()) {
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
    private SendResult sendBatchUdp(TglogConnection conn, ByteBuffer[] buffers) throws IOException {
        // UDP sends first buffer only
        ByteBuffer buffer = buffers[0];
        // int totalSize = buffer.remaining(); // Unused, but kept for potential future logging
        int maxTries = Math.min(connections.size(), 3); // Try up to 3 connections
        int tryCount = 0;

        while (tryCount < maxTries) {
            int sent = conn.send(buffer);

            if (sent < 0) {
                // EAGAIN or error - try another connection
                tryCount++;
                if (tryCount < maxTries) {
                    conn = getConnection();
                    if (conn == null) {
                        return SendResult.error(TglogErrorCode.NO_AVAILABLE_CONN);
                    }
                    logger.debug("UDP send would block, trying another connection (attempt {}/{})",
                            (tryCount + 1), maxTries);
                    continue;
                }

                handleConnectionError(conn);
                return SendResult.error(TglogErrorCode.SEND_FAILED);
            }

            // UDP sends all or nothing - if sent > 0, all data was sent
            if (sent > 0) {
                return SendResult.ok();
            }

            // sent == 0 means EAGAIN, try another connection
            tryCount++;
            if (tryCount < maxTries) {
                conn = getConnection();
                if (conn == null) {
                    return SendResult.error(TglogErrorCode.NO_AVAILABLE_CONN);
                }
                logger.debug("UDP send would block (sent=0), trying another connection (attempt {}/{})",
                        (tryCount + 1), maxTries);
            }
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
                        TglogConnection conn = new TglogConnection(endpoint, config);
                        conn.connect(selector);
                        connectingConns.add(conn);
                        successfulConns++;
                    } catch (IOException e) {
                        logger.warn("Failed to recover connection {}/{} for {}", (i + 1), connectionsPerEndpoint, addr, e);
                    }
                }

                if (successfulConns > 0) {
                    // Remove from unavailable (will be re-added if connections fail)
                    unavailableEndpoints.remove(addr);
                    endpointRetryCount.remove(addr);
                } else {
                    // Increment retry count
                    int retries = endpointRetryCount.getOrDefault(addr, 0);
                    endpointRetryCount.put(addr, retries + 1);
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
                TglogConnection newConn = new TglogConnection(endpoint, config);
                newConn.connect(selector);
                connectingConns.add(newConn);

                // Remove old connection (will be closed after delay)
                connections.remove(oldConn);
                closeConnectionWithDelay(oldConn);

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
        // Cancel any existing close task for this connection
        ScheduledFuture<?> existingTask = pendingCloseTasks.remove(conn);
        if (existingTask != null && !existingTask.isDone()) {
            existingTask.cancel(false);
        }

        // Determine delay based on close reason and buffered data
        int delayMs;
        if (isError) {
            if (conn.hasBufferedData()) {
                delayMs = ERROR_CONN_CLOSE_DELAY_WITH_DATA_MS; // 30 seconds
                logger.info("Worker {} scheduling error connection close with buffered data in {}ms: {}",
                        workerId, delayMs, conn.getEndpoint());
            } else {
                delayMs = ERROR_CONN_CLOSE_DELAY_NO_DATA_MS; // 5 seconds
                logger.info("Worker {} scheduling error connection close without buffered data in {}ms: {}",
                        workerId, delayMs, conn.getEndpoint());
            }
        } else {
            delayMs = DEFAULT_CONN_CLOSE_DELAY_MS; // 30 seconds (normal replacement)
            logger.info("Worker {} scheduling normal connection close in {}ms: {}",
                    workerId, delayMs, conn.getEndpoint());
        }

        // Schedule delayed close with data rescue
        ScheduledFuture<?> closeTask = closeScheduler.schedule(() -> {
            try {
                // Remove from pending tasks
                pendingCloseTasks.remove(conn);

                // Try to rescue pending data before closing
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

                // Now close the connection
                conn.close();
                metrics.incrementClosedConn(workerId);
                logger.info("Worker {} closed connection after delay: {}", workerId, conn.getEndpoint());

            } catch (Exception e) {
                logger.error("Worker {} error during delayed connection close", workerId, e);
                // Ensure connection is closed even on error
                try {
                    conn.close();
                } catch (Exception e2) {
                    logger.error("Worker {} error closing connection", workerId, e2);
                }
            }
        }, delayMs, TimeUnit.MILLISECONDS);

        // Track the close task
        pendingCloseTasks.put(conn, closeTask);
    }

    /**
     * Close connection with delay (normal replacement).
     */
    private void closeConnectionWithDelay(TglogConnection conn) {
        closeConnectionWithDelay(conn, false);
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
                    TglogConnection conn = new TglogConnection(endpoint, config);
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

        // Like C version: if all endpoints are at target, randomly select one and force supplement
        if (currentEndpoints.isEmpty()) {
            logger.warn("Worker {} no endpoints available", workerId);
            return false;
        }

        // Randomly select an endpoint
        int randomIndex = random.nextInt(currentEndpoints.size());
        InetSocketAddress endpoint = currentEndpoints.get(randomIndex);
        try {
            TglogConnection conn = new TglogConnection(endpoint, config);
            conn.connect(selector);
            connectingConns.add(conn);
            logger.info("Worker {} force supplement connection to {} (exceeds target)", workerId, endpoint);
            return true;
        } catch (IOException e) {
            logger.error("Worker {} failed to force supplement connection for {}", workerId, endpoint, e);
            return false;
        }
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
                TglogConnection conn = new TglogConnection(endpoint, config);
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
                conn.close();
                metrics.incrementClosedConn(workerId);
            }
        }
    }

    /**
     * Enqueue a message (called from client thread - lock-free).
     */
    public boolean enqueue(LogMessage message) {
        if (state != State.READY) {
            return false;
        }

        boolean success = messageQueue.offer(message);
        if (success) {
            metrics.incrementEnqueuedMsg();
            // No need to wake up selector - worker thread polls queue every 2ms
        }

        return success;
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
     */
    public boolean hasPendingWork() {
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

        return false;
    }

    /**
     * Stop the worker.
     */
    public void stop() {
        if (state == State.READY) {
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

        // Flush remaining messages (without retry)
        while (!messageQueue.isEmpty() || pendingMessage != null) {
            processMessages();

            // Send final batch
            if (!currentBatch.isEmpty()) {
                sendBatch(false);
            }
        }

        // Send final batch if any
        if (!currentBatch.isEmpty()) {
            logger.info("Worker {} sending final batch with {} messages", workerId, currentBatch.size());
            sendBatch(false);
        }

        // Drain connection buffers (like C version's tglog_worker_drain_and_stop)
        drainConnectionBuffers();

        // Cancel all pending close tasks and close those connections immediately
        // (SDK is shutting down, we've already flushed)
        logger.info("Worker {} cancelling {} pending close tasks and closing connections",
                workerId, pendingCloseTasks.size());
        for (Map.Entry<TglogConnection, ScheduledFuture<?>> entry : pendingCloseTasks.entrySet()) {
            TglogConnection conn = entry.getKey();
            ScheduledFuture<?> task = entry.getValue();

            // Cancel the scheduled close task
            if (!task.isDone()) {
                task.cancel(false);
            }

            // Close the connection immediately
            conn.close();
        }
        pendingCloseTasks.clear();

        // Shutdown close scheduler
        closeScheduler.shutdown();
        try {
            if (!closeScheduler.awaitTermination(1, TimeUnit.SECONDS)) {
                closeScheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            closeScheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }

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
                logger.error("{}", "Error closing selector", e);
            }
        }

        state = State.CLOSED;
        logger.info("Worker {} cleanup complete", workerId);
    }

}
