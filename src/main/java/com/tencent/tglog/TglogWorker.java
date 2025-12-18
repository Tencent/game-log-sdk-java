package com.tencent.tglog;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Worker thread that handles message batching and network I/O.
 *
 * Key design principles (matching C version):
 * - Lock-free message queue (MPSC - Multi-Producer Single-Consumer)
 * - Lock-free endpoint update queue
 * - All connections list access happens in worker thread only (no locks needed)
 * - Batch retry with exponential backoff
 * - Connection lifecycle management
 * - Endpoint recovery with backoff
 */
class TglogWorker implements Runnable {

    enum State {
        INIT,       // Worker initialized
        READY,      // Worker ready to accept messages
        CLOSING,    // Worker is closing
        CLOSED      // Worker is closed
    }

    // Worker identification
    private final int workerId;
    private final TglogConfig config;
    private final TglogLogger logger;
    private final TglogMetrics metrics;

    // State management
    private volatile State state = State.INIT;
    private final Object readyLock = new Object();
    private volatile int readyConnCount = 0;
    private volatile int failedConnCount = 0;
    private volatile int initConnCount = 0;
    private volatile boolean initStarted = false;

    // Wakeup optimization - avoid excessive wakeup calls
    private final AtomicInteger pendingWakeup = new AtomicInteger(0);

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
    private Selector selector;
    private Thread workerThread;

    // Timers and intervals
    private long lastBatchTime = 0;
    private long lastEndpointRecoverTime = 0;
    private long lastConnectionReplaceTime = 0;
    private long lastEmergencySupplementTime = 0;

    // Retry management
    private int currentBatchRetries = 0;
    private long batchRetryStartTime = 0;
    private long batchRetryDeadline = 0;

    // Constants
    private static final int MSG_PER_RUN = 3000;
    private static final long ENDPOINT_RECOVER_INTERVAL_MS = 30000; // 30 seconds
    private static final long CONNECTION_REPLACE_CHECK_INTERVAL_MS = 20000; // 20 seconds
    private static final long EMERGENCY_SUPPLEMENT_INTERVAL_MS = 5000; // 5 seconds
    private static final long EMERGENCY_SUPPLEMENT_MIN_INTERVAL_MS = 2000; // 2 seconds
    private static final int DEFAULT_CONN_CLOSE_DELAY_MS = 30000; // 30 seconds

    /**
     * Create a new worker.
     */
    public TglogWorker(int workerId, TglogConfig config, TglogLogger logger, TglogMetrics metrics) {
        this.workerId = workerId;
        this.config = config;
        this.logger = logger != null ? logger : new TglogLogger.NoOpLogger();
        this.metrics = metrics;

        // Initialize lock-free queues
        this.messageQueue = new LockFreeQueue<>(config.getMaxPendingMessages());
        this.endpointUpdateQueue = new ConcurrentLinkedQueue<>();

        // Initialize collections (accessed only by worker thread)
        this.currentEndpoints = new ArrayList<>();
        this.connections = new ArrayList<>();
        this.connectingConns = new ArrayList<>();
        this.unavailableEndpoints = new HashMap<>();
        this.endpointRetryCount = new HashMap<>();

        // Initialize batch buffer
        this.currentBatch = new BatchBuffer(config);
    }

    /**
     * Set initial endpoints (called by Client before start).
     * This is the lock-free way to pass initial data to worker.
     */
    public void setInitialEndpoints(List<InetSocketAddress> endpoints) {
        this.initialEndpoints = new ArrayList<>(endpoints);
        logger.log(TglogLogger.Level.INFO,
            "Worker " + workerId + " received " + endpoints.size() + " initial endpoints");
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

        logger.log(TglogLogger.Level.DEBUG,
            "Worker " + workerId + " notified of endpoint update: " + newEndpoints.size() + " endpoints");
    }

    /**
     * Start the worker thread.
     */
    public void start() {
        logger.log(TglogLogger.Level.INFO,
            "Worker " + workerId + " starting thread, initialEndpoints=" +
            (initialEndpoints == null ? "null" : initialEndpoints.size()));

        workerThread = new Thread(this, "TglogWorker-" + workerId);
        workerThread.setDaemon(false);
        workerThread.start();

        logger.log(TglogLogger.Level.DEBUG,
            "Worker " + workerId + " thread started, waiting for ready...");

        // Wait for worker to be ready
        long startTime = System.currentTimeMillis();
        long timeout = config.getConnectionTimeout() * 2; // Give enough time

        synchronized (readyLock) {
            // First wait for initialization to start
            while (!initStarted && state == State.INIT) {
                long elapsed = System.currentTimeMillis() - startTime;
                if (elapsed >= timeout) {
                    logger.log(TglogLogger.Level.ERROR,
                        "Worker " + workerId + " timeout waiting for init to start");
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
            while (state == State.INIT && readyConnCount + failedConnCount < initConnCount) {
                long elapsed = System.currentTimeMillis() - startTime;
                if (elapsed >= timeout) {
                    logger.log(TglogLogger.Level.ERROR,
                        "Worker " + workerId + " timeout waiting for connections");
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

        logger.log(TglogLogger.Level.DEBUG,
            "Worker " + workerId + " wait complete: ready=" + readyConnCount +
            ", failed=" + failedConnCount + ", init=" + initConnCount);

        if (readyConnCount <= 0) {
            throw new RuntimeException("Worker " + workerId + " failed to initialize connections");
        }

        logger.log(TglogLogger.Level.INFO,
            "Worker " + workerId + " started with " + readyConnCount + " ready connections");
    }

    /**
     * Worker thread main loop.
     */
    @Override
    public void run() {
        try {
            logger.log(TglogLogger.Level.INFO, "Worker " + workerId + " thread started");

            // Initialize NIO selector
            logger.log(TglogLogger.Level.DEBUG, "Worker " + workerId + " opening selector");
            selector = Selector.open();

            // Initialize connections from initial endpoints
            logger.log(TglogLogger.Level.DEBUG, "Worker " + workerId + " initializing connections");
            initializeConnections();

            // Wait for initial connections to be ready
            logger.log(TglogLogger.Level.DEBUG, "Worker " + workerId + " waiting for connections");
            waitForInitialConnections();

            // Mark as ready
            state = State.READY;
            logger.log(TglogLogger.Level.INFO, "Worker " + workerId + " is ready");

            // Main event loop
            runEventLoop();

        } catch (Exception e) {
            logger.log(TglogLogger.Level.ERROR, "Worker " + workerId + " error", e);
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
        logger.log(TglogLogger.Level.INFO,
            "Worker " + workerId + " initializing connections, initialEndpoints=" +
            (initialEndpoints == null ? "null" : initialEndpoints.size()));

        if (initialEndpoints == null || initialEndpoints.isEmpty()) {
            throw new IOException("No initial endpoints provided");
        }

        // Copy initial endpoints to current endpoints
        currentEndpoints = new ArrayList<>(initialEndpoints);

        // Determine connections per endpoint
        connectionsPerEndpoint = currentEndpoints.size() <= 2 ? 2 : 1;
        logger.log(TglogLogger.Level.INFO,
            "Worker " + workerId + " using " + connectionsPerEndpoint +
            " connections per endpoint (total endpoints: " + currentEndpoints.size() + ")");

        // Create connections for each endpoint
        for (InetSocketAddress endpoint : currentEndpoints) {
            int successfulConns = 0;
            for (int i = 0; i < connectionsPerEndpoint; i++) {
                try {
                    TglogConnection conn = new TglogConnection(endpoint, config, logger);
                    conn.connect(selector);
                    connectingConns.add(conn);
                    initConnCount++;
                    successfulConns++;
                } catch (IOException e) {
                    logger.log(TglogLogger.Level.WARN,
                        "Failed to create connection " + (i + 1) + "/" + connectionsPerEndpoint +
                        " for endpoint " + endpoint, e);
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

        logger.log(TglogLogger.Level.DEBUG,
            "Worker " + workerId + " created " + connectingConns.size() + " initial connections");

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

        while (readyConnCount + failedConnCount < initConnCount) {
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
                logger.log(TglogLogger.Level.ERROR, "Error waiting for connections", e);
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
        lastBatchTime = System.currentTimeMillis();
        lastEndpointRecoverTime = System.currentTimeMillis();
        lastConnectionReplaceTime = System.currentTimeMillis();
        lastEmergencySupplementTime = 0; // Allow first supplement immediately

        while (state == State.READY) {
            try {
                // Reset wakeup flag before select
                pendingWakeup.set(0);

                // Process endpoint updates (lock-free queue)
                processEndpointUpdates();

                // Handle NIO events with short timeout (2ms like C version)
                int readyKeys = selector.select(2);

                if (readyKeys > 0) {
                    processSelectionKeys();
                }

                // Check connecting connections
                checkConnectingConnections();

                // Process messages from queue (up to MSG_PER_RUN)
                processMessages();

                // Check batch timeout
                checkBatchTimeout();

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
                    emergencySupplementCheck();
                    lastEmergencySupplementTime = now;
                }

            } catch (Exception e) {
                logger.log(TglogLogger.Level.ERROR, "Error in event loop", e);
            }
        }

        logger.log(TglogLogger.Level.INFO, "Worker " + workerId + " event loop ended");
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
        logger.log(TglogLogger.Level.INFO,
            "Worker " + workerId + " processing endpoint update: " + newEndpoints.size() + " endpoints");

        // Find added and removed endpoints
        Set<InetSocketAddress> oldSet = new HashSet<>(currentEndpoints);
        Set<InetSocketAddress> newSet = new HashSet<>(newEndpoints);

        // Find removed endpoints
        Set<InetSocketAddress> removed = new HashSet<>(oldSet);
        removed.removeAll(newSet);

        // Find added endpoints
        Set<InetSocketAddress> added = new HashSet<>(newSet);
        added.removeAll(oldSet);

        // Remove connections for removed endpoints
        for (InetSocketAddress endpoint : removed) {
            removeConnectionsByEndpoint(endpoint);
            unavailableEndpoints.remove(endpoint.toString());
            endpointRetryCount.remove(endpoint.toString());
            metrics.incrementRemoveEndpoint();
        }

        // Add connections for new endpoints
        for (InetSocketAddress endpoint : added) {
            addConnectionsForEndpoint(endpoint);
            metrics.incrementAddEndpoint();
        }

        // Update current endpoints
        currentEndpoints = new ArrayList<>(newEndpoints);
        metrics.setEndpointCount(currentEndpoints.size());

        logger.log(TglogLogger.Level.INFO,
            "Worker " + workerId + " endpoint update complete: added=" + added.size() +
            ", removed=" + removed.size() + ", total=" + currentEndpoints.size());
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
                } else if (key.isWritable()) {
                    conn.tryFlush();
                }
            } catch (Exception e) {
                logger.log(TglogLogger.Level.ERROR,
                    "Error processing key for " + conn.getEndpoint(), e);
                handleConnectionError(conn);
            }
        }
    }

    /**
     * Check connecting connections for completion.
     */
    private void checkConnectingConnections() {
        logger.log(TglogLogger.Level.DEBUG,
            "Worker " + workerId + " checking " + connectingConns.size() + " connecting connections");

        for (int i = connectingConns.size() - 1; i >= 0; i--) {
            TglogConnection conn = connectingConns.get(i);
            TglogConnection.State connState = conn.getState();

            logger.log(TglogLogger.Level.DEBUG,
                "Worker " + workerId + " connection " + i + " state: " + connState +
                ", endpoint: " + conn.getEndpoint());

            if (connState == TglogConnection.State.CONNECTED) {
                // Move to connected list
                connectingConns.remove(i);
                connections.add(conn);

                // Update metrics
                metrics.incrementOpenedConn();

                // Notify if this is an init connection
                if (state == State.INIT) {
                    synchronized (readyLock) {
                        readyConnCount++;
                        logger.log(TglogLogger.Level.INFO,
                            "Worker " + workerId + " ready conn count: " + readyConnCount +
                            "/" + initConnCount);
                        readyLock.notifyAll();
                    }
                }

                logger.log(TglogLogger.Level.INFO,
                    "Worker " + workerId + " connection ready: " + conn.getEndpoint() +
                    " (total: " + connections.size() + ")");

            } else if (connState == TglogConnection.State.ERROR) {
                // Remove failed connection
                connectingConns.remove(i);

                // Mark endpoint unavailable
                markEndpointUnavailable(conn.getEndpoint());

                // Notify if this is an init connection
                if (state == State.INIT) {
                    synchronized (readyLock) {
                        failedConnCount++;
                        logger.log(TglogLogger.Level.WARN,
                            "Worker " + workerId + " failed conn count: " + failedConnCount +
                            "/" + initConnCount);
                        readyLock.notifyAll();
                    }
                }

                logger.log(TglogLogger.Level.WARN,
                    "Worker " + workerId + " connection failed: " + conn.getEndpoint());
            }
        }
    }

    /**
     * Handle connectable event.
     */
    private void handleConnectable(TglogConnection conn) {
        if (conn.finishConnect()) {
            logger.log(TglogLogger.Level.DEBUG,
                "Connection established: " + conn.getEndpoint());
        } else {
            logger.log(TglogLogger.Level.WARN,
                "Failed to finish connection: " + conn.getEndpoint());
            handleConnectionError(conn);
        }
    }

    /**
     * Handle connection error.
     */
    private void handleConnectionError(TglogConnection conn) {
        // Remove from both lists
        connections.remove(conn);
        connectingConns.remove(conn);

        // Mark endpoint unavailable
        markEndpointUnavailable(conn.getEndpoint());

        // Close connection with data rescue
        closeConnectionWithDelay(conn);

        // Update metrics
        metrics.incrementClosedConn();

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
                if (!isRetryingTimeout()) {
                    break; // Still in retry backoff
                }
                // Retry timeout - fail the batch
                logger.log(TglogLogger.Level.WARN,
                    "Worker " + workerId + " batch retry timeout");
                failCurrentBatch(TglogException.ERR_SEND_TIMEOUT);
            }

            // Check batch send timeout
            if (isBatchSendTimeout()) {
                logger.log(TglogLogger.Level.WARN,
                    "Worker " + workerId + " batch send timeout");
                failCurrentBatch(TglogException.ERR_SEND_TIMEOUT);
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
            int msgSize = msg.getPayload().getBytes().length;
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
        if (currentBatch.isEmpty()) {
            return false;
        }

        // Check message count
        if (currentBatch.size() >= config.getBatchingMaxMessages()) {
            return true;
        }

        // Check size
        if (currentBatch.getCurrentSize() >= config.getBatchingMaxSize()) {
            return true;
        }

        // Check timeout
        long elapsed = System.currentTimeMillis() - lastBatchTime;
        return elapsed >= config.getBatchingMaxDelayMs();
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
     * Check batch timeout.
     */
    private void checkBatchTimeout() {
        if (canSendBatch()) {
            sendBatch(true);
        }
    }

    /**
     * Check if batch send has timed out.
     */
    private boolean isBatchSendTimeout() {
        if (currentBatch.isEmpty() || config.getSendTimeout() <= 0) {
            return false;
        }

        long elapsed = System.currentTimeMillis() - lastBatchTime;
        return elapsed >= config.getSendTimeout();
    }

    /**
     * Check if currently retrying.
     */
    private boolean isRetrying() {
        return batchRetryStartTime > 0;
    }

    /**
     * Check if retry has timed out.
     */
    private boolean isRetryingTimeout() {
        if (!isRetrying()) {
            return false;
        }

        return System.currentTimeMillis() >= batchRetryDeadline;
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

        int errorCode = doSendBatch();

        if (errorCode == TglogException.ERR_OK) {
            // Success
            completeBatch(TglogException.ERR_OK);
            return true;
        }

        // Failed
        if (!retry) {
            completeBatch(errorCode);
            return true;
        }

        // Retry
        return retryBatch();
    }

    /**
     * Actually send the batch.
     */
    private int doSendBatch() {
        // Get a connection
        TglogConnection conn = getConnection();
        if (conn == null) {
            logger.log(TglogLogger.Level.ERROR,
                "Worker " + workerId + " no available connections");
            return TglogException.ERR_NO_AVAILABLE_CONN;
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
            logger.log(TglogLogger.Level.ERROR,
                "Worker " + workerId + " send error", e);
            handleConnectionError(conn);
            return TglogException.ERR_SEND_FAILED;
        }
    }

    /**
     * Send batch via TCP.
     */
    private int sendBatchTcp(TglogConnection conn, ByteBuffer[] buffers) throws IOException {
        int tryCount = 0;
        long totalSize = 0;
        for (ByteBuffer buf : buffers) {
            totalSize += buf.remaining();
        }

        while (tryCount < connections.size()) {
            long sent = conn.send(buffers);

            if (sent < 0) {
                // Error - try another connection
                handleConnectionError(conn);
                tryCount++;

                if (tryCount < connections.size()) {
                    conn = getConnection();
                    if (conn == null) {
                        return TglogException.ERR_NO_AVAILABLE_CONN;
                    }
                    continue;
                }

                return TglogException.ERR_SEND_FAILED;
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
                return TglogException.ERR_OK;
            }

            // Partial send - data is buffered, consider it success
            logger.log(TglogLogger.Level.DEBUG,
                "Worker " + workerId + " partial TCP send, data buffered");
            return TglogException.ERR_OK;
        }

        return TglogException.ERR_SEND_FAILED;
    }

    /**
     * Send batch via UDP.
     */
    private int sendBatchUdp(TglogConnection conn, ByteBuffer[] buffers) throws IOException {
        // UDP sends first buffer only
        ByteBuffer buffer = buffers[0];
        int totalSize = buffer.remaining();
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
                        return TglogException.ERR_NO_AVAILABLE_CONN;
                    }
                    logger.log(TglogLogger.Level.DEBUG,
                        "UDP send would block, trying another connection (attempt " +
                        (tryCount + 1) + "/" + maxTries + ")");
                    continue;
                }

                handleConnectionError(conn);
                return TglogException.ERR_SEND_FAILED;
            }

            // UDP sends all or nothing - if sent > 0, all data was sent
            if (sent > 0) {
                return TglogException.ERR_OK;
            }

            // sent == 0 means EAGAIN, try another connection
            tryCount++;
            if (tryCount < maxTries) {
                conn = getConnection();
                if (conn == null) {
                    return TglogException.ERR_NO_AVAILABLE_CONN;
                }
                logger.log(TglogLogger.Level.DEBUG,
                    "UDP send would block (sent=0), trying another connection (attempt " +
                    (tryCount + 1) + "/" + maxTries + ")");
            }
        }

        return TglogException.ERR_SEND_FAILED;
    }

    /**
     * Retry current batch.
     *
     * @return true if batch is done, false if retrying
     */
    private boolean retryBatch() {
        // Check max retries
        if (currentBatchRetries >= config.getMaxRetries()) {
            logger.log(TglogLogger.Level.WARN,
                "Worker " + workerId + " max retries exceeded");
            completeBatch(TglogException.ERR_SEND_TIMEOUT);
            return true;
        }

        // Start retry
        currentBatchRetries++;
        metrics.incrementRetryMsg();

        // Calculate backoff
        long backoffMs = Backoff.BATCH_RETRY.getNextInterval(currentBatchRetries);

        // Set retry deadline on first retry
        if (currentBatchRetries == 1) {
            batchRetryStartTime = System.currentTimeMillis();
            long totalBackoff = Backoff.BATCH_RETRY.getNextInterval(config.getMaxRetries() + 1);
            batchRetryDeadline = batchRetryStartTime + totalBackoff;
        }

        logger.log(TglogLogger.Level.DEBUG,
            "Worker " + workerId + " retrying batch in " + backoffMs + "ms (attempt " +
            currentBatchRetries + "/" + config.getMaxRetries() + ")");

        return false; // Still retrying
    }

    /**
     * Complete current batch (success or failure).
     */
    private void completeBatch(int errorCode) {
        if (currentBatch.isEmpty()) {
            return;
        }

        // Invoke callbacks
        int msgCount = currentBatch.size();
        if (errorCode == TglogException.ERR_OK) {
            currentBatch.notifyAndRemove(msgCount);
            for (int i = 0; i < msgCount; i++) {
                metrics.incrementSucceededMsg();
            }
        } else {
            String errorMsg = getErrorMessage(errorCode);
            currentBatch.notifyAllFailed(errorCode, errorMsg);

            if (errorCode == TglogException.ERR_SEND_TIMEOUT) {
                for (int i = 0; i < msgCount; i++) {
                    metrics.incrementTimeoutMsg();
                }
            } else {
                for (int i = 0; i < msgCount; i++) {
                    metrics.incrementFailedMsg();
                }
            }
        }

        // Clear batch
        currentBatch.clear();
        lastBatchTime = System.currentTimeMillis();

        // Reset retry state
        currentBatchRetries = 0;
        batchRetryStartTime = 0;
        batchRetryDeadline = 0;
    }

    /**
     * Fail current batch.
     */
    private void failCurrentBatch(int errorCode) {
        completeBatch(errorCode);
    }

    /**
     * Get a connection using round-robin.
     */
    private TglogConnection getConnection() {
        if (connections.isEmpty()) {
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
            logger.log(TglogLogger.Level.WARN,
                "Worker " + workerId + " skip marking " + addr +
                " unavailable (only one endpoint)");
            return;
        }

        // Don't mark if there are still connections
        for (TglogConnection conn : connections) {
            if (conn.getEndpoint().equals(endpoint)) {
                logger.log(TglogLogger.Level.WARN,
                    "Worker " + workerId + " skip marking " + addr +
                    " unavailable (still has connections)");
                return;
            }
        }

        unavailableEndpoints.put(addr, System.currentTimeMillis());
        endpointRetryCount.put(addr, 0);

        logger.log(TglogLogger.Level.WARN,
            "Worker " + workerId + " marked endpoint unavailable: " + addr);
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
                logger.log(TglogLogger.Level.INFO,
                    "Worker " + workerId + " attempting to recover endpoint: " + addr);

                // Try to create connections
                int successfulConns = 0;
                for (int i = 0; i < connectionsPerEndpoint; i++) {
                    try {
                        TglogConnection conn = new TglogConnection(endpoint, config, logger);
                        conn.connect(selector);
                        connectingConns.add(conn);
                        successfulConns++;
                    } catch (IOException e) {
                        logger.log(TglogLogger.Level.WARN,
                            "Failed to recover connection " + (i + 1) + "/" +
                            connectionsPerEndpoint + " for " + addr, e);
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
        long now = System.currentTimeMillis();

        for (int i = connections.size() - 1; i >= 0; i--) {
            TglogConnection conn = connections.get(i);

            if (conn.isExpired()) {
                logger.log(TglogLogger.Level.INFO,
                    "Worker " + workerId + " replacing expired connection: " +
                    conn.getEndpoint());

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
                TglogConnection newConn = new TglogConnection(endpoint, config, logger);
                newConn.connect(selector);
                connectingConns.add(newConn);

                // Remove old connection (will be closed after delay)
                connections.remove(oldConn);
                closeConnectionWithDelay(oldConn);

                return;
            } catch (IOException e) {
                logger.log(TglogLogger.Level.WARN,
                    "Failed to create replacement connection for " + endpoint, e);
                markEndpointUnavailable(endpoint);
            }
        }
    }

    /**
     * Close connection with delay (to allow pending data to be sent).
     */
    private void closeConnectionWithDelay(TglogConnection conn) {
        // Try to rescue pending data
        if (conn.hasBufferedData()) {
            TglogConnection altConn = getConnection();
            if (altConn != null) {
                Queue<ByteBuffer> pending = conn.removePendingData();
                if (pending != null && !pending.isEmpty()) {
                    if (altConn.writePendingData(pending)) {
                        int rescuedBytes = 0;
                        for (ByteBuffer buf : pending) {
                            rescuedBytes += buf.remaining();
                        }
                        // Note: TglogMetrics doesn't have incrementRescuedBytes, skip it
                        logger.log(TglogLogger.Level.INFO,
                            "Worker " + workerId + " rescued " + rescuedBytes + " bytes");
                    }
                }
            }
        }

        // Close immediately (in Java we don't have delayed close like C version's timer)
        conn.close();
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
                    TglogConnection conn = new TglogConnection(endpoint, config, logger);
                    conn.connect(selector);
                    connectingConns.add(conn);
                    return true;
                } catch (IOException e) {
                    logger.log(TglogLogger.Level.WARN,
                        "Failed to supplement connection for " + endpoint, e);
                    markEndpointUnavailable(endpoint);
                }
            }
        }

        return false;
    }

    /**
     * Emergency supplement check.
     */
    private void emergencySupplementCheck() {
        if (connections.isEmpty()) {
            long now = System.currentTimeMillis();
            long elapsed = now - lastEmergencySupplementTime;

            if (elapsed >= EMERGENCY_SUPPLEMENT_MIN_INTERVAL_MS) {
                logger.log(TglogLogger.Level.WARN,
                    "Worker " + workerId + " emergency supplement: no connections available");
                supplementConnection();
                lastEmergencySupplementTime = now;
            }
        }
    }

    /**
     * Add connections for a new endpoint.
     */
    private void addConnectionsForEndpoint(InetSocketAddress endpoint) {
        logger.log(TglogLogger.Level.INFO,
            "Worker " + workerId + " adding connections for new endpoint: " + endpoint);

        int successfulConns = 0;
        for (int i = 0; i < connectionsPerEndpoint; i++) {
            try {
                TglogConnection conn = new TglogConnection(endpoint, config, logger);
                conn.connect(selector);
                connectingConns.add(conn);
                successfulConns++;
            } catch (IOException e) {
                logger.log(TglogLogger.Level.WARN,
                    "Failed to create connection " + (i + 1) + "/" +
                    connectionsPerEndpoint + " for " + endpoint, e);
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
        logger.log(TglogLogger.Level.INFO,
            "Worker " + workerId + " removing connections for endpoint: " + endpoint);

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

            // Wake up selector (optimized - avoid excessive wakeup calls)
            // Only wakeup if selector is likely blocked
            if (selector != null && pendingWakeup.compareAndSet(0, 1)) {
                selector.wakeup();
            }
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
     * Check if worker has pending work (messages in queue, batch, or connection buffers).
     */
    public boolean hasPendingWork() {
        // Check message queue
        if (messageQueue.size() > 0) {
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
     * Cleanup resources.
     */
    private void cleanup() {
        logger.log(TglogLogger.Level.INFO, "Worker " + workerId + " cleaning up");

        // Flush remaining messages (without retry)
        logger.log(TglogLogger.Level.INFO,
            "Worker " + workerId + " flushing " + messageQueue.size() + " remaining messages");

        while (!messageQueue.isEmpty() || pendingMessage != null) {
            processMessages();

            // Send final batch
            if (!currentBatch.isEmpty()) {
                sendBatch(false);
            }
        }

        // Send final batch if any
        if (!currentBatch.isEmpty()) {
            sendBatch(false);
        }

        // Close all connections
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
                logger.log(TglogLogger.Level.ERROR, "Error closing selector", e);
            }
        }

        state = State.CLOSED;
        logger.log(TglogLogger.Level.INFO, "Worker " + workerId + " cleanup complete");
    }

    /**
     * Get error message for error code.
     */
    private String getErrorMessage(int errorCode) {
        switch (errorCode) {
            case TglogException.ERR_OK:
                return "Success";
            case TglogException.ERR_SEND_FAILED:
                return "Send failed";
            case TglogException.ERR_SEND_TIMEOUT:
                return "Send timeout";
            case TglogException.ERR_NO_AVAILABLE_CONN:
                return "No available connections";
            case TglogException.ERR_PRODUCER_CLOSED:
                return "Producer closed";
            default:
                return "Unknown error: " + errorCode;
        }
    }
}
