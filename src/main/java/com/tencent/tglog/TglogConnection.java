package com.tencent.tglog;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Represents a single network connection (TCP or UDP).
 * <p>
 * This class handles:
 * - Non-blocking I/O operations
 * - Connection lifecycle management
 * - Automatic reconnection
 * - Send buffer management
 */
@edu.umd.cs.findbugs.annotations.SuppressFBWarnings(
    value = "RV_RETURN_VALUE_IGNORED_BAD_PRACTICE",
    justification = "Queue.offer() return value ignored - queue is unbounded LinkedList, offer always succeeds")
class TglogConnection {

    private static final Logger logger = LoggerFactory.getLogger(TglogConnection.class);
    private static final Random random = new Random();

    enum State {
        DISCONNECTED,
        CONNECTING,
        CONNECTED,
        ERROR
    }

    private final TglogConfig.Protocol protocol;
    private final InetSocketAddress endpoint;
    private final TglogConfig config;

    private SocketChannel tcpChannel;
    private DatagramChannel udpChannel;
    private SelectionKey selectionKey;

    private volatile State state = State.DISCONNECTED;
    private final AtomicLong connectTime = new AtomicLong(0);
    private final AtomicLong lastActiveTime = new AtomicLong(0);
    private int retryCount = 0;
    private long lastRetryAttemptTime = 0;
    private final Backoff connectionBackoff = Backoff.CONNECTION_RETRY;

    // Connection lifetime with jitter (to avoid all connections expiring at the same time)
    private long lifetimeWithJitterMs = 0;

    // Output buffer for partial writes (similar to C's bufferevent)
    // Ensures data integrity by keeping unsent data bound to this connection
    private final Queue<ByteBuffer> outputBuffer = new LinkedList<>();
    private static final int MAX_OUTPUT_BUFFER_SIZE = 32 * 1024 * 1024; // 32MB like C version
    private int outputBufferSize = 0;

    public TglogConnection(InetSocketAddress endpoint, TglogConfig config) {
        this.endpoint = endpoint;
        this.protocol = config.getProtocol();
        this.config = config;
    }

    /**
     * Connect to the remote endpoint (non-blocking).
     *
     * @param selector NIO selector for registration
     * @throws IOException if connection fails
     */
    public void connect(Selector selector) throws IOException {
        if (state == State.CONNECTED || state == State.CONNECTING) {
            return;
        }

        try {
            if (protocol == TglogConfig.Protocol.TCP) {
                state = State.CONNECTING;
                logger.info("Connecting to {}", endpoint);
                connectTcp(selector);
            } else {
                // UDP connects immediately
                connectUdp(selector);
                logger.info("Connected to {} (UDP)", endpoint);
            }
        } catch (IOException e) {
            state = State.ERROR;
            retryCount++;
            throw e;
        }
    }

    private void connectTcp(Selector selector) throws IOException {
        tcpChannel = SocketChannel.open();
        tcpChannel.configureBlocking(false);

        // Set socket options
        // tcpChannel.setOption(StandardSocketOptions.TCP_NODELAY, true);
        tcpChannel.setOption(StandardSocketOptions.SO_KEEPALIVE, true);
        tcpChannel.setOption(StandardSocketOptions.SO_SNDBUF, config.getSocketSendBufferSize());
        tcpChannel.setOption(StandardSocketOptions.SO_RCVBUF, config.getSocketRecvBufferSize());

        // Start connection
        boolean connected = tcpChannel.connect(endpoint);
        if (connected) {
            finishConnect();
        } else {
            // Register for connect completion
            selectionKey = tcpChannel.register(selector, SelectionKey.OP_CONNECT, this);
        }
    }

    private void connectUdp(Selector selector) throws IOException {
        udpChannel = DatagramChannel.open();
        udpChannel.configureBlocking(false);

        // Set socket options
        udpChannel.setOption(StandardSocketOptions.SO_SNDBUF, config.getSocketSendBufferSize());
        udpChannel.setOption(StandardSocketOptions.SO_RCVBUF, config.getSocketRecvBufferSize());

        // Connect to endpoint (for UDP this just sets the default destination)
        udpChannel.connect(endpoint);

        // Register with no events by default (will register OP_WRITE when needed)
        selectionKey = udpChannel.register(selector, 0, this);

        // UDP is immediately "connected"
        finishConnect();
    }

    /**
     * Finish the connection process (called when OP_CONNECT is ready).
     *
     * @return true if connection completed successfully
     */
    public boolean finishConnect() {
        try {
            if (protocol == TglogConfig.Protocol.TCP) {
                if (tcpChannel.finishConnect()) {
                    state = State.CONNECTED;
                    connectTime.set(System.currentTimeMillis());
                    lastActiveTime.set(System.currentTimeMillis());
                    retryCount = 0;

                    // Calculate lifetime with jitter to avoid all connections expiring at the same time
                    lifetimeWithJitterMs = calculateLifetimeWithJitter();

                    // Monitor OP_READ to detect connection close by peer
                    // OP_WRITE will be added dynamically when we have data to send
                    if (selectionKey != null) {
                        selectionKey.interestOps(SelectionKey.OP_READ);
                    }

                    logger.info("Connected to {} (lifetime: {}ms)", endpoint, lifetimeWithJitterMs);
                    return true;
                }
            } else {
                // UDP is always "connected"
                state = State.CONNECTED;
                connectTime.set(System.currentTimeMillis());
                lastActiveTime.set(System.currentTimeMillis());
                retryCount = 0;

                // Calculate lifetime with jitter to avoid all connections expiring at the same time
                lifetimeWithJitterMs = calculateLifetimeWithJitter();

                logger.info("Connected to {} (UDP, lifetime: {}ms)", endpoint, lifetimeWithJitterMs);
                return true;
            }
        } catch (IOException e) {
            logger.error("Failed to finish connection to {}", endpoint, e);
            state = State.ERROR;
            retryCount++;
        }
        return false;
    }

    /**
     * Send data through this connection.
     * Handles partial writes by buffering unsent data (like C's bufferevent).
     *
     * @param buffer Data to send
     * @return Number of bytes that were accepted (may be buffered), 0 if high watermark exceeded, or -1 on error
     */
    public int send(ByteBuffer buffer) throws IOException {
        if (state != State.CONNECTED) {
            return -1;
        }

        int originalRemaining = buffer.remaining();

        try {
            // First, try to flush any buffered data
            if (!outputBuffer.isEmpty()) {
                flushOutputBuffer();
            }

            // If output buffer is not empty after flush, buffer this data too
            if (!outputBuffer.isEmpty()) {
                // Check high watermark limit (like C version)
                // Use default if configured value is <= 0 (like C version)
                int highWatermark = config.getWriteHighWatermark() > 0 ?
                        config.getWriteHighWatermark() : MAX_OUTPUT_BUFFER_SIZE;
                if (outputBufferSize > highWatermark) {
                    logger.error("Output buffer exceeds high watermark: {} > {}", outputBufferSize, highWatermark);
                    return 0; // Return 0 to indicate high watermark exceeded (not an error)
                }

                // Buffer the data
                ByteBuffer copy = ByteBuffer.allocate(originalRemaining);
                copy.put(buffer);
                copy.flip();

                outputBuffer.offer(copy);
                outputBufferSize += originalRemaining;

                // Register OP_WRITE to get notified when socket becomes writable
                registerWriteInterest();

                logger.debug("Buffered {} bytes (total buffered: {})", originalRemaining, outputBufferSize);

                return originalRemaining; // Data accepted (buffered)
            }

            // Try to send directly
            int sent;
            if (protocol == TglogConfig.Protocol.TCP) {
                sent = tcpChannel.write(buffer);
            } else {
                sent = udpChannel.write(buffer);
            }

            if (sent > 0) {
                lastActiveTime.set(System.currentTimeMillis());
            }

            // Handle partial write - buffer remaining data
            if (buffer.hasRemaining()) {
                int remaining = buffer.remaining();

                logger.debug("Partial write: sent {} bytes, buffering {} bytes", sent, remaining);

                // Copy remaining data to output buffer
                ByteBuffer copy = ByteBuffer.allocate(remaining);
                copy.put(buffer);
                copy.flip();
                outputBuffer.offer(copy);
                outputBufferSize += remaining;

                // Register OP_WRITE to get notified when socket becomes writable
                registerWriteInterest();

                return originalRemaining; // All data accepted (some buffered)
            }

            return sent; // All data sent directly
        } catch (IOException e) {
            logger.error("Failed to send data to {}", endpoint, e);
            state = State.ERROR;
            throw e;
        }
    }

    /**
     * Send multiple buffers (gathering write for TCP).
     * Handles partial writes by buffering unsent data (like C's bufferevent).
     *
     * @param buffers Array of buffers to send
     * @return Number of bytes that were accepted (may be buffered), 0 if high watermark exceeded, or -1 on error
     */
    public long send(ByteBuffer[] buffers) throws IOException {
        if (state != State.CONNECTED) {
            return -1;
        }

        // Calculate total size
        long totalSize = 0;
        for (ByteBuffer buf : buffers) {
            totalSize += buf.remaining();
        }

        try {
            // First, try to flush any buffered data
            if (!outputBuffer.isEmpty()) {
                flushOutputBuffer();
            }

            // If output buffer is not empty after flush, buffer all data
            if (!outputBuffer.isEmpty()) {
                // Check high watermark limit (like C version)
                // Use default if configured value is <= 0 (like C version)
                int highWatermark = config.getWriteHighWatermark() > 0 ?
                        config.getWriteHighWatermark() : MAX_OUTPUT_BUFFER_SIZE;
                if (outputBufferSize > highWatermark) {
                    logger.error("Output buffer exceeds high watermark: {} > {}", outputBufferSize, highWatermark);
                    return 0; // Return 0 to indicate high watermark exceeded (not an error)
                }

                // Buffer all data
                for (ByteBuffer buf : buffers) {
                    if (buf.hasRemaining()) {
                        ByteBuffer copy = ByteBuffer.allocate(buf.remaining());
                        copy.put(buf);
                        copy.flip();
                        outputBuffer.offer(copy);
                        outputBufferSize += copy.remaining();
                    }
                }

                // Register OP_WRITE to get notified when socket becomes writable
                registerWriteInterest();

                logger.debug("Buffered {} bytes (total buffered: {})", totalSize, outputBufferSize);

                return totalSize; // All data accepted (buffered)
            }

            // Try to send directly
            long sent;
            if (protocol == TglogConfig.Protocol.TCP) {
                sent = tcpChannel.write(buffers);
            } else {
                // UDP doesn't support gathering write, send first buffer only
                sent = udpChannel.write(buffers[0]);
            }

            if (sent > 0) {
                lastActiveTime.set(System.currentTimeMillis());
            }

            // Check if all data was sent
            boolean hasRemaining = false;
            for (ByteBuffer buf : buffers) {
                if (buf.hasRemaining()) {
                    hasRemaining = true;
                    break;
                }
            }

            // Handle partial write - buffer remaining data
            if (hasRemaining) {
                long remaining = 0;
                for (ByteBuffer buf : buffers) {
                    if (buf.hasRemaining()) {
                        int bufRemaining = buf.remaining();
                        remaining += bufRemaining;

                        // Copy remaining data to output buffer
                        ByteBuffer copy = ByteBuffer.allocate(bufRemaining);
                        copy.put(buf);
                        copy.flip();
                        outputBuffer.offer(copy);
                        outputBufferSize += bufRemaining;
                    }
                }

                // Register OP_WRITE to get notified when socket becomes writable
                registerWriteInterest();

                logger.debug("Partial write: sent {} bytes, buffering {} bytes", sent, remaining);

                return totalSize; // All data accepted (some buffered)
            }

            return sent; // All data sent directly
        } catch (IOException e) {
            logger.error("Failed to send data to {}", endpoint, e);
            state = State.ERROR;
            throw e;
        }
    }

    /**
     * Flush output buffer (try to send buffered data).
     * Called automatically when sending new data or when connection becomes writable.
     */
    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings(
        value = "RV_RETURN_VALUE_IGNORED",
        justification = "poll() return value intentionally ignored - we already checked with peek() and only poll to remove")
    private void flushOutputBuffer() throws IOException {
        while (!outputBuffer.isEmpty()) {
            ByteBuffer buf = outputBuffer.peek();
            if (buf == null || !buf.hasRemaining()) {
                outputBuffer.poll();
                continue;
            }

            int sent;
            if (protocol == TglogConfig.Protocol.TCP) {
                sent = tcpChannel.write(buf);
            } else {
                sent = udpChannel.write(buf);
            }

            if (sent > 0) {
                lastActiveTime.set(System.currentTimeMillis());
                outputBufferSize -= sent;
            }

            // If buffer not fully sent, stop (socket buffer full)
            if (buf.hasRemaining()) {
                break;
            }

            // Buffer fully sent, remove from queue
            outputBuffer.poll();
        }
    }

    /**
     * Check if connection has buffered data waiting to be sent.
     */
    public boolean hasBufferedData() {
        return !outputBuffer.isEmpty();
    }

    /**
     * Get size of buffered data.
     */
    public int getBufferedDataSize() {
        return outputBufferSize;
    }

    /**
     * Try to flush buffered data (called when connection is writable).
     * This is called by the event loop when the connection becomes writable.
     */
    public void tryFlush() {
        if (!outputBuffer.isEmpty() && state == State.CONNECTED) {
            try {
                flushOutputBuffer();

                // If all data sent, unregister OP_WRITE to avoid busy-wait
                if (outputBuffer.isEmpty()) {
                    unregisterWriteInterest();
                }
            } catch (IOException e) {
                logger.error("{}", "Failed to flush output buffer", e);
                state = State.ERROR;
            }
        }
    }

    /**
     * Get pending data from output buffer (for rescue when closing).
     * Returns a list of ByteBuffers containing unsent data.
     * The buffers are removed from the output buffer.
     */
    public Queue<ByteBuffer> getPendingData() {
        if (outputBuffer.isEmpty()) {
            return null;
        }

        // Return a copy of the output buffer
        Queue<ByteBuffer> pending = new LinkedList<>(outputBuffer);
        return pending;
    }

    /**
     * Remove pending data from output buffer.
     * This is called when data is being rescued to another connection.
     */
    public Queue<ByteBuffer> removePendingData() {
        if (outputBuffer.isEmpty()) {
            return null;
        }

        Queue<ByteBuffer> pending = new LinkedList<>(outputBuffer);
        outputBuffer.clear();
        outputBufferSize = 0;
        return pending;
    }

    /**
     * Write pending data from another connection (for rescue).
     * This is used to transfer unsent data from a closing connection.
     * Note: We don't check high watermark here because this is rescue data (like C version).
     */
    public boolean writePendingData(Queue<ByteBuffer> pendingData) {
        if (pendingData == null || pendingData.isEmpty()) {
            return true;
        }

        // Calculate total size
        int totalSize = 0;
        for (ByteBuffer buf : pendingData) {
            totalSize += buf.remaining();
        }

        // Add all pending buffers to output buffer
        // Note: We don't check high watermark here because this is rescue data (like C version)
        for (ByteBuffer buf : pendingData) {
            if (buf.hasRemaining()) {
                outputBuffer.offer(buf);
                outputBufferSize += buf.remaining();
            }
        }

        // Register OP_WRITE to flush the rescued data
        registerWriteInterest();

        logger.info("Rescued {} bytes of pending data from another connection", totalSize);

        return true;
    }

    /**
     * Close the connection.
     */
    public void close() {
        state = State.DISCONNECTED;

        try {
            if (selectionKey != null) {
                selectionKey.cancel();
                selectionKey = null;
            }

            if (tcpChannel != null) {
                tcpChannel.close();
                tcpChannel = null;
            }

            if (udpChannel != null) {
                udpChannel.close();
                udpChannel = null;
            }

            // Clear output buffer to prevent memory leak
            outputBuffer.clear();
            outputBufferSize = 0;

            logger.info("Closed connection to {}", endpoint);
        } catch (IOException e) {
            logger.error("Error closing connection to {}", endpoint, e);
        }
    }

    /**
     * Calculate connection lifetime with jitter.
     * Following C version's logic: max_jitter is clamped to [20, 30] seconds,
     * then a random jitter in [0, max_jitter) is added to the base lifetime.
     * This prevents all connections from expiring at the same time.
     *
     * @return lifetime in milliseconds with jitter added
     */
    private long calculateLifetimeWithJitter() {
        int maxLifetimeSec = config.getMaxConnectionLifetime();

        // Calculate max jitter: max_lifetime / 10, clamped to [20, 30] seconds
        int maxJitterSec = maxLifetimeSec / 10;
        if (maxJitterSec < 20) {
            maxJitterSec = 20;
        } else if (maxJitterSec > 30) {
            maxJitterSec = 30;
        }

        // Random jitter in [0, maxJitterSec) seconds
        int jitterSec = random.nextInt(maxJitterSec);

        // Total lifetime = base + jitter
        long lifetimeMs = (maxLifetimeSec + jitterSec) * 1000L;

        logger.debug("Calculated lifetime: base={}s, maxJitter={}s, jitter={}s, total={}ms",
                maxLifetimeSec, maxJitterSec, jitterSec, lifetimeMs);

        return lifetimeMs;
    }

    /**
     * Check if connection is expired based on lifetime with jitter.
     *
     * @return true if connection should be closed
     */
    public boolean isExpired() {
        if (state != State.CONNECTED) {
            return false;
        }
        if (lifetimeWithJitterMs == 0) {
            // Fallback: if jitter not calculated (shouldn't happen), use base lifetime
            long lifetime = System.currentTimeMillis() - connectTime.get();
            return lifetime > config.getMaxConnectionLifetime() * 1000L;
        }
        long lifetime = System.currentTimeMillis() - connectTime.get();
        return lifetime > lifetimeWithJitterMs;
    }

    /**
     * Check if connection is writable.
     *
     * @return true if ready to send data
     */
    public boolean isWritable() {
        if (state != State.CONNECTED) {
            return false;
        }

        // UDP is always writable when connected (unless buffer exceeds high watermark)
        if (protocol == TglogConfig.Protocol.UDP) {
            return outputBufferSize <= config.getWriteHighWatermark();
        }

        // TCP needs to check selection key
        return selectionKey != null &&
                selectionKey.isValid() &&
                selectionKey.isWritable();
    }

    // Getters

    public State getState() {
        return state;
    }

    public boolean isConnected() {
        return state == State.CONNECTED;
    }

    public InetSocketAddress getEndpoint() {
        return endpoint;
    }

    public int getRetryCount() {
        return retryCount;
    }

    /**
     * Check if enough time has passed for next connection retry.
     */
    public boolean shouldRetryConnection() {
        if (lastRetryAttemptTime == 0) {
            return true;
        }
        return connectionBackoff.shouldRetry(lastRetryAttemptTime, retryCount);
    }

    /**
     * Mark that a connection retry was attempted.
     */
    public void markRetryAttempt() {
        lastRetryAttemptTime = System.currentTimeMillis();
    }

    public long getConnectTime() {
        return connectTime.get();
    }

    public long getLastActiveTime() {
        return lastActiveTime.get();
    }

    /**
     * Register interest in OP_WRITE events (when we have data to send).
     */
    private void registerWriteInterest() {
        if (selectionKey != null && selectionKey.isValid()) {
            int ops = selectionKey.interestOps();
            if ((ops & SelectionKey.OP_WRITE) == 0) {
                selectionKey.interestOps(ops | SelectionKey.OP_WRITE);
            }
        }
    }

    /**
     * Unregister interest in OP_WRITE events (when buffer is empty).
     */
    private void unregisterWriteInterest() {
        if (selectionKey != null && selectionKey.isValid()) {
            int ops = selectionKey.interestOps();
            if ((ops & SelectionKey.OP_WRITE) != 0) {
                selectionKey.interestOps(ops & ~SelectionKey.OP_WRITE);
            }
        }
    }

    public SelectionKey getSelectionKey() {
        return selectionKey;
    }

    @Override
    public String toString() {
        return String.format("TglogConnection{endpoint=%s, protocol=%s, state=%s, retries=%d}",
                endpoint, protocol, state, retryCount);
    }
}
