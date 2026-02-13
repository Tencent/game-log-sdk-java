package com.tencent.tglog;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Batch buffer for accumulating log messages before sending.
 *
 * Features:
 * - Accumulates messages until batch is full or timeout
 * - Supports both TCP (gathering write) and UDP (single message)
 * - Tracks message callbacks for result notification
 */
class BatchBuffer {

    private final TglogConfig config;
    private final int maxMessages;
    private final int maxSize;
    private final int maxDelayMs;

    // Message storage
    private final List<LogMessage> messages = new ArrayList<>();
    private int currentSize = 0;
    private long createTime = 0;

    // Retry tracking
    private int retryCount = 0;
    private long retryStartTime = 0;
    private long lastRetryAttemptTime = 0;

    // ByteBuffers for sending
    private ByteBuffer[] sendBuffers = null;
    private int sendBufferCount = 0;

    public BatchBuffer(TglogConfig config) {
        this.config = config;
        this.maxMessages = config.getBatchingMaxMessages();
        this.maxSize = config.getBatchingMaxSize();
        this.maxDelayMs = config.getBatchingMaxDelayMs();
    }

    /**
     * Add a message to the batch.
     *
     * @param message Message to add
     * @return true if message was added, false if batch is full
     */
    public boolean add(LogMessage message) {
        if (messages.isEmpty()) {
            createTime = System.currentTimeMillis();
        }

        int messageSize = message.getPayloadSize();

        // Check if adding this message would exceed limits
        if (!messages.isEmpty()) {
            // Check message count limit
            if (messages.size() >= maxMessages) {
                return false;
            }

            // Check configured batch size limit
            if (currentSize + messageSize > maxSize) {
                return false;
            }

            // Check protocol hard limit
            int maxRequestSize = config.getMaxRequestSize();
            if (currentSize + messageSize > maxRequestSize) {
                return false;
            }
        } else {
            // First message: check if it exceeds protocol limit by itself
            int maxRequestSize = config.getMaxRequestSize();
            if (messageSize > maxRequestSize) {
                // This should have been caught in TglogClient.send(), but double-check
                return false;
            }
        }

        messages.add(message);
        currentSize += messageSize;

        return true;
    }

    /**
     * Check if batch should be flushed.
     *
     * @return true if batch is ready to send
     */
    public boolean shouldFlush() {
        if (messages.isEmpty()) {
            return false;
        }

        // Check message count
        if (messages.size() >= maxMessages) {
            return true;
        }

        // Check size
        if (currentSize >= maxSize) {
            return true;
        }

        // Check timeout
        long age = System.currentTimeMillis() - createTime;
        return age >= maxDelayMs;
    }

    /**
     * Prepare buffers for sending.
     * For TCP: creates multiple buffers for gathering write
     * For UDP: creates single buffer with all messages
     *
     * Buffers are cached after first preparation and reused on retry
     * to avoid repeated allocation and string-to-bytes conversion.
     *
     * @return Array of ByteBuffers ready to send
     */
    public ByteBuffer[] prepareForSend() {
        if (messages.isEmpty()) {
            return new ByteBuffer[0];
        }

        // If buffers already prepared (for retry), rewind and reuse them
        if (sendBuffers != null && sendBufferCount > 0) {
            for (int i = 0; i < sendBufferCount; i++) {
                sendBuffers[i].rewind();
            }
            return sendBuffers;
        }

        // First time preparation
        if (config.getProtocol() == TglogConfig.Protocol.TCP) {
            return prepareForTcp();
        } else {
            return prepareForUdp();
        }
    }

    /**
     * Prepare buffers for TCP (one buffer per message for gathering write).
     */
    private ByteBuffer[] prepareForTcp() {
        // Verify batch size doesn't exceed TCP limit
        if (currentSize > TglogConfig.MAX_TCP_REQ_SIZE) {
            throw new IllegalStateException(
                    "Batch size " + currentSize + " exceeds TCP limit " +
                    TglogConfig.MAX_TCP_REQ_SIZE + ", should not happen");
        }

        sendBuffers = new ByteBuffer[messages.size()];
        sendBufferCount = 0;

        for (LogMessage message : messages) {
            byte[] data = message.getPayload().getBytes(StandardCharsets.UTF_8);
            // Use heap buffer instead of direct buffer to avoid off-heap memory leak
            ByteBuffer buffer = ByteBuffer.allocate(data.length);
            buffer.put(data);
            buffer.flip();
            sendBuffers[sendBufferCount++] = buffer;
        }

        return sendBuffers;
    }

    /**
     * Prepare buffer for UDP (single buffer with all messages).
     */
    private ByteBuffer[] prepareForUdp() {
        // Verify batch size doesn't exceed UDP limit
        if (currentSize > TglogConfig.MAX_UDP_REQ_SIZE) {
            throw new IllegalStateException(
                    "Batch size " + currentSize + " exceeds UDP limit " +
                    TglogConfig.MAX_UDP_REQ_SIZE + ", should not happen");
        }

        // Use heap buffer instead of direct buffer to avoid off-heap memory leak
        ByteBuffer buffer = ByteBuffer.allocate(currentSize);

        for (LogMessage message : messages) {
            byte[] data = message.getPayload().getBytes(StandardCharsets.UTF_8);
            buffer.put(data);
        }

        buffer.flip();
        sendBuffers = new ByteBuffer[]{buffer};
        sendBufferCount = 1;

        return sendBuffers;
    }

    /**
     * Calculate how many complete messages were sent based on bytes sent.
     * This is used for TCP partial sends.
     *
     * @param bytesSent Number of bytes successfully sent
     * @return Number of complete messages sent
     */
    public int calculateSentMessages(long bytesSent) {
        if (config.getProtocol() == TglogConfig.Protocol.UDP) {
            // UDP sends all or nothing
            return bytesSent > 0 ? messages.size() : 0;
        }

        // TCP: count complete messages
        long counted = 0;
        int sentCount = 0;

        for (LogMessage message : messages) {
            int messageSize = message.getPayloadSize();
            if (counted + messageSize <= bytesSent) {
                counted += messageSize;
                sentCount++;
            } else {
                break;
            }
        }

        return sentCount;
    }

    /**
     * Notify callbacks for sent messages and remove them from batch.
     *
     * @param sentCount Number of messages successfully sent
     */
    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings(
        value = "DE_MIGHT_IGNORE",
        justification = "Intentionally ignoring callback exceptions to prevent one bad callback from affecting others")
    public void notifyAndRemove(int sentCount) {
        if (sentCount <= 0 || sentCount > messages.size()) {
            return;
        }

        // Notify success callbacks
        for (int i = 0; i < sentCount; i++) {
            LogMessage message = messages.get(i);
            if (message.getCallback() != null) {
                try {
                    message.getCallback().onCompletion(message.getPayload(), null);
                } catch (Exception e) {
                    // Ignore callback exceptions
                }
            }
            currentSize -= message.getPayloadSize();
        }

        // Remove sent messages
        messages.subList(0, sentCount).clear();

        // Trim sendBuffers to match remaining messages (avoid stale cache on retry)
        if (sendBuffers != null && sendBufferCount > 0) {
            int remaining = sendBufferCount - sentCount;
            if (remaining > 0) {
                System.arraycopy(sendBuffers, sentCount, sendBuffers, 0, remaining);
                for (int i = remaining; i < sendBufferCount; i++) {
                    sendBuffers[i] = null;
                }
                sendBufferCount = remaining;
            } else {
                sendBuffers = null;
                sendBufferCount = 0;
            }
        }

        // Reset create time if batch is now empty
        if (messages.isEmpty()) {
            createTime = 0;
            currentSize = 0;
        }
    }

    /**
     * Notify all messages as failed and clear batch.
     *
     * @param errorCode Error code
     */
    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings(
        value = "DE_MIGHT_IGNORE",
        justification = "Intentionally ignoring callback exceptions to prevent one bad callback from affecting others")
    public void notifyAllFailed(TglogErrorCode errorCode) {
        TglogException exception = new TglogException(errorCode);
        for (LogMessage message : messages) {
            if (message.getCallback() != null) {
                try {
                    message.getCallback().onCompletion(message.getPayload(), exception);
                } catch (Exception e) {
                    // Ignore callback exceptions
                }
            }
        }
        clear();
    }

    /**
     * Clear the batch.
     */
    public void clear() {
        messages.clear();
        currentSize = 0;
        createTime = 0;
        retryCount = 0;
        retryStartTime = 0;
        lastRetryAttemptTime = 0;
        sendBuffers = null;
        sendBufferCount = 0;
    }

    /**
     * Get messages in this batch (for retry logic).
     */
    public List<LogMessage> getMessages() {
        return new ArrayList<>(messages);
    }

    /**
     * Check if batch is empty.
     */
    public boolean isEmpty() {
        return messages.isEmpty();
    }

    /**
     * Get number of messages in batch.
     */
    public int size() {
        return messages.size();
    }

    /**
     * Get total size of batch in bytes.
     */
    public int getCurrentSize() {
        return currentSize;
    }

    /**
     * Get age of batch in milliseconds.
     */
    public long getAge() {
        if (createTime == 0) {
            return 0;
        }
        return System.currentTimeMillis() - createTime;
    }

    /**
     * Check if batch send has timed out.
     * Like C version: tglog_worker_batch_send_timeout()
     *
     * @param sendTimeoutMs Send timeout in milliseconds
     * @return true if batch has exceeded send timeout
     */
    public boolean isSendTimeout(int sendTimeoutMs) {
        if (messages.isEmpty() || sendTimeoutMs <= 0 || createTime == 0) {
            return false;
        }
        return getAge() >= sendTimeoutMs;
    }

    /**
     * Mark batch as being retried.
     */
    public void markForRetry() {
        if (retryStartTime == 0) {
            retryStartTime = System.currentTimeMillis();
        }
        lastRetryAttemptTime = System.currentTimeMillis();
        retryCount++;
    }

    /**
     * Check if batch is being retried.
     */
    public boolean isRetrying() {
        return retryStartTime > 0;
    }

    /**
     * Get retry count.
     */
    public int getRetryCount() {
        return retryCount;
    }

    /**
     * Get time since retry started.
     */
    public long getRetryDuration() {
        if (retryStartTime == 0) {
            return 0;
        }
        return System.currentTimeMillis() - retryStartTime;
    }

    /**
     * Check if batch has exceeded retry deadline.
     */
    public boolean isRetryTimeout(int timeoutMs) {
        if (retryStartTime == 0) {
            return false;
        }
        return getRetryDuration() > timeoutMs;
    }

    /**
     * Check if enough time has passed for next retry attempt.
     */
    public boolean shouldRetryNow(Backoff backoff) {
        if (lastRetryAttemptTime == 0) {
            return true;
        }
        return backoff.shouldRetry(lastRetryAttemptTime, retryCount);
    }

    /**
     * Get last retry attempt time.
     */
    public long getLastRetryAttemptTime() {
        return lastRetryAttemptTime;
    }

    @Override
    public String toString() {
        return String.format("BatchBuffer{messages=%d, size=%d bytes, age=%d ms, retries=%d}",
                messages.size(), currentSize, getAge(), retryCount);
    }
}
