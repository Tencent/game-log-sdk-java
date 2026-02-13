package com.tencent.tglog;

/**
 * Log message wrapper.
 * Represents a single log message with its metadata.
 */
class LogMessage {

    private final String payload;
    private final TglogCallback callback;
    private final long createTime;
    private int retryCount;
    private final byte[] payloadBytes; // Cache UTF-8 bytes to avoid repeated encoding

    public LogMessage(String payload, TglogCallback callback) {
        this.payload = payload;
        this.callback = callback;
        this.createTime = System.currentTimeMillis();
        this.retryCount = 0;
        // Cache UTF-8 bytes at construction time, reused by BatchBuffer
        this.payloadBytes = payload.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    public String getPayload() {
        return payload;
    }

    public TglogCallback getCallback() {
        return callback;
    }

    public long getCreateTime() {
        return createTime;
    }

    public int getRetryCount() {
        return retryCount;
    }

    public void incrementRetry() {
        this.retryCount++;
    }

    public boolean isTimeout(int timeoutMs) {
        return System.currentTimeMillis() - createTime > timeoutMs;
    }

    public int getPayloadSize() {
        return payloadBytes.length;
    }

    public byte[] getPayloadBytes() {
        return payloadBytes;
    }
}
