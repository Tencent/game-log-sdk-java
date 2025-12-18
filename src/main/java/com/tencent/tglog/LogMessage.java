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
    private final int payloadSize; // Cache payload size to avoid repeated conversion

    public LogMessage(String payload, TglogCallback callback) {
        this.payload = payload;
        this.callback = callback;
        this.createTime = System.currentTimeMillis();
        this.retryCount = 0;
        // Cache payload size at construction time
        this.payloadSize = payload.getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
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
        return payloadSize;
    }
}
