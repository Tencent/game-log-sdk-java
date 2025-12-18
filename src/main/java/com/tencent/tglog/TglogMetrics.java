package com.tencent.tglog;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * Performance metrics for TGLog client.
 *
 * This class tracks various performance counters and gauges using
 * lock-free atomic operations for thread-safety.
 */
public class TglogMetrics {

    // ========== Counter Metrics (累计计数) ==========
    private final LongAdder requestMsgCount = new LongAdder();      // 请求发送的消息数
    private final LongAdder enqueuedMsgCount = new LongAdder();     // 成功入队的消息数
    private final LongAdder dequeuedMsgCount = new LongAdder();     // 成功出队的消息数
    private final LongAdder succeededMsgCount = new LongAdder();    // 成功发送的消息数
    private final LongAdder failedMsgCount = new LongAdder();       // 失败的消息数
    private final LongAdder timeoutMsgCount = new LongAdder();      // 超时的消息数
    private final LongAdder retryMsgCount = new LongAdder();        // 重试次数
    private final LongAdder droppedMsgCount = new LongAdder();      // 丢弃的消息数

    private final LongAdder openedConnCount = new LongAdder();      // 打开的连接数
    private final LongAdder closedConnCount = new LongAdder();      // 关闭的连接数
    private final LongAdder errorConnCount = new LongAdder();       // 错误的连接数

    private final LongAdder addEndpointCount = new LongAdder();     // 添加端点次数
    private final LongAdder removeEndpointCount = new LongAdder();  // 移除端点次数

    // Additional metrics (like C version)
    private final LongAdder rescuedMsgBytes = new LongAdder();      // 抢救的数据字节数

    // ========== Gauge Metrics (瞬时状态) ==========
    private final AtomicLong msgQueueSize = new AtomicLong(0);      // 当前队列大小
    private final AtomicLong msgQueueCapacity = new AtomicLong(0);  // 队列容量
    private final AtomicLong endpointCount = new AtomicLong(0);     // 当前端点数量
    private final AtomicLong activeConnCount = new AtomicLong(0);   // 活跃连接数

    // ========== Counter Increments ==========

    public void incrementRequestMsg() {
        requestMsgCount.increment();
    }

    public void incrementEnqueuedMsg() {
        enqueuedMsgCount.increment();
    }

    public void incrementDequeuedMsg() {
        dequeuedMsgCount.increment();
    }

    public void incrementSucceededMsg() {
        succeededMsgCount.increment();
    }

    public void incrementFailedMsg() {
        failedMsgCount.increment();
    }

    public void incrementTimeoutMsg() {
        timeoutMsgCount.increment();
    }

    public void incrementRetryMsg() {
        retryMsgCount.increment();
    }

    public void incrementDroppedMsg() {
        droppedMsgCount.increment();
    }

    public void incrementOpenedConn() {
        openedConnCount.increment();
    }

    public void incrementClosedConn() {
        closedConnCount.increment();
    }

    public void incrementErrorConn() {
        errorConnCount.increment();
    }

    public void incrementAddEndpoint() {
        addEndpointCount.increment();
    }

    public void incrementRemoveEndpoint() {
        removeEndpointCount.increment();
    }

    public void addRescuedMsgBytes(long bytes) {
        rescuedMsgBytes.add(bytes);
    }

    // ========== Gauge Updates ==========

    public void setMsgQueueSize(long size) {
        msgQueueSize.set(size);
    }

    public void setMsgQueueCapacity(long capacity) {
        msgQueueCapacity.set(capacity);
    }

    public void setEndpointCount(long count) {
        endpointCount.set(count);
    }

    public void setActiveConnCount(long count) {
        activeConnCount.set(count);
    }

    // ========== Getters ==========

    public long getRequestMsgCount() {
        return requestMsgCount.sum();
    }

    public long getEnqueuedMsgCount() {
        return enqueuedMsgCount.sum();
    }

    public long getDequeuedMsgCount() {
        return dequeuedMsgCount.sum();
    }

    public long getSucceededMsgCount() {
        return succeededMsgCount.sum();
    }

    public long getFailedMsgCount() {
        return failedMsgCount.sum();
    }

    public long getTimeoutMsgCount() {
        return timeoutMsgCount.sum();
    }

    public long getRetryMsgCount() {
        return retryMsgCount.sum();
    }

    public long getDroppedMsgCount() {
        return droppedMsgCount.sum();
    }

    public long getOpenedConnCount() {
        return openedConnCount.sum();
    }

    public long getClosedConnCount() {
        return closedConnCount.sum();
    }

    public long getErrorConnCount() {
        return errorConnCount.sum();
    }

    public long getAddEndpointCount() {
        return addEndpointCount.sum();
    }

    public long getRemoveEndpointCount() {
        return removeEndpointCount.sum();
    }

    public long getRescuedMsgBytes() {
        return rescuedMsgBytes.sum();
    }

    public long getMsgQueueSize() {
        return msgQueueSize.get();
    }

    public long getMsgQueueCapacity() {
        return msgQueueCapacity.get();
    }

    public long getEndpointCount() {
        return endpointCount.get();
    }

    public long getActiveConnCount() {
        return activeConnCount.get();
    }

    // ========== Derived Metrics ==========

    /**
     * Calculate success rate.
     *
     * @return Success rate (0.0 to 1.0)
     */
    public double getSuccessRate() {
        long total = getSucceededMsgCount() + getFailedMsgCount();
        if (total == 0) {
            return 0.0;
        }
        return (double) getSucceededMsgCount() / total;
    }

    /**
     * Calculate queue utilization.
     *
     * @return Queue utilization (0.0 to 1.0)
     */
    public double getQueueUtilization() {
        long capacity = getMsgQueueCapacity();
        if (capacity == 0) {
            return 0.0;
        }
        return (double) getMsgQueueSize() / capacity;
    }

    // ========== JSON Output ==========

    /**
     * Export metrics as JSON string.
     *
     * @return JSON representation of metrics
     */
    public String toJson() {
        StringBuilder sb = new StringBuilder();
        sb.append("{\n");
        sb.append("  \"counters\": {\n");
        sb.append("    \"request_msg_count\": ").append(getRequestMsgCount()).append(",\n");
        sb.append("    \"enqueued_msg_count\": ").append(getEnqueuedMsgCount()).append(",\n");
        sb.append("    \"dequeued_msg_count\": ").append(getDequeuedMsgCount()).append(",\n");
        sb.append("    \"succeeded_msg_count\": ").append(getSucceededMsgCount()).append(",\n");
        sb.append("    \"failed_msg_count\": ").append(getFailedMsgCount()).append(",\n");
        sb.append("    \"timeout_msg_count\": ").append(getTimeoutMsgCount()).append(",\n");
        sb.append("    \"retry_msg_count\": ").append(getRetryMsgCount()).append(",\n");
        sb.append("    \"dropped_msg_count\": ").append(getDroppedMsgCount()).append(",\n");
        sb.append("    \"opened_conn_count\": ").append(getOpenedConnCount()).append(",\n");
        sb.append("    \"closed_conn_count\": ").append(getClosedConnCount()).append(",\n");
        sb.append("    \"error_conn_count\": ").append(getErrorConnCount()).append(",\n");
        sb.append("    \"add_endpoint_count\": ").append(getAddEndpointCount()).append(",\n");
        sb.append("    \"remove_endpoint_count\": ").append(getRemoveEndpointCount()).append("\n");
        sb.append("  },\n");
        sb.append("  \"gauges\": {\n");
        sb.append("    \"msg_queue_size\": ").append(getMsgQueueSize()).append(",\n");
        sb.append("    \"msg_queue_capacity\": ").append(getMsgQueueCapacity()).append(",\n");
        sb.append("    \"endpoint_count\": ").append(getEndpointCount()).append(",\n");
        sb.append("    \"active_conn_count\": ").append(getActiveConnCount()).append("\n");
        sb.append("  },\n");
        sb.append("  \"derived\": {\n");
        sb.append("    \"success_rate\": ").append(String.format("%.4f", getSuccessRate())).append(",\n");
        sb.append("    \"queue_utilization\": ").append(String.format("%.4f", getQueueUtilization())).append("\n");
        sb.append("  }\n");
        sb.append("}");
        return sb.toString();
    }

    @Override
    public String toString() {
        return String.format("TglogMetrics{requests=%d, succeeded=%d, failed=%d, successRate=%.2f%%, queueSize=%d/%d}",
                getRequestMsgCount(), getSucceededMsgCount(), getFailedMsgCount(),
                getSuccessRate() * 100, getMsgQueueSize(), getMsgQueueCapacity());
    }
}
