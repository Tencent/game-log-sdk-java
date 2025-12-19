package com.tencent.tglog;

import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.CollectorRegistry;

/**
 * Performance metrics for TGLog client using Prometheus Java Client.
 *
 * This class tracks various performance counters and gauges using Prometheus metrics.
 * The metrics are registered with a CollectorRegistry which can be exposed by the
 * application integrating this SDK.
 *
 * Design principles:
 * - SDK only collects metrics, exposure is decided by the integrating application
 * - Uses Prometheus standard metric types (Counter, Gauge)
 * - Thread-safe by design (Prometheus client handles concurrency)
 * - Backward compatible API for existing code
 */
public class TglogMetrics {

    private final CollectorRegistry registry;

    // ========== Counter Metrics (累计计数) ==========
    private final Counter requestMsgCount;
    private final Counter enqueuedMsgCount;
    private final Counter dequeuedMsgCount;
    private final Counter succeededMsgCount;
    private final Counter failedMsgCount;
    private final Counter timeoutMsgCount;
    private final Counter retryMsgCount;
    private final Counter droppedMsgCount;

    private final Counter openedConnCount;
    private final Counter closedConnCount;
    private final Counter errorConnCount;

    private final Counter addEndpointCount;
    private final Counter removeEndpointCount;

    private final Counter rescuedMsgBytes;

    // ========== Gauge Metrics (瞬时状态) ==========
    private final Gauge msgQueueSize;
    private final Gauge msgQueueCapacity;
    private final Gauge endpointCount;
    private final Gauge activeConnCount;

    /**
     * Create TglogMetrics with default CollectorRegistry.
     */
    public TglogMetrics() {
        this(CollectorRegistry.defaultRegistry);
    }

    /**
     * Create TglogMetrics with custom CollectorRegistry.
     * This allows applications to use separate registries for different purposes.
     *
     * @param registry The CollectorRegistry to register metrics with
     */
    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings(
        value = "EI_EXPOSE_REP2",
        justification = "Registry must be shared with application for Prometheus integration")
    public TglogMetrics(CollectorRegistry registry) {
        this.registry = registry;

        // Initialize Counter metrics
        this.requestMsgCount = Counter.build()
                .name("tglog_request_messages_total")
                .help("Total number of messages requested to send")
                .register(registry);

        this.enqueuedMsgCount = Counter.build()
                .name("tglog_enqueued_messages_total")
                .help("Total number of messages successfully enqueued")
                .register(registry);

        this.dequeuedMsgCount = Counter.build()
                .name("tglog_dequeued_messages_total")
                .help("Total number of messages successfully dequeued")
                .register(registry);

        this.succeededMsgCount = Counter.build()
                .name("tglog_succeeded_messages_total")
                .help("Total number of messages successfully sent")
                .register(registry);

        this.failedMsgCount = Counter.build()
                .name("tglog_failed_messages_total")
                .help("Total number of messages failed to send")
                .register(registry);

        this.timeoutMsgCount = Counter.build()
                .name("tglog_timeout_messages_total")
                .help("Total number of messages timed out")
                .register(registry);

        this.retryMsgCount = Counter.build()
                .name("tglog_retry_messages_total")
                .help("Total number of message retries")
                .register(registry);

        this.droppedMsgCount = Counter.build()
                .name("tglog_dropped_messages_total")
                .help("Total number of messages dropped (queue full)")
                .register(registry);

        this.openedConnCount = Counter.build()
                .name("tglog_opened_connections_total")
                .help("Total number of connections opened")
                .register(registry);

        this.closedConnCount = Counter.build()
                .name("tglog_closed_connections_total")
                .help("Total number of connections closed")
                .register(registry);

        this.errorConnCount = Counter.build()
                .name("tglog_error_connections_total")
                .help("Total number of connection errors")
                .register(registry);

        this.addEndpointCount = Counter.build()
                .name("tglog_add_endpoints_total")
                .help("Total number of endpoints added")
                .register(registry);

        this.removeEndpointCount = Counter.build()
                .name("tglog_remove_endpoints_total")
                .help("Total number of endpoints removed")
                .register(registry);

        this.rescuedMsgBytes = Counter.build()
                .name("tglog_rescued_message_bytes_total")
                .help("Total bytes of rescued messages")
                .register(registry);

        // Initialize Gauge metrics
        this.msgQueueSize = Gauge.build()
                .name("tglog_message_queue_size")
                .help("Current size of the message queue")
                .register(registry);

        this.msgQueueCapacity = Gauge.build()
                .name("tglog_message_queue_capacity")
                .help("Capacity of the message queue")
                .register(registry);

        this.endpointCount = Gauge.build()
                .name("tglog_endpoints")
                .help("Current number of endpoints")
                .register(registry);

        this.activeConnCount = Gauge.build()
                .name("tglog_active_connections")
                .help("Current number of active connections")
                .register(registry);
    }

    // ========== Counter Increments ==========

    public void incrementRequestMsg() {
        requestMsgCount.inc();
    }

    public void incrementEnqueuedMsg() {
        enqueuedMsgCount.inc();
    }

    public void incrementDequeuedMsg() {
        dequeuedMsgCount.inc();
    }

    public void incrementSucceededMsg() {
        succeededMsgCount.inc();
    }

    public void incrementFailedMsg() {
        failedMsgCount.inc();
    }

    public void incrementTimeoutMsg() {
        timeoutMsgCount.inc();
    }

    public void incrementRetryMsg() {
        retryMsgCount.inc();
    }

    public void incrementDroppedMsg() {
        droppedMsgCount.inc();
    }

    public void incrementOpenedConn() {
        openedConnCount.inc();
    }

    public void incrementClosedConn() {
        closedConnCount.inc();
    }

    public void incrementErrorConn() {
        errorConnCount.inc();
    }

    public void incrementAddEndpoint() {
        addEndpointCount.inc();
    }

    public void incrementRemoveEndpoint() {
        removeEndpointCount.inc();
    }

    public void addRescuedMsgBytes(long bytes) {
        rescuedMsgBytes.inc(bytes);
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
        return (long) requestMsgCount.get();
    }

    public long getEnqueuedMsgCount() {
        return (long) enqueuedMsgCount.get();
    }

    public long getDequeuedMsgCount() {
        return (long) dequeuedMsgCount.get();
    }

    public long getSucceededMsgCount() {
        return (long) succeededMsgCount.get();
    }

    public long getFailedMsgCount() {
        return (long) failedMsgCount.get();
    }

    public long getTimeoutMsgCount() {
        return (long) timeoutMsgCount.get();
    }

    public long getRetryMsgCount() {
        return (long) retryMsgCount.get();
    }

    public long getDroppedMsgCount() {
        return (long) droppedMsgCount.get();
    }

    public long getOpenedConnCount() {
        return (long) openedConnCount.get();
    }

    public long getClosedConnCount() {
        return (long) closedConnCount.get();
    }

    public long getErrorConnCount() {
        return (long) errorConnCount.get();
    }

    public long getAddEndpointCount() {
        return (long) addEndpointCount.get();
    }

    public long getRemoveEndpointCount() {
        return (long) removeEndpointCount.get();
    }

    public long getRescuedMsgBytes() {
        return (long) rescuedMsgBytes.get();
    }

    public long getMsgQueueSize() {
        return (long) msgQueueSize.get();
    }

    public long getMsgQueueCapacity() {
        return (long) msgQueueCapacity.get();
    }

    public long getEndpointCount() {
        return (long) endpointCount.get();
    }

    public long getActiveConnCount() {
        return (long) activeConnCount.get();
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

    /**
     * Get the CollectorRegistry used by this metrics instance.
     * Applications can use this to expose metrics via HTTP endpoint.
     *
     * @return The CollectorRegistry
     */
    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings(
        value = "EI_EXPOSE_REP",
        justification = "Registry must be exposed for application to set up Prometheus HTTP endpoint")
    public CollectorRegistry getRegistry() {
        return registry;
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
