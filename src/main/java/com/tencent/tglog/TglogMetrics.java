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
 * <p><b>Lifecycle:</b> Metrics are registered with the provided
 * {@link CollectorRegistry} in the constructor and unregistered via
 * {@link #unregister()}. When reusing a single registry across multiple
 * TglogClient instances in the same JVM (e.g. config hot-reload), the previous
 * client must complete its {@code close()} (which calls {@code unregister()})
 * before the new client is created. Overlapping instances on the same registry
 * will fail with {@code Collector already registered}. If overlap is
 * unavoidable, give each client its own {@link CollectorRegistry} via
 * {@link TglogConfig#setCollectorRegistry}.
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
    private final Counter eagainErrCount;

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
                .labelNames("worker_id")
                .register(registry);

        this.closedConnCount = Counter.build()
                .name("tglog_closed_connections_total")
                .help("Total number of connections closed")
                .labelNames("worker_id")
                .register(registry);

        this.errorConnCount = Counter.build()
                .name("tglog_error_connections_total")
                .help("Total number of connection errors")
                .labelNames("worker_id")
                .register(registry);

        this.eagainErrCount = Counter.build()
                .name("tglog_eagain_errors_total")
                .help("Total number of EAGAIN errors (socket buffer full)")
                .labelNames("worker_id")
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
        // Queue size and connection count have worker_id label for per-worker monitoring
        this.msgQueueSize = Gauge.build()
                .name("tglog_message_queue_size")
                .help("Current size of the message queue")
                .labelNames("worker_id")
                .register(registry);

        this.msgQueueCapacity = Gauge.build()
                .name("tglog_message_queue_capacity")
                .help("Capacity of the message queue")
                .labelNames("worker_id")
                .register(registry);

        this.endpointCount = Gauge.build()
                .name("tglog_endpoints")
                .help("Current number of endpoints")
                .register(registry);

        this.activeConnCount = Gauge.build()
                .name("tglog_active_connections")
                .help("Current number of active connections")
                .labelNames("worker_id")
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

    public void incrementOpenedConn(int workerId) {
        openedConnCount.labels(String.valueOf(workerId)).inc();
    }

    public void incrementClosedConn(int workerId) {
        closedConnCount.labels(String.valueOf(workerId)).inc();
    }

    public void incrementErrorConn(int workerId) {
        errorConnCount.labels(String.valueOf(workerId)).inc();
    }

    public void incrementEagainErr(int workerId) {
        eagainErrCount.labels(String.valueOf(workerId)).inc();
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

    public void setMsgQueueSize(int workerId, long size) {
        msgQueueSize.labels(String.valueOf(workerId)).set(size);
    }

    public void setMsgQueueCapacity(int workerId, long capacity) {
        msgQueueCapacity.labels(String.valueOf(workerId)).set(capacity);
    }

    public void setEndpointCount(long count) {
        endpointCount.set(count);
    }

    public void setActiveConnCount(int workerId, long count) {
        activeConnCount.labels(String.valueOf(workerId)).set(count);
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
        // Sum across all worker_id labels (only _total samples, not _created)
        return (long) openedConnCount.collect().stream()
                .flatMap(mf -> mf.samples.stream())
                .filter(s -> s.name.endsWith("_total"))
                .mapToDouble(s -> s.value)
                .sum();
    }

    public long getClosedConnCount() {
        // Sum across all worker_id labels (only _total samples, not _created)
        return (long) closedConnCount.collect().stream()
                .flatMap(mf -> mf.samples.stream())
                .filter(s -> s.name.endsWith("_total"))
                .mapToDouble(s -> s.value)
                .sum();
    }

    public long getErrorConnCount() {
        // Sum across all worker_id labels (only _total samples, not _created)
        return (long) errorConnCount.collect().stream()
                .flatMap(mf -> mf.samples.stream())
                .filter(s -> s.name.endsWith("_total"))
                .mapToDouble(s -> s.value)
                .sum();
    }

    public long getEagainErrCount() {
        // Sum across all worker_id labels (only _total samples, not _created)
        return (long) eagainErrCount.collect().stream()
                .flatMap(mf -> mf.samples.stream())
                .filter(s -> s.name.endsWith("_total"))
                .mapToDouble(s -> s.value)
                .sum();
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
        // Sum across all worker_id labels
        return (long) msgQueueSize.collect().stream()
                .flatMap(mf -> mf.samples.stream())
                .mapToDouble(s -> s.value)
                .sum();
    }

    public long getMsgQueueCapacity() {
        // Sum across all worker_id labels
        return (long) msgQueueCapacity.collect().stream()
                .flatMap(mf -> mf.samples.stream())
                .mapToDouble(s -> s.value)
                .sum();
    }

    public long getEndpointCount() {
        return (long) endpointCount.get();
    }

    public long getActiveConnCount() {
        // Sum across all worker_id labels
        return (long) activeConnCount.collect().stream()
                .flatMap(mf -> mf.samples.stream())
                .mapToDouble(s -> s.value)
                .sum();
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

    // ========== Lifecycle ==========

    /**
     * Unregister all metrics from the CollectorRegistry.
     *
     * <p>Must be called after the owning TglogClient has stopped all workers and will
     * no longer touch any metric. Otherwise a concurrent {@code inc()} call may race
     * with unregistration and leave the registry in an inconsistent state.
     *
     * <p>This method is idempotent - calling it twice is safe (Prometheus'
     * {@code CollectorRegistry.unregister} silently ignores collectors that are
     * not registered).
     *
     * <p>This is essential when the application may create/destroy multiple
     * TglogClient instances over the JVM lifetime (e.g. config hot-reload,
     * servlet container redeploy, test suites). Without unregistration, the
     * second {@code new TglogClient(...)} will throw
     * {@code IllegalArgumentException: Collector already registered}.
     */
    public void unregister() {
        // Unregister each collector. Wrap in try/catch so one failure doesn't
        // prevent the rest from being released.
        safeUnregister(requestMsgCount);
        safeUnregister(enqueuedMsgCount);
        safeUnregister(dequeuedMsgCount);
        safeUnregister(succeededMsgCount);
        safeUnregister(failedMsgCount);
        safeUnregister(timeoutMsgCount);
        safeUnregister(retryMsgCount);
        safeUnregister(droppedMsgCount);
        safeUnregister(openedConnCount);
        safeUnregister(closedConnCount);
        safeUnregister(errorConnCount);
        safeUnregister(eagainErrCount);
        safeUnregister(addEndpointCount);
        safeUnregister(removeEndpointCount);
        safeUnregister(rescuedMsgBytes);
        safeUnregister(msgQueueSize);
        safeUnregister(msgQueueCapacity);
        safeUnregister(endpointCount);
        safeUnregister(activeConnCount);
    }

    private void safeUnregister(io.prometheus.client.Collector collector) {
        if (collector == null) {
            return;
        }
        try {
            registry.unregister(collector);
        } catch (Exception e) {
            // Swallow: unregister is best-effort on shutdown. A failure here
            // shouldn't block the rest of cleanup.
        }
    }

    // ========== JSON Output ==========

    /**
     * Export metrics as JSON string.
     * Reads all metrics first to minimize inconsistency in concurrent scenarios.
     *
     * @return JSON representation of metrics
     */
    public String toJson() {
        // Read all metrics first to get a more consistent snapshot
        long requestMsgCount = getRequestMsgCount();
        long enqueuedMsgCount = getEnqueuedMsgCount();
        long dequeuedMsgCount = getDequeuedMsgCount();
        long succeededMsgCount = getSucceededMsgCount();
        long failedMsgCount = getFailedMsgCount();
        long timeoutMsgCount = getTimeoutMsgCount();
        long retryMsgCount = getRetryMsgCount();
        long droppedMsgCount = getDroppedMsgCount();
        long openedConnCount = getOpenedConnCount();
        long closedConnCount = getClosedConnCount();
        long errorConnCount = getErrorConnCount();
        long eagainErrCount = getEagainErrCount();
        long addEndpointCount = getAddEndpointCount();
        long removeEndpointCount = getRemoveEndpointCount();
        long rescuedMsgBytes = getRescuedMsgBytes();
        long msgQueueSize = getMsgQueueSize();
        long msgQueueCapacity = getMsgQueueCapacity();
        long endpointCount = getEndpointCount();
        long activeConnCount = getActiveConnCount();

        // Calculate derived metrics
        long total = succeededMsgCount + failedMsgCount;
        double successRate = total == 0 ? 0.0 : (double) succeededMsgCount / total;
        double queueUtilization = msgQueueCapacity == 0 ? 0.0 : (double) msgQueueSize / msgQueueCapacity;

        // Build JSON string
        StringBuilder sb = new StringBuilder();
        sb.append("{\n");
        sb.append("  \"counters\": {\n");
        sb.append("    \"request_msg_count\": ").append(requestMsgCount).append(",\n");
        sb.append("    \"enqueued_msg_count\": ").append(enqueuedMsgCount).append(",\n");
        sb.append("    \"dequeued_msg_count\": ").append(dequeuedMsgCount).append(",\n");
        sb.append("    \"succeeded_msg_count\": ").append(succeededMsgCount).append(",\n");
        sb.append("    \"failed_msg_count\": ").append(failedMsgCount).append(",\n");
        sb.append("    \"timeout_msg_count\": ").append(timeoutMsgCount).append(",\n");
        sb.append("    \"retry_msg_count\": ").append(retryMsgCount).append(",\n");
        sb.append("    \"dropped_msg_count\": ").append(droppedMsgCount).append(",\n");
        sb.append("    \"opened_conn_count\": ").append(openedConnCount).append(",\n");
        sb.append("    \"closed_conn_count\": ").append(closedConnCount).append(",\n");
        sb.append("    \"error_conn_count\": ").append(errorConnCount).append(",\n");
        sb.append("    \"eagain_err_count\": ").append(eagainErrCount).append(",\n");
        sb.append("    \"add_endpoint_count\": ").append(addEndpointCount).append(",\n");
        sb.append("    \"remove_endpoint_count\": ").append(removeEndpointCount).append(",\n");
        sb.append("    \"rescued_msg_bytes\": ").append(rescuedMsgBytes).append("\n");
        sb.append("  },\n");
        sb.append("  \"gauges\": {\n");
        sb.append("    \"msg_queue_size\": ").append(msgQueueSize).append(",\n");
        sb.append("    \"msg_queue_capacity\": ").append(msgQueueCapacity).append(",\n");
        sb.append("    \"endpoint_count\": ").append(endpointCount).append(",\n");
        sb.append("    \"active_conn_count\": ").append(activeConnCount).append("\n");
        sb.append("  },\n");
        sb.append("  \"derived\": {\n");
        sb.append("    \"success_rate\": ").append(String.format("%.4f", successRate)).append(",\n");
        sb.append("    \"queue_utilization\": ").append(String.format("%.4f", queueUtilization)).append("\n");
        sb.append("  }\n");
        sb.append("}");
        return sb.toString();
    }

    /**
     * Get human-readable metrics string with all metrics.
     * Reads all metrics first to minimize inconsistency in concurrent scenarios.
     *
     * @return Formatted metrics string
     */
    @Override
    public String toString() {
        // Read all metrics first to get a more consistent snapshot
        long requestMsgCount = getRequestMsgCount();
        long enqueuedMsgCount = getEnqueuedMsgCount();
        long dequeuedMsgCount = getDequeuedMsgCount();
        long succeededMsgCount = getSucceededMsgCount();
        long failedMsgCount = getFailedMsgCount();
        long timeoutMsgCount = getTimeoutMsgCount();
        long retryMsgCount = getRetryMsgCount();
        long droppedMsgCount = getDroppedMsgCount();
        long openedConnCount = getOpenedConnCount();
        long closedConnCount = getClosedConnCount();
        long errorConnCount = getErrorConnCount();
        long eagainErrCount = getEagainErrCount();
        long addEndpointCount = getAddEndpointCount();
        long removeEndpointCount = getRemoveEndpointCount();
        long rescuedMsgBytes = getRescuedMsgBytes();
        long msgQueueSize = getMsgQueueSize();
        long msgQueueCapacity = getMsgQueueCapacity();
        long endpointCount = getEndpointCount();
        long activeConnCount = getActiveConnCount();

        // Calculate derived metrics
        long total = succeededMsgCount + failedMsgCount;
        double successRate = total == 0 ? 0.0 : (double) succeededMsgCount / total;
        double queueUtilization = msgQueueCapacity == 0 ? 0.0 : (double) msgQueueSize / msgQueueCapacity;

        // Build formatted string
        StringBuilder sb = new StringBuilder();
        sb.append("TGLog Metrics:").append(System.lineSeparator());
        sb.append("====================").append(System.lineSeparator());

        // Message metrics
        sb.append(String.format("Requests:       %,d%n", requestMsgCount));
        sb.append(String.format("Enqueued:       %,d%n", enqueuedMsgCount));
        sb.append(String.format("Dequeued:       %,d%n", dequeuedMsgCount));
        sb.append(String.format("Succeeded:      %,d%n", succeededMsgCount));
        sb.append(String.format("Failed:         %,d%n", failedMsgCount));
        sb.append(String.format("Timeout:        %,d%n", timeoutMsgCount));
        sb.append(String.format("Retry:          %,d%n", retryMsgCount));
        sb.append(String.format("Dropped:        %,d%n", droppedMsgCount));
        sb.append(String.format("Success Rate:   %.2f%%%n", successRate * 100));

        // Queue metrics
        sb.append(String.format("Queue Size:     %,d / %,d (%.1f%%)%n",
                msgQueueSize,
                msgQueueCapacity,
                queueUtilization * 100));

        // Connection metrics
        sb.append(String.format("Connections:    %,d active%n", activeConnCount));
        sb.append(String.format("Conn Opened:    %,d%n", openedConnCount));
        sb.append(String.format("Conn Closed:    %,d%n", closedConnCount));
        sb.append(String.format("Conn Errors:    %,d%n", errorConnCount));
        sb.append(String.format("EAGAIN Errors:  %,d%n", eagainErrCount));

        // Endpoint metrics
        sb.append(String.format("Endpoints:      %,d%n", endpointCount));
        sb.append(String.format("EP Added:       %,d%n", addEndpointCount));
        sb.append(String.format("EP Removed:     %,d%n", removeEndpointCount));

        // Rescued bytes
        sb.append(String.format("Rescued Bytes:  %,d%n", rescuedMsgBytes));

        return sb.toString();
    }
}
