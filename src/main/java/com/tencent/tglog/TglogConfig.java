package com.tencent.tglog;

import io.prometheus.client.CollectorRegistry;

/**
 * Configuration for TGLog client.
 *
 * This class provides all configuration options for the TGLog client,
 * with reasonable defaults suitable for game servers.
 */
public class TglogConfig {

    // ========== Protocol Size Limits ==========
    /**
     * Maximum UDP request size (65507 bytes)
     * This is the theoretical maximum for UDP: 65535 (max IP packet) - 8 (UDP header) - 20 (IP header)
     */
    public static final int MAX_UDP_REQ_SIZE = 65507;

    /**
     * Maximum TCP request size (512KB)
     * TCP supports larger messages as it's a stream protocol
     */
    public static final int MAX_TCP_REQ_SIZE = 512 * 1024;

    /**
     * Network protocol type
     */
    public enum Protocol {
        UDP,
        TCP
    }

    // ========== Network Configuration ==========
    private Protocol protocol = Protocol.TCP;
    private String host;
    private int port;

    // ========== Connection Configuration ==========
    private int connectionTimeout = 3000;           // ms
    private int maxConnectionLifetime = 300;        // seconds
    private int dnsUpdateInterval = 60;             // seconds

    // ========== Batching Configuration ==========
    private int batchingMaxMessages = 50;           // messages
    private int batchingMaxSize = 32 * 1024;        // 32KB
    private int batchingMaxDelayMs = 10;            // ms

    // ========== Worker Configuration ==========
    private int workerNum = 8;                      // worker threads
    private int maxPendingMessages = 409600;        // per worker queue capacity

    // ========== Buffer Configuration ==========
    private int writeHighWatermark = 32 * 1024 * 1024;      // 32MB
    private int socketSendBufferSize = 64 * 1024 * 1024;    // 64MB
    private int socketRecvBufferSize = 16 * 1024;           // 16KB

    // ========== Retry Configuration ==========
    private int sendTimeout = 30000;                // ms
    private int maxRetries = 3;
    private int stopTimeout = 3000;                 // ms

    // ========== Debug Configuration ==========
    private boolean debugEnabled = false;

    // ========== Metrics Configuration ==========
    private CollectorRegistry collectorRegistry = null;  // null means use defaultRegistry

    /**
     * Create a new configuration with required parameters.
     *
     * @param protocol Network protocol (TCP or UDP)
     * @param host Server hostname or IP address
     * @param port Server port number
     */
    public TglogConfig(Protocol protocol, String host, int port) {
        if (protocol == null) {
            throw new IllegalArgumentException("Protocol cannot be null");
        }
        if (host == null || host.trim().isEmpty()) {
            throw new IllegalArgumentException("Host cannot be null or empty");
        }
        if (port <= 0 || port > 65535) {
            throw new IllegalArgumentException("Port must be between 1 and 65535");
        }
        this.protocol = protocol;
        this.host = host;
        this.port = port;
    }

    /**
     * Create a TCP configuration.
     */
    public static TglogConfig createTcp(String host, int port) {
        return new TglogConfig(Protocol.TCP, host, port);
    }

    /**
     * Create a UDP configuration.
     */
    public static TglogConfig createUdp(String host, int port) {
        return new TglogConfig(Protocol.UDP, host, port);
    }

    // ========== Getters and Setters with Fluent API ==========

    public Protocol getProtocol() {
        return protocol;
    }

    public TglogConfig setProtocol(Protocol protocol) {
        this.protocol = protocol;
        return this;
    }

    public String getHost() {
        return host;
    }

    public TglogConfig setHost(String host) {
        this.host = host;
        return this;
    }

    public int getPort() {
        return port;
    }

    public TglogConfig setPort(int port) {
        this.port = port;
        return this;
    }

    public int getConnectionTimeout() {
        return connectionTimeout;
    }

    public TglogConfig setConnectionTimeout(int connectionTimeout) {
        if (connectionTimeout <= 0) {
            throw new IllegalArgumentException("Connection timeout must be positive");
        }
        this.connectionTimeout = connectionTimeout;
        return this;
    }

    public int getMaxConnectionLifetime() {
        return maxConnectionLifetime;
    }

    public TglogConfig setMaxConnectionLifetime(int maxConnectionLifetime) {
        if (maxConnectionLifetime < 0) {
            throw new IllegalArgumentException("Max connection lifetime must be non-negative (0 means persistent connection)");
        }
        this.maxConnectionLifetime = maxConnectionLifetime;
        return this;
    }

    public int getDnsUpdateInterval() {
        return dnsUpdateInterval;
    }

    public TglogConfig setDnsUpdateInterval(int dnsUpdateInterval) {
        this.dnsUpdateInterval = dnsUpdateInterval;
        return this;
    }

    public int getBatchingMaxMessages() {
        return batchingMaxMessages;
    }

    public TglogConfig setBatchingMaxMessages(int batchingMaxMessages) {
        if (batchingMaxMessages <= 0) {
            throw new IllegalArgumentException("Batching max messages must be positive");
        }
        this.batchingMaxMessages = batchingMaxMessages;
        return this;
    }

    public int getBatchingMaxSize() {
        return batchingMaxSize;
    }

    public TglogConfig setBatchingMaxSize(int batchingMaxSize) {
        if (batchingMaxSize <= 0) {
            throw new IllegalArgumentException("Batching max size must be positive");
        }
        this.batchingMaxSize = batchingMaxSize;
        return this;
    }

    /**
     * Get the maximum request size based on protocol type.
     *
     * @return Maximum request size in bytes
     */
    public int getMaxRequestSize() {
        return protocol == Protocol.UDP ? MAX_UDP_REQ_SIZE : MAX_TCP_REQ_SIZE;
    }

    public int getBatchingMaxDelayMs() {
        return batchingMaxDelayMs;
    }

    public TglogConfig setBatchingMaxDelayMs(int batchingMaxDelayMs) {
        if (batchingMaxDelayMs <= 0) {
            throw new IllegalArgumentException("Batching max delay must be positive");
        }
        this.batchingMaxDelayMs = batchingMaxDelayMs;
        return this;
    }

    public int getWorkerNum() {
        return workerNum;
    }

    public TglogConfig setWorkerNum(int workerNum) {
        if (workerNum <= 0) {
            throw new IllegalArgumentException("Worker number must be positive");
        }
        this.workerNum = workerNum;
        return this;
    }

    public int getMaxPendingMessages() {
        return maxPendingMessages;
    }

    public TglogConfig setMaxPendingMessages(int maxPendingMessages) {
        if (maxPendingMessages <= 0) {
            throw new IllegalArgumentException("Max pending messages must be positive");
        }
        this.maxPendingMessages = maxPendingMessages;
        return this;
    }

    public int getWriteHighWatermark() {
        return writeHighWatermark;
    }

    public TglogConfig setWriteHighWatermark(int writeHighWatermark) {
        this.writeHighWatermark = writeHighWatermark;
        return this;
    }

    public int getSocketSendBufferSize() {
        return socketSendBufferSize;
    }

    public TglogConfig setSocketSendBufferSize(int socketSendBufferSize) {
        this.socketSendBufferSize = socketSendBufferSize;
        return this;
    }

    public int getSocketRecvBufferSize() {
        return socketRecvBufferSize;
    }

    public TglogConfig setSocketRecvBufferSize(int socketRecvBufferSize) {
        this.socketRecvBufferSize = socketRecvBufferSize;
        return this;
    }

    public int getSendTimeout() {
        return sendTimeout;
    }

    public TglogConfig setSendTimeout(int sendTimeout) {
        if (sendTimeout <= 0) {
            throw new IllegalArgumentException("Send timeout must be positive");
        }
        this.sendTimeout = sendTimeout;
        return this;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public TglogConfig setMaxRetries(int maxRetries) {
        if (maxRetries < 0) {
            throw new IllegalArgumentException("Max retries must be non-negative");
        }
        this.maxRetries = maxRetries;
        return this;
    }

    public int getStopTimeout() {
        return stopTimeout;
    }

    public TglogConfig setStopTimeout(int stopTimeout) {
        if (stopTimeout <= 0) {
            throw new IllegalArgumentException("Stop timeout must be positive");
        }
        this.stopTimeout = stopTimeout;
        return this;
    }

    public boolean isDebugEnabled() {
        return debugEnabled;
    }

    public TglogConfig setDebugEnabled(boolean debugEnabled) {
        this.debugEnabled = debugEnabled;
        return this;
    }

    /**
     * Get the Prometheus CollectorRegistry.
     * Returns null if using default registry.
     *
     * @return CollectorRegistry or null for default
     */
    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings(
        value = "EI_EXPOSE_REP",
        justification = "Registry must be shared for Prometheus integration")
    public CollectorRegistry getCollectorRegistry() {
        return collectorRegistry;
    }

    /**
     * Set a custom Prometheus CollectorRegistry.
     * If null, the default registry will be used.
     * This is useful for isolating metrics in tests or multi-tenant scenarios.
     *
     * @param collectorRegistry Custom registry, or null for default
     * @return this config for fluent API
     */
    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings(
        value = "EI_EXPOSE_REP2",
        justification = "Registry must be shared for Prometheus integration")
    public TglogConfig setCollectorRegistry(CollectorRegistry collectorRegistry) {
        this.collectorRegistry = collectorRegistry;
        return this;
    }

    @Override
    public String toString() {
        return String.format("TglogConfig{protocol=%s, host='%s', port=%d, workers=%d, batchSize=%d}",
                protocol, host, port, workerNum, batchingMaxMessages);
    }
}
