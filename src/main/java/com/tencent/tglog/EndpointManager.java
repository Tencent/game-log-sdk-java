package com.tencent.tglog;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;

/**
 * Manages DNS resolution and endpoint discovery.
 *
 * Features:
 * - Periodic DNS resolution
 * - Endpoint change detection
 * - Notification to connection pool
 */
class EndpointManager {

    private static final Logger logger = LoggerFactory.getLogger(EndpointManager.class);

    private final String host;
    private final int port;
    private final TglogConfig config;

    private volatile List<InetSocketAddress> currentEndpoints = new ArrayList<>();
    private final ScheduledExecutorService scheduler;
    private ScheduledFuture<?> updateTask;

    private final List<Consumer<List<InetSocketAddress>>> changeListeners = new CopyOnWriteArrayList<>();

    public EndpointManager(String host, int port, TglogConfig config) {
        this.host = host;
        this.port = port;
        this.config = config;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "tglog-dns-resolver");
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * Start periodic DNS updates.
     */
    public void start() {
        // Initial resolution
        updateEndpoints();

        // Schedule periodic updates
        int intervalSeconds = config.getDnsUpdateInterval();
        updateTask = scheduler.scheduleAtFixedRate(
                this::updateEndpoints,
                intervalSeconds,
                intervalSeconds,
                TimeUnit.SECONDS
        );

        logger.info("Started DNS resolver for {} (interval: {}s)", host, intervalSeconds);
    }

    /**
     * Stop DNS updates.
     */
    public void stop() {
        if (updateTask != null) {
            updateTask.cancel(false);
        }
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(1, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
        logger.info("Stopped DNS resolver");
    }

    /**
     * Update endpoints by resolving DNS.
     */
    private void updateEndpoints() {
        try {
            List<InetSocketAddress> newEndpoints = resolveHost();

            // Check if endpoints changed
            if (!endpointsEqual(currentEndpoints, newEndpoints)) {
                logger.info("Endpoints changed: {} -> {}", currentEndpoints.size(), newEndpoints.size());

                List<InetSocketAddress> oldEndpoints = currentEndpoints;
                currentEndpoints = newEndpoints;

                // Notify listeners
                notifyListeners(newEndpoints);

                logEndpointChanges(oldEndpoints, newEndpoints);
            }
        } catch (Exception e) {
            logger.error("Failed to resolve host: {}", host, e);
        }
    }

    /**
     * Resolve host to IP addresses.
     */
    private List<InetSocketAddress> resolveHost() throws UnknownHostException {
        List<InetSocketAddress> endpoints = new ArrayList<>();

        // Use getAllByName to get all IP addresses for the hostname
        // This will return multiple IPs if the host resolves to multiple addresses
        InetAddress[] addresses = InetAddress.getAllByName(host);
        for (InetAddress addr : addresses) {
            endpoints.add(new InetSocketAddress(addr, port));
        }

        if (endpoints.isEmpty()) {
            throw new UnknownHostException("No addresses resolved for: " + host);
        }

        logger.debug("Resolved {} to {} endpoint(s): {}", host, endpoints.size(), endpoints);

        return endpoints;
    }

    /**
     * Check if two endpoint lists are equal.
     */
    private boolean endpointsEqual(List<InetSocketAddress> list1, List<InetSocketAddress> list2) {
        if (list1.size() != list2.size()) {
            return false;
        }

        Set<InetSocketAddress> set1 = new HashSet<>(list1);
        Set<InetSocketAddress> set2 = new HashSet<>(list2);

        return set1.equals(set2);
    }

    /**
     * Log endpoint changes.
     */
    private void logEndpointChanges(List<InetSocketAddress> oldEndpoints,
                                    List<InetSocketAddress> newEndpoints) {
        Set<InetSocketAddress> oldSet = new HashSet<>(oldEndpoints);
        Set<InetSocketAddress> newSet = new HashSet<>(newEndpoints);

        // Find added endpoints
        Set<InetSocketAddress> added = new HashSet<>(newSet);
        added.removeAll(oldSet);
        if (!added.isEmpty()) {
            logger.info("Added endpoints: {}", added);
        }

        // Find removed endpoints
        Set<InetSocketAddress> removed = new HashSet<>(oldSet);
        removed.removeAll(newSet);
        if (!removed.isEmpty()) {
            logger.info("Removed endpoints: {}", removed);
        }
    }

    /**
     * Register a listener for endpoint changes.
     */
    public void addChangeListener(Consumer<List<InetSocketAddress>> listener) {
        changeListeners.add(listener);
    }

    /**
     * Notify all listeners of endpoint changes.
     */
    private void notifyListeners(List<InetSocketAddress> newEndpoints) {
        for (Consumer<List<InetSocketAddress>> listener : changeListeners) {
            try {
                listener.accept(new ArrayList<>(newEndpoints));
            } catch (Exception e) {
                logger.error("Error notifying endpoint change listener", e);
            }
        }
    }

    /**
     * Get current endpoints.
     */
    public List<InetSocketAddress> getCurrentEndpoints() {
        return new ArrayList<>(currentEndpoints);
    }

    /**
     * Get endpoint count.
     */
    public int getEndpointCount() {
        return currentEndpoints.size();
    }
}
