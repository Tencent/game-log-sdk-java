package com.tencent.tglog;

/**
 * Exponential backoff calculator for retry logic.
 *
 * Implements exponential backoff with configurable parameters:
 * - Initial interval: starting delay
 * - Multiplier: factor to increase delay each retry
 * - Max interval: upper bound on delay
 *
 * Formula: interval = min(initial * multiplier^(retryCount-1), max)
 */
class Backoff {

    private final long initialIntervalMs;
    private final double multiplier;
    private final long maxIntervalMs;

    /**
     * Create a backoff calculator.
     *
     * @param initialIntervalMs Initial retry interval in milliseconds
     * @param multiplier Multiplier for exponential growth (e.g., 2.0 for doubling)
     * @param maxIntervalMs Maximum retry interval in milliseconds
     */
    public Backoff(long initialIntervalMs, double multiplier, long maxIntervalMs) {
        if (initialIntervalMs <= 0) {
            throw new IllegalArgumentException("Initial interval must be positive");
        }
        if (multiplier <= 1.0) {
            throw new IllegalArgumentException("Multiplier must be greater than 1.0");
        }
        if (maxIntervalMs < initialIntervalMs) {
            throw new IllegalArgumentException("Max interval must be >= initial interval");
        }

        this.initialIntervalMs = initialIntervalMs;
        this.multiplier = multiplier;
        this.maxIntervalMs = maxIntervalMs;
    }

    /**
     * Calculate next retry interval based on retry count.
     *
     * @param retryCount Number of retries (0 = first attempt, 1 = first retry, etc.)
     * @return Delay in milliseconds before next retry
     */
    public long getNextInterval(int retryCount) {
        if (retryCount <= 0) {
            return initialIntervalMs;
        }

        // Calculate exponential backoff: initial * multiplier^(retryCount-1)
        double interval = initialIntervalMs * Math.pow(multiplier, retryCount - 1);

        // Cap at max interval
        if (interval > maxIntervalMs) {
            return maxIntervalMs;
        }

        return (long) interval;
    }

    /**
     * Check if enough time has passed for the next retry.
     *
     * @param lastAttemptTime Time of last attempt in milliseconds
     * @param retryCount Current retry count
     * @return true if enough time has passed
     */
    public boolean shouldRetry(long lastAttemptTime, int retryCount) {
        long now = System.currentTimeMillis();
        long elapsed = now - lastAttemptTime;
        long requiredInterval = getNextInterval(retryCount);
        return elapsed >= requiredInterval;
    }

    /**
     * Calculate total backoff time for all retries up to maxRetries.
     * This is the sum of all retry intervals: interval(1) + interval(2) + ... + interval(maxRetries)
     *
     * @param maxRetries Maximum number of retries
     * @return Total time in milliseconds for all retries
     */
    public long getTotalBackoffTime(int maxRetries) {
        if (maxRetries <= 0) {
            return 0;
        }

        long total = 0;
        for (int i = 1; i <= maxRetries; i++) {
            total += getNextInterval(i);
        }
        return total;
    }

    public long getInitialIntervalMs() {
        return initialIntervalMs;
    }

    public double getMultiplier() {
        return multiplier;
    }

    public long getMaxIntervalMs() {
        return maxIntervalMs;
    }

    @Override
    public String toString() {
        return String.format("Backoff{initial=%dms, multiplier=%.1f, max=%dms}",
                initialIntervalMs, multiplier, maxIntervalMs);
    }

    // Predefined backoff strategies

    /**
     * Backoff for batch retry: 100ms -> 200ms -> 400ms -> ... -> 5s
     */
    public static final Backoff BATCH_RETRY = new Backoff(100, 2.0, 5000);

    /**
     * Backoff for connection retry: 1s -> 2s -> 4s -> ... -> 60s
     */
    public static final Backoff CONNECTION_RETRY = new Backoff(1000, 2.0, 60000);

    /**
     * Backoff for endpoint recovery: 5s -> 10s -> 20s -> ... -> 300s
     */
    public static final Backoff ENDPOINT_RECOVERY = new Backoff(5000, 2.0, 300000);
}
