package com.tencent.tglog;

import io.prometheus.client.CollectorRegistry;

import java.util.Enumeration;

/**
 * Hot-reload / recreate test for TglogClient.
 *
 * <p>This test validates the lifecycle fix for scenario: close the old client, then
 * create a new one against the same {@link CollectorRegistry}. Before the fix, the
 * second constructor would throw {@code IllegalArgumentException: Collector already
 * registered} because metrics were never unregistered.
 *
 * <p>Scenarios covered:
 * <ol>
 *   <li>Basic hot reload: close + recreate on the default registry</li>
 *   <li>Multiple cycles: simulate repeated config hot-reload (stress)</li>
 *   <li>Custom registry: explicit {@link CollectorRegistry} passed via config</li>
 *   <li>Constructor failure cleanup: failed construction must not leak collectors</li>
 *   <li>Fresh counters: each new client starts from zero</li>
 * </ol>
 *
 * <p>No real server is required. We use UDP so the client doesn't block in TCP
 * connect/retry — this test only cares about the metrics registration lifecycle,
 * which is fully exercised by constructor + close() regardless of protocol.
 */
public class HotReloadTest {

    // UDP + localhost: send is best-effort (datagram goes nowhere if no listener),
    // DNS resolves instantly, and close() returns promptly because there's no
    // TCP reconnect loop to tear down. Exactly what we want for a lifecycle test.
    private static final String TEST_HOST = "127.0.0.1";
    private static final int TEST_PORT = 19999;

    public static void main(String[] args) throws Exception {
        System.out.println("=== TglogClient Hot-Reload Test ===\n");

        testBasicHotReload();
        testMultipleCycles();
        testCustomRegistry();
        testConstructorFailureCleanup();
        testFreshCounters();

        System.out.println("\n=== All Hot-Reload Tests Passed! ===");
    }

    /**
     * Scenario 1: Classic hot reload. Close an old client, then create a new one
     * using the default registry. Must not throw "Collector already registered".
     */
    private static void testBasicHotReload() throws Exception {
        System.out.println("Test 1: Basic hot reload (close -> recreate on default registry)");

        // Guard against test pollution from previous runs / other tests.
        CollectorRegistry.defaultRegistry.clear();

        TglogClient first = new TglogClient(newConfig());
        assertMetricsRegistered(CollectorRegistry.defaultRegistry,
                "after first client created");

        first.close();
        assertMetricsUnregistered(CollectorRegistry.defaultRegistry,
                "after first client closed");

        // The key assertion: second construction must not throw.
        TglogClient second;
        try {
            second = new TglogClient(newConfig());
        } catch (Exception e) {
            fail("Second client construction failed after first was closed: " + e);
            return;
        }
        assertMetricsRegistered(CollectorRegistry.defaultRegistry,
                "after second client created");

        second.close();
        assertMetricsUnregistered(CollectorRegistry.defaultRegistry,
                "after second client closed");

        System.out.println("  [OK] Close -> recreate cycle works on default registry\n");
    }

    /**
     * Scenario 2: Many cycles in a row. Simulates a long-running process that
     * hot-reloads config repeatedly (e.g. config watcher, Spring @RefreshScope).
     * This also verifies unregister() doesn't leak collectors over time.
     */
    private static void testMultipleCycles() throws Exception {
        System.out.println("Test 2: Multiple hot-reload cycles (N=20)");

        CollectorRegistry.defaultRegistry.clear();

        final int cycles = 20;
        for (int i = 0; i < cycles; i++) {
            TglogClient client = new TglogClient(newConfig());
            // Exercise metrics so we're sure the collectors are live.
            client.getMetrics().incrementRequestMsg();
            client.close();
        }

        assertMetricsUnregistered(CollectorRegistry.defaultRegistry,
                "after " + cycles + " cycles");

        System.out.println("  [OK] " + cycles + " close -> recreate cycles clean\n");
    }

    /**
     * Scenario 3: Application supplies its own CollectorRegistry via config.
     * Same hot-reload guarantees must hold on an isolated registry.
     */
    private static void testCustomRegistry() throws Exception {
        System.out.println("Test 3: Custom CollectorRegistry via config");

        CollectorRegistry custom = new CollectorRegistry();

        TglogClient first = new TglogClient(newConfig().setCollectorRegistry(custom));
        assertMetricsRegistered(custom, "after first (custom registry)");
        // Default registry must stay untouched when a custom one is used.
        assertMetricsUnregistered(CollectorRegistry.defaultRegistry,
                "default registry must stay clean when using custom registry");

        first.close();
        assertMetricsUnregistered(custom, "after first close (custom registry)");

        TglogClient second = new TglogClient(newConfig().setCollectorRegistry(custom));
        assertMetricsRegistered(custom, "after second (custom registry)");
        second.close();
        assertMetricsUnregistered(custom, "after second close (custom registry)");

        System.out.println("  [OK] Custom registry hot-reload cycle works\n");
    }

    /**
     * Scenario 4: Construction fails after metrics were registered. The finally
     * block in TglogClient's constructor must unregister them so the registry is
     * clean for a retry.
     *
     * <p>We trigger failure by pointing at an unresolvable hostname. EndpointManager
     * will fail to resolve any endpoint, and start() should throw.
     */
    private static void testConstructorFailureCleanup() throws Exception {
        System.out.println("Test 4: Constructor failure must not leak collectors");

        CollectorRegistry.defaultRegistry.clear();

        // Bogus host that shouldn't resolve anywhere — causes start() to throw
        // because EndpointManager ends up with zero endpoints.
        TglogConfig badConfig = TglogConfig.createUdp(
                "tglog-nonexistent-hot-reload-test.invalid", TEST_PORT);

        boolean threw = false;
        try {
            TglogClient doomed = new TglogClient(badConfig);
            // If we got here, something resolved — close to clean up before
            // failing the assertion.
            doomed.close();
        } catch (TglogException e) {
            threw = true;
        } catch (Exception e) {
            threw = true;
        }

        if (!threw) {
            System.out.println("  [SKIP] Bad hostname resolved (likely a DNS wildcard) — cannot "
                    + "exercise the failure path in this environment");
            System.out.println();
            return;
        }

        // The whole point of the fix: after a failed construction the registry
        // must be clean, so a subsequent retry with good config succeeds.
        assertMetricsUnregistered(CollectorRegistry.defaultRegistry,
                "after failed construction");

        TglogClient recovery;
        try {
            recovery = new TglogClient(newConfig());
        } catch (Exception e) {
            fail("Retry after failed construction should succeed, but got: " + e);
            return;
        }
        recovery.close();

        System.out.println("  [OK] Failed construction does not leak collectors\n");
    }

    /**
     * Scenario 5: A freshly created client starts with zero counters. This
     * sanity-checks that unregister() + re-register() really produces fresh
     * Counter objects (not stale ones from the previous instance).
     */
    private static void testFreshCounters() throws Exception {
        System.out.println("Test 5: New client starts with fresh counters");

        CollectorRegistry.defaultRegistry.clear();

        TglogClient first = new TglogClient(newConfig());
        // Bump a counter on the first instance.
        for (int i = 0; i < 42; i++) {
            first.getMetrics().incrementRequestMsg();
        }
        long firstCount = first.getMetrics().getRequestMsgCount();
        if (firstCount != 42) {
            fail("Expected requestMsg=42 on first client, got " + firstCount);
            first.close();
            return;
        }
        first.close();

        TglogClient second = new TglogClient(newConfig());
        long secondCount = second.getMetrics().getRequestMsgCount();
        if (secondCount != 0) {
            fail("Expected requestMsg=0 on freshly created second client, got " + secondCount);
            second.close();
            return;
        }
        second.close();

        System.out.println("  [OK] New client's counters start at 0 (not " + firstCount + ")\n");
    }

    // ---------- Helpers ----------

    private static TglogConfig newConfig() {
        return TglogConfig.createUdp(TEST_HOST, TEST_PORT)
                .setWorkerNum(1)
                // Short stop timeout so close() returns quickly.
                .setStopTimeout(200);
    }

    /**
     * Assert that at least one tglog metric is present in the given registry.
     * We don't enumerate every metric name — the point is "the SDK did register".
     */
    private static void assertMetricsRegistered(CollectorRegistry registry, String context) {
        if (!hasTglogMetric(registry)) {
            fail("Expected tglog_* metrics to be registered " + context
                    + ", but registry has none");
        }
    }

    /**
     * Assert that no tglog metric remains in the given registry. This is the
     * core post-condition of {@code TglogMetrics.unregister()}.
     */
    private static void assertMetricsUnregistered(CollectorRegistry registry, String context) {
        if (hasTglogMetric(registry)) {
            fail("Expected tglog_* metrics to be UNREGISTERED " + context
                    + ", but registry still has them: " + listTglogMetricNames(registry));
        }
    }

    private static boolean hasTglogMetric(CollectorRegistry registry) {
        Enumeration<io.prometheus.client.Collector.MetricFamilySamples> e =
                registry.metricFamilySamples();
        while (e.hasMoreElements()) {
            io.prometheus.client.Collector.MetricFamilySamples mfs = e.nextElement();
            if (mfs.name != null && mfs.name.startsWith("tglog_")) {
                return true;
            }
        }
        return false;
    }

    private static String listTglogMetricNames(CollectorRegistry registry) {
        StringBuilder sb = new StringBuilder("[");
        boolean first = true;
        Enumeration<io.prometheus.client.Collector.MetricFamilySamples> e =
                registry.metricFamilySamples();
        while (e.hasMoreElements()) {
            io.prometheus.client.Collector.MetricFamilySamples mfs = e.nextElement();
            if (mfs.name != null && mfs.name.startsWith("tglog_")) {
                if (!first) {
                    sb.append(", ");
                }
                sb.append(mfs.name);
                first = false;
            }
        }
        sb.append("]");
        return sb.toString();
    }

    private static void fail(String message) {
        System.out.println("  [FAIL] " + message);
        throw new AssertionError(message);
    }
}
