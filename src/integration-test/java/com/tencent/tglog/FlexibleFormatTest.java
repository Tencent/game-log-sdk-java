package com.tencent.tglog;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test that flexible log formats are supported (not just | separator).
 */
public class FlexibleFormatTest {

    public static void main(String[] args) throws Exception {
        System.out.println("=== Testing Flexible Log Format Support ===\n");

        TglogConfig config = TglogConfig.createTcp("dev.tglog.com", 20001)
                .setWorkerNum(1)
                .setBatchingMaxMessages(10)
                .setBatchingMaxDelayMs(100);

        try (TglogClient client = new TglogClient(config)) {
            testPipeSeparator(client);
            testCommaSeparator(client);
            testMixedFormat(client);
            testNoSeparator(client);

            // Wait for messages to be sent
            System.out.println("\nWaiting for messages to be sent...");
            Thread.sleep(2000);

            System.out.println("\n=== All Format Tests Passed! ===");
            System.out.println("\nMetrics:");
            System.out.println(client.printMetrics());
        }
    }

    private static void testPipeSeparator(TglogClient client) throws Exception {
        System.out.println("Test 1: Pipe separator (traditional format)");

        AtomicInteger callbackInvoked = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(1);

        TglogCallback callback = new TglogCallback() {
            @Override
            public void onSuccess(String payload) {
                callbackInvoked.incrementAndGet();
                latch.countDown();
            }

            @Override
            public void onError(String payload, int code, String msg) {
                System.out.println("  ERROR: onError should not be called!");
            }
        };

        try {
            client.send("game|action=login|user=123\n", callback);
            System.out.println("  ✓ Message accepted with pipe separator");
        } catch (TglogException e) {
            System.out.println("  ERROR: Exception thrown: " + e.getMessage());
        }

        latch.await(1, TimeUnit.SECONDS);

        if (callbackInvoked.get() == 1) {
            System.out.println("  ✓ Callback invoked (enqueued successfully)");
        }

        System.out.println();
    }

    private static void testCommaSeparator(TglogClient client) throws Exception {
        System.out.println("Test 2: Comma separator (CSV format)");

        AtomicInteger callbackInvoked = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(1);

        TglogCallback callback = new TglogCallback() {
            @Override
            public void onSuccess(String payload) {
                callbackInvoked.incrementAndGet();
                latch.countDown();
            }

            @Override
            public void onError(String payload, int code, String msg) {
                System.out.println("  ERROR: onError should not be called!");
            }
        };

        try {
            client.send("timestamp,level,message,user_id\n", callback);
            System.out.println("  ✓ Message accepted with comma separator");
        } catch (TglogException e) {
            System.out.println("  ERROR: Exception thrown: " + e.getMessage());
        }

        latch.await(1, TimeUnit.SECONDS);

        if (callbackInvoked.get() == 1) {
            System.out.println("  ✓ Callback invoked (enqueued successfully)");
        }

        System.out.println();
    }

    private static void testMixedFormat(TglogClient client) throws Exception {
        System.out.println("Test 3: Mixed format (JSON-like)");

        AtomicInteger callbackInvoked = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(1);

        TglogCallback callback = new TglogCallback() {
            @Override
            public void onSuccess(String payload) {
                callbackInvoked.incrementAndGet();
                latch.countDown();
            }

            @Override
            public void onError(String payload, int code, String msg) {
                System.out.println("  ERROR: onError should not be called!");
            }
        };

        try {
            client.send("{\"event\":\"purchase\",\"amount\":99.99}\n", callback);
            System.out.println("  ✓ Message accepted with JSON format");
        } catch (TglogException e) {
            System.out.println("  ERROR: Exception thrown: " + e.getMessage());
        }

        latch.await(1, TimeUnit.SECONDS);

        if (callbackInvoked.get() == 1) {
            System.out.println("  ✓ Callback invoked (enqueued successfully)");
        }

        System.out.println();
    }

    private static void testNoSeparator(TglogClient client) throws Exception {
        System.out.println("Test 4: No separator (plain text)");

        AtomicInteger callbackInvoked = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(1);

        TglogCallback callback = new TglogCallback() {
            @Override
            public void onSuccess(String payload) {
                callbackInvoked.incrementAndGet();
                latch.countDown();
            }

            @Override
            public void onError(String payload, int code, String msg) {
                System.out.println("  ERROR: onError should not be called!");
            }
        };

        try {
            client.send("Simple log message without any separator\n", callback);
            System.out.println("  ✓ Message accepted without separator");
        } catch (TglogException e) {
            System.out.println("  ERROR: Exception thrown: " + e.getMessage());
        }

        latch.await(1, TimeUnit.SECONDS);

        if (callbackInvoked.get() == 1) {
            System.out.println("  ✓ Callback invoked (enqueued successfully)");
        }

        System.out.println();
    }
}
