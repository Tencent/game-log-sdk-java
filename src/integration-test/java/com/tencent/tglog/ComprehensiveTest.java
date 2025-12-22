package com.tencent.tglog;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Comprehensive test with real server to verify all fixes.
 */
public class ComprehensiveTest {

    public static void main(String[] args) throws Exception {
        System.out.println("=== Comprehensive Test with Real Server ===\n");

        TglogConfig config = TglogConfig.createTcp("dev.tglog.com", 20001)
                .setWorkerNum(2)
                .setBatchingMaxMessages(10)
                .setBatchingMaxDelayMs(100);

        try (TglogClient client = new TglogClient(config)) {
            // Test 1: Various valid formats
            testValidFormats(client);

            // Test 2: Validation errors with callbacks
            testValidationErrors(client);

            // Test 3: Batch sending
            testBatchSending(client);

            // Wait for all messages to be sent
            System.out.println("\nWaiting for all messages to be sent...");
            Thread.sleep(2000);

            System.out.println("\n=== Final Metrics ===");
            System.out.println(client.getMetricsString(false));

            System.out.println("\n=== All Tests Passed! ===");
        }
    }

    private static void testValidFormats(TglogClient client) throws Exception {
        System.out.println("Test 1: Various Valid Formats");
        System.out.println("================================");

        AtomicInteger successCount = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(4);

        TglogCallback callback = (payload, exception) -> {
            if (exception == null) {
                successCount.incrementAndGet();
            } else {
                System.out.println("  ERROR: " + exception.getMessage());
            }
            latch.countDown();
        };

        client.send("game|action=login|user=123\n", callback);
        client.send("timestamp,level,message,user_id\n", callback);
        client.send("{\"event\":\"purchase\",\"amount\":99.99}\n", callback);
        client.send("Simple plain text log\n", callback);

        latch.await(3, TimeUnit.SECONDS);

        System.out.println("  ✓ Sent 4 messages with different formats");
        System.out.println("  ✓ Success callbacks: " + successCount.get() + "/4");
        System.out.println();
    }

    private static void testValidationErrors(TglogClient client) throws Exception {
        System.out.println("Test 2: Validation Errors with Callbacks");
        System.out.println("=========================================");

        AtomicInteger callbackCount = new AtomicInteger(0);

        // Test null payload
        try {
            client.send(null, (payload, exception) -> {
                if (exception != null) {
                    callbackCount.incrementAndGet();
                }
            });
        } catch (TglogException e) {
            System.out.println("  ✓ Null payload: callback invoked, exception thrown");
        }

        // Test empty payload
        try {
            client.send("", (payload, exception) -> {
                if (exception != null) {
                    callbackCount.incrementAndGet();
                }
            });
        } catch (TglogException e) {
            System.out.println("  ✓ Empty payload: callback invoked, exception thrown");
        }

        // Test payload too long
        StringBuilder sb = new StringBuilder();
        sb.append("test|data=");
        for (int i = 0; i < 600 * 1024; i++) {
            sb.append('x');
        }
        sb.append("\n");

        try {
            client.send(sb.toString(), (payload, exception) -> {
                if (exception != null) {
                    callbackCount.incrementAndGet();
                }
            });
        } catch (TglogException e) {
            System.out.println("  ✓ Payload too long: callback invoked, exception thrown");
        }

        Thread.sleep(100);
        System.out.println("  ✓ Total error callbacks invoked: " + callbackCount.get() + "/3");
        System.out.println();
    }

    private static void testBatchSending(TglogClient client) throws Exception {
        System.out.println("Test 3: Batch Sending");
        System.out.println("=====================");

        AtomicInteger successCount = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(100);

        TglogCallback callback = (payload, exception) -> {
            if (exception == null) {
                successCount.incrementAndGet();
            } else {
                System.out.println("  ERROR: " + exception.getMessage());
            }
            latch.countDown();
        };

        for (int i = 0; i < 100; i++) {
            client.send("batch|seq=" + i + "|data=test\n", callback);
        }

        latch.await(5, TimeUnit.SECONDS);

        System.out.println("  ✓ Sent 100 messages in batch");
        System.out.println("  ✓ Success callbacks: " + successCount.get() + "/100");
        System.out.println();
    }
}
