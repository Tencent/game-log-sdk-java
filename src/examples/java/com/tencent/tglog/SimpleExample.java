package com.tencent.tglog;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Simple example demonstrating TglogClient usage.
 */
public class SimpleExample {

    public static void main(String[] args) throws Exception {
        long programStart = System.currentTimeMillis();

        System.out.println("TGLog Simple Example");
        System.out.println("====================\n");

        // Create configuration
        long t0 = System.currentTimeMillis();
        TglogConfig config = TglogConfig.createTcp("dev.tglog.com", 20001)
                .setWorkerNum(4)
                .setBatchingMaxMessages(50)
                .setLogger(new TglogLogger.DefaultLogger());

        // Create client (AutoCloseable)
        long t1 = System.currentTimeMillis();
        try (TglogClient client = new TglogClient(config)) {
            long t2 = System.currentTimeMillis();
            System.out.printf("Client created in %d ms: %s\n", t2 - t1, client);
            System.out.println();

            // Example 1: Send without callback (fire and forget)
            System.out.println("Example 1: Fire and forget (10 messages)");
            for (int i = 0; i < 10; i++) {
                String log = String.format("test|userid=%d|action=login|timestamp=%d\n",
                        i, System.currentTimeMillis());
                client.send(log);
            }
            System.out.println("Sent 10 messages without callback\n");

            // Example 2: Send with callback
            System.out.println("Example 2: With callback (10 messages)");
            CountDownLatch latch2 = new CountDownLatch(10);
            AtomicInteger successCount2 = new AtomicInteger(0);
            AtomicInteger failCount2 = new AtomicInteger(0);

            for (int i = 0; i < 10; i++) {
                final int userId = i + 100;
                String log = String.format("test|userid=%d|action=battle|result=win\n", userId);

                client.send(log, new TglogCallback() {
                    @Override
                    public void onSuccess(String payload) {
                        successCount2.incrementAndGet();
                        System.out.println("  Success: user " + userId);
                        latch2.countDown();
                    }

                    @Override
                    public void onError(String payload, int errorCode, String errorMessage) {
                        failCount2.incrementAndGet();
                        System.err.println("  Failed: user " + userId + " - " + errorMessage);
                        latch2.countDown();
                    }
                });
            }

            // Wait for all callbacks to complete
            boolean completed2 = latch2.await(5, TimeUnit.SECONDS);
            System.out.printf("Callbacks completed: %d success, %d failed\n\n",
                    successCount2.get(), failCount2.get());

            // Example 3: Bulk send with callback tracking
            System.out.println("Example 3: Bulk send with callback (1000 messages)");
            CountDownLatch latch3 = new CountDownLatch(1000);
            AtomicInteger successCount3 = new AtomicInteger(0);
            AtomicInteger failCount3 = new AtomicInteger(0);

            long startTime = System.currentTimeMillis();

            for (int i = 0; i < 1000; i++) {
                String log = String.format("bulk|seq=%d|data=test_data_%d\n", i, i);
                client.send(log, new TglogCallback() {
                    @Override
                    public void onSuccess(String payload) {
                        successCount3.incrementAndGet();
                        latch3.countDown();
                    }

                    @Override
                    public void onError(String payload, int errorCode, String errorMessage) {
                        failCount3.incrementAndGet();
                        latch3.countDown();
                    }
                });
            }

            long sendDuration = System.currentTimeMillis() - startTime;
            System.out.printf("Sent 1000 messages in %d ms (%.0f msg/s)\n",
                    sendDuration, 1000.0 * 1000 / Math.max(sendDuration, 1));

            // Wait for all callbacks to complete
            System.out.println("Waiting for all callbacks to complete...");
            boolean completed3 = latch3.await(10, TimeUnit.SECONDS);

            long totalDuration = System.currentTimeMillis() - startTime;
            System.out.printf("All callbacks completed in %d ms\n", totalDuration);
            System.out.printf("Results: %d success, %d failed\n\n",
                    successCount3.get(), failCount3.get());

            // Print metrics
            System.out.println("Performance Metrics:");
            System.out.println(client.printMetrics());

            // Verify metrics
            TglogMetrics metrics = client.getMetrics();
            long totalWithCallback = successCount2.get() + successCount3.get();
            long totalRequested = metrics.getRequestMsgCount();
            long totalSucceeded = metrics.getSucceededMsgCount();

            System.out.printf("\nVerification:\n");
            System.out.printf("  Total requested:  %d\n", totalRequested);
            System.out.printf("  Total succeeded:  %d\n", totalSucceeded);
            System.out.printf("  With callback:    %d (Example 2 + 3)\n", totalWithCallback);
            System.out.printf("  Without callback: %d (Example 1)\n", totalSucceeded - totalWithCallback);
            System.out.printf("  Success rate:     %.2f%%\n", metrics.getSuccessRate() * 100);

        } // Client automatically closed here

        long programEnd = System.currentTimeMillis();
        System.out.printf("\nTotal program duration: %d ms\n", programEnd - programStart);
        System.out.println("Example completed");
    }
}
