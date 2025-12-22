package com.tencent.tglog;

import com.google.common.util.concurrent.RateLimiter;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Performance test for TglogClient.
 *
 * This test demonstrates the usage and performance of the pure Java TGLog client.
 */
public class TglogClientTest {

    public static void main(String[] args) {
        // Parse command line arguments
        TestConfig testConfig = parseArgs(args);

        printSeparator();
        System.out.println("TGLog Pure Java Client Performance Test");
        printSeparator();
        System.out.println("Configuration:");
        System.out.println("  Protocol:      " + testConfig.protocol);
        System.out.println("  Host:          " + testConfig.host);
        System.out.println("  Port:          " + testConfig.port);
        System.out.println("  Messages:      " + String.format("%,d", testConfig.messageCount));
        System.out.println("  Workers:       " + testConfig.workerNum);
        System.out.println("  Batch Size:    " + testConfig.batchSize);
        System.out.println("  With Callback: " + testConfig.withCallback);
        System.out.println("  Rate Limit:    " + (testConfig.rateLimit > 0 ?
            String.format("%,d msg/s", testConfig.rateLimit) : "unlimited"));
        printSeparator();

        try {
            runTest(testConfig);
        } catch (Exception e) {
            System.err.println("Test failed: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static void runTest(TestConfig testConfig) throws Exception {
        // Create configuration
        TglogConfig config = new TglogConfig(testConfig.protocol, testConfig.host, testConfig.port)
                .setWorkerNum(testConfig.workerNum)
                .setBatchingMaxMessages(testConfig.batchSize)
                .setBatchingMaxDelayMs(10)
                .setDebugEnabled(testConfig.debug);

        // Create client
        System.out.println("\nCreating client...");
        TglogClient client = new TglogClient(config);

        // Wait a bit for connections to establish
        Thread.sleep(500);

        System.out.println("Client created successfully");
        System.out.println("\nStarting performance test...\n");

        // Prepare test data - pre-generate log strings for better performance
        System.out.println("Preparing test data...");
        String[] logs = new String[Math.min(testConfig.messageCount, 10000)];
        long timestamp = System.currentTimeMillis();
        for (int i = 0; i < logs.length; i++) {
            logs[i] = "gametest|userid=" + i + "|CarryOutItemFlow|/cnprod-a/36/settlementsvr/nanjing/11.176.45.170/1|2025-07-14 13:39:23|wx39308a0a1bd83d22|1|36|18182882159345025898|18182882159345025898|2340077323481036542|11090000053|1|648520772929162293|0|4|0.100|109|1|0|||0|0|||648530419497318933|2340077323481036542||ts=" + timestamp + "\n";
        }
        System.out.println("Test data prepared");

        // Counters
        AtomicLong successCount = new AtomicLong(0);
        AtomicLong failureCount = new AtomicLong(0);
        CountDownLatch latch = testConfig.withCallback ?
                new CountDownLatch(testConfig.messageCount) : null;

        // Callback
        TglogCallback callback = testConfig.withCallback ? new TglogCallback() {
            @Override
            public void onSuccess(String payload) {
                successCount.incrementAndGet();
                latch.countDown();
            }

            @Override
            public void onError(String payload, int errorCode, String errorMessage) {
                failureCount.incrementAndGet();
                System.err.println("Send failed: " + errorMessage);
                latch.countDown();
            }
        } : null;

        // Create rate limiter if rate limit is specified
        RateLimiter rateLimiter = testConfig.rateLimit > 0 ?
                RateLimiter.create(testConfig.rateLimit) : null;

        // Send messages
        long startTime = System.currentTimeMillis();
        long lastReportTime = startTime;
        int reportInterval = testConfig.messageCount / 10; // Report every 10%

        for (int i = 0; i < testConfig.messageCount; i++) {
            // Apply rate limiting if enabled
            if (rateLimiter != null) {
                rateLimiter.acquire();
            }

            String log = logs[i % logs.length];

            try {
                client.send(log, callback);
            } catch (TglogException e) {
                System.err.println("Failed to send message " + i + ": " + e.getMessage());
                failureCount.incrementAndGet();
                if (latch != null) {
                    latch.countDown();
                }
            }

            // Progress report
            if ((i + 1) % reportInterval == 0) {
                long now = System.currentTimeMillis();
                double progress = (i + 1) * 100.0 / testConfig.messageCount;
                long elapsed = now - lastReportTime;
                long rate = reportInterval * 1000 / Math.max(elapsed, 1);
                System.out.printf("Progress: %.0f%% (%,d messages) - Rate: %,d msg/s\n",
                        progress, i + 1, rate);
                lastReportTime = now;
            }
        }

        long sendEndTime = System.currentTimeMillis();
        long sendDuration = sendEndTime - startTime;

        System.out.println("\nAll messages sent to queue");
        System.out.printf("Send duration: %,d ms\n", sendDuration);
        System.out.printf("Send rate: %,d msg/s\n", (long)testConfig.messageCount * 1000 / Math.max(sendDuration, 1));

        // Wait for callbacks if enabled
        if (testConfig.withCallback) {
            System.out.println("\nWaiting for all callbacks...");
            boolean completed = latch.await(60, TimeUnit.SECONDS);
            if (!completed) {
                System.err.println("Warning: Not all callbacks completed within timeout");
            }
        } else {
            // Wait for queues to drain
            System.out.println("\nWaiting for queues to drain...");
            client.flush(30000);
        }

        long endTime = System.currentTimeMillis();
        long totalDuration = endTime - startTime;

        // Print results
        System.out.println();
        printSeparator();
        System.out.println("Test Results");
        printSeparator();

        TglogMetrics metrics = client.getMetrics();

        System.out.printf("Total Duration:     %,d ms (%.2f seconds)\n",
                totalDuration, totalDuration / 1000.0);
        System.out.printf("Overall Throughput: %,d msg/s\n",
                (long)testConfig.messageCount * 1000 / Math.max(totalDuration, 1));
        System.out.println();
        System.out.printf("Messages Requested: %,d\n", metrics.getRequestMsgCount());
        System.out.printf("Messages Enqueued:  %,d\n", metrics.getEnqueuedMsgCount());
        System.out.printf("Messages Succeeded: %,d\n", metrics.getSucceededMsgCount());
        System.out.printf("Messages Failed:    %,d\n", metrics.getFailedMsgCount());
        System.out.printf("Messages Dropped:   %,d\n", metrics.getDroppedMsgCount());
        System.out.printf("Success Rate:       %.2f%%\n", metrics.getSuccessRate() * 100);
        System.out.println();
        System.out.printf("Connections Opened: %,d\n", metrics.getOpenedConnCount());
        System.out.printf("Connections Closed: %,d\n", metrics.getClosedConnCount());
        System.out.printf("Connection Errors:  %,d\n", metrics.getErrorConnCount());

        if (testConfig.withCallback) {
            System.out.println();
            System.out.printf("Callback Success:   %,d\n", successCount.get());
            System.out.printf("Callback Failure:   %,d\n", failureCount.get());
        }

        printSeparator();

        // Print detailed metrics if debug enabled
        if (testConfig.debug) {
            System.out.println("\nDetailed Metrics:");
            System.out.println(client.getMetricsString(false));
            System.out.println("\nMetrics JSON:");
            System.out.println(client.getMetricsString(true));
        }

        // Close client
        System.out.println("\nClosing client...");
        client.close();
        System.out.println("Client closed");
    }

    private static TestConfig parseArgs(String[] args) {
        TestConfig config = new TestConfig();

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "-h":
                case "--host":
                    config.host = args[++i];
                    break;
                case "-p":
                case "--port":
                    config.port = Integer.parseInt(args[++i]);
                    break;
                case "-m":
                case "--messages":
                    config.messageCount = Integer.parseInt(args[++i]);
                    break;
                case "-w":
                case "--workers":
                    config.workerNum = Integer.parseInt(args[++i]);
                    break;
                case "-b":
                case "--batch-size":
                    config.batchSize = Integer.parseInt(args[++i]);
                    break;
                case "--tcp":
                    config.protocol = TglogConfig.Protocol.TCP;
                    break;
                case "--udp":
                    config.protocol = TglogConfig.Protocol.UDP;
                    break;
                case "-c":
                case "--callback":
                    config.withCallback = true;
                    break;
                case "-d":
                case "--debug":
                    config.debug = true;
                    break;
                case "-r":
                case "--rate-limit":
                    config.rateLimit = Long.parseLong(args[++i]);
                    break;
                case "--help":
                    printUsage();
                    System.exit(0);
                    break;
                default:
                    System.err.println("Unknown option: " + args[i]);
                    printUsage();
                    System.exit(1);
            }
        }

        return config;
    }

    private static void printSeparator() {
        for (int i = 0; i < 60; i++) {
            System.out.print("=");
        }
        System.out.println();
    }

    private static void printUsage() {
        System.out.println("Usage: java TglogClientTest [options]");
        System.out.println();
        System.out.println("Options:");
        System.out.println("  -h, --host <host>        Server host (default: dev.tglog.com)");
        System.out.println("  -p, --port <port>        Server port (default: 20001)");
        System.out.println("  -m, --messages <count>   Number of messages to send (default: 100000)");
        System.out.println("  -w, --workers <count>    Number of worker threads (default: 8)");
        System.out.println("  -b, --batch-size <size>  Batch size (default: 50)");
        System.out.println("  -r, --rate-limit <rate>  Rate limit in messages/sec (default: 500000, 0 = unlimited)");
        System.out.println("  --tcp                    Use TCP protocol (default)");
        System.out.println("  --udp                    Use UDP protocol");
        System.out.println("  -c, --callback           Enable callbacks (slower but tracks results)");
        System.out.println("  -d, --debug              Enable debug logging");
        System.out.println("  --help                   Show this help message");
    }

    static class TestConfig {
        String host = "dev.tglog.com";
        int port = 20001;
        int messageCount = 100000;
        int workerNum = 8;
        int batchSize = 50;
        long rateLimit = 500000; // Default: 500,000 messages/sec
        TglogConfig.Protocol protocol = TglogConfig.Protocol.TCP;
        boolean withCallback = false;
        boolean debug = false;
    }
}
