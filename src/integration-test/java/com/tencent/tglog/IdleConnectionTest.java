package com.tencent.tglog;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Idle connection test for TglogClient.
 *
 * This test creates a TglogClient, waits for a configurable idle period (default 5 minutes),
 * then sends messages to verify that connections still work after being idle.
 *
 * Usage:
 *   java IdleConnectionTest [options]
 *
 * Options:
 *   -h, --host <host>           Server host (default: 127.0.0.1)
 *   -p, --port <port>           Server port (default: 8080)
 *   --tcp                       Use TCP protocol (default)
 *   --udp                       Use UDP protocol
 *   -w, --workers <num>         Number of worker threads (default: 8)
 *   -i, --idle <seconds>        Idle time in seconds before sending (default: 300 = 5 minutes)
 *   -m, --messages <num>        Number of messages to send after idle (default: 1000)
 *   -b, --batch <size>          Batch size (default: 50)
 *   --debug                     Enable debug logging
 *   --help                      Show this help message
 */
public class IdleConnectionTest {

    public static void main(String[] args) {
        // Parse command line arguments
        TestConfig testConfig = parseArgs(args);

        printSeparator();
        System.out.println("TGLog Idle Connection Test");
        printSeparator();
        System.out.println("Configuration:");
        System.out.println("  Protocol:      " + testConfig.protocol);
        System.out.println("  Host:          " + testConfig.host);
        System.out.println("  Port:          " + testConfig.port);
        System.out.println("  Workers:       " + testConfig.workerNum);
        System.out.println("  Idle Time:     " + testConfig.idleSeconds + " seconds (" +
                          formatDuration(testConfig.idleSeconds) + ")");
        System.out.println("  Messages:      " + String.format("%,d", testConfig.messageCount));
        System.out.println("  Batch Size:    " + testConfig.batchSize);
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
        System.out.println("\n[Phase 1] Creating client...");
        long createStartTime = System.currentTimeMillis();
        TglogClient client = new TglogClient(config);
        long createEndTime = System.currentTimeMillis();

        // Wait a bit for connections to establish
        Thread.sleep(500);

        System.out.println("Client created successfully in " + (createEndTime - createStartTime) + "ms");

        // Print initial metrics
        System.out.println("\nInitial metrics:");
        System.out.println(client.getMetricsString(false));

        // Idle phase
        System.out.println("\n[Phase 2] Idle phase - waiting for " + testConfig.idleSeconds + " seconds...");
        System.out.println("Start time: " + new java.util.Date());

        long idleStartTime = System.currentTimeMillis();
        int reportIntervalSeconds = Math.max(1, testConfig.idleSeconds / 10); // Report every 10%

        for (int elapsed = 0; elapsed < testConfig.idleSeconds; elapsed++) {
            Thread.sleep(1000);

            // Report progress periodically
            if ((elapsed + 1) % reportIntervalSeconds == 0 || elapsed == testConfig.idleSeconds - 1) {
                int progress = (int) (((elapsed + 1) * 100.0) / testConfig.idleSeconds);
                System.out.println("  Idle progress: " + progress + "% (" + (elapsed + 1) + "/" +
                                 testConfig.idleSeconds + " seconds)");
            }
        }

        long idleEndTime = System.currentTimeMillis();
        long actualIdleMs = idleEndTime - idleStartTime;

        System.out.println("\nIdle phase completed");
        System.out.println("End time: " + new java.util.Date());
        System.out.println("Actual idle duration: " + actualIdleMs + "ms (" + formatDuration(actualIdleMs / 1000) + ")");

        System.out.println("\nMetrics after idle:");
        System.out.println(client.getMetricsString(false));

        // Prepare test data
        System.out.println("\n[Phase 3] Preparing test data...");
        String[] logs = new String[Math.min(testConfig.messageCount, 10000)];
        long timestamp = System.currentTimeMillis();
        for (int i = 0; i < logs.length; i++) {
            logs[i] = "gametest|userid=" + i + "|IdleConnectionTest|/test/idle/connection|" +
                     new java.util.Date() + "|test|1|36|" + i + "|" + i + "|" + timestamp +
                     "|item|1|" + i + "|0|1|1.0|100|1|0|||0|0|||" + i + "|" + timestamp +
                     "||ts=" + timestamp + "|idle=" + actualIdleMs + "ms\n";
        }
        System.out.println("Test data prepared");

        // Counters
        AtomicLong successCount = new AtomicLong(0);
        AtomicLong failureCount = new AtomicLong(0);
        CountDownLatch latch = new CountDownLatch(testConfig.messageCount);

        // Callback to track success/failure
        TglogCallback callback = (payload, exception) -> {
            if (exception == null) {
                successCount.incrementAndGet();
            } else {
                failureCount.incrementAndGet();
                System.err.println("Send failed: " + exception.getMessage());
            }
            latch.countDown();
        };

        // Send messages
        System.out.println("\n[Phase 4] Sending messages after idle period...");
        long sendStartTime = System.currentTimeMillis();
        long lastReportTime = sendStartTime;
        int reportInterval = Math.max(1, testConfig.messageCount / 10); // Report every 10%

        for (int i = 0; i < testConfig.messageCount; i++) {
            String log = logs[i % logs.length];

            try {
                client.send(log, callback);
            } catch (TglogException e) {
                System.err.println("Failed to send message " + i + ": " + e.getMessage());
                failureCount.incrementAndGet();
                latch.countDown();
            }

            // Report progress
            if ((i + 1) % reportInterval == 0 || i == testConfig.messageCount - 1) {
                long now = System.currentTimeMillis();
                long elapsed = now - sendStartTime;
                double rate = (i + 1) * 1000.0 / elapsed;
                int progress = (int) (((i + 1) * 100.0) / testConfig.messageCount);

                System.out.println("  Progress: " + progress + "% (" + String.format("%,d", i + 1) +
                                 "/" + String.format("%,d", testConfig.messageCount) +
                                 ") - Rate: " + String.format("%,.0f", rate) + " msg/s");
                lastReportTime = now;
            }
        }

        long sendEndTime = System.currentTimeMillis();
        long sendDuration = sendEndTime - sendStartTime;

        System.out.println("\nAll messages enqueued in " + sendDuration + "ms");
        System.out.println("Enqueue rate: " + String.format("%,.0f", testConfig.messageCount * 1000.0 / sendDuration) + " msg/s");

        // Wait for all callbacks
        System.out.println("\nWaiting for all messages to be sent (timeout: 60 seconds)...");
        boolean completed = latch.await(60, TimeUnit.SECONDS);

        long totalDuration = System.currentTimeMillis() - sendStartTime;

        if (!completed) {
            System.err.println("\nWARNING: Timeout waiting for all messages to complete!");
            System.err.println("Remaining messages: " + latch.getCount());
        }

        // Print results
        printSeparator();
        System.out.println("Test Results");
        printSeparator();
        System.out.println("Idle Duration:     " + formatDuration(actualIdleMs / 1000));
        System.out.println("Messages Sent:     " + String.format("%,d", testConfig.messageCount));
        System.out.println("Send Duration:     " + sendDuration + "ms");
        System.out.println("Total Duration:    " + totalDuration + "ms");
        System.out.println("Enqueue Rate:      " + String.format("%,.0f", testConfig.messageCount * 1000.0 / sendDuration) + " msg/s");
        System.out.println("Overall Rate:      " + String.format("%,.0f", testConfig.messageCount * 1000.0 / totalDuration) + " msg/s");
        System.out.println("Success Count:     " + String.format("%,d", successCount.get()));
        System.out.println("Failure Count:     " + String.format("%,d", failureCount.get()));
        System.out.println("Success Rate:      " + String.format("%.2f%%", successCount.get() * 100.0 / testConfig.messageCount));
        printSeparator();

        // Print final metrics
        System.out.println("\nFinal Metrics:");
        System.out.println(client.getMetricsString(false));

        // Close client
        System.out.println("\nClosing client...");
        client.close();
        System.out.println("Client closed");

        // Exit with appropriate code
        if (failureCount.get() > 0) {
            System.err.println("\nTest FAILED: " + failureCount.get() + " messages failed");
            System.exit(1);
        } else {
            System.out.println("\nTest PASSED: All messages sent successfully after " +
                             formatDuration(actualIdleMs / 1000) + " idle period");
        }
    }

    private static TestConfig parseArgs(String[] args) {
        TestConfig config = new TestConfig();
        config.protocol = TglogConfig.Protocol.TCP;
        config.host = "dev.tglog.com";
        config.port = 20001;
        config.workerNum = 8;
        config.idleSeconds = 300; // 5 minutes default
        config.messageCount = 1000;
        config.batchSize = 50;
        config.debug = false;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "-h":
                case "--host":
                    if (i + 1 < args.length) {
                        config.host = args[++i];
                    }
                    break;
                case "-p":
                case "--port":
                    if (i + 1 < args.length) {
                        config.port = Integer.parseInt(args[++i]);
                    }
                    break;
                case "--tcp":
                    config.protocol = TglogConfig.Protocol.TCP;
                    break;
                case "--udp":
                    config.protocol = TglogConfig.Protocol.UDP;
                    break;
                case "-w":
                case "--workers":
                    if (i + 1 < args.length) {
                        config.workerNum = Integer.parseInt(args[++i]);
                    }
                    break;
                case "-i":
                case "--idle":
                    if (i + 1 < args.length) {
                        config.idleSeconds = Integer.parseInt(args[++i]);
                    }
                    break;
                case "-m":
                case "--messages":
                    if (i + 1 < args.length) {
                        config.messageCount = Integer.parseInt(args[++i]);
                    }
                    break;
                case "-b":
                case "--batch":
                    if (i + 1 < args.length) {
                        config.batchSize = Integer.parseInt(args[++i]);
                    }
                    break;
                case "-d":
                case "--debug":
                    config.debug = true;
                    break;
                case "--help":
                    printHelp();
                    System.exit(0);
                    break;
                default:
                    System.err.println("Unknown option: " + args[i]);
                    printHelp();
                    System.exit(1);
            }
        }

        return config;
    }

    private static void printHelp() {
        System.out.println("Usage: java IdleConnectionTest [options]");
        System.out.println();
        System.out.println("Options:");
        System.out.println("  -h, --host <host>       Server host (default: dev.tglog.com)");
        System.out.println("  -p, --port <port>       Server port (default: 20001)");
        System.out.println("  --tcp                   Use TCP protocol (default)");
        System.out.println("  --udp                   Use UDP protocol");
        System.out.println("  -w, --workers <num>     Number of worker threads (default: 8)");
        System.out.println("  -i, --idle <seconds>    Idle time in seconds before sending (default: 300 = 5 minutes)");
        System.out.println("  -m, --messages <num>    Number of messages to send after idle (default: 1000)");
        System.out.println("  -b, --batch <size>      Batch size (default: 50)");
        System.out.println("  -d, --debug             Enable debug logging");
        System.out.println("  --help                  Show this help message");
        System.out.println();
        System.out.println("Examples:");
        System.out.println("  # Test with default 5 minutes idle");
        System.out.println("  java IdleConnectionTest");
        System.out.println();
        System.out.println("  # Test with custom host and port");
        System.out.println("  java IdleConnectionTest -h logs.game.com -p 8080");
        System.out.println();
        System.out.println("  # Test with 10 minutes idle");
        System.out.println("  java IdleConnectionTest -i 600");
        System.out.println();
        System.out.println("  # Test with 30 seconds idle (quick test)");
        System.out.println("  java IdleConnectionTest -i 30 -m 100");
        System.out.println();
        System.out.println("  # Test with 1 hour idle");
        System.out.println("  java IdleConnectionTest -i 3600");
    }

    private static void printSeparator() {
        System.out.println("================================================================");
    }

    private static String formatDuration(long seconds) {
        if (seconds < 60) {
            return seconds + " seconds";
        } else if (seconds < 3600) {
            long minutes = seconds / 60;
            long secs = seconds % 60;
            return minutes + " minutes " + secs + " seconds";
        } else {
            long hours = seconds / 3600;
            long minutes = (seconds % 3600) / 60;
            long secs = seconds % 60;
            return hours + " hours " + minutes + " minutes " + secs + " seconds";
        }
    }

    private static class TestConfig {
        TglogConfig.Protocol protocol;
        String host;
        int port;
        int workerNum;
        int idleSeconds;
        int messageCount;
        int batchSize;
        boolean debug;
    }
}
