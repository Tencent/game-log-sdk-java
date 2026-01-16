package com.tencent.tglog;

import io.prometheus.client.CollectorRegistry;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Multi-threaded concurrent send test for TglogClient.
 *
 * This test creates a single TglogClient and sends messages from multiple threads concurrently.
 * After all messages are sent, it prints the metrics to verify consistency.
 */
public class ConcurrentSendTest {

    public static void main(String[] args) {
        // Parse command line arguments
        TestConfig testConfig = parseArgs(args);

        printSeparator();
        System.out.println("TGLog Concurrent Send Test");
        printSeparator();
        System.out.println("Configuration:");
        System.out.println("  Protocol:        " + testConfig.protocol);
        System.out.println("  Host:            " + testConfig.host);
        System.out.println("  Port:            " + testConfig.port);
        System.out.println("  Total Messages:  " + String.format("%,d", testConfig.totalMessages));
        System.out.println("  Sender Threads:  " + testConfig.senderThreads);
        System.out.println("  Messages/Thread: " + String.format("%,d", testConfig.totalMessages / testConfig.senderThreads));
        System.out.println("  Workers:         " + testConfig.workerNum);
        System.out.println("  Queue Capacity:  " + String.format("%,d", testConfig.queueCapacity) + " per worker");
        System.out.println("  Total Capacity:  " + String.format("%,d", testConfig.queueCapacity * testConfig.workerNum));
        System.out.println("  Batch Size:      " + testConfig.batchSize);
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
        // Create custom registry to isolate metrics
        CollectorRegistry registry = new CollectorRegistry();

        // Create configuration
        TglogConfig config = new TglogConfig(testConfig.protocol, testConfig.host, testConfig.port)
                .setWorkerNum(testConfig.workerNum)
                .setMaxPendingMessages(testConfig.queueCapacity)
                .setBatchingMaxMessages(testConfig.batchSize)
                .setBatchingMaxDelayMs(10)
                .setCollectorRegistry(registry)
                .setDebugEnabled(testConfig.debug);

        // Create client
        System.out.println("\nCreating client...");
        TglogClient client = new TglogClient(config);

        // Wait for connections to establish
        Thread.sleep(500);

        System.out.println("Client created successfully");
        System.out.println("\nStarting concurrent send test...\n");

        // Prepare test data
        System.out.println("Preparing test data...");
        String[] logs = new String[Math.min(testConfig.totalMessages / testConfig.senderThreads, 1000)];
        long timestamp = System.currentTimeMillis();
        for (int i = 0; i < logs.length; i++) {
            logs[i] = "gametest|userid=" + i + "|CarryOutItemFlow|/cnprod-a/36/settlementsvr/nanjing/11.176.45.170/1|2025-07-14 13:39:23|wx39308a0a1bd83d22|1|36|18182882159345025898|18182882159345025898|2340077323481036542|11090000053|1|648520772929162293|0|4|0.100|109|1|0|||0|0|||648530419497318933|2340077323481036542||ts=" + timestamp + "\n";
        }
        System.out.println("Test data prepared");

        // Counters for tracking results
        AtomicLong successCount = new AtomicLong(0);
        AtomicLong failureCount = new AtomicLong(0);
        AtomicLong exceptionCount = new AtomicLong(0);

        // Create thread pool
        ExecutorService executor = Executors.newFixedThreadPool(testConfig.senderThreads);
        CountDownLatch startLatch = new CountDownLatch(1);  // All threads wait for this
        CountDownLatch doneLatch = new CountDownLatch(testConfig.senderThreads);  // Main thread waits for all

        int messagesPerThread = testConfig.totalMessages / testConfig.senderThreads;

        // Create sender tasks
        for (int threadId = 0; threadId < testConfig.senderThreads; threadId++) {
            final int tid = threadId;
            executor.submit(() -> {
                try {
                    // Wait for start signal
                    startLatch.await();

                    // Send messages
                    for (int i = 0; i < messagesPerThread; i++) {
                        String log = logs[i % logs.length];

                        try {
                            client.send(log, (payload, exception) -> {
                                if (exception == null) {
                                    successCount.incrementAndGet();
                                } else {
                                    failureCount.incrementAndGet();
                                    if (testConfig.debug) {
                                        System.err.println("Send failed: " + exception.getMessage());
                                    }
                                }
                            });
                        } catch (TglogException e) {
                            exceptionCount.incrementAndGet();
                            if (testConfig.debug) {
                                System.err.println("Thread " + tid + " exception: " + e.getMessage());
                            }
                        }
                    }

                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        // Start all threads simultaneously
        System.out.println("Starting " + testConfig.senderThreads + " sender threads...");
        long startTime = System.currentTimeMillis();
        startLatch.countDown();

        // Wait for all threads to complete
        boolean completed = doneLatch.await(120, TimeUnit.SECONDS);
        if (!completed) {
            System.err.println("Warning: Not all threads completed within timeout");
        }

        long sendEndTime = System.currentTimeMillis();
        long sendDuration = sendEndTime - startTime;

        System.out.println("\nAll threads completed");
        System.out.printf("Send duration: %,d ms\n", sendDuration);
        System.out.printf("Send rate: %,d msg/s\n", testConfig.totalMessages * 1000L / Math.max(sendDuration, 1));

        // Wait for all callbacks to complete
        System.out.println("\nWaiting for all callbacks to complete...");
        long callbackWaitStart = System.currentTimeMillis();
        while (successCount.get() + failureCount.get() < testConfig.totalMessages - exceptionCount.get()) {
            if (System.currentTimeMillis() - callbackWaitStart > 60000) {
                System.err.println("Warning: Not all callbacks completed within 60 seconds");
                break;
            }
            Thread.sleep(100);
        }

        // Flush remaining messages
        System.out.println("Flushing remaining messages...");
        client.flush(30000);

        long endTime = System.currentTimeMillis();
        long totalDuration = endTime - startTime;

        // Print results
        System.out.println();
        printSeparator();
        System.out.println("Test Results");
        printSeparator();

        System.out.printf("Total Duration:       %,d ms (%.2f seconds)\n",
                totalDuration, totalDuration / 1000.0);
        System.out.printf("Overall Throughput:   %,d msg/s\n",
                testConfig.totalMessages * 1000L / Math.max(totalDuration, 1));
        System.out.println();

        System.out.println("=== Callback Counters ===");
        System.out.printf("Callback Success:     %,d\n", successCount.get());
        System.out.printf("Callback Failure:     %,d\n", failureCount.get());
        System.out.printf("Send Exceptions:      %,d\n", exceptionCount.get());
        System.out.printf("Total Accounted:      %,d\n", successCount.get() + failureCount.get() + exceptionCount.get());
        System.out.println();

        // Print metrics using client.getMetricsString()
        System.out.println("=== TGLog Metrics (Human-Readable) ===");
        System.out.println(client.getMetricsString(false));

        // Get metrics for consistency check
        TglogMetrics metrics = client.getMetrics();
        long expectedTotal = testConfig.totalMessages;
        long actualRequested = metrics.getRequestMsgCount();
        long actualEnqueued = metrics.getEnqueuedMsgCount();
        long actualDropped = metrics.getDroppedMsgCount();
        long callbackTotal = successCount.get() + failureCount.get() + exceptionCount.get();

        System.out.println("=== Consistency Check ===");
        System.out.printf("Expected Total:       %,d\n", expectedTotal);
        System.out.printf("Actual Requested:     %,d\n", actualRequested);
        System.out.printf("Callback Total:       %,d\n", callbackTotal);
        System.out.printf("Request = Enqueued + Dropped: %,d = %,d + %,d (%s)\n",
                actualRequested, actualEnqueued, actualDropped,
                (actualRequested == actualEnqueued + actualDropped) ? "✓ PASS" : "✗ FAIL");

        printSeparator();

        // Print JSON metrics if debug enabled
        if (testConfig.debug) {
            System.out.println("\nDetailed Metrics (JSON):");
            System.out.println(client.getMetricsString(true));
        }

        // Shutdown executor
        executor.shutdown();
        executor.awaitTermination(5, TimeUnit.SECONDS);

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
                    config.totalMessages = Integer.parseInt(args[++i]);
                    break;
                case "-t":
                case "--threads":
                    config.senderThreads = Integer.parseInt(args[++i]);
                    break;
                case "-w":
                case "--workers":
                    config.workerNum = Integer.parseInt(args[++i]);
                    break;
                case "-b":
                case "--batch-size":
                    config.batchSize = Integer.parseInt(args[++i]);
                    break;
                case "-q":
                case "--queue-capacity":
                    config.queueCapacity = Integer.parseInt(args[++i]);
                    break;
                case "--tcp":
                    config.protocol = TglogConfig.Protocol.TCP;
                    break;
                case "--udp":
                    config.protocol = TglogConfig.Protocol.UDP;
                    break;
                case "-d":
                case "--debug":
                    config.debug = true;
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
        for (int i = 0; i < 70; i++) {
            System.out.print("=");
        }
        System.out.println();
    }

    private static void printUsage() {
        System.out.println("Usage: java ConcurrentSendTest [options]");
        System.out.println();
        System.out.println("Options:");
        System.out.println("  -h, --host <host>        Server host (default: dev.tglog.com)");
        System.out.println("  -p, --port <port>        Server port (default: 20001)");
        System.out.println("  -m, --messages <count>   Total number of messages (default: 100000)");
        System.out.println("  -t, --threads <count>    Number of sender threads (default: 10)");
        System.out.println("  -w, --workers <count>    Number of worker threads (default: 8)");
        System.out.println("  -q, --queue-capacity <size>  Queue capacity per worker (default: 2000000)");
        System.out.println("  -b, --batch-size <size>  Batch size (default: 50)");
        System.out.println("  --tcp                    Use TCP protocol (default)");
        System.out.println("  --udp                    Use UDP protocol");
        System.out.println("  -d, --debug              Enable debug logging");
        System.out.println("  --help                   Show this help message");
    }

    static class TestConfig {
        String host = "dev.tglog.com";
        int port = 20001;
        int totalMessages = 100000;
        int senderThreads = 10;
        int workerNum = 8;
        int queueCapacity = 2000000;  // 2M per worker (default)
        int batchSize = 50;
        TglogConfig.Protocol protocol = TglogConfig.Protocol.TCP;
        boolean debug = false;
    }
}
