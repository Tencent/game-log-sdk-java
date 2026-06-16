# TGLog Java SDK - Quick Start

## Get Started in 5 Minutes

### 1. Compile Project

```bash
./compile.sh
```

Or use Maven:
```bash
mvn clean compile
```

### 2. Run Simple Example

```bash
./run-example.sh
```

This will run a simple example demonstrating how to use TglogClient.

### 3. Run Performance Tests

```bash
# Benchmark test (100k messages)
./run-test.sh

# 1 million messages test
./run-test.sh -m 1000000

# With callback test
./run-test.sh -m 1000000 -c
```

## Using in Your Project

### Method 1: Using Maven

Add the following dependency to your `pom.xml`:

```xml
<dependency>
    <groupId>com.tencent.tglog</groupId>
    <artifactId>tglog-java</artifactId>
    <version>1.0.2</version>
</dependency>
```

Then run:
```bash
mvn clean install
```

### Method 2: Package as JAR

```bash
# Package with Maven
mvn clean package

# JAR file will be in target/ directory
# Use the JAR
java -cp target/tglog-java-1.0.2.jar:your-app.jar YourApp
```

### Method 3: Use Compiled Classes Directly

```bash
# Copy compiled classes to your project
cp -r target/classes/com/tencent/tglog your-project/lib/

# Reference in your project
javac -cp lib YourApp.java
java -cp lib:. YourApp
```

## Code Examples

### Simplest Usage

```java
import com.tencent.tglog.*;

public class QuickStart {
    public static void main(String[] args) throws Exception {
        // Create client
        TglogConfig config = TglogConfig.createTcp("dev.tglog.com", 20001);
        TglogClient client = new TglogClient(config);

        // Send log
        client.send("test|hello=world\n");

        // Close client
        client.close();
    }
}
```

### With Callback

```java
import com.tencent.tglog.*;

public class WithCallback {
    public static void main(String[] args) throws Exception {
        TglogConfig config = TglogConfig.createTcp("dev.tglog.com", 20001);
        TglogClient client = new TglogClient(config);

        // Send log with callback (Lambda style)
        client.send("test|userid=123\n", (payload, exception) -> {
            if (exception == null) {
                System.out.println("Sent successfully: " + payload);
            } else {
                System.err.println("Send failed: " + exception.getMessage());
            }
        });

        // Or use CompletableFuture
        client.send("test|data=value\n")
              .thenRun(() -> System.out.println("Sent successfully"))
              .exceptionally(ex -> {
                  System.err.println("Send failed: " + ex.getMessage());
                  return null;
              });

        // Wait for completion
        Thread.sleep(1000);
        client.close();
    }
}
```

### Game Server Integration

```java
import com.tencent.tglog.*;

public class GameServer {
    private TglogClient logClient;

    public void init() throws TglogException {
        // Create configuration
        TglogConfig config = TglogConfig.createTcp("logs.game.com", 8080)
            .setWorkerNum(8)                    // 8 worker threads
            .setBatchingMaxMessages(50)         // 50 messages per batch
            .setBatchingMaxDelayMs(10);         // Max 10ms delay

        // Create client
        logClient = new TglogClient(config);
    }

    // Player login
    public void onPlayerLogin(int userId, String username) {
        String log = String.format("login|userid=%d|username=%s|timestamp=%d\n",
            userId, username, System.currentTimeMillis());

        try {
            logClient.send(log);
        } catch (TglogException e) {
            // Queue full or other error, log locally
            System.err.println("Failed to send log: " + e.getMessage());
        }
    }

    // Battle end
    public void onBattleEnd(int winnerId, int loserId, int score) {
        String log = String.format("battle|winner=%d|loser=%d|score=%d\n",
            winnerId, loserId, score);

        try {
            logClient.send(log, (payload, exception) -> {
                if (exception == null) {
                    // Success, can update database
                    System.out.println("Battle log sent");
                } else {
                    // Failed, save to local file
                    saveToLocalFile(payload);
                }
            });
        } catch (TglogException e) {
            saveToLocalFile(log);
        }
    }

    private void saveToLocalFile(String log) {
        // Logic to save to local file
    }

    public void shutdown() {
        // Close client, auto-flush all pending messages
        logClient.close();
    }
}
```

## Configuration Guide

### Basic Configuration

```java
TglogConfig config = TglogConfig.createTcp("host", port)
    .setWorkerNum(8)                    // Worker thread count
    .setBatchingMaxMessages(50)         // Max messages per batch
    .setBatchingMaxDelayMs(10)          // Max batch delay (ms)
    .setMaxPendingMessages(409600);     // Queue capacity
```

### High Throughput Configuration

```java
TglogConfig config = TglogConfig.createTcp("host", port)
    .setWorkerNum(16)                   // More workers
    .setBatchingMaxMessages(100)        // Larger batch
    .setBatchingMaxSize(64 * 1024)      // 64KB batch
    .setSocketSendBufferSize(128 * 1024 * 1024); // 128MB buffer
```

### Low Latency Configuration

```java
TglogConfig config = TglogConfig.createTcp("host", port)
    .setWorkerNum(4)
    .setBatchingMaxMessages(10)         // Small batch
    .setBatchingMaxDelayMs(5);          // 5ms delay
```

## Performance Monitoring

```java
// Get performance metrics
TglogMetrics metrics = client.getMetrics();

// Print human-readable format
System.out.println(client.getMetricsString(false));

// Export JSON
String json = client.getMetricsString(true);
System.out.println(json);

// Periodic monitoring
ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
scheduler.scheduleAtFixedRate(() -> {
    System.out.println("Success Rate: " + metrics.getSuccessRate() * 100 + "%");
    System.out.println("Queue Size: " + metrics.getMsgQueueSize());
}, 0, 10, TimeUnit.SECONDS);
```

## FAQ

### Q: How to ensure messages are not lost?

A: Use callback mechanism:

```java
client.send(log, (payload, exception) -> {
    if (exception == null) {
        // Success
    } else {
        // Failed, save to local file
        saveToLocalFile(payload);
    }
});
```

### Q: What to do when queue is full?

A: Three strategies:

1. **Increase queue capacity**
```java
config.setMaxPendingMessages(1000000);
```

2. **Increase worker count**
```java
config.setWorkerNum(16);
```

3. **Degradation handling**
```java
try {
    client.send(log);
} catch (TglogException e) {
    if (e.getErrorCode() == TglogErrorCode.SEND_QUEUE_IS_FULL) {
        // Queue full, save locally
        saveToLocalFile(log);
    }
}
```

### Q: How to optimize performance?

A: Follow these recommendations:

1. **Properly configure worker count** - Usually set to CPU core count
2. **Adjust batch size** - Based on log size
3. **Use fire-and-forget mode** - Don't pass callback when not needed
4. **Warm up connections** - Wait for connection establishment after startup

```java
TglogClient client = new TglogClient(config);
Thread.sleep(500);  // Wait for connection establishment
// Start sending logs
```

## Next Steps

- Read full documentation: [README_EN.md](README_EN.md)
- View project summary: [docs/PROJECT_SUMMARY.md](docs/PROJECT_SUMMARY.md)
- Run performance tests: `./run-test.sh -m 1000000 -c`
- View source code: `src/main/java/com/tencent/tglog/`

## Getting Help

For questions, please check:
- [README_EN.md](README_EN.md) - Full documentation
- [docs/PROJECT_SUMMARY.md](docs/PROJECT_SUMMARY.md) - Project summary
- Source code comments - Detailed code comments

Enjoy using TGLog!
