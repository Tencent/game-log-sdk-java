# TGLog Pure Java Client

High-performance pure Java implementation of TGLog client, designed for game servers.

## Maven Dependency

### Add Dependency

Add the following to your project's `pom.xml`:

```xml
<dependency>
    <groupId>com.tencent.tglog</groupId>
    <artifactId>tglog-java</artifactId>
    <version>1.0.0</version>
</dependency>
```

### Configure Repository

Add Tencent Maven repository to your project's `pom.xml`:

```xml
<repositories>
    <repository>
        <id>tencent_public</id>
        <name>Tencent Maven Repository</name>
        <url>https://mirrors.tencent.com/repository/maven/tglog/releases/</url>
        <releases>
            <enabled>true</enabled>
        </releases>
        <snapshots>
            <enabled>false</enabled>
        </snapshots>
    </repository>
    <repository>
        <id>tencent_public_snapshots</id>
        <name>Tencent Maven Snapshots Repository</name>
        <url>https://mirrors.tencent.com/repository/maven/tglog/snapshots/</url>
        <releases>
            <enabled>false</enabled>
        </releases>
        <snapshots>
            <enabled>true</enabled>
        </snapshots>
    </repository>
</repositories>
```

**Or** configure globally in `~/.m2/settings.xml` (recommended):

```xml
<?xml version="1.0" encoding="UTF-8"?>
<settings xmlns="http://maven.apache.org/SETTINGS/1.0.0"
          xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
          xsi:schemaLocation="http://maven.apache.org/SETTINGS/1.0.0
                              http://maven.apache.org/xsd/settings-1.0.0.xsd">
    <profiles>
        <profile>
            <id>tencent-repos</id>
            <repositories>
                <repository>
                    <id>tencent_public</id>
                    <name>Tencent Maven Repository</name>
                    <url>https://mirrors.tencent.com/repository/maven/tglog/releases/</url>
                    <releases>
                        <enabled>true</enabled>
                    </releases>
                    <snapshots>
                        <enabled>false</enabled>
                    </snapshots>
                </repository>
                <repository>
                    <id>tencent_public_snapshots</id>
                    <name>Tencent Maven Snapshots Repository</name>
                    <url>https://mirrors.tencent.com/repository/maven/tglog/snapshots/</url>
                    <releases>
                        <enabled>false</enabled>
                    </releases>
                    <snapshots>
                        <enabled>true</enabled>
                    </snapshots>
                </repository>
            </repositories>
        </profile>
    </profiles>

    <activeProfiles>
        <activeProfile>tencent-repos</activeProfile>
    </activeProfiles>
</settings>
```

After configuration, run `mvn clean install` to download dependencies automatically.

## Features

### Core Features
- ✅ **Pure Java** - No JNI dependencies, cross-platform, easy deployment
- ✅ **Lock-Free Architecture** - High concurrency with lock-free MPSC queues
- ✅ **Multi-Threading** - Multiple worker threads for parallel processing
- ✅ **NIO Async** - Non-blocking I/O based on Java NIO
- ✅ **Batching** - Intelligent batch aggregation to reduce system calls
- ✅ **UDP Batching** - UDP also supports batch sending (single packet mode)
- ✅ **Auto Reconnect** - Automatic reconnection with exponential backoff
- ✅ **DNS Resolution** - Periodic DNS resolution with load balancing support
- ✅ **Performance Monitoring** - Rich metrics with JSON export support

### Performance Characteristics
- **High Throughput**: Achieves 1M+ messages/sec
- **Low Latency**: Configurable batch delay (default 10ms)
- **Non-Blocking**: Send operations don't block business threads
- **Backpressure Control**: Reject new messages when queue is full

## Quick Start

### Project Structure

This project follows standard Maven project structure:

```
sdk-java/
├── pom.xml                          # Maven configuration
├── src/
│   ├── main/java/                   # Main source code
│   │   └── com/tencent/tglog/
│   ├── test/java/                   # Unit tests
│   ├── examples/java/               # Usage examples
│   └── integration-test/java/       # Integration tests
├── docs/                            # Documentation
├── compile.sh                       # Compilation script
├── run-example.sh                   # Run examples
└── run-test.sh                      # Run performance tests
```

### 1. Compilation

**Using compilation script (recommended):**
```bash
./compile.sh
```

**Or compile directly with Maven:**
```bash
# Compile
mvn compile

# Clean and compile
mvn clean compile

# Package as JAR
mvn package
```

After successful compilation, class files will be generated in `target/classes`, and `mvn package` will generate JAR files in `target/`.

### 2. Maven Dependency

**Use as dependency:**

Add to your project's `pom.xml`:

```xml
<dependency>
    <groupId>com.tencent.tglog</groupId>
    <artifactId>tglog-java</artifactId>
    <version>1.0.0</version>
</dependency>

<!-- SLF4J implementation (choose based on your project) -->
<dependency>
    <groupId>ch.qos.logback</groupId>
    <artifactId>logback-classic</artifactId>
    <version>1.4.14</version>
</dependency>
```

**Or use JAR file directly:**
```bash
mvn package
# Use target/tglog-java-1.0.0.jar
```

**System Requirements:**
- Java 8 or higher
- Maven 3.6+ (for building)
- SLF4J API (included in dependencies)
- SLF4J implementation (e.g., Logback, Log4j2, chosen by application)

### 3. Run Examples

```bash
# Simple example
./run-example.sh

# Performance test (default 1 million messages)
./run-test.sh

# Performance test (1 million messages with callbacks)
./run-test.sh -m 1000000 -c

# Custom parameters
./run-test.sh -h logs.game.com -p 8080 -m 500000 -w 16

# View all available parameters
./run-test.sh --help
```

### 4. Basic Usage

```java
import com.tencent.tglog.*;

// Create configuration
TglogConfig config = TglogConfig.createTcp("logs.game.com", 8080)
    .setWorkerNum(8)                    // 8 worker threads
    .setBatchingMaxMessages(50)         // Max 50 messages per batch
    .setBatchingMaxDelayMs(10);         // Max 10ms batch delay

// Create client (AutoCloseable)
try (TglogClient client = new TglogClient(config)) {

    // Method 1: Using CompletableFuture (recommended)
    client.send("battle|winner=1|loser=2|score=100\n")
          .thenRun(() -> System.out.println("Sent successfully"))
          .exceptionally(ex -> {
              System.err.println("Send failed: " + ex.getMessage());
              return null;
          });

    // Method 2: Using Lambda callback (Kafka style)
    client.send("login|userid=12345\n", (payload, exception) -> {
        if (exception == null) {
            System.out.println("Sent successfully: " + payload);
        } else {
            System.err.println("Send failed: " + exception.getMessage());
        }
    });

    // Method 3: Chaining calls
    client.send("log1\n")
          .thenCompose(v -> client.send("log2\n"))
          .thenRun(() -> System.out.println("All sent"));

    // Get performance metrics
    TglogMetrics metrics = client.getMetrics();
    System.out.println("Success rate: " + metrics.getSuccessRate() * 100 + "%");
    System.out.println(client.getMetricsString(false));

} // Auto-close, flush all pending messages
```

## Architecture Design

### Overall Architecture

```
Application (multi-threaded)
     │
     ▼ (lock-free send)
TglogClient
     │
     ├── EndpointManager (DNS resolution)
     │
     ├── TglogWorker[] (multiple worker threads)
     │    │
     │    ├── LockFreeQueue (lock-free message queue)
     │    ├── NIO Selector (non-blocking I/O)
     │    ├── ConnectionPool (connection pool)
     │    └── BatchBuffer (batch buffer)
     │
     └── TglogMetrics (performance statistics)
```

### Core Components

#### 1. LockFreeQueue (Lock-Free Queue)
- **Type**: MPSC (Multi-Producer Single-Consumer)
- **Implementation**: CAS-based ring buffer
- **Features**:
  - Multiple business threads enqueue concurrently
  - Single worker thread consumes
  - Lock-free design, high performance

#### 2. TglogWorker (Worker Thread)
- **Count**: Configurable (default 8)
- **Responsibilities**:
  - Dequeue messages from lock-free queue
  - Batch aggregation
  - NIO async sending
  - Connection management
- **Scheduling**: Round-robin

#### 3. BatchBuffer (Batch Buffer)
- **TCP Mode**: Uses Gathering Write for batch sending
- **UDP Mode**: Single packet sends all messages
- **Trigger Conditions**:
  - Message count reaches limit (default 50)
  - Batch size reaches limit (default 32KB)
  - Timeout reached (default 10ms)

#### 4. EndpointManager (Endpoint Manager)
- **DNS Resolution**: Periodic resolution (default 60s)
- **Change Detection**: Auto-detect IP changes
- **Load Balancing**: Supports multiple endpoint rotation

## Configuration

### Configuration Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| **Network** | | | |
| protocol | Protocol | TCP | Protocol type (TCP/UDP) |
| host | String | - | Server address |
| port | int | - | Server port |
| **Connection** | | | |
| connectionTimeout | int | 3000ms | Connection timeout |
| maxConnectionLifetime | int | 300s | Connection lifetime |
| dnsUpdateInterval | int | 60s | DNS update interval |
| **Batching** | | | |
| batchingMaxMessages | int | 50 | Max messages per batch |
| batchingMaxSize | int | 32KB | Max batch size |
| batchingMaxDelayMs | int | 10ms | Max batch delay |
| **Worker** | | | |
| workerNum | int | 8 | Number of worker threads |
| maxPendingMessages | int | 409600 | Queue capacity per worker |
| **Buffer** | | | |
| writeHighWatermark | int | 32MB | Write buffer high watermark |
| socketSendBufferSize | int | 64MB | Socket send buffer |
| socketRecvBufferSize | int | 16KB | Socket receive buffer |
| **Retry** | | | |
| sendTimeout | int | 30000ms | Send timeout |
| maxRetries | int | 10 | Max retry attempts |
| stopTimeout | int | 3000ms | Stop timeout |

### Configuration Examples

```java
// High throughput configuration
TglogConfig config = TglogConfig.createTcp("logs.game.com", 8080)
    .setWorkerNum(16)                   // Increase worker count
    .setBatchingMaxMessages(100)        // Larger batch
    .setBatchingMaxSize(64 * 1024)      // 64KB batch
    .setSocketSendBufferSize(128 * 1024 * 1024); // 128MB buffer

// Low latency configuration
TglogConfig config = TglogConfig.createTcp("logs.game.com", 8080)
    .setWorkerNum(4)
    .setBatchingMaxMessages(10)         // Small batch
    .setBatchingMaxDelayMs(5);          // 5ms delay

// UDP configuration - Internet (avoid IP fragmentation)
TglogConfig config = TglogConfig.createUdp("logs.game.com", 8080)
    .setWorkerNum(8)
    .setBatchingMaxSize(1400);          // Less than MTU(1500) to avoid fragmentation

// UDP configuration - LAN (use default)
TglogConfig config = TglogConfig.createUdp("192.168.1.100", 8080)
    .setWorkerNum(8);                   // Use default 32KB, no need to limit in LAN
```

## Performance Metrics

### Metric Types

#### Counter (Cumulative)
- `requestMsgCount`: Messages requested by application
- `enqueuedMsgCount`: Messages successfully enqueued
- `dequeuedMsgCount`: Messages successfully dequeued
- `succeededMsgCount`: Messages successfully sent
- `failedMsgCount`: Failed messages
- `timeoutMsgCount`: Timed out messages
- `retryMsgCount`: Retry count
- `droppedMsgCount`: Dropped messages (queue full)

#### Gauge (Instantaneous)
- `msgQueueSize`: Current queue size
- `msgQueueCapacity`: Queue capacity
- `endpointCount`: Current endpoint count
- `activeConnCount`: Active connection count

#### Derived Metrics
- `successRate`: Success rate = succeeded / (succeeded + failed)
- `queueUtilization`: Queue utilization = queueSize / queueCapacity

### Getting Metrics

```java
// Get metrics object
TglogMetrics metrics = client.getMetrics();
System.out.println("Success rate: " + metrics.getSuccessRate() * 100 + "%");
System.out.println("Queue utilization: " + metrics.getQueueUtilization() * 100 + "%");

// Print human-readable format
System.out.println(client.getMetricsString(false));

// Export JSON format
String json = client.getMetricsString(true);
```

## Game Server Integration

### Example: Game Main Loop Integration

```java
public class GameServer {
    private TglogClient logClient;

    public void init() throws TglogException {
        TglogConfig config = TglogConfig.createTcp("logs.game.com", 8080)
            .setWorkerNum(8)
            .setBatchingMaxDelayMs(10);

        logClient = new TglogClient(config);
    }

    public void gameLoop() {
        while (running) {
            // Process game logic
            processPlayerActions();
            processNPCLogic();
            processBattles();

            // Log events (non-blocking, doesn't affect game logic)
            // Logs are sent asynchronously
        }
    }

    // Player login
    public void onPlayerLogin(int userId, String username) {
        String log = String.format("login|userid=%d|username=%s|timestamp=%d\n",
            userId, username, System.currentTimeMillis());

        try {
            logClient.send(log);
        } catch (TglogException e) {
            // Queue full or other error, log locally
            logger.error("Failed to send log", e);
        }
    }

    // Battle end
    public void onBattleEnd(int winnerId, int loserId, int score) {
        String log = String.format("battle|winner=%d|loser=%d|score=%d\n",
            winnerId, loserId, score);

        // Method 1: Using Lambda callback
        try {
            logClient.send(log, (payload, exception) -> {
                if (exception == null) {
                    // Success, can update database
                    updateBattleStats(winnerId, loserId);
                } else {
                    // Failed, save to local file
                    saveToLocalFile(payload);
                }
            });
        } catch (TglogException e) {
            saveToLocalFile(log);
        }

        // Method 2: Using CompletableFuture
        try {
            logClient.send(log)
                     .thenRun(() -> updateBattleStats(winnerId, loserId))
                     .exceptionally(ex -> {
                         saveToLocalFile(log);
                         return null;
                     });
        } catch (TglogException e) {
            saveToLocalFile(log);
        }
    }

    public void shutdown() {
        // Close client, auto-flush all pending messages
        logClient.close();
    }
}
```

## Performance Testing

### Test Environment
- CPU: 8 cores
- Memory: 16GB
- Network: Gigabit NIC
- JVM: OpenJDK 11

### Test Results

| Scenario | Messages | Time | Throughput | Notes |
|----------|----------|------|------------|-------|
| TCP With Callback | 10M | ~7.8s | ~1.28M/s | Production recommended |
| TCP No Callback | 1M | ~0.6s | ~1.6M/s | Max performance |
| UDP No Callback | 1M | ~0.5s | ~2.0M/s | Max performance |
| UDP With Callback | 1M | ~0.7s | ~1.4M/s | Production recommended |

**Note**: Above data is based on discard mode testing (-d flag). Actual network transmission performance depends on network bandwidth and server processing capacity.

### Running Performance Tests

```bash
# Benchmark test (100k messages)
./run-test.sh

# High load test (1M messages)
./run-test.sh -m 1000000

# With callback test
./run-test.sh -m 1000000 -c

# UDP test
./run-test.sh --udp -m 1000000

# Custom worker count
./run-test.sh -m 1000000 -w 16 -b 100
```

## Comparison with C Version

| Feature | C Version (JNI) | Pure Java Version |
|---------|----------------|-------------------|
| **Performance** | 1M/s | 1M+/s |
| **Deployment** | Need JNI compilation | Pure JAR |
| **Maintenance** | C+Java dual language | Pure Java |
| **Debugging** | Difficult | Easy |
| **Cross-platform** | Need compilation | Fully cross-platform |
| **Dependencies** | libevent, ck | No external deps |
| **Memory** | Need jemalloc | JVM managed |

## Troubleshooting

### Common Issues

#### 1. Connection Failed
```
Error: Failed to resolve host
```
**Solution**: Check host and port configuration, ensure server is reachable

#### 2. Queue Full
```
TglogException: Message queue is full
```
**Solution**:
- Increase worker count: `setWorkerNum(16)`
- Increase queue capacity: `setMaxPendingMessages(1000000)`
- Reduce send rate

#### 3. Send Timeout
```
Error: Message timeout
```
**Solution**:
- Increase timeout: `setSendTimeout(60000)`
- Check network connection
- Check server load

### Logging Configuration

TGLog SDK uses SLF4J as logging facade. You can choose any SLF4J implementation (Logback, Log4j2, etc.).

**Using Logback (recommended):**

Create `src/main/resources/logback.xml`:

```xml
<configuration>
    <appender name="STDOUT" class="ch.qos.logback.core.ConsoleAppender">
        <encoder>
            <pattern>%d{HH:mm:ss.SSS} [%thread] %-5level %logger{36} - %msg%n</pattern>
        </encoder>
    </appender>

    <!-- TGLog SDK log level -->
    <logger name="com.tencent.tglog" level="INFO"/>

    <!-- Set to DEBUG for debugging -->
    <!-- <logger name="com.tencent.tglog" level="DEBUG"/> -->

    <root level="INFO">
        <appender-ref ref="STDOUT"/>
    </root>
</configuration>
```

**Using SLF4J Simple (for testing):**

```xml
<dependency>
    <groupId>org.slf4j</groupId>
    <artifactId>slf4j-simple</artifactId>
    <version>2.0.9</version>
</dependency>
```

Configure via system properties:
```bash
java -Dorg.slf4j.simpleLogger.defaultLogLevel=debug \
     -Dorg.slf4j.simpleLogger.log.com.tencent.tglog=debug \
     -jar your-app.jar
```

### Debugging Tips

```java
// Periodically print metrics
ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
scheduler.scheduleAtFixedRate(() -> {
    System.out.println(client.getMetricsString(false));
}, 0, 10, TimeUnit.SECONDS);
```

### Error Code Reference

When operations fail, `TglogException` contains an error code accessible via `getErrorCode()`:

| Error Code | Constant | Description |
|------------|----------|-------------|
| 0 | OK | Success |
| 10001 | SEND_TIMEOUT | Send timeout |
| 10002 | SEND_FAILED | Send failed |
| 10003 | PRODUCER_CLOSED | Client closed |
| 10004 | SEND_QUEUE_IS_FULL | Message queue full |
| 10005 | NO_AVAILABLE_CONN | No available connections |
| 10006 | NEW_CONN_FAILED | Failed to create connection |
| 10007 | CONN_WRITE_FAILED | Connection write failed |
| 10008 | CONN_READ_FAILED | Connection read failed |
| 10009 | LOG_TOO_LONG | Log too long |
| 10010 | BAD_LOG | Invalid log format |
| 10011 | BATCH_SIZE_EXCEEDED | Batch size exceeded |
| 10012 | MEMORY_ALLOC_FAILED | Memory allocation failed |
| 10013 | WORKER_BUSY | Worker busy |
| 20001 | UNKNOWN | Unknown error |

**Usage Example:**
```java
try {
    client.send("log content\n");
} catch (TglogException e) {
    if (e.getErrorCode() == TglogErrorCode.SEND_QUEUE_IS_FULL) {
        // Queue full, can choose to drop or retry
        System.err.println("Queue is full, dropping message");
    } else if (e.getErrorCode() == TglogErrorCode.SEND_TIMEOUT) {
        // Send timeout
        System.err.println("Send timeout: " + e.getMessage());
    } else {
        // Other errors
        System.err.println("Error: " + e.getMessage());
    }
}
```

## API Documentation

### TglogClient

#### Constructor
```java
public TglogClient(TglogConfig config) throws TglogException
```

#### Main Methods
```java
// Send log (returns CompletableFuture)
public CompletableFuture<Void> send(String logPayload) throws TglogException
public CompletableFuture<Void> send(String logPayload, TglogCallback callback) throws TglogException

// Flush pending messages
public boolean flush(long timeoutMs)

// Get metrics
public TglogMetrics getMetrics()
public String getMetricsString(boolean json)

// Close client
public void close()
```

### TglogConfig

#### Creation Methods
```java
public static TglogConfig createTcp(String host, int port)
public static TglogConfig createUdp(String host, int port)
```

#### Configuration Methods (fluent API)
```java
public TglogConfig setWorkerNum(int workerNum)
public TglogConfig setBatchingMaxMessages(int maxMessages)
public TglogConfig setBatchingMaxDelayMs(int delayMs)
// ... more configuration methods
```

### TglogCallback

```java
@FunctionalInterface
public interface TglogCallback {
    /**
     * Called when send operation completes.
     * @param payload The log payload
     * @param exception null if success, otherwise contains error details
     */
    void onCompletion(String payload, TglogException exception);
}
```

**Usage Examples:**
```java
// Lambda expression
client.send("log\n", (payload, exception) -> {
    if (exception == null) {
        System.out.println("Success");
    } else {
        System.err.println("Failed: " + exception.getMessage());
    }
});

// CompletableFuture
client.send("log\n")
      .thenRun(() -> System.out.println("Success"))
      .exceptionally(ex -> {
          System.err.println("Failed");
          return null;
      });
```

## License

MIT License

## Support

- Issue Reporting: Submit an issue
- Technical Discussion: PRs and discussions welcome
