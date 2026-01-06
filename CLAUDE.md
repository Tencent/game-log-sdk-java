# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

TGLog Pure Java Client is a high-performance logging client designed for game servers. It's a pure Java implementation (no JNI dependencies) that achieves 100万+/秒 throughput using lock-free architecture, NIO async I/O, and batch sending.

## Build and Test Commands

### Building
```bash
# Quick compile (recommended)
./compile.sh

# Maven compile
mvn compile

# Package JAR with dependencies
mvn package

# Clean and rebuild
mvn clean compile
```

### Running Tests
```bash
# Run unit tests
mvn test

# Run simple example
./run-example.sh

# Run performance test (default 100万 messages)
./run-test.sh

# Performance test with custom parameters
./run-test.sh -m 1000000 -w 8 -c    # 1M messages, 8 workers, with callbacks
./run-test.sh --udp -m 500000       # UDP mode, 500K messages
./run-test.sh -h logs.game.com -p 8080 -b 100  # Custom host, port, batch size

# Show all test options
./run-test.sh --help
```

### Running Single Test
```bash
# Compile and run specific integration test
mvn test-compile
java -cp target/classes:target/test-classes:$(mvn dependency:build-classpath -q -Dmdep.outputFile=/dev/stdout) \
  com.tencent.tglog.CallbackValidationTest
```

### Code Quality
```bash
# Run Checkstyle
mvn checkstyle:check

# Run PMD static analysis
mvn pmd:check

# Run SpotBugs
mvn spotbugs:check
```

## Architecture

### Core Components

The codebase follows a multi-threaded, lock-free architecture:

```
Application Threads (多线程)
    │
    ▼ (无锁发送)
TglogClient (主客户端入口)
    │
    ├── EndpointManager (DNS解析与端点管理)
    │   └── 定时DNS解析(60秒)，支持多端点负载均衡
    │
    ├── TglogWorker[] (多Worker线程，默认8个)
    │   │
    │   ├── LockFreeQueue (MPSC无锁队列)
    │   │   └── 基于CAS的环形缓冲区，多生产者单消费者
    │   │
    │   ├── NIO Selector (非阻塞I/O)
    │   │   └── 每个Worker一个Selector，事件驱动
    │   │
    │   ├── TglogConnection[] (连接池)
    │   │   └── 自动重连、生命周期管理、指数退避
    │   │
    │   └── BatchBuffer (批次缓冲)
    │       ├── TCP: Gathering Write批量发送
    │       └── UDP: 单包发送所有消息
    │
    └── TglogMetrics (Prometheus指标)
        └── Counter和Gauge指标，支持JSON导出
```

### Key Design Patterns

**Lock-Free MPSC Queue** (`LockFreeQueue.java`):
- Multi-Producer Single-Consumer pattern
- CAS-based ring buffer (capacity must be power of 2)
- Each Worker has its own queue to avoid contention
- Messages are distributed to Workers via Round-Robin

**Worker Thread Model** (`TglogWorker.java`):
- Each Worker runs in its own thread
- Continuously polls its LockFreeQueue for messages
- Uses NIO Selector for non-blocking I/O operations
- Manages its own connection pool and batch buffer
- Batch sending triggered by: message count (50), size (32KB), or timeout (10ms)

**Connection Management** (`TglogConnection.java`):
- Non-blocking NIO SocketChannel
- Automatic reconnection with exponential backoff
- Connection lifecycle management (max 300s)
- Handles both TCP and UDP protocols

**Endpoint Resolution** (`EndpointManager.java`):
- Periodic DNS resolution (every 60s)
- Detects IP changes automatically
- Supports multiple endpoints for load balancing
- Notifies Workers when endpoints change

### Message Flow

1. Application calls `client.send(logPayload)`
2. Message wrapped in `LogMessage` with callback/future
3. Round-Robin selects a Worker
4. Message enqueued to Worker's `LockFreeQueue` (lock-free CAS operation)
5. Worker thread dequeues message
6. Message added to `BatchBuffer`
7. When batch triggers (count/size/timeout), Worker sends via NIO
8. On completion, callback invoked or future completed

## Important Implementation Details

### Batching Strategy
- **TCP Mode**: Uses `ByteBuffer[]` with Gathering Write for efficient batch sending
- **UDP Mode**: Packs all messages into a single UDP packet (respects MTU, default 1400 bytes)
- **Trigger Conditions**: Batch sent when any condition met:
  - Message count reaches `batchingMaxMessages` (default 50)
  - Batch size reaches `batchingMaxSize` (default 32KB)
  - Time since first message exceeds `batchingMaxDelayMs` (default 10ms)

### Memory Management
- Uses `DirectByteBuffer` to reduce memory copying
- Pre-allocates buffers to reduce GC pressure
- Object pooling for frequently created objects
- Configurable socket buffers (send: 64MB, recv: 16KB)

### Error Handling
- Queue full: Throws `TglogException` with `ERR_SEND_QUEUE_IS_FULL`
- Connection errors: Automatic retry with exponential backoff (max 10 retries)
- Send timeout: Configurable timeout (default 30s)
- All errors reported via callback/future with specific error codes

### Metrics (Prometheus)
- Uses `io.prometheus:simpleclient` for metrics collection
- SDK only collects metrics; application decides how to expose them
- Supports custom `CollectorRegistry` for isolation
- Counter metrics: requests, enqueued, succeeded, failed, timeout, retry, dropped
- Gauge metrics: queue size, queue capacity, endpoint count, active connections
- Access via `client.getMetrics()` or `client.getMetricsString(json)`

## Configuration Guidelines

### Performance Tuning

**High Throughput** (maximize messages/sec):
```java
TglogConfig.createTcp(host, port)
    .setWorkerNum(16)                    // More workers for parallelism
    .setBatchingMaxMessages(100)         // Larger batches
    .setBatchingMaxSize(64 * 1024)       // 64KB batches
    .setSocketSendBufferSize(128 * 1024 * 1024)  // 128MB buffer
```

**Low Latency** (minimize delay):
```java
TglogConfig.createTcp(host, port)
    .setWorkerNum(4)                     // Fewer workers
    .setBatchingMaxMessages(10)          // Smaller batches
    .setBatchingMaxDelayMs(5)            // 5ms max delay
```

**High Reliability** (prevent message loss):
```java
TglogConfig.createTcp(host, port)
    .setMaxPendingMessages(1000000)      // Larger queue
    .setSendTimeout(60000)               // Longer timeout
    .setMaxRetries(20)                   // More retries
```

### Worker Count Selection
- Typically set to number of CPU cores
- More workers = higher throughput but more memory
- Default 8 workers is suitable for most cases
- Monitor queue utilization to determine if more workers needed

## Testing Strategy

### Test Structure
- **Unit tests**: `src/test/java/` (currently minimal, mostly placeholder)
- **Integration tests**: `src/integration-test/java/` (main test suite)
  - `TglogClientTest.java`: Performance and stress testing
  - `CallbackValidationTest.java`: Callback mechanism validation
  - `ComprehensiveTest.java`: End-to-end scenarios
  - `FlexibleFormatTest.java`: Log format flexibility testing
- **Examples**: `src/examples/java/` (usage examples)

### Performance Testing
The `run-test.sh` script compiles integration tests and runs `TglogClientTest` with various parameters. Key test modes:
- Fire-and-forget (no callback): Maximum throughput
- With callback: Production-realistic scenario
- TCP vs UDP: Protocol comparison
- Rate limiting: Controlled load testing

## Common Development Tasks

### Adding New Configuration Option
1. Add field to `TglogConfig.java`
2. Add setter method with builder pattern (return `this`)
3. Add getter method
4. Update `TglogWorker.java` to use the new config
5. Update README.md configuration table

### Adding New Metric
1. Add Counter/Gauge to `TglogMetrics.java`
2. Register with Prometheus `CollectorRegistry`
3. Update increment/set calls in relevant classes
4. Add getter method for programmatic access
5. Update `docs/PROMETHEUS_METRICS.md`

### Modifying Batch Logic
Batch logic is in `TglogWorker.java` and `BatchBuffer.java`:
- `BatchBuffer.add()`: Adds message to batch
- `BatchBuffer.shouldFlush()`: Checks trigger conditions
- `TglogWorker.flushBatch()`: Sends batch via NIO

### Debugging Connection Issues
Enable SLF4J debug logging:
```java
// In logback.xml
<logger name="com.tencent.tglog" level="DEBUG"/>
```
Key log points:
- Connection establishment: `TglogConnection.connect()`
- Send operations: `TglogConnection.send()`
- Reconnection: `TglogConnection.reconnect()`
- DNS resolution: `EndpointManager.updateEndpoints()`

## Dependencies

### Runtime Dependencies
- `slf4j-api` (2.0.9): Logging facade (required)
- `simpleclient` (0.16.0): Prometheus metrics collection (required)
- SLF4J implementation: User chooses (Logback, Log4j2, etc.)

### Test Dependencies
- `junit` (4.13.2): Unit testing
- `slf4j-simple` (2.0.9): Simple logging for tests
- `guava` (33.5.0-jre): RateLimiter for performance tests

### Build Plugins
- `build-helper-maven-plugin`: Adds integration-test and examples sources
- `maven-assembly-plugin`: Creates fat JAR with dependencies
- `maven-checkstyle-plugin`: Code style checking (Google style)
- `maven-pmd-plugin`: Static code analysis
- `spotbugs-maven-plugin`: Bug detection

## File Organization

### Main Source (`src/main/java/com/tencent/tglog/`)
- `TglogClient.java`: Public API entry point (14KB)
- `TglogWorker.java`: Worker thread implementation (56KB, most complex)
- `TglogConnection.java`: NIO connection handling (23KB)
- `TglogConfig.java`: Configuration builder (8KB)
- `TglogMetrics.java`: Prometheus metrics (15KB)
- `LockFreeQueue.java`: Lock-free MPSC queue (4KB)
- `BatchBuffer.java`: Batch aggregation logic (12KB)
- `EndpointManager.java`: DNS and endpoint management (6KB)
- `LogMessage.java`: Message wrapper (1KB)
- `TglogCallback.java`: Callback interface (1KB)
- `TglogException.java`: Exception with error codes (2KB)
- `TglogErrorCode.java`: Error code constants (3KB)
- `Backoff.java`: Exponential backoff strategy (4KB)

### Scripts
- `compile.sh`: Quick compile wrapper
- `run-example.sh`: Runs simple example
- `run-test.sh`: Runs performance tests with argument parsing
- `package.sh`: Packages release tarball

## Logging Philosophy

The SDK uses SLF4J for logging:
- **INFO**: Important lifecycle events (client start/stop, connection changes)
- **DEBUG**: Detailed operation logs (message send, batch flush, reconnection)
- **WARN**: Recoverable errors (connection timeout, retry)
- **ERROR**: Unrecoverable errors (queue full, max retries exceeded)

Users must provide their own SLF4J implementation (Logback recommended).

## Thread Safety

- `TglogClient.send()`: Thread-safe, can be called from multiple threads
- `LockFreeQueue`: Lock-free multi-producer, single-consumer
- Each Worker thread owns its queue and connections (no sharing)
- Metrics use Prometheus thread-safe counters/gauges
- `TglogClient.close()`: Thread-safe, can be called multiple times

## Callback vs CompletableFuture

Both patterns supported for async result handling:

**Callback (Kafka-style)**:
```java
client.send(log, (payload, exception) -> {
    if (exception == null) {
        // Success
    } else {
        // Handle error
    }
});
```

**CompletableFuture (Java 8+)**:
```java
client.send(log)
    .thenRun(() -> System.out.println("Success"))
    .exceptionally(ex -> { /* handle error */ return null; });
```

Internally, callback is wrapped in CompletableFuture. Use callback for simpler code, CompletableFuture for chaining operations.
