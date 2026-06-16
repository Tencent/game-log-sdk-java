# CODEBUDDY.md

This file provides guidance to CodeBuddy Code when working with code in this repository.

## Project overview

TGLog Pure Java Client is a high-performance Java 8 logging SDK for game servers. It is a pure Java implementation with no JNI/native dependency. The core design is lock-free multi-producer enqueueing, multiple worker threads, Java NIO non-blocking I/O, batching, retry/backoff, DNS endpoint refresh, and Prometheus metrics.

## Common commands

```bash
# Compile main, integration-test, and example sources
./compile.sh

# Maven compile only
mvn compile

# Clean compile
mvn clean compile

# Run Maven test phase
mvn test

# Package slim jar, fat jar, and sources jar under target/
mvn package

# Package release directory/tarball (overwrites tglog-java-release/ and tglog-java-release.tar.gz)
./package.sh

# Run the simple example
./run-example.sh

# Run performance test; pass --help for all options
./run-test.sh
./run-test.sh -m 1000000 -w 8 -c
./run-test.sh --udp -m 500000

# Run concurrent send and idle-connection tests
./run-concurrent-test.sh
./run-idle-test.sh --help

# Static checks configured in pom.xml
mvn checkstyle:check
mvn pmd:check
mvn spotbugs:check
```

Most integration/performance checks in `src/integration-test/java/com/tencent/tglog/` are executable `main` classes, not JUnit `@Test` classes. To run one directly after compilation:

```bash
./compile.sh
mvn -q dependency:copy-dependencies -DoutputDirectory=target/lib -DincludeScope=test
java -cp "target/classes:target/test-classes:target/lib/*" com.tencent.tglog.CallbackValidationTest
java -cp "target/classes:target/test-classes:target/lib/*" com.tencent.tglog.ComprehensiveTest
java -cp "target/classes:target/test-classes:target/lib/*" com.tencent.tglog.FlexibleFormatTest
```

Some tests use real endpoints such as `localhost`, `127.0.0.1`, or `dev.tglog.com`; inspect the target test class before running it if network availability matters.

## Repository structure

- `pom.xml` — Maven Java 8 project (`com.tencent.tglog:tglog-java:1.0.2`), with build-helper adding integration tests and examples as test sources.
- `src/main/java/com/tencent/tglog/` — SDK implementation.
- `src/integration-test/java/com/tencent/tglog/` — executable integration/performance test programs.
- `src/examples/java/com/tencent/tglog/SimpleExample.java` — sample client usage.
- `src/test/resources/simplelogger.properties` — test logging configuration.
- `checkstyle.xml` — Checkstyle rules used by the Maven plugin.
- `tglog-java-release/` and `tglog-java-release.tar.gz` — generated release package artifacts.

## Core architecture

`TglogClient` is the public entry point. It owns an `EndpointManager`, a list of `TglogWorker` instances, and `TglogMetrics`. `send(String, TglogCallback)` validates payloads, wraps the callback to also complete a `CompletableFuture<Void>`, chooses a worker with round-robin, and enqueues a `LogMessage`.

Message flow:

1. Application calls `TglogClient.send(...)` with a payload that must end in `\n`.
2. `LogMessage` stores the payload and caches UTF-8 bytes.
3. `TglogClient` selects a `TglogWorker` by round-robin.
4. The worker receives the message through `LockFreeQueue`, an MPSC bounded lock-free queue.
5. `BatchBuffer` accumulates messages until max message count, max byte size, or max delay is reached.
6. `TglogWorker` sends the prepared buffers through `TglogConnection` using NIO.
7. `BatchBuffer` invokes callbacks for accepted messages and removes them from the batch.

Important classes:

- `TglogConfig` — protocol, host/port, batching, worker, buffer, retry, DNS, and metrics registry configuration. Defaults include TCP, 8 workers, 50 messages per batch, 32 KB batch size, 10 ms batch delay, 409600 pending messages per worker, 30 s send timeout, and 10 retries.
- `EndpointManager` — periodically resolves DNS, tracks endpoint changes, and notifies workers.
- `TglogWorker` — owns a worker thread, queue, batches, connections, retries, and endpoint updates. Connection/endpoint state is intended to be handled inside the worker thread.
- `LockFreeQueue` — multi-producer/single-consumer queue used by application threads to hand messages to one worker.
- `BatchBuffer` — builds TCP gathering-write buffers or one UDP buffer, tracks callbacks, supports retry buffer reuse.
- `TglogConnection` — TCP/UDP non-blocking connection with partial-write output buffering and reconnect/lifetime handling.
- `TglogMetrics` — Prometheus simpleclient counters/gauges; applications decide how to expose them.
- `Backoff`, `TglogException`, and `TglogErrorCode` — retry timing and typed SDK errors.

## API and behavior notes

- `TglogCallback` has one method: `onCompletion(String payload, TglogException exception)`. Treat `exception == null` as success.
- `send(String)` and `send(String, TglogCallback)` return `CompletableFuture<Void>`; synchronous validation/enqueue failures throw `TglogException`.
- Payload validation includes non-null, non-empty, trailing newline, and protocol size limit checks.
- Protocol limits are `TglogConfig.MAX_UDP_REQ_SIZE` (65507 bytes) and `MAX_TCP_REQ_SIZE` (512 KB). `TglogClient` lowers configured batch size if it exceeds the selected protocol limit.
- Send success is SDK acceptance: bytes were written directly or accepted into `TglogConnection`'s `outputBuffer` for later flushing. It is not an application-level server acknowledgment.
- TCP uses one `ByteBuffer` per message for gathering write; UDP creates a single combined buffer for the whole batch.
- `TglogClient.close()` calls `flush(stopTimeout)`, stops endpoint management and workers, then unregisters metrics last so shared Prometheus registries stay reusable.
- User callback exceptions are caught/ignored around batch notification so one bad callback does not prevent other callbacks.

## Dependencies

Runtime dependencies are `slf4j-api` and Prometheus `simpleclient`; applications provide their own SLF4J implementation. Test dependencies include JUnit 4, `slf4j-simple`, and Guava for rate limiting in performance tests.
