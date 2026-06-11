# TGLog Java SDK - 严格代码审查报告

## 问题分类

按严重程度分为：**严重(Critical)**、**高(High)**、**中(Medium)**、**低(Low)**。

---

## 一、严重问题 (Critical)

### C1. 批量重试时 buffer 数据可能被重复发送到多个连接

### C2. 批量重试时 buffer 数据可能被重复发送到多个连接

**位置**: `TglogWorker.java:895-961` (`sendBatchTcp`)

**问题描述**:

在 `sendBatchTcp` 中，当一个连接失败 (`sent < 0`) 后，调用 `handleConnectionError(conn)`，然后尝试下一个连接。但 `TglogConnection.send(ByteBuffer[])` 内部在 flush outputBuffer 时可能已经将部分数据 buffered 到了 outputBuffer，此时 state 变 ERROR，但这些 buffered 数据在 pending close 时会被 rescue 到另一个连接，导致同一批数据被发送两次。

---

### C2. `processPendingCloses` 中 data rescue 可能导致循环补充

**位置**: `TglogWorker.java:1324-1378` (`processPendingCloses`)

**问题描述**:

`processPendingCloses` 中调用 `getConnection()` 来获取 rescue 目标连接，而 `getConnection()` 在 `connections.isEmpty()` 时会触发 `emergencySupplement`。如果系统处于所有后端都不可达的异常状态，这个循环会持续创建连接-失败-创建连接。

---

## 二、高优先级问题 (High)

### H1. endpoint recovery 的 backoff 被 supplementConnection 绕过

**位置**: `TglogWorker.java:1140-1203` (`recoverUnavailableEndpoints`) + `TglogWorker.java:1193-1202`

**问题描述**:

`recoverUnavailableEndpoints()` 使用 `ENDPOINT_RECOVERY` backoff (5s→10s→...→300s)，这是对的。但该方法末尾会调用 `supplementConnection()` 来补充到目标连接数。

`supplementConnection()` 的 Phase 1 中，如果连接创建失败，会调用 `markEndpointUnavailable(endpoint)` (line 1412)，**这会重置 endpoint 的 retryCount 为 0** (line 1132)，从而重置 backoff 计时。

**这意味着**: 每 5 秒的 `recoverUnavailableEndpoints` → `supplementConnection` 都可能重置异常节点的 backoff 状态，导致 backoff 失效。

### H2. `markEndpointUnavailable` 不标记有活跃连接的 endpoint，导致异常节点长期不被标记

**位置**: `TglogWorker.java:1112-1135`

**问题描述**:

`markEndpointUnavailable` 只检查 `connections` 列表中有无该 endpoint 的连接，不检查 `connectingConns`。如果 endpoint 有一个连接正在建立中（即将失败），endpoint 仍会被标记为 unavailable，然后 recover 时又创建新连接，可能导致连接堆积。

### H3. `processPendingCloses` 每 1 秒执行，但 getConnection() 可能每 2 秒才允许一次 emergency supplement

**位置**: `TglogWorker.java:1083-1107` (`getConnection`) + `TglogWorker.java:1324-1378` (`processPendingCloses`)

**问题描述**:

`getConnection()` 的 emergency supplement 有 2 秒最小间隔限制。但 `processPendingCloses` 每 1 秒执行一次。如果 connections 为空，data rescue 可能因 2 秒间隔未到而延迟。

### H4. `sendBatchUdp` 不在每次失败时标记连接错误

**位置**: `TglogWorker.java:966-1011`

**问题描述**:

UDP send 失败后，在 `tryCount < maxTries` 时，只换连接重试，不标记原连接为错误。如果原因是真正的错误（非 EAGAIN），这个失败的连接仍留在 `connections` 列表中，可能被后续的 round-robin 选到。

---

## 三、中优先级问题 (Medium)

### M1. Backoff 没有 jitter，可能导致惊群效应

**位置**: `Backoff.java:48-66`

**问题描述**:

所有 backoff 策略都是确定性的。如果多个 worker 同时遇到相同错误，它们会在完全相同的时间点重试，造成惊群效应。

### M2. `recoverUnavailableEndpoints` 创建所有 `connectionsPerEndpoint` 个连接，失败时重置 backoff

**位置**: `TglogWorker.java:1167-1189`

**问题描述**:

每次恢复尝试都会创建 `connectionsPerEndpoint` 个连接。如果 1 个成功 1 个失败，endpoint 被标记为已恢复但实际只有 1 个连接。同时连接失败会重置 backoff。

### M3. `processMessages` 中 retry 时的逻辑可能导致消息饥饿

**位置**: `TglogWorker.java:699-781`

**问题描述**:

当 batch 正在 retry backoff 期间，`processMessages` 会 `break`，不处理队列中的新消息。如果 backoff 间隔较大，新消息会堆积。

### M4. `cleanup()` 中 flush 剩余消息的逻辑可能丢消息

**位置**: `TglogWorker.java:1714-1741`

**问题描述**:

`sendBatch(false)` 不重试，如果发送失败，消息直接丢失。cleanup 中应该显式 notifyAllFailed 所有剩余消息。

### M5. `drainConnectionBuffers` 中 InterruptedException 被吞掉

**位置**: `TglogWorker.java:1698-1700`

**问题描述**:

catch IOException 后直接 break，如果是因为 Thread.interrupt() 导致的 ClosedSelectorException，interrupt 状态被丢失。

### M6. `LogMessage.retryCount` 是死代码

**位置**: `LogMessage.java:12,40`

**问题描述**:

`LogMessage.retryCount` 从未被任何代码使用。重试计数完全由 `BatchBuffer.retryCount` 管理。

---

## 四、低优先级问题 (Low)

### L1. `notifyListeners` 为每个 listener 创建新的 ArrayList

**位置**: `EndpointManager.java:180`

每次通知都创建新 copy，可以优化为创建一次。

### L2. `checkConnectingConnections` 中的连接失败后不尝试补充

**位置**: `TglogWorker.java:578-595`

连接在 `connectingConns` 中变成 ERROR 时，只标记 endpoint unavailable 并通知 init 计数，不尝试补充连接。

### L3. `handleReadable` 中 TCP 每次读取分配新的 1024 字节 ByteBuffer

**位置**: `TglogWorker.java:629`

可以复用 static buffer。

---

## 五、修复优先级建议

1. **立即修复 C1**: `sendBatchTcp` 切换连接前检查 buffer 状态
2. **立即修复 C2**: `processPendingCloses` 中 data rescue 不应依赖 `getConnection()` 的 side effect
3. **尽快修复 H4**: UDP send 失败时立即标记连接错误
5. **计划修复 H2, H3, M1**: markEndpointUnavailable 检查 connectingConns，backoff 加 jitter
6. **后续优化 M2-M6**: 逐步改进
