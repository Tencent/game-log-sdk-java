# TGLog Java SDK 严格代码 Review 问题记录

> 本文基于静态代码审查整理，重点关注异常场景下 SDK 是否会出现运行异常、资源膨胀、恢复退避失效、消息丢失、Future 不完成、flush/close 语义不可靠等问题。
>
> 结论：常规发送路径基本可用，但在坏节点、网络异常、慢 socket、连接恢复、shutdown/flush 等边界场景存在较高风险。尤其是 `flush()/close()`、TCP 非阻塞恢复、异步 output buffer、cleanup 超时丢消息这几类问题需要优先处理。

## 处理状态汇总

| 问题 | 状态 | 说明 |
| --- | --- | --- |
| #3 cleanup 超时丢消息不回调 | ✅ 已修复 | commit 6c0c327 |
| #4 partial write 即 success | ❎ 非问题 | 单向协议设计，success=accepted |
| #6 recovery backoff 被重置 | ✅ 已修复 | commit 08799a8 |
| #8 新增坏 endpoint 漏标记 unavailable | ✅ 已修复 | commit 86f7deb |
| #15 enqueue 失败统一报 queue full | ❎ 非问题 | 约定：ctor 返回即 ready、close 后不 send |
| #16 初始 DNS cause 被吞 / interval 未校验 | ✅ 已修复 | commit 21d86ab |
| #14 callback 只 catch Exception | ✅ 已修复 | catch Throwable + finally 完成 future |
| #13 metrics 部分注册失败不回滚 | ✅ 已修复 | 构造器跟踪已注册 collector，失败时回滚 |
| #12 close 未真正 join worker | ✅ 已修复 | worker 保存线程引用 + join(timeout)，超时仍 unregister |
| #11 watermark 非硬上限 | ❎ 非问题 | 源于 #4 单向协议，success 不可反悔，partial write 必须无条件缓冲 |
| #10 tryFlush 失败不回传 worker | ✅ 已修复 | tryFlush 返回 boolean，writable 分支失败即 handleConnectionError（严重程度修正为低） |
| #5 pendingClose/buffer 不受 cap | ⏸️ 暂不修 | 仅故障窗口期短暂膨胀，不影响正确性，属容量调优 |
| #7 单 endpoint 无 backoff | ⏸️ 暂不修 | 有意设计：唯一 endpoint 必须持续重连，限流可控 |
| #9 connectionsPerEndpoint 不重算 | ⏸️ 暂不修 | 仅影响连接冗余度，endpoint 通常稳定，改动回归风险高 |
| #1 flush 跨线程读 worker 内部状态 | ✅ 已修复 | 改为投递 flush 请求，worker 线程内判 hasPendingWork()（已私有化）；flush() 经 CompletableFuture 等待 |
| #2 close 忽略 flush 返回值 | ✅ 已修复 | 方案 A：close 接住返回值，false 时打 WARN（严重程度修正为低）；单条丢失已由 #3 的 PRODUCER_CLOSED 回调覆盖 |
| #17 sendBatch IOException 穿透换连接循环 | ✅ 已修复 | review 之外补充发现；循环内 try/catch 捕 IOException 等同 sent<0 换连接，去掉外层 catch |

## 总体结论

1. `connections + connectingConns` 有 per-endpoint cap，不太会出现 active 连接无界增长。
2. 但 `pendingCloseList`、connection `outputBuffer`、rescue buffer 不受同一套 cap 约束，故障窗口内资源仍可能膨胀。
3. endpoint recovery 名义上有 backoff，但 TCP 非阻塞连接失败路径会提前清理 retry 状态，实际可能退化成固定间隔重试。
4. `flush()/close()` 存在较严重并发设计问题，可能误判、抛异常、忽略 flush 失败，甚至导致消息无回调丢弃。
5. TCP partial write 被视为 success，shutdown drain 超时后可能出现“业务已收到成功回调，但 bytes 实际未写出”的情况。
6. 普通 payload 校验、queue full 同步失败、batch 普通失败回调/Future 完成路径整体较完整，主要风险集中在异常和 shutdown 边界。

---

## P0 / 高优先级问题

### 1. `flush()` 跨线程读取 worker 内部非线程安全结构  ✅ 已修复

> 状态：已修复。改为「投递 flush 请求 + worker 线程内判定」模型：
> - `TglogClient.flush(long)` 签名保持 `public`，实现改为向每个 worker 调
>   `requestFlush()` 拿到 `CompletableFuture<Boolean>`，再在 deadline 内 `get()` 等待；
>   全部 drained 返回 true，超时/未 drained 返回 false。不再跨线程读 worker 内部状态。
> - `TglogWorker.requestFlush()` 仅把 future offer 进 `flushRequestQueue` 并 `wakeup()`；
>   若此时 worker 已非 READY/INIT，则就地把队列 future 以 `false` 完成（无事件循环可服务）。
> - worker 主循环新增 `processFlushRequests()`（在 `processMessages()` 之后调用）：在
>   **worker 线程内**调一次 `hasPendingWork()`，为 false 则把所有挂起 future 以 true 完成，
>   为 true 则留待下一轮，不阻塞事件循环。
> - `hasPendingWork()` 已收敛为 `private`，只允许 worker 线程访问。
> - `flush(long)` 保持 `public` 且**不再标 `@Deprecated`**：重构后它已是线程安全的异步
>   实现（投递请求 + Future 等待），作为公开 API 保留给用户主动调用（如不关闭客户端
>   也想确认数据已发出）无问题；Javadoc 仅说明运行时通常无需手动调、关闭只需 `close()`。
> - `cleanup()` 末尾两次调用 `completeFlushRequestsOnStop()`（CLOSED 翻转前后各一次），
>   兜底完成停机期间入队的 flush 请求，避免 `flush()`/`close()` 永久阻塞。
> 这同时为 #2（close 拿真实 flush 结果）铺好基础（本次未改 close 行为）。

- 严重程度：高
- 类型：确定并发设计问题
- 相关代码：
  - `src/main/java/com/tencent/tglog/TglogClient.java:334`
  - `src/main/java/com/tencent/tglog/TglogClient.java:340`
  - `src/main/java/com/tencent/tglog/TglogWorker.java:83`
  - `src/main/java/com/tencent/tglog/TglogWorker.java:1591`
  - `src/main/java/com/tencent/tglog/TglogWorker.java:1608`
  - `src/main/java/com/tencent/tglog/TglogWorker.java:1615`

`TglogClient.flush()` 在调用线程里直接调用 `worker.hasPendingWork()`。但 `TglogWorker` 内部注释明确 `connections`、`currentBatch` 等结构只应由 worker 线程访问。

`hasPendingWork()` 会读取：

- `messageQueue`
- `pendingMessage`
- `currentBatch`
- `connections`
- `pendingCloseList`
- `TglogConnection.outputBufferSize`

其中 `connections` / `pendingCloseList` 是 `ArrayList`，worker 线程会同时增删；`pendingMessage`、`currentBatch`、`outputBufferSize` 也没有完整同步语义。

风险：

- 可能抛 `ConcurrentModificationException`。
- 可能读到旧值或不一致状态，导致 flush 误判完成。
- `close()` 里直接调用 `flush()`，如果这里抛运行时异常，后续 `endpointManager.stop()`、`worker.stop()`、`metrics.unregister()` 可能被跳过。

建议：

- 不要让外部线程直接窥探 worker 内部状态。
- 改成向 worker 投递 flush request，由 worker 线程内完成强制 flush 和 pending 状态判断。
- flush 结果通过 `Future`、`CountDownLatch` 或专用命令队列返回。

---

### 2. `close()` 忽略 `flush()` 返回值  ✅ 已修复（严重程度修正为「低」）

> 状态：已修复（方案 A，最小改动，不破坏 API）。`close()` 现在接住 `flush()` 返回值，
> 为 false（flush 预算内未 drain 完）时打 WARN 日志，再继续关闭（AutoCloseable 必须释放资源）。
> 说明：这并非静默数据丢失——#3 修复后，cleanup 超时的未发送消息都会经
> `failRemainingMessages()` 触发 `PRODUCER_CLOSED` 回调 + dropped metrics，调用方在
> **单条消息粒度**已可感知；本次补的 WARN 只是 close **聚合层面**的可观测性信号。
> 故严重程度由「高」修正为「低」。

- 严重程度：低（原评「高」，已修正）
- 类型：确定行为缺陷
- 相关代码：
  - `src/main/java/com/tencent/tglog/TglogClient.java:360`
  - `src/main/java/com/tencent/tglog/TglogClient.java:372`
  - `src/main/java/com/tencent/tglog/TglogClient.java:373`

`flush()` 返回 `false` 表示超时，但 `close()` 直接忽略返回值。

风险：

- 调用方无法知道 close 前是否仍有未发送数据。
- flush 超时后仍继续关闭 worker 和 unregister metrics。
- 和 cleanup 丢消息问题叠加后，会产生业务侧不可感知的数据丢失。

建议：

- `close()` 至少记录 flush 失败。
- 更好是提供 `flushAndClose(timeout)` 或让 close 返回/暴露关闭结果。
- 如果保持 `AutoCloseable.close()` 不能抛 checked exception，也应通过日志、metrics、callback/Future 明确失败。

---

### 3. worker cleanup 超时会丢弃 queue/pending message，但不触发 callback/Future  ✅ 已修复

> 状态：已修复（commit 6c0c327）。cleanup 在 flush 超时后调用 `failRemainingMessages()`，
> 逐条 drain `currentBatch` / `pendingMessage` / `messageQueue` 并以 `PRODUCER_CLOSED`
> 触发失败回调，同时计入 dropped metrics，保证 Future 必定完成。

- 严重程度：高
- 类型：确定消息丢失 / Future 悬挂路径
- 相关代码：
  - `src/main/java/com/tencent/tglog/TglogWorker.java:1747`
  - `src/main/java/com/tencent/tglog/TglogWorker.java:1750`
  - `src/main/java/com/tencent/tglog/TglogWorker.java:1752`
  - `src/main/java/com/tencent/tglog/TglogWorker.java:1753`
  - `src/main/java/com/tencent/tglog/TglogWorker.java:1764`

cleanup 时，如果 `messageQueue` 或 `pendingMessage` 在 `stopTimeout` 内处理不完，只记录 warning 后 break。之后只尝试发送 `currentBatch`，不会处理仍在 queue/pending 中的消息。

风险：

- 这些消息已经被 `send()` 接受，并返回了 `CompletableFuture`。
- cleanup 超时后消息被丢弃，但 callback 不触发，Future 不完成。
- metrics 也没有准确记录这些 dropped/timeout 消息。

建议：

- cleanup 超时时必须 drain `messageQueue` 和 `pendingMessage`，逐条回调失败。
- 错误码可考虑 `SEND_TIMEOUT`、`PRODUCER_CLOSED` 或新增 `CLIENT_CLOSED`。
- 同步更新 failed/dropped/timeout metrics。

---

### 4. TCP partial write 被立即视为成功，drain 超时后可能成功回调但实际丢数据  ❎ 非问题（设计如此）

> 状态：确认非问题。TGLog v1 是单向协议（只有请求、没有响应），SDK 的 success 语义本就
> 是「SDK accepted / 已交给本地发送缓冲」，而非「对端已确认」或「bytes 已写出 socket」。
> partial write 进 outputBuffer 即视为成功符合该协议设计，不是缺陷。

- 严重程度：高
- 类型：可靠性语义风险
- 相关代码：
  - `src/main/java/com/tencent/tglog/TglogConnection.java:255`
  - `src/main/java/com/tencent/tglog/TglogConnection.java:265`
  - `src/main/java/com/tencent/tglog/TglogConnection.java:271`
  - `src/main/java/com/tencent/tglog/TglogWorker.java:963`
  - `src/main/java/com/tencent/tglog/TglogWorker.java:965`
  - `src/main/java/com/tencent/tglog/TglogWorker.java:1057`
  - `src/main/java/com/tencent/tglog/TglogWorker.java:1058`
  - `src/main/java/com/tencent/tglog/TglogWorker.java:1694`
  - `src/main/java/com/tencent/tglog/TglogWorker.java:1695`
  - `src/main/java/com/tencent/tglog/TglogConnection.java:558`
  - `src/main/java/com/tencent/tglog/TglogConnection.java:560`

TCP partial write 时，剩余 bytes 被写入 connection `outputBuffer` 后，SDK 返回“accepted”。`TglogWorker` 随即把 batch 标记为 OK 并触发成功 callback/Future。

但 shutdown 阶段如果 `drainConnectionBuffers()` 超时，只记录 warning。后续 `conn.close()` 会清空 `outputBuffer`。

风险：

- 用户已经收到 success callback/Future completed。
- 实际 bytes 可能仍停留在 SDK outputBuffer，最终 close 时被清空。
- 如果用户期望 success 表示已经写入 socket，这里会违反直觉。

建议：

- 明确 success 语义：是“SDK accepted”还是“socket write completed”。
- 如果保持当前 accepted 语义，必须在文档中说明，并让 `flush()/close()` 能报告 buffered data 未 drain 完成。
- 如果要强可靠，callback 成功时机需要后移到数据真正写出。

---

## P1 / 连接恢复与补连接问题

### 5. active + connecting 有 cap，但 delayed-close 和 outputBuffer 不受 cap 约束  ⏸️ 暂不修

> 状态：暂不修。pendingClose 连接和 outputBuffer/rescue buffer 不计入 active cap 属实，但只在
> 故障窗口期（节点反复失败、endpoint 频繁变更、慢 socket）短暂膨胀，正常稳态下会被 delayed-close
> 与 flush 收敛，不会无界增长，也不影响正确性。属容量观测/调优而非缺陷，等真实压测或线上观测到
> 内存异常再针对性加 metrics/上限，避免现在引入回归。

- 严重程度：中到高
- 类型：资源膨胀风险
- 相关代码：
  - `src/main/java/com/tencent/tglog/TglogWorker.java:120`
  - `src/main/java/com/tencent/tglog/TglogWorker.java:116`
  - `src/main/java/com/tencent/tglog/TglogWorker.java:1297`
  - `src/main/java/com/tencent/tglog/TglogWorker.java:1411`
  - `src/main/java/com/tencent/tglog/TglogWorker.java:1418`
  - `src/main/java/com/tencent/tglog/TglogWorker.java:1449`
  - `src/main/java/com/tencent/tglog/TglogWorker.java:1473`
  - `src/main/java/com/tencent/tglog/TglogWorker.java:1474`

`supplementConnection()` 和 force supplement 对 active + connecting 做了上限控制。因此 active 连接数不太会无界增长。

但 `pendingCloseList` 不计入 cap，错误连接和替换连接可能延迟 5s/30s 关闭。同时 connection `outputBuffer` 和 rescue buffer 也不受 active connection cap 约束。

风险：

- 监控 active connection 数可能正常，但实际资源占用更高。
- 故障节点反复失败、endpoint 频繁变更、慢 socket 场景下，pending close 连接和 buffer 可能在窗口期膨胀。

建议：

- 将 pending close 连接数量和 buffered bytes 纳入 metrics。
- 对 pending close 总数、总 buffered bytes 设置上限。
- 超限时明确 fail/drop，并更新 metrics，而不是隐式积压。

---

### 6. endpoint recovery 的 backoff 在 TCP 非阻塞连接失败场景下会被重置  ✅ 已修复

> 状态：已修复（commit 08799a8）。`recoverUnavailableEndpoints()` 不再因 connect 发起成功
> 就清理 unavailable/retry，改为统一推进 backoff；unavailable/retry 状态只在
> `checkConnectingConnections()` 中连接真正进入 CONNECTED 时清理。

- 严重程度：中到高
- 类型：确定恢复退避缺陷
- 相关代码：
  - `src/main/java/com/tencent/tglog/TglogWorker.java:1169`
  - `src/main/java/com/tencent/tglog/TglogWorker.java:1174`
  - `src/main/java/com/tencent/tglog/TglogWorker.java:1191`
  - `src/main/java/com/tencent/tglog/TglogWorker.java:1192`
  - `src/main/java/com/tencent/tglog/TglogWorker.java:1200`
  - `src/main/java/com/tencent/tglog/TglogWorker.java:1202`
  - `src/main/java/com/tencent/tglog/TglogWorker.java:1203`
  - `src/main/java/com/tencent/tglog/TglogWorker.java:610`
  - `src/main/java/com/tencent/tglog/TglogWorker.java:619`
  - `src/main/java/com/tencent/tglog/TglogWorker.java:1152`
  - `src/main/java/com/tencent/tglog/TglogWorker.java:1153`

恢复逻辑使用 `Backoff.ENDPOINT_RECOVERY`，但只要 `conn.connect(selector)` 没有同步抛异常，就立刻把 endpoint 从 unavailable 中移除，并清空 retry count。

TCP 非阻塞连接真正成功要等 `finishConnect()`。如果后续 `finishConnect()` 失败，endpoint 会重新 mark unavailable，retry count 又从 0 开始。

风险：

- 持续异常节点可能一直按初始恢复间隔重试，而不是指数退避。
- 多 worker、多 SDK 实例会放大对异常节点的连接压力。

建议：

- 只有连接真正进入 `CONNECTED` 后，才能清理 unavailable/backoff。
- `recoverUnavailableEndpoints()` 只负责发起尝试，不应把发起成功当恢复成功。
- 在 `finishConnect()` 成功路径中清理 retry 状态。

---

### 7. 单 endpoint 场景没有 unavailable/backoff，只会固定间隔 emergency supplement  ⏸️ 暂不修

> 状态：暂不修。单 endpoint 故障时确实没有指数退避，靠 emergency supplement 以 2s/5s 级别固定
> 间隔重试——但这是有意的：唯一 endpoint 不能从候选池移除，必须持续尝试重连，否则单点故障恢复后
> 无法自动恢复发送。重试不是 busy-loop（有 MIN_INTERVAL 限流），稳态压力可控。给单 endpoint 加
> connection-level backoff 会延长单点恢复时间，取舍上弊大于利；如确有大规模单点部署重试压力问题，
> 再单独评估。
- 类型：故障节点恢复频率风险
- 相关代码：
  - `src/main/java/com/tencent/tglog/TglogWorker.java:1128`
  - `src/main/java/com/tencent/tglog/TglogWorker.java:1132`
  - `src/main/java/com/tencent/tglog/TglogWorker.java:1091`
  - `src/main/java/com/tencent/tglog/TglogWorker.java:1099`
  - `src/main/java/com/tencent/tglog/TglogWorker.java:1103`
  - `src/main/java/com/tencent/tglog/TglogWorker.java:1500`
  - `src/main/java/com/tencent/tglog/TglogWorker.java:1504`

只有一个 endpoint 时，`markEndpointUnavailable()` 会直接跳过。没有连接时，worker 依靠 emergency supplement 固定间隔重试。

风险：

- 单节点故障时没有指数退避。
- 不是 tight busy-loop，但会持续以 2s/5s 级别尝试。
- 大规模部署时可能对故障节点形成稳定重试流量。

建议：

- 单 endpoint 也应该有 connection-level backoff。
- 区分“不能从候选池移除单 endpoint”和“不需要 backoff”这两个概念。

---

### 8. 新增 endpoint 连接失败时，用旧 endpoint 数判断，可能跳过 unavailable 标记  ✅ 已修复

> 状态：已修复（commit 86f7deb）。`handleEndpointUpdate()` 把 `currentEndpoints` 视图的
> 更新提前到 add/remove 连接之前，使 `markEndpointUnavailable()` 在新增坏节点的同步异常路径
> 中也能基于新拓扑判断，不再误命中 size<=1 而跳过标记。

- 严重程度：中
- 类型：确定 bug
- 相关代码：
  - `src/main/java/com/tencent/tglog/TglogWorker.java:490`
  - `src/main/java/com/tencent/tglog/TglogWorker.java:492`
  - `src/main/java/com/tencent/tglog/TglogWorker.java:504`
  - `src/main/java/com/tencent/tglog/TglogWorker.java:505`
  - `src/main/java/com/tencent/tglog/TglogWorker.java:1511`
  - `src/main/java/com/tencent/tglog/TglogWorker.java:1526`
  - `src/main/java/com/tencent/tglog/TglogWorker.java:1527`
  - `src/main/java/com/tencent/tglog/TglogWorker.java:1128`
  - `src/main/java/com/tencent/tglog/TglogWorker.java:1129`

endpoint update 里先对新增 endpoint 调 `addConnectionsForEndpoint()`，最后才更新 `currentEndpoints`。如果当前只有 A，DNS 更新为 A+B，B 连接失败时，`markEndpointUnavailable(B)` 看到的还是旧 `currentEndpoints=[A]`，于是因为 size <= 1 跳过 unavailable 标记。

风险：

- 新增坏节点不会进入 unavailable/backoff。
- 后续 supplement 可能继续把它当正常候选反复尝试。

建议：

- endpoint update 时先更新 worker 的 endpoint 视图，再处理新增/删除连接。
- 或让 `markEndpointUnavailable()` 使用新 endpoint list 的 size 作为上下文。

---

### 9. endpoint 数变化后 `connectionsPerEndpoint` 不重新计算  ⏸️ 暂不修

> 状态：暂不修。`connectionsPerEndpoint` 只在 start() 按初始 endpoint 数算一次属实，DNS 后续
> 变更不重算，导致连接数偏多/偏少——但只影响连接冗余度，不影响正确性（不丢消息、不发坏数据）。
> TGLog 后端 endpoint 通常稳定，大幅动态增减是少见场景；重算还涉及缩容时是否主动关多余连接的取舍，
> 改动触及 supplement/recover/hardCap 多处，回归风险高于收益。如线上确实会大幅动态增减 endpoint，
> 再按重算方案处理。

- 严重程度：中
- 类型：容量策略偏差
- 相关代码：
  - `src/main/java/com/tencent/tglog/TglogWorker.java:305`
  - `src/main/java/com/tencent/tglog/TglogWorker.java:306`
  - `src/main/java/com/tencent/tglog/TglogWorker.java:504`
  - `src/main/java/com/tencent/tglog/TglogWorker.java:505`
  - `src/main/java/com/tencent/tglog/TglogWorker.java:1215`
  - `src/main/java/com/tencent/tglog/TglogWorker.java:1415`
  - `src/main/java/com/tencent/tglog/TglogWorker.java:1425`

`connectionsPerEndpoint` 只在初始化时按 endpoint 数计算。DNS 后续变更 endpoint 数时，该值不会重算。

风险：

- 从多个 endpoint 变成单 endpoint 后，连接冗余可能低于设计。
- 从单 endpoint 变成多个 endpoint 后，连接总数可能高于预期。
- 故障恢复和 supplement 都会继续使用旧值。

建议：

- 每次 endpoint list 变化时重新计算 `connectionsPerEndpoint`。
- 同步调整当前连接目标数和 hard cap 计算。

---

## P1 / 异步 flush 与错误传播问题

### 10. `tryFlush()` IOException 没有回传 worker，错误连接不会立即移除/补充  ✅ 已修复（严重程度修正为「低」）

> 状态：核心观察属实，但原「中」严重程度高估，实际为「低」——错误连接会自愈：
> 两个 `send()` 重载在 `state != CONNECTED` 时立即返回 `-1`（TglogConnection.java:198 / 290），
> 故一旦连接变 ERROR，下一次被 send 选中即 `sent<0` → `handleConnectionError()`（移除+补充+标记
> endpoint），不会永久泄漏，也不会发出半条坏数据。真正的小缺口是「延迟清理」：纯靠 tryFlush 失败
> 发现的 ERROR 连接，要等下次被 send 选中才清理，低流量 worker 下窗口可能较长，且这条路径只
> logger.error、不走错误计数/补充。
> 修复（最小改动，未触碰单向协议/buffer 语义）：
> - `tryFlush()` 改为返回 `boolean`（flush 成功/无数据=true；IOException 置 ERROR 后=false）。
> - event loop 的 writable 分支检查返回值：`if (!conn.tryFlush()) handleConnectionError(conn);`
>   失败立即移除+补充+标记 endpoint，与现有 send 失败路径行为一致。
> - drain（关闭）路径的 3 处 tryFlush 调用沿用旧语义忽略返回值——此时正在拆除，连接错误无意义，
>   且其循环本就以 `hasBufferedData()` 为准。

- 严重程度：低（原评「中」，已修正）
- 类型：异常传播断链
- 相关代码：
  - `src/main/java/com/tencent/tglog/TglogWorker.java:536`
  - `src/main/java/com/tencent/tglog/TglogWorker.java:537`
  - `src/main/java/com/tencent/tglog/TglogConnection.java:457`
  - `src/main/java/com/tencent/tglog/TglogConnection.java:466`
  - `src/main/java/com/tencent/tglog/TglogConnection.java:468`

worker 在 writable 事件里调用 `conn.tryFlush()`。`tryFlush()` 捕获 `IOException` 后只把 connection state 设为 `ERROR`，不会通知 worker。

风险：

- worker 不会立即 `handleConnectionError()`。
- 不会立即从 `connections` 移除错误连接。
- 不会立即 mark endpoint unavailable。
- 不会立即 supplement replacement connection。
- 如果后续没有新 send 选中这个连接，它可能长期留在 active list。

建议：

- `tryFlush()` 返回 flush 结果或抛出异常。
- worker 层统一处理 flush 失败，调用 `handleConnectionError()`。

---

### 11. `writeHighWatermark` 不是硬上限，buffer 可超过阈值  ❎ 非问题（设计如此，源于 #4 单向协议）

> 状态：确认非问题，是 #4（单向协议、success=已接受不可反悔）的直接设计后果。
> "watermark 是软阈值"属实，但这是协议单向性逼出来的：TGLog v1 没有 ack，SDK 一旦对某条
> 消息返回 success 就不能反悔。partial write 意味着该消息的一部分字节已写入 socket，剩余
> 部分必须缓冲——此时若因 watermark 拒绝缓冲剩余部分，会发出半条损坏的请求并违背"已接受
> 不反悔"。因此 partial-write 路径必须无条件 buffer，不能受硬上限约束。
> watermark 只能作用在"下一个完整 batch 进入前"的入口（`outputBufferSize > highWatermark`
> 时返回 0 整批拒绝），这是唯一能干净拒绝、不切碎消息的时机。
> 故 review 建议的"检查追加后大小 / partial write 也设上限 / 超限砍当前 batch"与单向协议
> 冲突，不予采纳。watermark 本就只能是"新 batch 入口处的软背压点"，无法做成 in-flight 硬上限。

- 严重程度：中
- 类型：内存保护不足
- 相关代码：
  - `src/main/java/com/tencent/tglog/TglogConnection.java:211`
  - `src/main/java/com/tencent/tglog/TglogConnection.java:216`
  - `src/main/java/com/tencent/tglog/TglogConnection.java:227`
  - `src/main/java/com/tencent/tglog/TglogConnection.java:306`
  - `src/main/java/com/tencent/tglog/TglogConnection.java:312`
  - `src/main/java/com/tencent/tglog/TglogConnection.java:324`
  - `src/main/java/com/tencent/tglog/TglogConnection.java:364`
  - `src/main/java/com/tencent/tglog/TglogConnection.java:376`
  - `src/main/java/com/tencent/tglog/TglogConnection.java:377`
  - `src/main/java/com/tencent/tglog/TglogConnection.java:508`
  - `src/main/java/com/tencent/tglog/TglogConnection.java:520`
  - `src/main/java/com/tencent/tglog/TglogConnection.java:524`

当前检查只判断 `outputBufferSize > highWatermark`，没有判断 `outputBufferSize + incomingSize > highWatermark`。partial write 和 rescue data 场景还会绕过或弱化检查。

风险：

- watermark 更像软阈值，不是硬限制。
- 慢连接、网络阻塞、频繁 rescue 时，内存使用可能超过配置。
- 和“partial write 即 success”叠加后，用户侧看不到背压。

建议：

- 检查追加后的大小是否超过 watermark。
- 对 rescue data 也设置总量上限。
- 超限时明确失败当前 batch 或进入 backpressure，而不是继续无感知接受。

---

## P2 / 生命周期、metrics、配置与诊断问题

### 12. `close()` 没有真正 join worker，超时后仍 unregister metrics  ✅ 已修复

> 状态：已修复。
> 现状核对：worker 线程确实未保存引用、`stop()` 只设状态+wakeup、`close()` 用
> `sleep(10)` 轮询 `isStopped()` 到 deadline 后照常 unregister。正常路径无竞态（worker
> 跑完 `cleanup()` 后置 `state=CLOSED`，`volatile` 可见），仅超时路径存在 worker 仍在
> `cleanup()` 内 touch metrics 与 unregister 并发的窗口（后果较轻，registry 用
> ConcurrentMap 不会结构性损坏，但仍应消除）。
> 修复：
> (1) `TglogWorker` 新增 `volatile Thread workerThread` 字段并在 `start()` 中保存；
> (2) 新增 `join(long timeoutMs)` 直接 join 线程，确保 `cleanup()` 完成后才返回；
> (3) `TglogClient.close()` 用 `worker.join(remaining)` 替换 sleep 轮询：join 成功即彻底
> 消除并发窗口；若超时未停止则记 warn，但**仍照常 unregister**——否则 collector 永久泄漏、
> 下次 client 重建必失败，比轻微并发更糟。给每个 worker 至少 1ms join 窗口避免被前面的
> worker 耗尽预算而跳过。

- 严重程度：中
- 类型：生命周期竞态
- 相关代码：
  - `src/main/java/com/tencent/tglog/TglogWorker.java:196`
  - `src/main/java/com/tencent/tglog/TglogWorker.java:199`
  - `src/main/java/com/tencent/tglog/TglogWorker.java:201`
  - `src/main/java/com/tencent/tglog/TglogWorker.java:1627`
  - `src/main/java/com/tencent/tglog/TglogWorker.java:1629`
  - `src/main/java/com/tencent/tglog/TglogWorker.java:1632`
  - `src/main/java/com/tencent/tglog/TglogClient.java:383`
  - `src/main/java/com/tencent/tglog/TglogClient.java:388`
  - `src/main/java/com/tencent/tglog/TglogClient.java:407`
  - `src/main/java/com/tencent/tglog/TglogMetrics.java:421`
  - `src/main/java/com/tencent/tglog/TglogMetrics.java:423`

worker thread 没保存引用，因此 client 无法 join。`stop()` 只是设置状态并 wakeup selector。`TglogClient.close()` 轮询到 deadline 后，无论 worker 是否真的停止，都会继续 unregister metrics。

风险：

- worker cleanup/drain 仍可能继续访问 metrics。
- metrics 自己的注释要求 unregister 前 worker 不再 touch metrics，当前实现不一定满足。
- Prometheus registry 可能出现并发注销/访问问题。

建议：

- `TglogWorker` 保存 thread 引用。
- `stop()` 后明确 `join(timeout)`。
- 如果超时未停止，不应直接 unregister，至少要记录错误并避免 registry 竞态。

---

### 13. `TglogMetrics` 构造阶段部分注册失败时，已注册 collector 没有 cleanup  ✅ 已修复

> 状态：已修复。
> 注意 `TglogClient` 构造已用 `initSucceeded`+`finally` 兜底，metrics 构造成功后的任何
> 初始化失败都会 `metrics.unregister()`，所以"client cleanup 覆盖不到"那一半早已不成立。
> 真正残留的缺口是 `TglogMetrics` 构造器**自身**中途注册失败：此时对象未逸出构造器，
> `this.metrics` 未赋值，外层 finally 拿不到失败实例无从回滚，前面已注册的 collector 变成
> 孤儿，导致后续 client 因 "Collector already registered" 持续失败。
> 修复：构造器内用一个 `List<Collector> registered` 跟踪每个成功注册的 collector（经
> `register()` 辅助方法），若中途任意 `.register()` 抛 `RuntimeException`，在 catch 中逐个
> `safeUnregister()` 回滚后再 rethrow，保证 registry 干净、可重建。`safeUnregister` 复用
> 既有幂等实现。

- 严重程度：中
- 类型：资源泄漏 / 重建失败风险
- 相关代码：
  - `src/main/java/com/tencent/tglog/TglogClient.java:74`
  - `src/main/java/com/tencent/tglog/TglogClient.java:79`
  - `src/main/java/com/tencent/tglog/TglogMetrics.java:76`
  - `src/main/java/com/tencent/tglog/TglogMetrics.java:80`
  - `src/main/java/com/tencent/tglog/TglogMetrics.java:83`
  - `src/main/java/com/tencent/tglog/TglogMetrics.java:178`
  - `src/main/java/com/tencent/tglog/TglogMetrics.java:182`

`TglogClient` 在 try/finally 之前构造 `TglogMetrics`。`TglogMetrics` 构造器连续注册多个 collector。

如果中途某个 `.register(registry)` 抛异常，前面已经注册成功的 collector 不会被 unregister。

风险：

- 默认 registry 复用、热重载、测试重复创建时，后续 client 可能因 collector already registered 继续失败。
- 这类失败发生在 `TglogClient` cleanup finally 之前，当前 cleanup 覆盖不到。

建议：

- `TglogMetrics` 构造器内部 try/catch，失败时注销已注册 collector。
- 或先构造 collector，最后统一注册，保证失败可回滚。

---

### 14. 用户 callback 只 catch `Exception`，不 catch `Throwable`  ✅ 已修复

> 状态：已修复。
> (1) `TglogClient` 的 wrappedCallback：catch 扩大到 `Throwable`，并把 `future.complete()/
> completeExceptionally()` 放入 `finally`，保证用户 callback 即使抛 `Error` 也不会让
> `CompletableFuture` 永久挂起；
> (2) `BatchBuffer.notifyAndRemove()/notifyAllFailed()`：catch 扩大到 `Throwable` 并记录
> 日志（原先静默吞掉），保证逐条隔离在 `Error` 下仍成立，一条坏 callback 不会中断回调
> 循环或拖垮 worker 线程。

- 严重程度：中低
- 类型：callback 隔离不完整
- 相关代码：
  - `src/main/java/com/tencent/tglog/TglogClient.java:222`
  - `src/main/java/com/tencent/tglog/TglogClient.java:225`
  - `src/main/java/com/tencent/tglog/TglogClient.java:227`
  - `src/main/java/com/tencent/tglog/TglogClient.java:233`
  - `src/main/java/com/tencent/tglog/BatchBuffer.java:241`
  - `src/main/java/com/tencent/tglog/BatchBuffer.java:243`
  - `src/main/java/com/tencent/tglog/BatchBuffer.java:287`
  - `src/main/java/com/tencent/tglog/BatchBuffer.java:289`

普通 `RuntimeException` 已被隔离，Future 仍会完成。但如果用户 callback 抛 `Error`，可能绕过保护。

风险：

- Future completion 可能被跳过。
- worker 线程可能受影响。

建议：

- 如果 SDK 目标是强隔离用户 callback，可考虑 catch `Throwable`，但对 `OutOfMemoryError` 等严重错误要谨慎。
- 至少明确 callback 不应抛 `Error`，并在测试中覆盖 `RuntimeException` 场景。

---

### 15. enqueue 失败统一报 `SEND_QUEUE_IS_FULL`，close 竞态下错误码误导  ❎ 非问题（约定如此）

> 状态：确认非问题。两个约定使其它失败原因不会出现：
> (1) `new TglogClient(...)` 成功返回时所有 worker 必然已 READY（`worker.start()` 同步阻塞
> 等待 READY，否则抛异常），不存在 running 但 worker 未 ready 的窗口；
> (2) 约定 close 后不得再 send，`send()` 入口已用 `closed/running` 检查挡掉。
> 因此正常使用下 `enqueue()==false` 只可能是队列满，映射为 `SEND_QUEUE_IS_FULL` 是准确的。
> send/close 并发竞态属于用户违反约定，不是 SDK 缺陷。

- 严重程度：中低
- 类型：诊断问题
- 相关代码：
  - `src/main/java/com/tencent/tglog/TglogWorker.java:1560`
  - `src/main/java/com/tencent/tglog/TglogWorker.java:1561`
  - `src/main/java/com/tencent/tglog/TglogWorker.java:1562`
  - `src/main/java/com/tencent/tglog/TglogClient.java:260`
  - `src/main/java/com/tencent/tglog/TglogClient.java:262`

`worker.enqueue()` 返回 false 可能代表：

- 队列满；
- worker 正在 closing；
- worker not ready；
- worker closed。

但 `TglogClient.send()` 全部映射为 `SEND_QUEUE_IS_FULL`。

风险：

- close/send 竞态下会误报 queue full。
- metrics `droppedMsgCount` 也会混入非队列满原因。
- 排障被误导。

建议：

- `enqueue()` 返回更细粒度结果。
- 区分 `QUEUE_FULL`、`CLIENT_CLOSED`、`WORKER_NOT_READY` 等错误码。

---

### 16. 初始 DNS 失败真实原因被吞，`dnsUpdateInterval <= 0` 未校验  ✅ 已修复

> 状态：已修复（commit 21d86ab）。`EndpointManager.start()` 初始解析直接调 `resolveHost()`
> 并向上抛真实 cause（周期任务仍 catch+log+保留旧 endpoints），`TglogClient` 把 cause 包进
> `TglogException`；`setDnsUpdateInterval()` 增加 `<= 0` 校验。

- 严重程度：低到中
- 类型：异常传播 / 配置校验问题
- 相关代码：
  - `src/main/java/com/tencent/tglog/EndpointManager.java:49`
  - `src/main/java/com/tencent/tglog/EndpointManager.java:51`
  - `src/main/java/com/tencent/tglog/EndpointManager.java:87`
  - `src/main/java/com/tencent/tglog/EndpointManager.java:103`
  - `src/main/java/com/tencent/tglog/EndpointManager.java:104`
  - `src/main/java/com/tencent/tglog/TglogClient.java:142`
  - `src/main/java/com/tencent/tglog/TglogClient.java:144`
  - `src/main/java/com/tencent/tglog/TglogConfig.java:162`
  - `src/main/java/com/tencent/tglog/TglogConfig.java:163`
  - `src/main/java/com/tencent/tglog/EndpointManager.java:54`
  - `src/main/java/com/tencent/tglog/EndpointManager.java:55`
  - `src/main/java/com/tencent/tglog/EndpointManager.java:58`

`EndpointManager.updateEndpoints()` catch 住所有异常并只打日志。初始 DNS 失败后，`TglogClient` 只能看到 endpoints 为空，抛出较泛化的错误。

另外 `setDnsUpdateInterval()` 不校验正数，但 `scheduleAtFixedRate()` 要求 period > 0。

风险：

- 调用方拿不到真实 DNS 异常 cause。
- `dnsUpdateInterval` 设置为 0 或负数时，在 start 阶段运行时失败，而不是配置阶段明确失败。

建议：

- 初始解析失败应向上抛出真实 cause。
- 周期性解析失败可以继续保留旧 endpoints，但应有 metrics/status 暴露。
- `setDnsUpdateInterval()` 校验必须大于 0。

---

## 补充发现（review 之外）

### 17. `sendBatchTcp/sendBatchUdp` 的 IOException 穿透换连接循环，单连接失败即整批失败  ✅ 已修复

> 状态：已修复。`conn.send()` 失败有两种表达：返回 `<0`/`==0`（不抛异常）和**抛 IOException**（mid-write
> 出错，如 RST/EPIPE）。原 `sendBatchTcp/sendBatchUdp` 的「换连接重试」循环只覆盖了返回值路径；
> 一旦 `send()` 抛 IOException，异常穿透整个 `while` 循环，被 `doSendBatch` 外层 catch，直接整批
> `SEND_FAILED`，**不再尝试其他连接**，违背「一个连接发送失败就换一个再试」的设计意图。
> 修复：在两个 sender 的循环内对 `conn.send()` 包 try/catch，捕获 IOException 时等同 `sent < 0`
> 处理（`handleConnectionError` + `tryCount++` + `getConnection()` 换下一个），耗尽全部连接后才返回
> `SEND_FAILED`；两个方法签名去掉 `throws IOException`；`doSendBatch` 移除现在多余的外层 catch。

- 严重程度：中
- 类型：错误处理 / 故障恢复
- 相关代码：
  - `src/main/java/com/tencent/tglog/TglogWorker.java`（`sendBatchTcp` / `sendBatchUdp` / `doSendBatch`）
  - `src/main/java/com/tencent/tglog/TglogConnection.java:197`（`send(ByteBuffer)` 失败时抛 IOException）
  - `src/main/java/com/tencent/tglog/TglogConnection.java:289`（`send(ByteBuffer[])` 失败时抛 IOException）

风险：在多连接场景下，只要轮到的那个连接 mid-write 抛异常，整批消息就会进入重试/失败流程，浪费了
其余健康连接，故障连接的隔离与重试能力形同虚设。

---

## 优先修复建议

建议按以下顺序修复：

1. 重做 `flush()/close()` 语义：flush request 进入 worker 线程执行，不跨线程读 worker 内部结构。
2. cleanup 超时时 fail 掉所有 queue/pending/currentBatch 消息，保证 callback/Future 必定完成。
3. 修复 TCP endpoint recovery backoff：只有 `finishConnect()` 成功后才清理 unavailable/retry。
4. 修复 `tryFlush()` 异常传播，让 worker 能立即移除错误连接、mark unavailable、补连接。
5. 明确 TCP buffered success 语义；如果 success 只是 SDK accepted，需要文档和 metrics 明确暴露。
6. 修复 endpoint update 顺序，以及 endpoint 数变化后的 `connectionsPerEndpoint` 重算。
7. 对 pending close 连接数、outputBuffer bytes、rescue bytes 建立统一上限和 metrics。
8. 补齐 metrics 构造失败回滚、DNS 配置校验、enqueue 错误码区分。

## 建议补充的测试场景

1. `flush()` 与 endpoint 更新、连接断开并发执行，确认无 `ConcurrentModificationException`。
2. `close()` 时 queue 中仍有大量消息，stopTimeout 很小，确认所有 Future 都完成失败而不是悬挂。
3. TCP server accept 后不读数据，制造 outputBuffer 积压，确认 high watermark 和 close/flush 行为符合预期。
4. TCP endpoint 拒绝连接，确认 recovery backoff 是指数退避而不是固定 5s。
5. 单 endpoint 不可达，确认 emergency supplement 有 backoff。
6. DNS 从 `[A]` 更新到 `[A, B]`，B 不可达，确认 B 会被标记 unavailable。
7. endpoint 数从 1 到 3、从 3 到 1 变化，确认 `connectionsPerEndpoint` 和目标连接数正确更新。
8. writable flush 抛 IOException，确认 worker 立即移除错误连接并补充连接。
9. 默认 CollectorRegistry 重复创建 client，确认 metrics 注册失败不会留下半注册状态。
10. callback 抛 `RuntimeException`，确认 Future 仍完成；如选择 catch `Throwable`，补充 `Error` 场景测试。
