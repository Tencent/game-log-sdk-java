# TglogWorker.java 代码详解（面向 Java 零基础读者）

> 本文假设你**完全不懂 Java**。`TglogWorker` 是整个 SDK 里**最复杂、最核心**的类，建议先读完
> `TglogConnection_解读.md`（它负责单条连接），再读本文（它负责管理一组连接 + 一个工作线程）。

---

## 一、预备知识

### 1. Worker 是什么？
`TglogWorker`（工作者）= **一个独立的工作线程 + 它管理的一批连接 + 一个消息队列**。

打个比方：把发日志想象成一个快递分拣中心：
- 应用程序（游戏服务器）不停地往传送带上放包裹（日志消息）。
- 分拣中心有好几个**分拣员（Worker）**，每个分拣员有自己的传送带（消息队列）和几辆货车（连接）。
- 分拣员把包裹**攒一批（batch）**装满一车，再开车送出去（发送），效率比一个一个送高。
- 货车坏了/到油量上限了（连接出错/到期），分拣员负责换新车，并把没送的包裹搬到别的车上。

SDK 默认有 8 个 Worker，互相独立工作。

### 2. 关键设计：「单消费者」无锁
- 应用程序可能有**很多线程**同时发日志（多生产者）。
- 但每个 Worker 内部只有**一个线程**在处理（单消费者）。
- 它们之间用一个**无锁队列 `LockFreeQueue`** 传递消息（MPSC = Multi-Producer Single-Consumer）。
- **好处**：因为「连接列表、批次、各种 Map」都只被这一个 worker 线程访问，所以**完全不需要加锁**，性能极高，也不会有并发 bug。
- **代价**：外部线程绝对不能直接碰 worker 的内部数据，只能通过「无锁队列」投递请求。

> 记住这条贯穿全文的铁律：**带 worker 内部状态的方法（如 `hasPendingWork()`、`processMessages()`）只能在 worker 线程里调用。** 外部线程（如调用 `flush()` 的线程）只能往队列里塞东西。

### 3. Java 语法补充
- **`implements Runnable`**：表示这个类「可以作为一个线程的任务来运行」，必须提供一个 `run()` 方法，线程启动后就执行 `run()` 里的代码。
- **`ConcurrentLinkedQueue`**：Java 自带的线程安全队列，多线程同时放/取都安全。
- **`AtomicInteger`/`AtomicLong`**：线程安全的整数/长整数。
- **`CompletableFuture<T>`**：一个「未来才会有结果的凭证」。比如你请求 flush，立刻拿到一张凭证，等真正刷完了，这张凭证才「兑现」。`<Boolean>` 表示结果是 true/false。
- **`synchronized (锁对象) { ... }`**：同一时刻只允许一个线程进入这段代码。
- **`volatile`**：保证变量的修改对其他线程立即可见。

---

## 二、整体结构鸟瞰

```
TglogWorker（一个分拣员 = 一条线程）
│
├── 状态机 State：INIT（初始化）→ READY（就绪）→ CLOSING（关闭中）→ CLOSED（已关闭）
│
├── 三个跨线程队列（外部线程往里放，worker 线程取）
│     ├── messageQueue        ：待发送的日志消息（无锁 MPSC 队列）
│     ├── endpointUpdateQueue  ：DNS 变化时的新地址列表
│     └── flushRequestQueue    ：外部请求"刷干净"的凭证
│
├── 连接管理（只在 worker 线程访问，无需加锁）
│     ├── connections          ：已就绪的连接
│     ├── connectingConns      ：正在连接中的连接
│     ├── unavailableEndpoints ：被标记为"不可用"的服务器（带退避恢复）
│     ├── pendingCloseList     ：计划延迟关闭的连接
│     └── pendingReplacements  ：到期连接的"在途替换"映射（新连接→待退休的旧连接）
│
├── 批次管理：currentBatch（当前正在攒的一批消息）+ pendingMessage（暂存的下一条）
│
└── 主循环 run() → runEventLoop()：不停地"处理事件→收消息→攒批→发送→周期任务"
```

---

## 三、生命周期总览（先建立直觉）

```
1. 创建：new TglogWorker(...)            → 状态 INIT
2. 启动：start()                          → 开一条新线程跑 run()，并阻塞等待"连接就绪"
3. run() 里：
     建 Selector → 初始化连接 → 等连接就绪 → 状态变 READY → 进入主循环 runEventLoop()
4. 主循环不停转：处理网络事件、攒批发送、周期维护
5. 关闭：stop()（外部调用，状态变 CLOSING）
     → 主循环退出 → finally 里 cleanup()（把残留消息发完/失败掉，关闭所有连接）→ 状态 CLOSED
```

---

## 四、字段逐个解释

```java
private final int workerId;          // 编号（0,1,2...）
private final TglogConfig config;    // 配置
private final TglogMetrics metrics;  // 监控指标
```

```java
private volatile State state = State.INIT;   // 当前状态
private volatile Thread workerThread;          // 工作线程本身（关闭时用它来 join 等待）
private final Object readyLock = new Object();  // 一把锁，用于"启动时等待就绪"
private final AtomicInteger readyConnCount;     // 已就绪连接数
private final AtomicInteger failedConnCount;    // 失败连接数
private final AtomicInteger initConnCount;      // 初始要建立的连接总数
private volatile boolean initStarted = false;   // 初始化是否已开始
```
- 这几个 `AtomicInteger` 和 `readyLock` 配合，解决一个问题：`start()` 在**主线程**调用，但连接是在**worker 线程**里建立的，主线程需要「等到连接就绪」才返回。它们就是用来跨线程同步这个「就绪」信号的。

```java
private final LockFreeQueue<LogMessage> messageQueue;                       // 日志消息队列
private final ConcurrentLinkedQueue<List<InetSocketAddress>> endpointUpdateQueue;  // 地址更新队列
private final ConcurrentLinkedQueue<CompletableFuture<Boolean>> flushRequestQueue; // flush 请求队列
```
- 三个跨线程通道，前面图里讲过。

```java
private List<InetSocketAddress> currentEndpoints;       // 当前的服务器地址列表
private final List<TglogConnection> connections;         // 已就绪连接
private final List<TglogConnection> connectingConns;     // 连接中
private final AtomicLong connectionIndex;                // 轮询选连接用的计数器
private int connectionsPerEndpoint;                      // 每个地址建几条连接
private final Map<String, Long> unavailableEndpoints;    // 不可用地址 → 失败时间
private final Map<String, Integer> endpointRetryCount;   // 不可用地址 → 重试次数
```

```java
private final BatchBuffer currentBatch;   // 当前正在攒的批
private LogMessage pendingMessage = null;  // 一条"放不进当前批、暂存待下一批"的消息
private volatile Selector selector;        // NIO 门卫
private final List<DelayedClose> pendingCloseList;  // 待延迟关闭的连接
private final Map<TglogConnection, TglogConnection> pendingReplacements;  // 在途替换：新连接 → 待退休的旧连接
```

一堆时间常量（单位毫秒），定义各种周期任务多久跑一次：
```java
MSG_PER_RUN = 3000                         // 每轮主循环最多处理 3000 条消息
ENDPOINT_RECOVER_INTERVAL_MS = 5000        // 每 5 秒尝试恢复不可用的服务器
CONNECTION_REPLACE_CHECK_INTERVAL_MS=20000 // 每 20 秒检查并替换到期连接
EMERGENCY_SUPPLEMENT_INTERVAL_MS = 5000    // 每 5 秒紧急补充连接
DEFAULT_CONN_CLOSE_DELAY_MS = 30000        // 正常替换连接：延迟 30 秒关闭
ERROR_CONN_CLOSE_DELAY_WITH_DATA_MS=30000  // 出错且有残留数据：延迟 30 秒
ERROR_CONN_CLOSE_DELAY_NO_DATA_MS = 5000   // 出错且无残留数据：延迟 5 秒
```

内部小类 `DelayedClose`：记录「某条连接，到了某个时间点(deadline)，因为是否出错(isError)，需要关闭」。

---

## 五、方法逐个解释

### A. 启动与初始化

#### `setInitialEndpoints(endpoints)`
在 `start()` 之前由 Client 调用，告诉 worker「初始要连哪些服务器」。这是无锁传数据的方式（启动前设置，之后只读）。

#### `start()` — 启动并等待就绪（在主线程调用）
```java
Thread thread = new Thread(this, "TglogWorker-" + workerId);
thread.setDaemon(false);   // 非守护线程：JVM 会等它结束才退出
this.workerThread = thread;
thread.start();            // 启动线程，开始执行 run()

// 然后主线程在这里"等"，分两步：
synchronized (readyLock) {
    // 第一步：等 worker 线程开始初始化（initStarted 变 true）
    while (!initStarted && state == INIT) { readyLock.wait(100); ... }
    // 第二步：等连接有结果（就绪数 + 失败数 >= 初始总数）
    while (state == INIT && readyConnCount + failedConnCount < initConnCount) { readyLock.wait(100); ... }
}

if (readyConnCount.get() <= 0) {
    throw new RuntimeException("...failed to initialize connections");  // 一条都没连上 → 启动失败
}
```
- `readyLock.wait(100)`：释放锁并最多睡 100 毫秒，期间 worker 线程会 `notifyAll()` 唤醒它。
- 有超时保护（`timeout`），避免永久卡死。
- **核心保证：`start()` 返回时，至少有一条连接是就绪的**，否则抛异常。

#### `run()` — 工作线程的入口（线程启动后执行）
```java
try {
    selector = Selector.open();        // 开门卫
    initializeConnections();           // 根据初始地址建立连接
    waitForInitialConnections();       // 等连接握手完成
    if (state == State.INIT) {
        state = State.READY;            // 标记就绪
        runEventLoop();                 // 进入主循环（核心！）
    }
} catch (Exception e) {
    ... 出错也要唤醒等待的主线程 ...
} finally {
    cleanup();                          // 无论如何，最后都要清理
}
```

#### `initializeConnections()` — 建立初始连接
```java
currentEndpoints = 初始地址列表的副本;
connectionsPerEndpoint = (地址数 <= 2) ? 2 : 1;   // 地址少就每个建2条，地址多就每个建1条
for (每个 endpoint) {
    for (i in 0..connectionsPerEndpoint) {
        TglogConnection conn = new TglogConnection(...);
        conn.connect(selector);          // 发起连接（非阻塞）
        connectingConns.add(conn);        // 放进"连接中"列表
        initConnCount++;
    }
    if (这个地址一条都没建成功) markEndpointUnavailable(endpoint);  // 标记不可用
}
synchronized (readyLock) { initStarted = true; readyLock.notifyAll(); }  // 通知主线程"初始化开始了"
if (UDP) checkConnectingConnections();   // UDP 立即就绪，马上检查
```

#### `waitForInitialConnections()` — 等待初始连接握手完成
在一个有截止时间的循环里反复：`selector.select()` 等事件 → `processSelectionKeys()` 处理 → `checkConnectingConnections()` 检查谁连上了。直到「就绪+失败 >= 总数」或超时。

### B. 主循环（最核心）

#### `runEventLoop()` — 主事件循环
只要状态是 `READY` 就一直转，每一轮做这些事：
```java
while (state == READY) {
    processEndpointUpdates();    // 1. 处理 DNS 地址更新

    // 2. 等网络事件：队列空时等 10ms（省 CPU），队列忙时等 5ms（高吞吐）
    int readyKeys = messageQueue.isEmpty() ? selector.select(10) : selector.select(5);
    if (readyKeys > 0) processSelectionKeys();   // 处理就绪的连接事件

    checkConnectingConnections();  // 3. 检查"连接中"的连接是否连上了
    processMessages();             // 4. 从队列取消息、攒批、发送（最多 3000 条/轮）
    processFlushRequests();        // 5. 处理 flush 请求

    // 6. 周期任务（按时间间隔触发）
    每 5 秒：recoverUnavailableEndpoints();   // 恢复不可用服务器
    每 20 秒：replaceExpiredConnections();      // 替换到期连接
    每 5 秒：emergencySupplementConnection();   // 紧急补连接
    每 1 秒：processPendingCloses();            // 处理延迟关闭
    每 5 秒：更新监控指标
}
```
- **不需要别人"唤醒"它来收消息**：因为 worker 线程每隔 5~10ms 就主动轮询一次队列。这是性能和延迟的折中。

#### `processEndpointUpdates()` / `handleEndpointUpdate()` — DNS 地址变化处理
当 DNS 解析出新地址时（比如服务器扩容/缩容），Client 会往 `endpointUpdateQueue` 投递新列表。worker 线程取出后：
```java
if (newEndpoints == null || 空) { 打 warn 日志并 return; }  // 忽略空更新，见下方说明
对比新旧地址，算出"新增的 added"和"移除的 removed"；
currentEndpoints = 新列表;   // 注意：先更新这个视图，再增删连接（注释解释了顺序的重要性）
对每个新增地址：addConnectionsForEndpoint(endpoint);   // 先加新连接，保证可用性
对每个移除地址：removeConnectionsByEndpoint(endpoint); // 再删旧连接
```
- **为什么先更新 `currentEndpoints` 再增删？** 因为 `markEndpointUnavailable()` 会查 `currentEndpoints.size()` 来决定要不要退避，必须先反映新拓扑，否则新加的不可达地址可能被错误跳过。
- **为什么忽略空更新？** 若直接应用一个空/`null` 列表，会把 `currentEndpoints` 清空并关掉所有连接，让 worker 无处可发。而空列表绝大多数情况是 DNS 解析临时失败，而非真的「没有服务器」，所以保留现有地址继续工作更稳妥。

#### `processSelectionKeys()` — 处理门卫报告的就绪事件
```java
遍历每个就绪的 SelectionKey：
    conn = key.attachment();   // 取回当初登记时附上的连接对象
    if (key.isConnectable()) {
        handleConnectable(conn);   // 连接完成事件（独占处理，因为还不是数据通道）
    } else {
        if (key.isReadable()) handleReadable(conn);  // 可读：探测对端是否关闭/丢弃意外数据
        // 注意：读和写不是 else-if，而是各自独立判断，
        // 这样处理完读不会把"写"推迟到下一轮
        if (key.isValid() && key.isWritable()) {
            if (!conn.tryFlush()) handleConnectionError(conn);  // 刷缓冲；失败说明连接坏了
        }
    }
```
- 注释里特别说明：读和写可能在同一次同时就绪，要分别处理，不能用 `else if`，否则会延迟刷写。

#### `checkConnectingConnections()` — 检查连接中的连接
遍历 `connectingConns`：
- 若某条变成 `CONNECTED`：从「连接中」移到「已就绪 connections」；清除该地址的「不可用」标记；打点；如果还在 INIT 阶段，增加 `readyConnCount` 并唤醒主线程。**若这条连接是某个到期连接的「在途替换」（在 `pendingReplacements` 里），现在替换可用了，就把对应的旧连接安排延迟关闭退休，并清除映射。**
- 若某条变成 `ERROR`：移除；标记地址不可用；INIT 阶段则增加 `failedConnCount` 并唤醒主线程。**若它是一次替换尝试，丢弃映射，保留旧（到期）连接继续服务，等下个周期重试。**

#### `handleConnectable(conn)`
```java
if (conn.finishConnect()) { 连接成功 }
else {
    // finishConnect 失败：连接从未真正建立（还在握手阶段）
    // 不补充新连接（supplement 是给"运行中突然断的连接"用的）
    // 重连由 recoverUnavailableEndpoints() 定时器带退避地处理
    handleConnectionError(conn, false);
}
```

#### `handleReadable(conn)` — 处理可读事件
TGLog v1 协议没有响应，所以「可读」通常意味着：
```java
if (TCP) {
    bytesRead = channel.read(testBuf);
    if (bytesRead < 0) handleConnectionError(conn);   // -1 = 对端关闭了连接
    else if (bytesRead > 0) 丢弃意外数据;               // 收到不该收的数据，直接丢
    // bytesRead == 0 = 暂无数据，忽略
} else { // UDP：收到意外数据就丢弃，UDP 不会有"关闭"
}
```

### C. 连接错误处理

#### `handleConnectionError(conn, supplement)` — 连接出错的统一处理
```java
// 特殊情况：如果这条连接本来就在"延迟关闭列表"里，说明它已经被判过"死刑"了
// 它出错/被对端关闭是预期内的，不要再标记地址不可用、也不要补连接，
// 只把它残留的数据救走然后关闭
if (removeFromPendingClose(conn)) { flushOrRescueAndClose(conn); return; }

connections.remove(conn);            // 从两个列表移除
connectingConns.remove(conn);
// 清理"在途替换"映射的两端：
//   若 conn 是在途替换的【新连接】（刚出错）→ 移除映射，保留旧连接继续服务
//   若 conn 是某个在途替换指向的【旧连接】（在替换连上前就断了）→ 从映射的值里移除
pendingReplacements.remove(conn);
pendingReplacements.values().remove(conn);
conn 的 selectionKey 关闭所有事件关注;  // 立即停止关注，避免延迟关闭期间空转
markEndpointUnavailable(conn.getEndpoint());  // 标记地址不可用
closeConnectionWithDelay(conn, true);          // 延迟关闭（带数据救援）
metrics.incrementErrorConn(workerId);

if (supplement) supplementConnection();  // 只有"运行中断掉"才立即补一条新连接
```
- `handleConnectionError(conn)`（单参数版）= `handleConnectionError(conn, true)`，即默认要补充。

### D. 消息处理与发送（业务核心）

#### `processMessages(boolean shutdown)` — 从队列取消息、攒批、发送
这是分拣员的核心工作。`shutdown` 参数表示「是否在关闭流程中清空队列」（关闭时不讲究退避，立即发，发不成就直接判失败，保证收尾能快速结束）。

循环最多处理 `MSG_PER_RUN`(3000) 条，每轮大致逻辑：
```java
boolean retry = !shutdown;   // 正常模式允许重试，关闭模式不重试

if (isRetrying()) {           // 如果当前批正在重试中
    if (重试超时 || 发送超时) failCurrentBatch(SEND_TIMEOUT);  // 超时则整批判失败
    else if (到了重试时间) { sendBatch(retry) ... }            // 时机到了就再发一次
    else break;                                                // 还在退避等待期，跳过
}
if (发送超时) failCurrentBatch(SEND_TIMEOUT);
if (canSendBatch()) sendBatch(retry);   // 批攒够了/超时了就发

LogMessage msg = pendingMessage != null ? pendingMessage : messageQueue.poll();  // 取一条消息
if (msg == null) break;       // 没消息了，退出

if (这条消息加进去会超过单请求上限) { 暂存它，先把当前批发了; continue; }
if (!currentBatch.add(msg))  { 批满了，暂存它，先发当前批; continue; }
if (canSendBatchIgnoreTimeout()) sendBatch(retry);  // 加完后如果批够了就发
```
- 「攒批」就是把多条小日志合并成一次网络发送，大幅提升吞吐。
- `pendingMessage` 处理「这条消息放不进当前批，得等当前批发走后再放进下一批」的情况。

#### 几个判断方法
- `canSendBatch()` = `currentBatch.shouldFlush()`：批是否该发（条数够/字节够/攒够时间）。
- `canSendBatchIgnoreTimeout()`：不看时间，只看条数或字节是否到上限。
- `isBatchSendTimeout()`：这批从创建到现在是否超过发送超时。
- `isRetrying()` / `isRetryingTimeout()`：是否在重试 / 重试是否已超总退避时间。

#### `sendBatch(boolean retry)` — 发送当前批
```java
if (currentBatch.isEmpty()) return true;
SendResult result = doSendBatch();      // 真正发送
if (result.isOk()) { completeBatch(OK); return true; }   // 成功 → 触发成功回调
if (!retry)        { completeBatch(result.errorCode); return true; }  // 不重试 → 判失败
return retryBatch();                     // 重试
```
- 返回 `true` = 这批处理完了（成功或失败）；返回 `false` = 还在重试中。

#### `doSendBatch()` — 真正发送
```java
TglogConnection conn = getConnection();  // 轮询拿一条连接
if (conn == null) return error(NO_AVAILABLE_CONN);
ByteBuffer[] buffers = currentBatch.prepareForSend();  // 准备好要发的字节
return (TCP) ? sendBatchTcp(conn, buffers) : sendBatchUdp(conn, buffers);
```

#### `sendBatchTcp(conn, buffers)` — TCP 发送（含连接故障转移）
```java
final int maxTries = connections.size();  // 关键：循环前就固定重试次数（见下方注释）
while (tryCount < maxTries) {
    try { sent = conn.send(buffers); }
    catch (IOException e) {                 // 写到一半连接崩了（RST/EPIPE）
        handleConnectionError(conn); tryCount++;
        换下一条连接，没有了就返回失败; continue;
    }
    if (sent < 0)  { 连接错误 → 同上换连接 }
    if (sent == 0) { 超过高水位 → 换连接（不算连接错误） }
    // sent > 0：检查是否全发完
    if (全发完 || 部分发出但已缓冲) return ok();   // 都算成功（缓冲的会后续刷出）
}
return error(SEND_FAILED);
```
- **注释里一个重要的坑**：`maxTries` 必须在循环**外**固定。因为 `handleConnectionError()` 会从 `connections` 里删连接，列表变短；如果循环条件实时读 `connections.size()`，会导致只尝试了大约一半的连接就放弃。
- **故障转移**：一条连接发不动，自动换下一条，最多尝试「原始连接数」次。

#### `sendBatchUdp(conn, buffers)` — UDP 发送
逻辑类似，但最多尝试 `min(连接数, 3)` 次。UDP 是「全发或不发」，`sent > 0` 即成功，`sent == 0` 视为系统缓冲满（EAGAIN）换连接重试。

#### `retryBatch()` — 安排重试
```java
if (重试次数 >= 最大重试数) { completeBatch(SEND_TIMEOUT); return true; }  // 重试够了，判失败
currentBatch.markForRetry();   // 标记重试（次数+1，记录退避时间戳）
metrics.incrementRetryMsg();
return false;                   // 还在重试
```

#### `completeBatch(errorCode)` — 完成一批（触发回调）
```java
if (OK) {
    currentBatch.notifyAndRemove(msgCount);  // 对每条消息调用"成功"回调
    每条 metrics.incrementSucceededMsg();
} else {
    currentBatch.notifyAllFailed(errorCode); // 对每条消息调用"失败"回调
    超时 → incrementTimeoutMsg()，否则 incrementFailedMsg();
}
currentBatch.clear();   // 清空批，准备攒下一批
```
- **「回调」**：应用程序发日志时可以传一个回调函数，发送有结果时 SDK 就调用它告诉应用「成功了/失败了」。

### E. 连接池维护

#### `getConnection()` — 轮询取一条可用连接
```java
if (connections.isEmpty()) {
    // 没连接了，但限制紧急补充的频率（至少隔 2 秒），避免疯狂尝试
    if (距上次紧急补充 >= 2秒) { supplementConnection(); 更新时间; }
    return null;
}
long index = connectionIndex.getAndIncrement() & Long.MAX_VALUE;  // 自增计数器（保证非负）
return connections.get((int)(index % connections.size()));         // 轮询
```

#### `markEndpointUnavailable(endpoint)` — 标记地址不可用
```java
if (currentEndpoints.size() <= 1) return;       // 只剩一个地址，不标记（标了就没法发了）
if (该地址还有活着的连接) return;                 // 还有连接就不标记
if (已经标记过) return;                           // 已标记则保留原退避状态，不重置时间戳
unavailableEndpoints.put(addr, 现在);            // 记录失败时间
endpointRetryCount.put(addr, 0);                  // 重置该地址的恢复重试计数
```
- **保留退避状态**很关键：否则反复失败会不断重置退避，导致恢复尝试过于频繁。

#### `recoverUnavailableEndpoints()` — 恢复不可用地址（每 5 秒）
```java
对每个不可用地址，按退避策略判断"是否到了该重试的时间"，到了就加入 toRecover；
对每个 toRecover 地址：尝试建 connectionsPerEndpoint 条连接（放入 connectingConns）；
    // 注意：非阻塞 TCP 的 connect 只是"发起"，没真正连上！
    // 所以这里【不能】清除不可用状态，否则会把退避退化成固定间隔重试。
    // 真正连上后，由 checkConnectingConnections() 清除不可用状态。
    // 这里只推进退避计数：endpointRetryCount++，unavailableEndpoints 时间戳更新为现在
最后：补充连接到目标数量（currentEndpoints.size() * connectionsPerEndpoint）
```

#### `replaceExpiredConnections()` / `replaceConnection()` — 替换到期连接（每 20 秒）
- 遍历 `connections`，对 `isExpired()` 的连接调 `replaceConnection()`。
- `replaceConnection(old)`：轮询找一个可用地址，建一条新连接放入 `connectingConns`，并**记录「在途替换」映射 `pendingReplacements.put(新连接, 旧连接)`**。
- **关键点（延迟退休旧连接）**：这里**不会立即移除/关闭旧连接**。非阻塞 TCP 的 `connect()` 只是「发起握手」，新连接此刻还没真正可用（还在 `connectingConns`）。如果此时就把旧连接移走，整个握手期间该地址就少了一条可用连接，极端情况下（旧连接是最后一条）会让 worker 暂时「零可用连接」，触发不必要的紧急补充。
- 旧连接会**继续承载流量**，直到新连接在 `checkConnectingConnections()` 里变成 `CONNECTED`，才把旧连接安排「正常延迟关闭（30秒）」退休——既消除了可用连接数的瞬时下降，又给旧连接时间把残留数据发完。
- 几种异常的兜底（都在 worker 线程内处理，靠 `pendingReplacements` 映射）：
  - 新连接**连接失败**（变 ERROR / 出错）：丢弃映射，**保留旧（到期）连接继续服务**，等下个 20 秒周期再重试替换——地址不会丢失可用连接。
  - 旧连接在新连接连上之前**自己先断了**：从映射里把它移除，将来新连接连上时只会「找不到要退休的旧连接」而已。
  - DNS 移除了相关地址（`removeConnectionsByEndpoint`）：相应清理映射，不会去为已不存在的替换/旧连接做退休。
  - `replaceConnection` 开头有 `containsValue(old)` 守卫：若某旧连接的替换还在途中，下个周期不会重复为它再建一条替换，避免该地址连接数膨胀。

#### 延迟关闭机制
- `closeConnectionWithDelay(conn, isError)`：把连接加入 `pendingCloseList`，设定关闭时间点（deadline）。
  - 正常替换：延迟 30 秒。
  - 出错有残留数据：30 秒；出错无残留数据：5 秒。
  - 若已在列表里，**只会缩短 deadline，绝不延长**。
- `processPendingCloses()`（每 1 秒）：遍历列表，到期的就 `flushOrRescueAndClose()`。
- `removeFromPendingClose(conn)`：从列表移除某连接，返回它是否曾在列表里。

#### `flushOrRescueAndClose(conn)` — 关闭前抢救数据
策略「先自刷，救援兜底」：
```java
1. 若连接还 CONNECTED 且有缓冲数据 → conn.tryFlush()（让它自己把数据发出去，保证顺序）
2. 若还有残留 → 找一条健康的备用连接，把数据 removePendingData() 搬过去 writePendingData()
                并 metrics.addRescuedMsgBytes() 打点
3. conn.close()，打点 incrementClosedConn
```
- 用在三处：延迟关闭到期、延迟关闭连接写失败、以及关闭流程 `cleanup()`。目的是**绝不静默丢数据**。

#### `supplementConnection()` — 补充连接
两阶段：
1. 优先给「连接数不足 `connectionsPerEndpoint`」的可用地址补。
2. 都满了的话，强制补，但有**硬上限 `connectionsPerEndpoint * 2`**，防止某台服务器挂了时连接无限累积。

#### `emergencySupplementConnection()`（每 5 秒）：若 `connections` 空了就补。
#### `addConnectionsForEndpoint(endpoint)` / `removeConnectionsByEndpoint(endpoint)`：DNS 变化时增删某地址的连接。

### F. 对外接口（供 Client 调用）

#### `enqueue(LogMessage message)` — 投递一条消息（应用线程调用，无锁）
```java
if (state != READY) return PRODUCER_CLOSED;     // worker 不在接收状态
if (!messageQueue.offer(message)) return SEND_QUEUE_IS_FULL;  // 队列满了（背压）
metrics.incrementEnqueuedMsg();
return OK;
```
- **关键设计**：区分两种失败——
  - `PRODUCER_CLOSED`：worker 没在干活（初始化/关闭中/线程挂了），重试也没用。
  - `SEND_QUEUE_IS_FULL`：worker 在干活但队列满了（真正的背压），可以稍后重试。
  - 这样调用方不会被误导，对一个「死掉的 worker」一直当背压重试。

#### `notifyEndpointUpdate(newEndpoints)`：往 `endpointUpdateQueue` 投递新地址并 `selector.wakeup()` 唤醒。

### G. Flush（确保数据发干净）

#### `requestFlush()` — 外部请求 flush（不阻塞、不碰内部状态）
```java
CompletableFuture<Boolean> future = new CompletableFuture<>();
flushRequestQueue.offer(future);   // 先入队
if (state != READY && state != INIT) {
    // worker 已关闭，可能再也不会处理队列了 → 自己把队列里的请求全部 complete(false)
    把队列里所有请求 complete(false); return future;
}
selector.wakeup();   // 唤醒 worker 尽快处理
return future;
```
- 注释强调「先入队再检查状态」的顺序很重要，避免请求被遗漏或永久挂起。

#### `processFlushRequests()`（worker 线程，每轮主循环）：
```java
if (flushRequestQueue 空) return;
if (hasPendingWork()) return;       // 还有活没干完，先不兑现，下一轮再看
否则把所有等待的 future complete(true);   // 干净了，全部兑现
```

#### `completeFlushRequestsOnStop()`（worker 关闭时）：根据此刻是否还有残留工作，把所有 flush 请求 complete(true/false)，避免调用方永久挂起。

#### `hasPendingWork()` — 是否还有未完成的工作（**只能 worker 线程调用**）
检查：消息队列非空？有 pendingMessage？当前批非空？任何连接的输出缓冲有数据？延迟关闭连接里有数据？任一为真就返回 true。

### H. 停止与清理

#### `stop()`：若状态是 READY/INIT，改为 CLOSING 并 `selector.wakeup()`（主循环会因状态变化而退出）。
#### `isStopped()`：状态是否为 CLOSED。
#### `join(timeoutMs)` — 等待工作线程真正结束
```java
Thread thread = this.workerThread;
if (thread == null) return isStopped();   // 从没启动过
thread.join(timeoutMs);                     // 最多等 timeoutMs，等线程跑完
return !thread.isAlive();                    // 线程是否已经死了
```
- **为什么用 `join` 而不是轮询 `isStopped()`？** 因为 `join` 能确保 worker 的 `cleanup()` **完全跑完**才返回。Client 关闭时要在所有 worker 真正结束后才注销 Prometheus 指标，否则会出现「worker 还在用指标，指标却被注销了」的并发竞争。

#### `drainConnectionBuffers()` — 关闭前榨干连接缓冲
在一个有超时（`stopTimeout`）的循环里，反复检查所有连接（包括延迟关闭的）的输出缓冲，调 `tryFlush()` 把残留数据发出去，并用 `selector.select(5)` 等连接变可写。全部清空或超时则结束。

#### `cleanup()` — 最终清理（worker 线程退出前在 `finally` 里执行）
这是关闭流程的收尾，顺序很讲究：
```java
1. 统计残留消息数（队列 + 批 + pending），打日志
2. 在有截止时间的循环里 processMessages(true)（关闭模式）把队列里的消息尽量发完
     —— 关闭模式不讲退避、不重试，保证循环能快速推进，不会空转耗光 stopTimeout
3. 发最后一批 sendBatch(false)
4. 若超时仍有残留 → failRemainingMessages()：把发不出去的消息全部以 PRODUCER_CLOSED 判失败
     （触发它们的回调/兑现 Future，避免应用永久挂起）
5. drainConnectionBuffers()：榨干连接缓冲
6. 对 pendingCloseList 里的每条连接 flushOrRescueAndClose()：把残留数据救到健康连接
     —— 必须在 connections 还非空时做，因为救援目标要从 connections 里挑
7. 再 drainConnectionBuffers() 一次：把刚救过来的数据也刷出去
8. 关闭所有连接（connections + connectingConns）、关闭 selector
9. completeFlushRequestsOnStop()：兑现关闭期间收到的 flush 请求
10. state = CLOSED
11. 再 completeFlushRequestsOnStop() 一次：兜底处理"在状态翻转前一刻挤进来"的 flush 请求
```

#### `failRemainingMessages()` — 把未发出的消息全部判失败
关闭时若有消息没能发出去，逐一以 `PRODUCER_CLOSED` 调用其回调、更新 `incrementDroppedMsg()` 指标。否则这些消息会被静默丢弃，应用程序的回调永远不被调用、`CompletableFuture` 永远不兑现（导致挂起）。

#### `failMessage(message, exception)` — 安全地调用单条消息的失败回调
用 try-catch 包住回调，**隔离用户回调里抛出的异常**，避免一个坏回调影响其他消息的处理。

---

## 六、把整个流程串起来（一条日志的旅程）

```
应用线程：client.send("日志\n", 回调)
   → Client 选一个 worker，调 worker.enqueue(message)
   → message 进入该 worker 的 messageQueue（无锁，立即返回）

worker 线程（独立运行的主循环）：
   → processMessages() 从 messageQueue 取出 message
   → currentBatch.add(message) 攒进当前批
   → 批攒够（条数/字节/时间）→ sendBatch()
   → doSendBatch() → getConnection() 轮询一条连接 → sendBatchTcp/Udp()
   → conn.send(buffers)（发不完就进 conn 的 outputBuffer）
   → 成功 → completeBatch(OK) → 对每条 message 调用"成功"回调 → 应用的 Future 兑现

如果连接坏了：
   → handleConnectionError() → 标记地址不可用 + 延迟关闭（救数据）+ 补一条新连接
   → recoverUnavailableEndpoints() 定时带退避地尝试恢复

关闭时：
   → stop() → 主循环退出 → cleanup() 把残留数据发完/救走/判失败 → CLOSED
```

---

## 七、一句话总结
`TglogWorker` 是一个**自带工作线程的连接池 + 批处理引擎**。它用「无锁队列接收消息、单线程内部处理、攒批发送、连接故障自动转移、地址不可用退避恢复、连接到期轮换、关闭前数据救援」这一整套机制，在**高吞吐、不丢数据、自愈**之间取得平衡。理解它的关键是记住一条铁律：**所有内部状态只由它自己的那一个线程访问，外部一律通过三个跨线程队列交互。**
