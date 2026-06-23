# 更新日志

本文档记录了 TGLog Pure Java Client 的所有重要变更。

格式基于 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.0.0/)，
版本号遵循 [语义化版本](https://semver.org/lang/zh-CN/)。

## [1.0.2] - 2026-06-23

### 问题修复

- **修复 TCP 立即连接成功时未注册 Selector 的问题**：
  - 非阻塞 TCP `connect()` 立即成功时也注册到 `Selector`
  - 确保后续可正确安装 `OP_READ`，并在分片写入时注册 `OP_WRITE`

- **修复 CONNECTING 连接超时后未清理的问题**：
  - 记录非阻塞连接发起时间，周期性清理超过 `connectionTimeout` 的连接
  - 初始化阶段超时连接会计入失败数，避免启动等待遗漏
  - 替换连接超时时保留旧连接继续服务，等待后续恢复周期重试

- **修复过期连接重复创建替换连接的问题**：
  - 检查进行中的 replacement，避免为同一个旧连接创建多个替换连接
  - 防止 `connectingConns` 持续增长和连接数异常膨胀

- **修复连接创建失败时的通道资源泄漏**：
  - 连接 setup 任一步骤失败时，主动取消选择键并关闭已打开的 TCP/UDP channel
  - 关闭资源时逐项处理，避免单个关闭异常影响其他资源清理

- **修复可变配置 setter 缺少校验的问题**：
  - `setProtocol()`、`setHost()`、`setPort()` 现在拒绝非法值
  - Socket 发送/接收缓冲区大小 setter 现在要求正数

- **修复关闭清理阶段用户回调异常可能中断清理的问题**：
  - shutdown 失败回调路径改为捕获 `Throwable`
  - 避免用户回调抛出 `Error` 时中断 Worker 清理流程

- **修复空端点更新导致所有连接被清空**：
  - DNS 解析瞬时故障不再清空现有连接
  - 忽略空的端点更新，保持连接稳定性

- **修复过期连接替换时的连接空窗期**：
  - 追踪进行中的替换连接，仅在新连接建立后才关闭旧连接
  - 防止因连接空窗期触发不必要的紧急连接补充

- **修复入队失败时的错误码区分**：
  - `enqueue()` 现在返回 `TglogErrorCode`（`PRODUCER_CLOSED` 或 `SEND_QUEUE_IS_FULL`），替代原有的 boolean 返回值
  - 调用方可准确区分 Worker 未运行与队列已满两种情况

- **修复延迟关闭连接的错误处理**：
  - 延迟关闭连接上的连接错误现在路由到救援/自刷新逻辑，而非标记端点不可用

- **修复关闭时延迟关闭连接的数据丢失**：
  - `cleanup()` 现在使用 `rescueAndClose()` 处理待关闭连接，防止静默数据丢失

- **修复选择键的可读/可写事件独立处理**：
  - 移除 else-if 链，可读和可写事件现在独立处理
  - 防止写刷新被推迟到下一次迭代

- **修复关闭模式下的 CPU 空转**：
  - `processMessages()` 在关闭模式下忽略退避窗口
  - 消除 close() 期间的 CPU 空转问题

- **修复 TCP 发送重试循环中的连接计数快照**：
  - 重试循环前先快照连接数，防止 `handleConnectionError()` 期间出现僵尸连接

- **修复批次中途连接异常时的重试逻辑**：
  - `conn.send()` 抛出 `IOException` 时不再导致整个批次失败，而是重试其他连接

- **修复 close() 的 flush 未在超时内排空时缺少日志**：
  - 停止超时场景下新增 INFO 级别日志输出

- **修复 flush 跨线程状态读取的线程安全问题**：
  - flush 通过 Worker 线程请求方式实现，替代原有的跨线程状态读取

- **修复 tryFlush 失败未传递给 Worker 的问题**：
  - Worker 现在能立即处理 flush 失败，及时进行连接清理

- **修复 close() 时 Worker 线程 join 缺失导致的指标注销竞争**：
  - 指标注销前先 join Worker 线程，防止并发竞争

- **修复 LockFreeQueue 最小容量边界问题**：
  - 队列容量向上取 2 的幂时最小返回 2，防止 capacity=1 下 sequence 状态重叠导致覆盖未消费元素
  - 增加超大容量保护，避免容量取整时整数溢出
  - 扩展压力测试覆盖最小容量、非 2 的幂容量和 wrap-around 场景

### 性能优化

- **调整每端点连接数，提升冗余度**：
  - 端点数 ≤ 2 时：每端点连接数从 2 提升至 3
  - 端点数 > 2 时：每端点连接数从 1 提升至 2

- **替换过期连接的追踪方式优化**：
  - 用单个 `TglogConnection.replacingConn` 字段替代 `Map<new, old>`
  - 消除 O(n) 反向查找，提升性能

- **UDP 发送重试行为与 TCP 对齐**：
  - UDP 现在使用快照的 `connections.size()` 作为重试预算
  - 统一错误处理语义：`sent < 0` 表示连接错误，`sent == 0` 表示 EAGAIN

- **Worker 连接并行初始化，加快客户端启动**：
  - `TglogWorker.start()` 拆分为 `start()`（仅启动线程，非阻塞）与 `awaitReady()`（等待连接就绪）
  - `TglogClient` 先循环 `start()` 所有 Worker 再循环 `awaitReady()`，使各 Worker 的连接初始化并行进行
  - 启动耗时从「N 个 Worker 串行累加」降为「最慢 Worker 的等待时间」

- **优化 LockFreeQueue 槽位写入清理开销**：
  - 在 per-slot sequence 发布语义保持不变的前提下，buffer 写入和清理改用 `lazySet()`，降低热路径 volatile 写屏障开销

### 代码改进

- 提取 `handleWritable()` 方法，与 `handleConnectable()`/`handleReadable()` 保持一致的代码结构

- **合并不可用端点的追踪状态**：
  - 将 `unavailableEndpoints`（时间戳）与 `endpointRetryCount`（重试次数）合并为单个 `Map<String, UnavailableState>`
  - 时间戳与重试次数始终同步增删，消除两个 map 不一致的可能

- **提取 `createConnection()` 方法**：
  - 将「构造 `TglogConnection` + `connect(selector)` + 加入 `connectingConns`」三步连同 `IOException` 处理收口为单个方法
  - 消除 6 处重复代码，各调用点的差异化逻辑（计数、标记不可用、设置替换引用）保留在调用方

---

## [1.0.1] - 2026-04-29

### 问题修复

- **修复 flush/close 时遗漏 pendingCloseList 缓冲数据**：
  - 优雅关闭时确保 pendingCloseList 中的连接缓冲数据也被正确刷新，防止数据丢失

- **修复 Worker 线程启动初始化失败时的线程泄漏**：
  - 当 Worker 启动阶段初始化失败时，正确清理已创建的线程资源，防止线程泄漏

- **修复成功率计算未包含 dropped 消息**：
  - 成功率计算现在将 dropped 消息纳入统计，提供更准确的成功率指标

### 性能优化

- **LockFreeQueue 添加缓存行填充防止伪共享**：
  - 对 LockFreeQueue 的关键字段添加 cache line padding，避免多核 CPU 上的伪共享问题，提升高并发场景下的队列性能

- **Selector 字段添加 volatile 修饰**：
  - 确保 selector 的可见性，提高多线程环境下的正确性

- **客户端关闭时注销 Prometheus 指标**：
  - 客户端关闭时主动注销已注册的 Prometheus 指标，避免指标泄漏

- **TCP 批量发送减少内存分配**：
  - 使用 `ByteBuffer.wrap()` 替代 `allocate + copy`，每条消息少一次内存分配和数组拷贝，降低高 QPS 下的 GC 压力

### 代码改进

- 更新代码风格配置

---

## [1.0.0] - 2026-02-13

这是 TGLog Pure Java Client 的首个正式稳定版本。本实现基于 C 版本，提供了等价的功能，同时采用了 Java 原生的特性和编程习惯。

### 核心特性

#### 高性能架构
- **纯 Java 实现** - 无 JNI 依赖，跨平台部署
- **无锁架构** - 使用无锁 MPSC 队列（Multi-Producer Single-Consumer）实现高并发
- **多线程处理** - 多 Worker 线程并行处理，充分利用多核 CPU
- **NIO 异步 I/O** - 基于 Java NIO 实现非阻塞网络操作
- **智能批量发送** - 自动批次聚合，减少系统调用
- **目标吞吐量**：100 万条消息/秒
- **低延迟**：可配置批次延迟（默认 10ms）

#### 协议支持
- **TCP/UDP 支持** - 同时支持 TCP 和 UDP 协议
- **UDP 批量发送** - UDP 支持单包批量发送模式
- **自动重连** - 连接断开自动重连，指数退避策略
- **DNS 解析** - 定时 DNS 解析，支持负载均衡

#### 连接管理
- **连接生命周期管理** - 可配置连接最大存活时间，自动替换过期连接
- **紧急连接补充** - 当连接不足时自动补充
- **端点恢复** - 不可用端点自动恢复，带退避策略
- **连接关闭检测** - 注册 `OP_READ` 事件，及时检测服务器主动关闭连接

#### 数据可靠性
- **连接输出缓冲区管理** - 类似 C 版本的 bufferevent 机制
- **延迟关闭机制** - 连接关闭前尝试救援数据
- **SDK 关闭时排空缓冲区** - 类似 C 版本的 `tglog_worker_drain_and_stop` 功能
- **数据救援机制** - 关闭连接前自动将缓存数据转移到其他连接
- **优雅关闭** - 可配置停止超时时间，确保数据发送完成

#### 回调系统
- **Java 风格回调接口** - `TglogCallback` 接口
- **双模式支持** - 同时支持回调和 CompletableFuture 模式
- **异步错误处理** - 详细的错误码和错误信息
- **单消息回调** - 支持为每条消息设置独立回调

#### 性能监控
- **丰富的性能指标** - 集成 Prometheus 指标收集
- **可配置的指标注册表** - 支持自定义 `CollectorRegistry`，便于测试隔离和多租户场景
- **指标内容**：
  - 消息计数器（入队、出队、成功、失败、丢弃、超时）
  - 连接统计（打开、关闭、错误）
  - 端点管理（新增、移除、当前数量）
  - 重试统计
- **多种导出格式** - 支持 JSON 和人类可读格式

#### 配置管理
- **构建器模式** - 灵活的配置构建器
- **可配置参数**：
  - Worker 线程数
  - 批次大小、消息数、延迟时间
  - 每个端点的连接数
  - Socket 缓冲区大小
  - 重试策略和超时时间
  - 连接最大存活时间
  - 优雅停止超时时间

### 问题修复

- **修复高水位处理逻辑**：
  - 使用 `config.getWriteHighWatermark()` 替代硬编码的 `MAX_OUTPUT_BUFFER_SIZE`
  - 当输出缓冲区超过高水位时返回 0（而非 -1），表示"尝试下一个连接"而非"连接错误"
  - Worker 现在能正确区分：`sent==0`（高水位超限，尝试下一个连接）和 `sent<0`（连接错误，关闭连接）
  - 添加配置值 ≤ 0 时的保护，自动回退到默认值 32MB（防止误配置）
  - 移除 `writePendingData()` 中的高水位检查（救援数据不受限制，与 C 版本一致）
  - 修复 UDP 连接的 `isWritable()` 检查使用正确的高水位配置
  - 这些修复防止了高负载下连接被错误标记为失败，允许 Worker 尝试其他连接

- **修复紧急连接补充逻辑**：
  - `getConnection()` 现在会在发现没有连接时尝试紧急补充（与 C 版本一致）
  - 添加 2 秒间隔保护，避免过于频繁的补充尝试
  - 重命名 `emergencySupplementCheck()` 为 `emergencySupplementConnection()`，更准确反映其功能
  - 移除定时器回调中的冗余时间间隔检查（已由 5 秒定时器保证）
  - 修复重复更新 `lastEmergencySupplementTime` 的问题（现在由调用者统一更新）
  - 两个补充机制（定时器和 getConnection）共享同一个时间戳，协同工作
  - 提高了连接全部丢失时的恢复能力

- **修复批次重试逻辑缺陷**：
  - 移除 `processMessages()` 中重复的批次超时检查，该检查会破坏重试退避机制
  - 重构批次重试逻辑，将重试状态和时间戳管理封装到 `BatchBuffer` 中
  - 修复重试退避计算错误，现在正确使用指数退避策略
  - 重试期间也检查是否发送超时

- **修复指标统计问题**：
  - 修复 `TglogMetrics.toJson()` 缺少 `rescued_msg_bytes` 字段
  - 修复 `handleConnectionError()` 中重复调用 `incrementClosedConn()` 导致的重复计数
  - 连接关闭指标现在只在实际关闭时计数一次

- **修复数据救援问题**：
  - 修复 `writePendingData()` 后未注册 `OP_WRITE` 导致救援数据无法及时发送的问题
  - 现在救援数据后会立即注册写事件，确保数据尽快发送

- **修复延迟关闭连接的数据竞争**：
  - 将延迟关闭逻辑从独立的 `closeScheduler` 线程收归 Worker 主线程
  - 使用时间戳检查替代 `ScheduledExecutorService`，彻底消除跨线程并发访问风险
  - 移除 `closeScheduler` 和 `pendingCloseTasks`，改用 `pendingCloseList` 在 Worker 主循环中处理
  - 数据救援逻辑全部在 Worker 线程内执行，不再跨线程访问 `connections` 等非线程安全数据结构

- **修复 cleanup 中潜在的无限循环**：
  - 修复优雅关闭时 `drainConnectionBuffers` 可能无限循环的问题

- **修复调试日志格式问题**

- **其他修复**：
  - 修复清理过程中的调试日志问题
  - 修复日志消息未以换行符结尾时的验证问题
  - 修复连接资源泄漏，正确关闭待关闭任务中的连接
  - 修复连接关闭时可能的数据丢失问题，实现缓冲区排空机制
  - 修复 Javadoc 格式问题
  - 增加配置参数校验

### 性能优化

- **优化批次重试性能**：
  - `BatchBuffer.prepareForSend()` 现在会缓存已准备的 ByteBuffer
  - 重试时直接重用缓存的 ByteBuffer，避免重复的内存分配和字符串转换
  - 在高重试率场景下显著降低 CPU 和内存开销

- **缓存消息 payload 字节**：
  - `LogMessage` 缓存序列化后的 payload 字节，避免重复序列化

- **优化退避策略**：
  - 退避间隔增加上限，防止指数退避时间过长

- **裁剪 sendBuffers**：
  - 批量发送后裁剪 `sendBuffers` 数组，使其匹配剩余消息数量，减少内存占用

- **使用 ThreadLocalRandom**：
  - 替换 `Random` 为 `ThreadLocalRandom`，减少多线程竞争

- **优化指标方法**，提升性能
- **优化端点更新处理逻辑**（先处理新增端点，再处理移除端点）

### 测试改进

- **新增并发发送测试**（`ConcurrentSendTest`）：
  - 测试多线程并发发送场景
  - 可配置消息数量、发送线程数、Worker 数量、队列容量等参数
  - 提供详细的指标输出和一致性检查
  - 验证回调机制的正确性

- **新增空闲连接测试**（`IdleConnectionTest`）：
  - 测试连接在空闲期后的稳定性（默认 5 分钟）
  - 验证空闲后消息仍能成功发送
  - 可配置空闲时长和测试消息数量
  - 提供详细的测试文档（`docs/IDLE_CONNECTION_TEST.md`）

- **完整的集成测试** - 覆盖主要功能场景
- **性能测试套件** - 带限流的性能测试
- **多种使用示例**：
  - 基本使用示例
  - 回调使用示例
  - CompletableFuture 使用示例
  - 灵活日志格式示例
- **便捷的测试脚本** - 一键运行测试（`run-test.sh`、`run-concurrent-test.sh`、`run-idle-test.sh`）

### 代码改进

- 统一 `toString()` 和 `toJson()` 的输出格式，减少不一致性
- 改进日志输出，提供更清晰的调试信息
- 添加详细注释说明与 C 版本的对应关系
- 将批次时间管理逻辑（`createTime`、重试时间戳）封装到 `BatchBuffer` 中
- 添加 `BatchBuffer.isSendTimeout()` 和 `isRetryTimeout()` 方法，提高代码可读性
- 重构回调机制，从 C 风格改为 Java 接口风格
- 改进异常处理，使用自定义异常类型和错误码

### 构建与部署

- **Maven 构建系统** - 标准 Maven 项目结构
- **代码质量检查** - 集成 Checkstyle、PMD、SpotBugs
- **可执行 JAR 打包** - Maven Assembly 插件支持
- **源码 JAR 生成** - 便于调试和学习
- **Maven 仓库部署配置** - 支持发布到 Maven 私服

### 日志系统

- **SLF4J 日志框架** - 标准 Java 日志接口
- **可配置日志级别** - 支持 DEBUG、INFO、WARN、ERROR
- **详细的调试日志** - 便于问题排查

### 文档

- 完整的 README 快速入门指南（中英文）
- 详细的 API 文档
- 配置指南
- 性能调优指南
- 构建指南（BUILD.md）
- 部署指南（DEPLOYMENT_GUIDE.md）
- 快速开始指南（QUICKSTART.md）
- 常见使用场景示例代码

### 依赖项

- Java 8+ 兼容
- SLF4J 2.0.9（日志框架）
- Prometheus simpleclient 0.16.0（指标收集）
- SpotBugs annotations 4.7.3（代码质量）
- JUnit 4.13.2（单元测试）
- Guava 33.5.0-jre（测试限流）

### 从 C 版本迁移

如果你正在从 C 版本迁移，请注意以下差异：

1. **回调风格**：Java 版本使用接口回调，而不是函数指针
2. **Future 支持**：Java 版本除了回调外，还提供 CompletableFuture 支持
3. **日志系统**：使用 SLF4J 而不是自定义日志
4. **指标收集**：集成 Prometheus 进行指标收集
5. **配置方式**：使用构建器模式而不是结构体初始化

### 已知限制

- 集成测试需要运行中的 TGLog 服务器（dev.tglog.com:20001）
- UDP 批量发送目前仅支持单包模式（暂不支持多包）

### 兼容性

- **Java 版本**：Java 8 或更高版本
- **协议**：与 TGLog 服务器协议兼容
- **平台**：跨平台（Linux、macOS、Windows）

---

## [1.0.0-rc.3] - 2026-01-21

### 问题修复
- **修复高水位处理逻辑**：
  - 使用 `config.getWriteHighWatermark()` 替代硬编码的 `MAX_OUTPUT_BUFFER_SIZE`
  - 当输出缓冲区超过高水位时返回 0（而非 -1），表示"尝试下一个连接"而非"连接错误"
  - Worker 现在能正确区分：`sent==0`（高水位超限，尝试下一个连接）和 `sent<0`（连接错误，关闭连接）
  - 添加配置值 ≤ 0 时的保护，自动回退到默认值 32MB（防止误配置）
  - 移除 `writePendingData()` 中的高水位检查（救援数据不受限制，与 C 版本一致）
  - 修复 UDP 连接的 `isWritable()` 检查使用正确的高水位配置
  - 这些修复防止了高负载下连接被错误标记为失败，允许 Worker 尝试其他连接

- **修复紧急连接补充逻辑**：
  - `getConnection()` 现在会在发现没有连接时尝试紧急补充（与 C 版本一致）
  - 添加 2 秒间隔保护，避免过于频繁的补充尝试
  - 重命名 `emergencySupplementCheck()` 为 `emergencySupplementConnection()`，更准确反映其功能
  - 移除定时器回调中的冗余时间间隔检查（已由 5 秒定时器保证）
  - 修复重复更新 `lastEmergencySupplementTime` 的问题（现在由调用者统一更新）
  - 两个补充机制（定时器和 getConnection）共享同一个时间戳，协同工作
  - 提高了连接全部丢失时的恢复能力

### 新增功能
- **连接关闭检测**：
  - 连接建立后注册 `OP_READ` 事件，及时检测服务器主动关闭连接
  - 添加 `handleReadable()` 方法处理可读事件：
    - TCP：检测连接关闭（`bytesRead < 0`）并触发错误处理
    - TCP/UDP：丢弃意外接收的数据（TGLog v1 协议无响应）
  - 在 `handleConnectionError()` 中立即取消 `OP_READ` 监听，避免延迟关闭期间的 busy-wait
  - 添加 INFO 级别日志便于诊断问题
  - 提高了连接状态准确性，不再仅依赖写入失败来检测连接关闭

- **可配置的 Prometheus 指标注册表**：
  - `TglogConfig` 新增 `setCollectorRegistry()` 方法
  - 支持自定义 `CollectorRegistry`，便于测试隔离和多租户场景
  - 默认使用 `CollectorRegistry.defaultRegistry`
  - 应用程序可自行决定如何暴露指标（HTTP endpoint、pushgateway 等）

### 测试改进
- **新增并发发送测试**（`ConcurrentSendTest`）：
  - 测试多线程并发发送场景
  - 可配置消息数量、发送线程数、Worker 数量、队列容量等参数
  - 提供详细的指标输出和一致性检查
  - 验证回调机制的正确性

- **新增空闲连接测试**（`IdleConnectionTest`）：
  - 测试连接在空闲期后的稳定性（默认 5 分钟）
  - 验证空闲后消息仍能成功发送
  - 可配置空闲时长和测试消息数量
  - 提供详细的测试文档（`docs/IDLE_CONNECTION_TEST.md`）

- **测试脚本改进**：
  - 简化测试脚本参数处理
  - 添加 `run-concurrent-test.sh` 和 `run-idle-test.sh`
  - 编译脚本添加新测试的使用说明

### 代码改进
- 统一 `toString()` 和 `toJson()` 的输出格式，减少不一致性
- 改进日志输出，提供更清晰的调试信息
- 添加详细注释说明与 C 版本的对应关系

## [1.0.0-rc.2] - 2026-01-15

### 问题修复
- **修复批次重试逻辑缺陷**：
  - 移除 `processMessages()` 中重复的批次超时检查，该检查会破坏重试退避机制
  - 重构批次重试逻辑，将重试状态和时间戳管理封装到 `BatchBuffer` 中
  - 修复重试退避计算错误，现在正确使用指数退避策略
  - 重试期间也检查是否发送超时
- **修复指标统计问题**：
  - 修复 `TglogMetrics.toJson()` 缺少 `rescued_msg_bytes` 字段
  - 修复 `handleConnectionError()` 中重复调用 `incrementClosedConn()` 导致的重复计数
  - 连接关闭指标现在只在实际关闭时计数一次
- **修复数据救援问题**：
  - 修复 `writePendingData()` 后未注册 `OP_WRITE` 导致救援数据无法及时发送的问题
  - 现在救援数据后会立即注册写事件，确保数据尽快发送

### 性能优化
- **优化批次重试性能**：
  - `BatchBuffer.prepareForSend()` 现在会缓存已准备的 ByteBuffer
  - 重试时直接重用缓存的 ByteBuffer，避免重复的内存分配和字符串转换
  - 在高重试率场景下显著降低 CPU 和内存开销

### 代码改进
- 将批次时间管理逻辑（`createTime`、重试时间戳）封装到 `BatchBuffer` 中
- 添加 `BatchBuffer.isSendTimeout()` 和 `isRetryTimeout()` 方法，提高代码可读性
- 改进日志输出，添加时间戳以便调试重试和超时问题

## [1.0.0-rc.1] - 2025-12-31

### 新增功能

#### 核心特性
- **纯 Java 实现** - 无 JNI 依赖，跨平台部署
- **无锁架构** - 使用无锁 MPSC 队列（Multi-Producer Single-Consumer）实现高并发
- **多线程处理** - 多 Worker 线程并行处理，充分利用多核 CPU
- **NIO 异步 I/O** - 基于 Java NIO 实现非阻塞网络操作
- **智能批量发送** - 自动批次聚合，减少系统调用
- **TCP/UDP 支持** - 同时支持 TCP 和 UDP 协议
- **UDP 批量发送** - UDP 支持单包批量发送模式
- **自动重连** - 连接断开自动重连，指数退避策略
- **DNS 解析** - 定时 DNS 解析，支持负载均衡
- **连接生命周期管理** - 可配置连接最大存活时间，自动替换过期连接
- **紧急连接补充** - 当连接不足时自动补充
- **端点恢复** - 不可用端点自动恢复，带退避策略

#### 数据可靠性
- **连接输出缓冲区管理** - 类似 C 版本的 bufferevent 机制
- **延迟关闭机制** - 连接关闭前尝试救援数据：
  - 正常替换：延迟 10 秒关闭
  - 异常关闭（有缓存数据）：延迟 10 秒关闭
  - 异常关闭（无缓存数据）：延迟 5 秒关闭
- **SDK 关闭时排空缓冲区** - 类似 C 版本的 `tglog_worker_drain_and_stop` 功能
- **数据救援机制** - 关闭连接前自动将缓存数据转移到其他连接
- **优雅关闭** - 可配置停止超时时间，确保数据发送完成

#### 回调系统
- **Java 风格回调接口** - `TglogCallback` 接口
- **双模式支持** - 同时支持回调和 CompletableFuture 模式
- **异步错误处理** - 详细的错误码和错误信息
- **单消息回调** - 支持为每条消息设置独立回调

#### 性能监控
- **丰富的性能指标** - 集成 Prometheus 指标收集
- **指标内容**：
  - 消息计数器（入队、出队、成功、失败、丢弃、超时）
  - 连接统计（打开、关闭、错误）
  - 端点管理（新增、移除、当前数量）
  - 重试统计
- **多种导出格式** - 支持 JSON 和人类可读格式

#### 配置管理
- **构建器模式** - 灵活的配置构建器
- **可配置参数**：
  - Worker 线程数
  - 批次大小、消息数、延迟时间
  - 每个端点的连接数
  - Socket 缓冲区大小
  - 重试策略和超时时间
  - 连接最大存活时间
  - 优雅停止超时时间

#### 日志系统
- **SLF4J 日志框架** - 标准 Java 日志接口
- **可配置日志级别** - 支持 DEBUG、INFO、WARN、ERROR
- **详细的调试日志** - 便于问题排查

#### 测试与示例
- **完整的集成测试** - 覆盖主要功能场景
- **性能测试套件** - 带限流的性能测试
- **多种使用示例**：
  - 基本使用示例
  - 回调使用示例
  - CompletableFuture 使用示例
  - 灵活日志格式示例
- **便捷的测试脚本** - 一键运行测试

#### 构建与部署
- **Maven 构建系统** - 标准 Maven 项目结构
- **代码质量检查** - 集成 Checkstyle、PMD、SpotBugs
- **可执行 JAR 打包** - Maven Assembly 插件支持
- **源码 JAR 生成** - 便于调试和学习
- **Maven 仓库部署配置** - 支持发布到 Maven 私服

### 优化改进
- 重构回调机制，从 C 风格改为 Java 接口风格
- 优化指标方法，提升性能
- 改进异常处理，使用自定义异常类型和错误码
- 优化端点更新处理逻辑（先处理新增端点，再处理移除端点）

### 问题修复
- 修复清理过程中的调试日志问题
- 修复日志消息未以换行符结尾时的验证问题
- 修复连接资源泄漏，正确关闭待关闭任务中的连接
- 修复连接关闭时可能的数据丢失问题，实现缓冲区排空机制

### 性能指标
- **目标吞吐量**：100 万条消息/秒
- **低延迟**：可配置批次延迟（默认 10ms）
- **非阻塞发送**：发送操作不阻塞业务线程
- **背压控制**：队列满时拒绝新消息，保护系统稳定性

### 文档
- 完整的 README 快速入门指南
- 详细的 API 文档
- 配置指南
- 性能调优指南
- 常见使用场景示例代码

### 依赖项
- Java 8+ 兼容
- SLF4J 2.0.9（日志框架）
- Prometheus simpleclient 0.16.0（指标收集）
- SpotBugs annotations 4.7.3（代码质量）
- JUnit 4.13.2（单元测试）
- Guava 32.1.3-jre（测试限流）

---

[1.0.2]: https://git.woa.com/tglog/v3/sdk-java/-/tags/v1.0.2
[1.0.1]: https://git.woa.com/tglog/v3/sdk-java/-/tags/v1.0.1
[1.0.0]: https://git.woa.com/tglog/v3/sdk-java/-/tags/v1.0.0
[1.0.0-rc.3]: https://git.woa.com/tglog/v3/sdk-java/-/tags/v1.0.0-rc.3
[1.0.0-rc.2]: https://git.woa.com/tglog/v3/sdk-java/-/tags/v1.0.0-rc.2
[1.0.0-rc.1]: https://git.woa.com/tglog/v3/sdk-java/-/tags/v1.0.0-rc.1
