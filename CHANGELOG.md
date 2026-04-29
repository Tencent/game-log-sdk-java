# 更新日志

本文档记录了 TGLog Pure Java Client 的所有重要变更。

格式基于 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.0.0/)，
版本号遵循 [语义化版本](https://semver.org/lang/zh-CN/)。

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

[1.0.1]: https://git.woa.com/tglog/v3/sdk-java/-/tags/v1.0.1
[1.0.0]: https://git.woa.com/tglog/v3/sdk-java/-/tags/v1.0.0
[1.0.0-rc.3]: https://git.woa.com/tglog/v3/sdk-java/-/tags/v1.0.0-rc.3
[1.0.0-rc.2]: https://git.woa.com/tglog/v3/sdk-java/-/tags/v1.0.0-rc.2
[1.0.0-rc.1]: https://git.woa.com/tglog/v3/sdk-java/-/tags/v1.0.0-rc.1
