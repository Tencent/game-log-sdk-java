# Idle Connection Test

## 概述

`IdleConnectionTest` 是一个专门用于测试 TglogClient 在长时间空闲后连接稳定性的测试程序。

## 测试场景

该测试模拟以下场景：
1. 创建 TglogClient 并建立连接
2. 等待一段时间（默认 5 分钟，可配置）不发送任何数据
3. 在空闲期后发送一批消息
4. 验证连接是否仍然正常工作

## 为什么需要这个测试？

在生产环境中，可能会出现以下情况：
- 应用启动后，一段时间内没有日志需要发送
- 业务低峰期，日志量很少
- 服务器可能因为连接空闲而主动关闭连接（idle timeout）
- 网络设备（防火墙、NAT）可能清理长时间空闲的连接

这个测试可以帮助验证：
- ✅ 连接在空闲后是否仍然可用
- ✅ 如果连接被关闭，客户端能否正确检测并重连
- ✅ 空闲期间的连接管理是否正常
- ✅ TCP Keep-Alive 是否生效

## 使用方法

### 快速开始

```bash
# 编译项目
./compile.sh

# 运行测试（默认 5 分钟空闲，连接到 dev.tglog.com:20001）
./run-idle-test.sh
```

### 常用场景

```bash
# 快速测试（30 秒空闲）
./run-idle-test.sh -i 30 -m 100

# 测试 10 分钟空闲
./run-idle-test.sh -i 600

# 测试 1 小时空闲
./run-idle-test.sh -i 3600

# 使用自定义服务器
./run-idle-test.sh -h logs.game.com -p 8080

# 使用 UDP 协议测试
./run-idle-test.sh --udp -i 300
```

### 完整参数

```
-h, --host <host>       服务器地址（默认：dev.tglog.com）
-p, --port <port>       服务器端口（默认：20001）
--tcp                   使用 TCP 协议（默认）
--udp                   使用 UDP 协议
-w, --workers <num>     Worker 线程数（默认：8）
-i, --idle <seconds>    空闲时间（秒）（默认：300 = 5 分钟）
-m, --messages <num>    空闲后发送的消息数（默认：1000）
-b, --batch <size>      批次大小（默认：50）
--debug                 启用调试日志
--help                  显示帮助信息
```

## 测试流程

### Phase 1: 创建客户端
- 创建 TglogClient
- 等待连接建立（500ms）
- 打印初始 metrics

### Phase 2: 空闲阶段
- 等待指定的空闲时间
- 每 10% 进度打印一次状态
- 记录实际空闲时长

### Phase 3: 准备测试数据
- 预生成测试日志数据
- 日志中包含空闲时长信息

### Phase 4: 发送消息
- 在空闲后发送指定数量的消息
- 使用 callback 跟踪成功/失败
- 每 10% 进度打印一次状态
- 等待所有消息发送完成（超时 60 秒）

### 结果输出
- 空闲时长
- 发送速率
- 成功/失败统计
- 最终 metrics

## 输出示例

```
================================================================
TGLog Idle Connection Test
================================================================
Configuration:
  Protocol:      TCP
  Host:          dev.tglog.com
  Port:          20001
  Workers:       8
  Idle Time:     300 seconds (5 minutes 0 seconds)
  Messages:      1,000
  Batch Size:    50
================================================================

[Phase 1] Creating client...
Client created successfully in 123ms

Initial metrics:
endpoint_count=2
active_conn_count=16
...

[Phase 2] Idle phase - waiting for 300 seconds...
Start time: Mon Jan 20 18:00:00 CST 2026
  Idle progress: 10% (30/300 seconds)
  Idle progress: 20% (60/300 seconds)
  ...
  Idle progress: 100% (300/300 seconds)

Idle phase completed
End time: Mon Jan 20 18:05:00 CST 2026
Actual idle duration: 300012ms (5 minutes 0 seconds)

Metrics after idle:
endpoint_count=2
active_conn_count=16
...

[Phase 3] Preparing test data...
Test data prepared

[Phase 4] Sending messages after idle period...
  Progress: 10% (100/1,000) - Rate: 12,345 msg/s
  Progress: 20% (200/1,000) - Rate: 13,456 msg/s
  ...
  Progress: 100% (1,000/1,000) - Rate: 14,567 msg/s

All messages enqueued in 68ms
Enqueue rate: 14,706 msg/s

Waiting for all messages to be sent (timeout: 60 seconds)...

================================================================
Test Results
================================================================
Idle Duration:     5 minutes 0 seconds
Messages Sent:     1,000
Send Duration:     68ms
Total Duration:    156ms
Enqueue Rate:      14,706 msg/s
Overall Rate:      6,410 msg/s
Success Count:     1,000
Failure Count:     0
Success Rate:      100.00%
================================================================

Final Metrics:
endpoint_count=2
active_conn_count=16
enqueued_msg=1000
dequeued_msg=1000
succeeded_msg=1000
...

Closing client...
Client closed

Test PASSED: All messages sent successfully after 5 minutes 0 seconds idle period
```

## 预期结果

### 正常情况
- ✅ Success Rate: 100%
- ✅ Failure Count: 0
- ✅ 所有消息成功发送
- ✅ Test PASSED

### 异常情况
如果测试失败，可能的原因：
1. **服务器关闭了空闲连接**
   - 检查服务器的 idle timeout 配置
   - 考虑调整 TCP Keep-Alive 参数

2. **网络设备清理了连接**
   - 检查防火墙/NAT 的超时设置
   - 考虑缩短连接生命周期

3. **客户端连接管理问题**
   - 查看日志中的 "Broken pipe" 或连接错误
   - 检查连接健康检查是否正常工作

## 调试建议

### 启用调试日志
```bash
./run-idle-test.sh --debug -i 30
```

### 观察 metrics
测试会在三个阶段打印 metrics：
1. 初始状态（Phase 1）
2. 空闲后状态（Phase 2）
3. 发送完成后状态（Phase 4）

重点关注：
- `active_conn_count`: 活跃连接数
- `error_conn`: 错误连接数
- `closed_conn`: 关闭连接数
- `succeeded_msg` vs `failed_msg`: 成功/失败消息数

### 逐步增加空闲时间
```bash
# 从 30 秒开始
./run-idle-test.sh -i 30

# 逐步增加到 1 分钟、5 分钟、10 分钟
./run-idle-test.sh -i 60
./run-idle-test.sh -i 300
./run-idle-test.sh -i 600
```

找出连接开始失败的临界点。

## 与其他测试的对比

| 测试 | 用途 | 特点 |
|------|------|------|
| `TglogClientTest` | 性能测试 | 持续高速发送，测试吞吐量 |
| `ConcurrentSendTest` | 并发测试 | 多线程并发发送，测试线程安全 |
| `IdleConnectionTest` | 空闲测试 | 长时间空闲后发送，测试连接稳定性 |

## 相关配置

### TglogConfig 相关参数
- `maxConnectionLifetime`: 连接最大生命周期（默认 300 秒）
- `connectionTimeout`: 连接超时时间（默认 5 秒）
- `socketSendBufferSize`: Socket 发送缓冲区大小
- `SO_KEEPALIVE`: TCP Keep-Alive（已启用）

### 服务器端配置
确保服务器的 idle timeout 设置合理：
- 如果服务器 idle timeout < 客户端连接生命周期，连接会被服务器关闭
- 建议服务器 idle timeout > 客户端连接生命周期

## 故障排查

### 问题：空闲后发送失败
**现象**：`Failure Count > 0` 或 `Success Rate < 100%`

**可能原因**：
1. 服务器关闭了空闲连接
2. 网络设备清理了连接
3. 客户端连接检测不及时

**解决方案**：
1. 检查服务器日志，确认是否主动关闭连接
2. 启用 `--debug` 查看详细日志
3. 查看是否有 "Broken pipe" 异常
4. 检查连接健康检查是否生效（`isSocketValidLight()`）

### 问题：连接数异常
**现象**：空闲后 `active_conn_count` 变化

**检查**：
1. 查看 `closed_conn` 和 `error_conn` 指标
2. 检查是否有连接被替换（生命周期到期）
3. 查看是否有连接错误

### 问题：测试超时
**现象**：等待消息发送完成超时

**可能原因**：
1. 连接全部失败，无法发送
2. 网络问题导致发送缓慢
3. 服务器处理缓慢

**解决方案**：
1. 减少消息数量（`-m 100`）
2. 检查网络连接
3. 查看服务器负载

## 最佳实践

1. **开发阶段**：使用短空闲时间（30-60 秒）快速验证
2. **测试阶段**：使用真实场景的空闲时间（5-10 分钟）
3. **压力测试**：使用长空闲时间（1 小时以上）测试极端情况
4. **生产验证**：根据业务特点设置合理的空闲时间

## 参考资料

- [TCP Keep-Alive](https://en.wikipedia.org/wiki/Keepalive)
- [Connection Lifecycle Management](../CLAUDE.md#connection-management)
- [Prometheus Metrics](PROMETHEUS_METRICS.md)
