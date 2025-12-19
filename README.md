# TGLog Pure Java Client

高性能纯Java实现的TGLog日志客户端，专为游戏服务器设计。

## 特性

### 核心特性
- ✅ **纯Java实现** - 无JNI依赖，跨平台，易部署
- ✅ **无锁架构** - 使用无锁队列(Lock-Free MPSC Queue)实现高并发
- ✅ **多线程** - 多Worker线程并行处理，充分利用多核CPU
- ✅ **NIO异步** - 基于Java NIO实现非阻塞I/O
- ✅ **批量发送** - 智能批次聚合，减少系统调用
- ✅ **UDP批量** - UDP也支持批量发送(单包模式)
- ✅ **自动重连** - 连接断开自动重连，指数退避
- ✅ **DNS解析** - 定时DNS解析，支持负载均衡
- ✅ **性能监控** - 丰富的性能指标，支持JSON导出

### 性能特点
- **高吞吐量**: 目标30-50万条/秒
- **低延迟**: 批次延迟可配置(默认10ms)
- **无阻塞**: 发送操作不阻塞业务线程
- **背压控制**: 队列满时拒绝新消息，保护系统

## 快速开始

### 项目结构

本项目采用标准Maven项目结构：

```
sdk-java/
├── pom.xml                          # Maven配置文件
├── src/
│   ├── main/java/                   # 主代码
│   │   └── com/tencent/tglog/
│   ├── test/java/                   # 单元测试
│   ├── examples/java/               # 使用示例
│   └── integration-test/java/       # 集成测试
├── docs/                            # 文档目录
├── compile.sh                       # 编译脚本
├── run-example.sh                   # 运行示例
└── run-test.sh                      # 运行性能测试
```

### 1. 编译

**使用编译脚本（推荐）：**
```bash
./compile.sh
```

**或使用Maven直接编译：**
```bash
# 编译
mvn compile

# 清理并编译
mvn clean compile

# 打包为JAR
mvn package
```

编译成功后会在 `target/classes` 目录生成class文件，`mvn package` 会在 `target/` 目录生成JAR文件。

### 2. Maven依赖

**作为依赖使用：**

在你的项目 `pom.xml` 中添加：

```xml
<dependency>
    <groupId>com.tencent.tglog</groupId>
    <artifactId>tglog-java</artifactId>
    <version>1.0.0</version>
</dependency>

<!-- SLF4J 实现（根据项目需要选择） -->
<dependency>
    <groupId>ch.qos.logback</groupId>
    <artifactId>logback-classic</artifactId>
    <version>1.4.14</version>
</dependency>
```

**或直接使用JAR文件：**
```bash
mvn package
# 使用 target/tglog-java-1.0.0.jar
```

**系统要求：**
- Java 8 或更高版本
- Maven 3.6+（用于构建）
- SLF4J API（已包含在依赖中）
- SLF4J 实现（如 Logback、Log4j2，由应用程序选择）

### 3. 运行示例

```bash
# 简单示例
./run-example.sh

# 性能测试 (默认100万条消息)
./run-test.sh

# 性能测试 (100万条消息，带回调)
./run-test.sh -m 1000000 -c

# 自定义参数
./run-test.sh -h logs.game.com -p 8080 -m 500000 -w 16

# 查看所有可用参数
./run-test.sh --help
```

### 4. 基本使用

```java
import com.tencent.tglog.*;

// 创建配置
TglogConfig config = TglogConfig.createTcp("logs.game.com", 8080)
    .setWorkerNum(8)                    // 8个Worker线程
    .setBatchingMaxMessages(50)         // 批次最大50条消息
    .setBatchingMaxDelayMs(10);         // 批次最大延迟10ms

// 创建客户端 (AutoCloseable)
try (TglogClient client = new TglogClient(config)) {

    // 发送日志 (fire and forget)
    client.send("battle|winner=1|loser=2|score=100\n");

    // 发送日志 (带回调)
    client.send("login|userid=12345\n", new TglogCallback() {
        @Override
        public void onSuccess(String payload) {
            System.out.println("发送成功: " + payload);
        }

        @Override
        public void onError(String payload, int errorCode, String errorMessage) {
            System.err.println("发送失败: " + errorMessage);
        }
    });

    // 获取性能指标
    TglogMetrics metrics = client.getMetrics();
    System.out.println("成功率: " + metrics.getSuccessRate() * 100 + "%");
    System.out.println(client.printMetrics());

} // 自动关闭，flush所有待发送消息
```

## 架构设计

### 整体架构

```
Application (多线程)
     │
     ▼ (无锁发送)
TglogClient
     │
     ├── EndpointManager (DNS解析)
     │
     ├── TglogWorker[] (多Worker线程)
     │    │
     │    ├── LockFreeQueue (无锁消息队列)
     │    ├── NIO Selector (非阻塞I/O)
     │    ├── ConnectionPool (连接池)
     │    └── BatchBuffer (批次缓冲)
     │
     └── TglogMetrics (性能统计)
```

### 核心组件

#### 1. LockFreeQueue (无锁队列)
- **类型**: MPSC (Multi-Producer Single-Consumer)
- **实现**: 基于CAS操作的环形缓冲区
- **特点**:
  - 多个业务线程并发入队
  - 单个Worker线程消费
  - 无锁设计，高性能

#### 2. TglogWorker (工作线程)
- **数量**: 可配置(默认8个)
- **职责**:
  - 从无锁队列取消息
  - 批次聚合
  - NIO异步发送
  - 连接管理
- **调度**: Round-Robin轮询

#### 3. BatchBuffer (批次缓冲)
- **TCP模式**: 使用Gathering Write批量发送
- **UDP模式**: 单包发送所有消息
- **触发条件**:
  - 消息数达到上限(默认50条)
  - 批次大小达到上限(默认32KB)
  - 超时时间到达(默认10ms)

#### 4. EndpointManager (端点管理)
- **DNS解析**: 定时解析(默认60秒)
- **变更检测**: 自动检测IP变化
- **负载均衡**: 支持多端点轮询

## 配置说明

### 配置项

| 配置项 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| **网络配置** | | | |
| protocol | Protocol | TCP | 协议类型(TCP/UDP) |
| host | String | - | 服务器地址 |
| port | int | - | 服务器端口 |
| **连接配置** | | | |
| connectionTimeout | int | 3000ms | 连接超时 |
| maxConnectionLifetime | int | 300s | 连接生命周期 |
| dnsUpdateInterval | int | 60s | DNS更新间隔 |
| **批次配置** | | | |
| batchingMaxMessages | int | 50 | 批次最大消息数 |
| batchingMaxSize | int | 32KB | 批次最大大小 |
| batchingMaxDelayMs | int | 10ms | 批次最大延迟 |
| **Worker配置** | | | |
| workerNum | int | 8 | Worker线程数 |
| maxPendingMessages | int | 409600 | 每个Worker队列容量 |
| **缓冲区配置** | | | |
| writeHighWatermark | int | 32MB | 写缓冲区高水位 |
| socketSendBufferSize | int | 64MB | Socket发送缓冲区 |
| socketRecvBufferSize | int | 16KB | Socket接收缓冲区 |
| **重试配置** | | | |
| sendTimeout | int | 30000ms | 发送超时 |
| maxRetries | int | 10 | 最大重试次数 |
| stopTimeout | int | 3000ms | 停止超时 |

### 配置示例

```java
// 高吞吐量配置
TglogConfig config = TglogConfig.createTcp("logs.game.com", 8080)
    .setWorkerNum(16)                   // 增加Worker数量
    .setBatchingMaxMessages(100)        // 增大批次
    .setBatchingMaxSize(64 * 1024)      // 64KB批次
    .setSocketSendBufferSize(128 * 1024 * 1024); // 128MB缓冲区

// 低延迟配置
TglogConfig config = TglogConfig.createTcp("logs.game.com", 8080)
    .setWorkerNum(4)
    .setBatchingMaxMessages(10)         // 小批次
    .setBatchingMaxDelayMs(5);          // 5ms延迟

// UDP配置
TglogConfig config = TglogConfig.createUdp("logs.game.com", 8080)
    .setWorkerNum(8)
    .setBatchingMaxSize(1400);          // UDP建议小于MTU
```

## 性能指标

### 指标说明

#### Counter类型 (累计计数)
- `requestMsgCount`: 应用请求发送的消息数
- `enqueuedMsgCount`: 成功入队的消息数
- `dequeuedMsgCount`: 成功出队的消息数
- `succeededMsgCount`: 成功发送的消息数
- `failedMsgCount`: 失败的消息数
- `timeoutMsgCount`: 超时的消息数
- `retryMsgCount`: 重试次数
- `droppedMsgCount`: 丢弃的消息数(队列满)

#### Gauge类型 (瞬时状态)
- `msgQueueSize`: 当前队列大小
- `msgQueueCapacity`: 队列容量
- `endpointCount`: 当前端点数量
- `activeConnCount`: 活跃连接数

#### 派生指标
- `successRate`: 成功率 = succeeded / (succeeded + failed)
- `queueUtilization`: 队列利用率 = queueSize / queueCapacity

### 获取指标

```java
// 获取指标对象
TglogMetrics metrics = client.getMetrics();
System.out.println("成功率: " + metrics.getSuccessRate() * 100 + "%");
System.out.println("队列利用率: " + metrics.getQueueUtilization() * 100 + "%");

// 打印可读格式
System.out.println(client.printMetrics());

// 导出JSON格式
String json = client.getMetricsJson();
```

## 游戏服务器集成

### 示例：游戏主循环集成

```java
public class GameServer {
    private TglogClient logClient;

    public void init() {
        TglogConfig config = TglogConfig.createTcp("logs.game.com", 8080)
            .setWorkerNum(8)
            .setBatchingMaxDelayMs(10);

        logClient = new TglogClient(config);
    }

    public void gameLoop() {
        while (running) {
            // 处理游戏逻辑
            processPlayerActions();
            processNPCLogic();
            processBattles();

            // 记录日志 (非阻塞，不影响游戏逻辑)
            // 日志会被异步发送
        }
    }

    // 玩家登录
    public void onPlayerLogin(int userId, String username) {
        String log = String.format("login|userid=%d|username=%s|timestamp=%d\n",
            userId, username, System.currentTimeMillis());

        try {
            logClient.send(log);
        } catch (TglogException e) {
            // 队列满或其他错误，记录到本地
            logger.error("Failed to send log", e);
        }
    }

    // 战斗结束
    public void onBattleEnd(int winnerId, int loserId, int score) {
        String log = String.format("battle|winner=%d|loser=%d|score=%d\n",
            winnerId, loserId, score);

        logClient.send(log, new TglogCallback() {
            @Override
            public void onSuccess(String payload) {
                // 发送成功，可以更新数据库
                updateBattleStats(winnerId, loserId);
            }

            @Override
            public void onError(String payload, int code, String msg) {
                // 发送失败，记录到本地文件
                saveToLocalFile(payload);
            }
        });
    }

    public void shutdown() {
        // 关闭客户端，会自动flush所有待发送消息
        logClient.close();
    }
}
```

## 性能测试

### 测试环境
- CPU: 8核
- 内存: 16GB
- 网络: 千兆网卡
- JVM: OpenJDK 11

### 测试结果

| 场景 | 消息数 | 耗时 | 吞吐量 | 说明 |
|------|--------|------|--------|------|
| TCP 无回调 | 100万 | ~2.5秒 | ~40万/秒 | 最高性能 |
| TCP 带回调 | 100万 | ~3.5秒 | ~28万/秒 | 生产推荐 |
| UDP 无回调 | 100万 | ~2.0秒 | ~50万/秒 | 最高性能 |
| UDP 带回调 | 100万 | ~3.0秒 | ~33万/秒 | 生产推荐 |

### 运行性能测试

```bash
# 基准测试 (10万条消息)
./run-test.sh

# 高负载测试 (100万条消息)
./run-test.sh -m 1000000

# 带回调测试
./run-test.sh -m 1000000 -c

# UDP测试
./run-test.sh --udp -m 1000000

# 自定义Worker数量
./run-test.sh -m 1000000 -w 16 -b 100
```

## 与C版本对比

| 特性 | C版本(JNI) | 纯Java版本 |
|------|-----------|-----------|
| **性能** | 100万/秒 | 30-50万/秒 |
| **部署** | 需要编译JNI | 纯JAR包 |
| **维护** | C+Java双语言 | 纯Java |
| **调试** | 困难 | 容易 |
| **跨平台** | 需要编译 | 完全跨平台 |
| **依赖** | libevent, ck | 无外部依赖 |
| **内存** | 需要jemalloc | JVM管理 |

## 故障排除

### 常见问题

#### 1. 连接失败
```
Error: Failed to resolve host
```
**解决**: 检查host和port配置，确保服务器可达

#### 2. 队列满
```
TglogException: Message queue is full
```
**解决**:
- 增加Worker数量: `setWorkerNum(16)`
- 增加队列容量: `setMaxPendingMessages(1000000)`
- 减少发送速率

#### 3. 发送超时
```
Error: Message timeout
```
**解决**:
- 增加超时时间: `setSendTimeout(60000)`
- 检查网络连接
- 检查服务器负载

### 日志配置

TGLog SDK 使用 SLF4J 作为日志门面，你可以选择任何 SLF4J 实现（Logback、Log4j2 等）。

**使用 Logback（推荐）：**

创建 `src/main/resources/logback.xml`：

```xml
<configuration>
    <appender name="STDOUT" class="ch.qos.logback.core.ConsoleAppender">
        <encoder>
            <pattern>%d{HH:mm:ss.SSS} [%thread] %-5level %logger{36} - %msg%n</pattern>
        </encoder>
    </appender>

    <!-- TGLog SDK 日志级别 -->
    <logger name="com.tencent.tglog" level="INFO"/>

    <!-- 调试时可以设置为 DEBUG -->
    <!-- <logger name="com.tencent.tglog" level="DEBUG"/> -->

    <root level="INFO">
        <appender-ref ref="STDOUT"/>
    </root>
</configuration>
```

**使用 SLF4J Simple（测试用）：**

```xml
<dependency>
    <groupId>org.slf4j</groupId>
    <artifactId>slf4j-simple</artifactId>
    <version>2.0.9</version>
</dependency>
```

通过系统属性配置：
```bash
java -Dorg.slf4j.simpleLogger.defaultLogLevel=debug \
     -Dorg.slf4j.simpleLogger.log.com.tencent.tglog=debug \
     -jar your-app.jar
```

### 调试技巧

```java
// 定期打印指标
ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
scheduler.scheduleAtFixedRate(() -> {
    System.out.println(client.printMetrics());
}, 0, 10, TimeUnit.SECONDS);
```

### 错误代码说明

当操作失败时，`TglogException` 会包含一个错误代码，可通过 `getErrorCode()` 方法获取：

| 错误代码 | 常量名 | 说明 |
|---------|--------|------|
| 0 | ERR_OK | 成功 |
| 10001 | ERR_SEND_TIMEOUT | 发送超时 |
| 10002 | ERR_SEND_FAILED | 发送失败 |
| 10003 | ERR_PRODUCER_CLOSED | 客户端已关闭 |
| 10004 | ERR_SEND_QUEUE_IS_FULL | 消息队列已满 |
| 10005 | ERR_NO_AVAILABLE_CONN | 无可用连接 |
| 10006 | ERR_NEW_CONN_FAILED | 创建连接失败 |
| 10007 | ERR_CONN_WRITE_FAILED | 连接写入失败 |
| 10008 | ERR_CONN_READ_FAILED | 连接读取失败 |
| 10009 | ERR_LOG_TOO_LONG | 日志过长 |
| 10010 | ERR_BAD_LOG | 日志格式错误 |
| 10011 | ERR_BATCH_SIZE_EXCEEDED | 批次大小超限 |
| 10012 | ERR_MEMORY_ALLOC_FAILED | 内存分配失败 |
| 10013 | ERR_WORKER_BUSY | Worker繁忙 |
| 20001 | ERR_UNKNOWN | 未知错误 |

**使用示例：**
```java
try {
    client.send("log_type", "log content");
} catch (TglogException e) {
    int errorCode = e.getErrorCode();
    if (errorCode == TglogException.ERR_SEND_QUEUE_IS_FULL) {
        // 队列满，可以选择丢弃或重试
        System.err.println("Queue is full, dropping message");
    } else if (errorCode == TglogException.ERR_SEND_TIMEOUT) {
        // 发送超时
        System.err.println("Send timeout: " + e.getMessage());
    } else {
        // 其他错误
        System.err.println("Error: " + e.getMessage());
    }
}
```

## API文档

### TglogClient

#### 构造方法
```java
public TglogClient(TglogConfig config) throws TglogException
```

#### 主要方法
```java
// 发送日志
public void send(String logPayload) throws TglogException
public void send(String logPayload, TglogCallback callback) throws TglogException

// 刷新待发送消息
public boolean flush(long timeoutMs)

// 获取指标
public TglogMetrics getMetrics()
public String getMetricsJson()
public String printMetrics()

// 关闭客户端
public void close()
```

### TglogConfig

#### 创建方法
```java
public static TglogConfig createTcp(String host, int port)
public static TglogConfig createUdp(String host, int port)
```

#### 配置方法 (链式调用)
```java
public TglogConfig setWorkerNum(int workerNum)
public TglogConfig setBatchingMaxMessages(int maxMessages)
public TglogConfig setBatchingMaxDelayMs(int delayMs)
// ... 更多配置方法
```

### TglogCallback

```java
public interface TglogCallback {
    void onSuccess(String payload);
    void onError(String payload, int errorCode, String errorMessage);
}
```

## 许可证

MIT License

## 支持

- 问题反馈: 提交Issue
- 技术讨论: 欢迎PR和讨论
