# TGLog Java SDK - 快速开始

## 5分钟快速上手

### 1. 编译项目

```bash
./compile.sh
```

或使用 Maven：
```bash
mvn clean compile
```

### 2. 运行简单示例

```bash
./run-example.sh
```

这将运行一个简单的示例，展示如何使用TglogClient。

### 3. 运行性能测试

```bash
# 基准测试 (10万条消息)
./run-test.sh

# 100万条消息测试
./run-test.sh -m 1000000

# 带回调测试
./run-test.sh -m 1000000 -c
```

## 在你的项目中使用

### 方式1: 使用Maven

将以下依赖添加到你的 `pom.xml`:

```xml
<dependency>
    <groupId>com.tencent.tglog</groupId>
    <artifactId>tglog-java</artifactId>
    <version>1.0.2</version>
</dependency>
```

然后运行：
```bash
mvn clean install
```

### 方式2: 打包成JAR

```bash
# 使用Maven打包
mvn clean package

# 打包后的JAR在 target/ 目录
# 使用JAR
java -cp target/tglog-java-1.0.2.jar:your-app.jar YourApp
```

### 方式3: 直接使用编译后的类

```bash
# 将编译后的类复制到你的项目
cp -r target/classes/com/tencent/tglog your-project/lib/

# 在你的项目中引用
javac -cp lib YourApp.java
java -cp lib:. YourApp
```

## 代码示例

### 最简单的使用

```java
import com.tencent.tglog.*;

public class QuickStart {
    public static void main(String[] args) throws Exception {
        // 创建客户端
        TglogConfig config = TglogConfig.createTcp("dev.tglog.com", 20001);
        TglogClient client = new TglogClient(config);

        // 发送日志
        client.send("test|hello=world\n");

        // 关闭客户端
        client.close();
    }
}
```

### 带回调的使用

```java
import com.tencent.tglog.*;

public class WithCallback {
    public static void main(String[] args) throws Exception {
        TglogConfig config = TglogConfig.createTcp("dev.tglog.com", 20001);
        TglogClient client = new TglogClient(config);

        // 发送日志并处理结果 (Lambda 风格)
        client.send("test|userid=123\n", (payload, exception) -> {
            if (exception == null) {
                System.out.println("发送成功: " + payload);
            } else {
                System.err.println("发送失败: " + exception.getMessage());
            }
        });

        // 或使用 CompletableFuture
        client.send("test|data=value\n")
              .thenRun(() -> System.out.println("发送成功"))
              .exceptionally(ex -> {
                  System.err.println("发送失败: " + ex.getMessage());
                  return null;
              });

        // 等待发送完成
        Thread.sleep(1000);
        client.close();
    }
}
```

### 游戏服务器集成

```java
import com.tencent.tglog.*;

public class GameServer {
    private TglogClient logClient;

    public void init() throws TglogException {
        // 创建配置
        TglogConfig config = TglogConfig.createTcp("logs.game.com", 8080)
            .setWorkerNum(8)                    // 8个Worker线程
            .setBatchingMaxMessages(50)         // 批次50条消息
            .setBatchingMaxDelayMs(10);         // 最大延迟10ms

        // 创建客户端
        logClient = new TglogClient(config);
    }

    // 玩家登录
    public void onPlayerLogin(int userId, String username) {
        String log = String.format("login|userid=%d|username=%s|timestamp=%d\n",
            userId, username, System.currentTimeMillis());

        try {
            logClient.send(log);
        } catch (TglogException e) {
            // 队列满或其他错误，记录到本地
            System.err.println("Failed to send log: " + e.getMessage());
        }
    }

    // 战斗结束
    public void onBattleEnd(int winnerId, int loserId, int score) {
        String log = String.format("battle|winner=%d|loser=%d|score=%d\n",
            winnerId, loserId, score);

        try {
            logClient.send(log, (payload, exception) -> {
                if (exception == null) {
                    // 发送成功，可以更新数据库
                    System.out.println("Battle log sent");
                } else {
                    // 发送失败，保存到本地文件
                    saveToLocalFile(payload);
                }
            });
        } catch (TglogException e) {
            saveToLocalFile(log);
        }
    }

    private void saveToLocalFile(String log) {
        // 保存到本地文件的逻辑
    }

    public void shutdown() {
        // 关闭客户端，会自动flush所有待发送消息
        logClient.close();
    }
}
```

## 配置说明

### 基本配置

```java
TglogConfig config = TglogConfig.createTcp("host", port)
    .setWorkerNum(8)                    // Worker线程数
    .setBatchingMaxMessages(50)         // 批次最大消息数
    .setBatchingMaxDelayMs(10)          // 批次最大延迟(ms)
    .setMaxPendingMessages(409600);     // 队列容量
```

### 高吞吐量配置

```java
TglogConfig config = TglogConfig.createTcp("host", port)
    .setWorkerNum(16)                   // 更多Worker
    .setBatchingMaxMessages(100)        // 更大批次
    .setBatchingMaxSize(64 * 1024)      // 64KB批次
    .setSocketSendBufferSize(128 * 1024 * 1024); // 128MB缓冲区
```

### 低延迟配置

```java
TglogConfig config = TglogConfig.createTcp("host", port)
    .setWorkerNum(4)
    .setBatchingMaxMessages(10)         // 小批次
    .setBatchingMaxDelayMs(5);          // 5ms延迟
```

## 性能监控

```java
// 获取性能指标
TglogMetrics metrics = client.getMetrics();

// 打印可读格式
System.out.println(client.getMetricsString(false));

// 导出JSON
String json = client.getMetricsString(true);
System.out.println(json);

// 定期监控
ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
scheduler.scheduleAtFixedRate(() -> {
    System.out.println("Success Rate: " + metrics.getSuccessRate() * 100 + "%");
    System.out.println("Queue Size: " + metrics.getMsgQueueSize());
}, 0, 10, TimeUnit.SECONDS);
```

## 常见问题

### Q: 如何确保消息不丢失？

A: 使用回调机制：

```java
client.send(log, (payload, exception) -> {
    if (exception == null) {
        // 发送成功
    } else {
        // 发送失败，保存到本地文件
        saveToLocalFile(payload);
    }
});
```

### Q: 队列满了怎么办？

A: 有三种策略：

1. **增加队列容量**
```java
config.setMaxPendingMessages(1000000);
```

2. **增加Worker数量**
```java
config.setWorkerNum(16);
```

3. **降级处理**
```java
try {
    client.send(log);
} catch (TglogException e) {
    if (e.getErrorCode() == TglogErrorCode.SEND_QUEUE_IS_FULL) {
        // 队列满，保存到本地
        saveToLocalFile(log);
    }
}
```

### Q: 如何优化性能？

A: 参考以下建议：

1. **合理配置Worker数量** - 通常设置为CPU核心数
2. **调整批次大小** - 根据日志大小调整
3. **使用fire-and-forget模式** - 不需要回调时不传callback
4. **预热连接** - 启动后等待连接建立

```java
TglogClient client = new TglogClient(config);
Thread.sleep(500);  // 等待连接建立
// 开始发送日志
```

## 下一步

- 阅读完整文档: [README.md](README.md)
- 查看项目总结: [docs/PROJECT_SUMMARY.md](docs/PROJECT_SUMMARY.md)
- 运行性能测试: `./run-test.sh -m 1000000 -c`
- 查看源代码: `src/main/java/com/tencent/tglog/`

## 获取帮助

如有问题，请查看：
- [README.md](README.md) - 完整文档
- [docs/PROJECT_SUMMARY.md](docs/PROJECT_SUMMARY.md) - 项目总结
- 源代码注释 - 详细的代码注释

祝使用愉快！
