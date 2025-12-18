# Prometheus 指标集成指南

TGLog Java SDK 现在使用 Prometheus Java Client 来收集指标。本指南展示如何在您的应用程序中暴露这些指标。

## 设计理念

- **SDK 职责**：SDK 仅使用 Prometheus 客户端库收集指标
- **应用职责**：应用程序决定是否以及如何暴露这些指标（HTTP 端点、Push Gateway 等）
- **灵活性**：应用程序可以使用自定义 `CollectorRegistry` 实例实现隔离

## 可用指标

### Counter 指标（累计计数）

| 指标名称 | 类型 | 说明 |
|---------|------|------|
| `tglog_request_messages_total` | Counter | 请求发送的消息总数 |
| `tglog_enqueued_messages_total` | Counter | 成功入队的消息总数 |
| `tglog_dequeued_messages_total` | Counter | 成功出队的消息总数 |
| `tglog_succeeded_messages_total` | Counter | 成功发送的消息总数 |
| `tglog_failed_messages_total` | Counter | 发送失败的消息总数 |
| `tglog_timeout_messages_total` | Counter | 超时的消息总数 |
| `tglog_retry_messages_total` | Counter | 重试的消息总数 |
| `tglog_dropped_messages_total` | Counter | 丢弃的消息总数（队列满）|
| `tglog_opened_connections_total` | Counter | 打开的连接总数 |
| `tglog_closed_connections_total` | Counter | 关闭的连接总数 |
| `tglog_error_connections_total` | Counter | 连接错误总数 |
| `tglog_add_endpoints_total` | Counter | 添加端点的总次数 |
| `tglog_remove_endpoints_total` | Counter | 移除端点的总次数 |
| `tglog_rescued_message_bytes_total` | Counter | 抢救的消息字节总数 |

### Gauge 指标（瞬时状态）

| 指标名称 | 类型 | 说明 |
|---------|------|------|
| `tglog_message_queue_size` | Gauge | 当前消息队列大小 |
| `tglog_message_queue_capacity` | Gauge | 消息队列容量 |
| `tglog_endpoints` | Gauge | 当前端点数量 |
| `tglog_active_connections` | Gauge | 当前活跃连接数 |

## 使用示例

### 示例 1：使用默认 Registry 和 HTTP Server

最简单的方式是使用默认 `CollectorRegistry` 并通过 HTTP 暴露指标：

```java
import com.tencent.tglog.*;
import io.prometheus.client.exporter.HTTPServer;
import java.io.IOException;

public class GameServerWithMetrics {
    public static void main(String[] args) throws IOException, TglogException {
        // 创建 TGLog 客户端（使用默认 registry）
        TglogConfig config = TglogConfig.createTcp("logs.game.com", 8080)
            .setWorkerNum(8)
            .setBatchingMaxDelayMs(10);

        TglogClient client = new TglogClient(config);

        // 在 9090 端口启动 Prometheus HTTP server
        // 指标将在 http://localhost:9090/metrics 可用
        HTTPServer server = new HTTPServer(9090);

        System.out.println("Prometheus 指标可在 http://localhost:9090/metrics 访问");

        // 您的应用逻辑
        for (int i = 0; i < 1000; i++) {
            client.send(String.format("game_event|id=%d|type=battle\n", i));
        }

        // 保持运行...
        Thread.sleep(60000);

        // 清理
        client.close();
        server.stop();
    }
}
```

**HTTP Server 所需的 Maven 依赖：**
```xml
<dependency>
    <groupId>io.prometheus</groupId>
    <artifactId>simpleclient</artifactId>
    <version>0.16.0</version>
</dependency>
<dependency>
    <groupId>io.prometheus</groupId>
    <artifactId>simpleclient_httpserver</artifactId>
    <version>0.16.0</version>
</dependency>
```

### 示例 2：使用自定义 Registry（隔离）

如果您想将 TGLog 指标与其他应用指标隔离：

```java
import com.tencent.tglog.*;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.HTTPServer;

public class IsolatedMetricsExample {
    public static void main(String[] args) throws Exception {
        // 为 TGLog 指标创建自定义 registry
        CollectorRegistry tglogRegistry = new CollectorRegistry();

        // 使用自定义 registry 创建 TGLog 客户端
        TglogConfig config = TglogConfig.createTcp("logs.game.com", 8080);
        TglogClient client = new TglogClient(config);

        // 从 metrics 访问 registry
        CollectorRegistry registry = client.getMetrics().getRegistry();

        // 在单独的端口暴露
        HTTPServer tglogMetricsServer = new HTTPServer(9091, registry);

        System.out.println("TGLog 指标: http://localhost:9091/metrics");

        // 您的应用逻辑...
        client.send("test_log\n");

        // 清理
        client.close();
        tglogMetricsServer.stop();
    }
}
```

### 示例 3：Spring Boot 集成

与 Spring Boot Actuator 和 Micrometer 集成：

```java
import com.tencent.tglog.*;
import io.prometheus.client.CollectorRegistry;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class GameServerApplication {

    @Bean
    public TglogClient tglogClient(CollectorRegistry collectorRegistry) throws TglogException {
        TglogConfig config = TglogConfig.createTcp("logs.game.com", 8080)
            .setWorkerNum(8);

        // TGLog 将使用 Spring 的 CollectorRegistry
        return new TglogClient(config);
    }

    @Bean
    public CollectorRegistry collectorRegistry() {
        return CollectorRegistry.defaultRegistry;
    }

    public static void main(String[] args) {
        SpringApplication.run(GameServerApplication.class, args);
    }
}
```

**Spring Boot 依赖：**
```xml
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-actuator</artifactId>
</dependency>
<dependency>
    <groupId>io.micrometer</groupId>
    <artifactId>micrometer-registry-prometheus</artifactId>
</dependency>
```

**application.yml：**
```yaml
management:
  endpoints:
    web:
      exposure:
        include: prometheus,health,info
  metrics:
    export:
      prometheus:
        enabled: true
```

指标将在以下地址可用：`http://localhost:8080/actuator/prometheus`

### 示例 4：Push Gateway 集成

适用于批处理任务或短期应用：

```java
import com.tencent.tglog.*;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.PushGateway;

public class BatchJobWithMetrics {
    public static void main(String[] args) throws Exception {
        TglogConfig config = TglogConfig.createTcp("logs.game.com", 8080);
        TglogClient client = new TglogClient(config);

        // 处理批处理任务
        for (int i = 0; i < 10000; i++) {
            client.send(String.format("batch_log|id=%d\n", i));
        }

        // 等待完成
        client.flush(5000);

        // 推送指标到 Prometheus Push Gateway
        CollectorRegistry registry = client.getMetrics().getRegistry();
        PushGateway pg = new PushGateway("pushgateway.example.com:9091");
        pg.pushAdd(registry, "tglog_batch_job");

        System.out.println("指标已推送到 Push Gateway");

        client.close();
    }
}
```

**Push Gateway 所需的 Maven 依赖：**
```xml
<dependency>
    <groupId>io.prometheus</groupId>
    <artifactId>simpleclient_pushgateway</artifactId>
    <version>0.16.0</version>
</dependency>
```

### 示例 5：程序化访问（不暴露 HTTP）

如果您只想通过程序访问指标而不暴露它们：

```java
import com.tencent.tglog.*;

public class ProgrammaticMetricsExample {
    public static void main(String[] args) throws Exception {
        TglogConfig config = TglogConfig.createTcp("logs.game.com", 8080);
        TglogClient client = new TglogClient(config);

        // 发送一些日志
        for (int i = 0; i < 1000; i++) {
            client.send("test_log\n");
        }

        // 通过程序访问指标
        TglogMetrics metrics = client.getMetrics();

        System.out.println("请求数: " + metrics.getRequestMsgCount());
        System.out.println("成功数: " + metrics.getSucceededMsgCount());
        System.out.println("失败数: " + metrics.getFailedMsgCount());
        System.out.println("成功率: " + metrics.getSuccessRate() * 100 + "%");

        // 或获取 JSON 格式
        System.out.println(client.getMetricsJson());

        // 或获取格式化字符串
        System.out.println(client.printMetrics());

        client.close();
    }
}
```

## Prometheus 查询示例

指标暴露后，您可以在 Prometheus 中查询它们：

### 成功率
```promql
rate(tglog_succeeded_messages_total[5m]) /
(rate(tglog_succeeded_messages_total[5m]) + rate(tglog_failed_messages_total[5m]))
```

### 消息吞吐量（消息/秒）
```promql
rate(tglog_succeeded_messages_total[1m])
```

### 队列利用率
```promql
tglog_message_queue_size / tglog_message_queue_capacity
```

### 连接变动率
```promql
rate(tglog_opened_connections_total[5m])
```

### 丢弃率
```promql
rate(tglog_dropped_messages_total[5m])
```

## Grafana 仪表板

以下是 TGLog 指标的 Grafana 仪表板 JSON 示例片段：

```json
{
  "dashboard": {
    "title": "TGLog 指标",
    "panels": [
      {
        "title": "消息吞吐量",
        "targets": [
          {
            "expr": "rate(tglog_succeeded_messages_total[1m])"
          }
        ]
      },
      {
        "title": "成功率",
        "targets": [
          {
            "expr": "rate(tglog_succeeded_messages_total[5m]) / (rate(tglog_succeeded_messages_total[5m]) + rate(tglog_failed_messages_total[5m]))"
          }
        ]
      },
      {
        "title": "队列大小",
        "targets": [
          {
            "expr": "tglog_message_queue_size"
          }
        ]
      },
      {
        "title": "活跃连接数",
        "targets": [
          {
            "expr": "tglog_active_connections"
          }
        ]
      }
    ]
  }
}
```

## 最佳实践

1. **选择正确的暴露方式**：
   - 长期运行的服务：使用 HTTP server（拉取模式）
   - 短期任务：使用 Push Gateway
   - Spring Boot 应用：使用 Actuator 集成

2. **使用自定义 Registry 实现隔离**：
   - 如果您的应用有许多其他指标
   - 如果您想为不同组件使用单独的端点

3. **监控关键指标**：
   - 成功率：`tglog_succeeded_messages_total / (tglog_succeeded_messages_total + tglog_failed_messages_total)`
   - 队列利用率：`tglog_message_queue_size / tglog_message_queue_capacity`
   - 吞吐量：`rate(tglog_succeeded_messages_total[1m])`

4. **设置告警**：
   - 当成功率低于阈值时告警
   - 当队列利用率持续较高时告警
   - 当丢弃率增加时告警

## 从旧指标迁移

新的基于 Prometheus 的指标**完全向后兼容**。所有现有的 getter 方法仍然有效：

```java
// 旧代码仍然有效
TglogMetrics metrics = client.getMetrics();
long requests = metrics.getRequestMsgCount();
double successRate = metrics.getSuccessRate();
String json = metrics.toJson();
```

唯一的区别是现在这些指标也可以以 Prometheus 格式供抓取使用。

## 故障排查

### 指标未显示

1. 验证 HTTP server 是否正在运行：
   ```bash
   curl http://localhost:9090/metrics
   ```

2. 检查指标是否已注册：
   ```java
   Enumeration<Collector.MetricFamilySamples> samples =
       client.getMetrics().getRegistry().metricFamilySamples();
   while (samples.hasMoreElements()) {
       System.out.println(samples.nextElement().name);
   }
   ```

### 重复指标错误

如果您看到"Collector already registered"错误，说明您正在尝试使用同一个 registry 创建多个 `TglogClient` 实例。解决方案：

1. 使用单个 `TglogClient` 实例（推荐）
2. 为每个客户端使用单独的 `CollectorRegistry` 实例
3. 在创建新客户端之前取消注册旧指标

## 其他资源

- [Prometheus Java Client 文档](https://github.com/prometheus/client_java)
- [Prometheus 最佳实践](https://prometheus.io/docs/practices/naming/)
- [Grafana Prometheus 集成](https://grafana.com/docs/grafana/latest/datasources/prometheus/)
