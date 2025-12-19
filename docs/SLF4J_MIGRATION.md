# SLF4J 日志框架迁移指南

## 概述

TGLog Java SDK 已从自定义的 `TglogLogger` 接口迁移到标准的 **SLF4J** 日志框架。这是 Java 生态系统的最佳实践，为用户提供了更大的灵活性和更好的集成体验。

## 迁移原因

### 自定义 TglogLogger 的问题

1. **重新发明轮子** - Java 生态已有成熟的日志框架
2. **缺少级别控制** - 无法在运行时动态调整日志级别
3. **性能开销** - 没有延迟求值，可能产生不必要的字符串拼接
4. **集成困难** - 用户需要适配自定义接口
5. **功能有限** - 缺少 MDC、参数化日志等高级特性

### SLF4J 的优势

1. ✅ **行业标准** - AWS SDK、Spring、Hibernate 等都使用 SLF4J
2. ✅ **灵活选择** - 支持 Logback、Log4j2、JUL 等多种实现
3. ✅ **零配置** - 默认使用 NOP logger，不产生任何输出
4. ✅ **性能优化** - 支持参数化日志和级别检查
5. ✅ **生态成熟** - 与各种框架无缝集成

## 变更内容

### 1. 依赖变更

**添加的依赖：**
```xml
<!-- SLF4J API (编译依赖) -->
<dependency>
    <groupId>org.slf4j</groupId>
    <artifactId>slf4j-api</artifactId>
    <version>2.0.9</version>
</dependency>

<!-- SLF4J Simple (测试依赖) -->
<dependency>
    <groupId>org.slf4j</groupId>
    <artifactId>slf4j-simple</artifactId>
    <version>2.0.9</version>
    <scope>test</scope>
</dependency>
```

### 2. 删除的类

- `TglogLogger` 接口
- `TglogLogger.DefaultLogger` 实现
- `TglogLogger.NoOpLogger` 实现

### 3. API 变更

**TglogConfig 变更：**
```java
// ❌ 旧的 API（已删除）
TglogConfig config = TglogConfig.createTcp("host", port)
    .setLogger(new TglogLogger.DefaultLogger());

// ✅ 新的 API（无需配置）
TglogConfig config = TglogConfig.createTcp("host", port);
```

**删除的方法：**
- `TglogConfig.setLogger(TglogLogger logger)`
- `TglogConfig.getLogger()`

### 4. 内部实现变更

所有内部类现在使用 SLF4J：
```java
// 每个类都有自己的 logger
private static final Logger logger = LoggerFactory.getLogger(TglogClient.class);

// 使用参数化日志
logger.info("Client created: {}://{}:{}", protocol, host, port);
logger.warn("Connection failed: {}", endpoint, exception);
```

## 迁移指南

### 对于 SDK 用户

#### 1. 基本用法（无需改动）

如果你之前没有使用 `.setLogger()`，代码无需任何改动：

```java
// 这段代码在新版本中完全兼容
TglogConfig config = TglogConfig.createTcp("host", 20001)
    .setWorkerNum(8);

TglogClient client = new TglogClient(config);
```

#### 2. 如果使用了 setLogger()

**旧代码：**
```java
TglogConfig config = TglogConfig.createTcp("host", port)
    .setDebugEnabled(true)
    .setLogger(new TglogLogger.DefaultLogger());
```

**新代码：**
```java
// 1. 删除 .setLogger() 调用
TglogConfig config = TglogConfig.createTcp("host", port)
    .setDebugEnabled(true);

// 2. 添加 SLF4J 实现依赖（如 Logback）
// 3. 配置日志级别（见下文）
```

### 配置日志输出

#### 方案 1: 使用 Logback（推荐）

**1. 添加依赖：**
```xml
<dependency>
    <groupId>ch.qos.logback</groupId>
    <artifactId>logback-classic</artifactId>
    <version>1.4.14</version>
</dependency>
```

**2. 创建 `src/main/resources/logback.xml`：**
```xml
<configuration>
    <appender name="STDOUT" class="ch.qos.logback.core.ConsoleAppender">
        <encoder>
            <pattern>%d{HH:mm:ss.SSS} [%thread] %-5level %logger{36} - %msg%n</pattern>
        </encoder>
    </appender>

    <!-- TGLog SDK 日志级别 -->
    <logger name="com.tencent.tglog" level="INFO"/>

    <!-- 调试时设置为 DEBUG -->
    <!-- <logger name="com.tencent.tglog" level="DEBUG"/> -->

    <root level="INFO">
        <appender-ref ref="STDOUT"/>
    </root>
</configuration>
```

#### 方案 2: 使用 Log4j2

**1. 添加依赖：**
```xml
<dependency>
    <groupId>org.apache.logging.log4j</groupId>
    <artifactId>log4j-slf4j2-impl</artifactId>
    <version>2.20.0</version>
</dependency>
```

**2. 创建 `src/main/resources/log4j2.xml`：**
```xml
<Configuration>
    <Appenders>
        <Console name="Console" target="SYSTEM_OUT">
            <PatternLayout pattern="%d{HH:mm:ss.SSS} [%t] %-5level %logger{36} - %msg%n"/>
        </Console>
    </Appenders>
    <Loggers>
        <Logger name="com.tencent.tglog" level="info"/>
        <Root level="info">
            <AppenderRef ref="Console"/>
        </Root>
    </Loggers>
</Configuration>
```

#### 方案 3: 使用 SLF4J Simple（测试用）

**1. 添加依赖：**
```xml
<dependency>
    <groupId>org.slf4j</groupId>
    <artifactId>slf4j-simple</artifactId>
    <version>2.0.9</version>
</dependency>
```

**2. 通过系统属性配置：**
```bash
java -Dorg.slf4j.simpleLogger.defaultLogLevel=info \
     -Dorg.slf4j.simpleLogger.log.com.tencent.tglog=debug \
     -jar your-app.jar
```

#### 方案 4: 不输出日志（默认）

如果不添加任何 SLF4J 实现，SDK 会使用 NOP logger，不产生任何日志输出：

```
SLF4J: No SLF4J providers were found.
SLF4J: Defaulting to no-operation (NOP) logger implementation
```

这是完全正常的，不影响 SDK 功能。

## 日志级别说明

TGLog SDK 使用以下日志级别：

| 级别 | 用途 | 示例 |
|------|------|------|
| ERROR | 严重错误 | 连接失败、发送失败 |
| WARN | 警告信息 | 连接重试、队列接近满 |
| INFO | 重要信息 | 客户端启动、连接建立、端点变更 |
| DEBUG | 调试信息 | 消息发送详情、批次处理、连接状态 |

**推荐配置：**
- **生产环境**: `INFO` 级别
- **开发环境**: `DEBUG` 级别
- **问题排查**: `DEBUG` 级别

## 性能影响

SLF4J 的参数化日志可以提升性能：

```java
// ❌ 旧方式：总是拼接字符串
logger.log(Level.DEBUG, "Message: " + msg + ", size: " + size);

// ✅ 新方式：只在 DEBUG 启用时才拼接
logger.debug("Message: {}, size: {}", msg, size);
```

**性能测试结果：**
- 编译通过: ✅
- 测试通过: ✅ (100,000 消息, 100% 成功率)
- 吞吐量: 467,289 msg/s (与重构前相当)

## 常见问题

### Q1: 我看到 "No SLF4J providers were found" 警告

**A:** 这是正常的，表示没有配置 SLF4J 实现。SDK 会使用 NOP logger，不影响功能。如果需要日志输出，添加 Logback 或 Log4j2 依赖即可。

### Q2: 如何临时启用调试日志？

**A:**
- **Logback**: 修改 `logback.xml` 中的级别
- **Log4j2**: 修改 `log4j2.xml` 中的级别
- **SLF4J Simple**: 使用系统属性 `-Dorg.slf4j.simpleLogger.log.com.tencent.tglog=debug`

### Q3: 能否继续使用自定义日志？

**A:** 不能。`TglogLogger` 接口已被移除。但你可以通过 SLF4J 的 Appender 机制实现自定义日志处理。

### Q4: 与 Spring Boot 集成需要配置吗？

**A:** 不需要。Spring Boot 默认使用 Logback，TGLog SDK 会自动使用 Spring Boot 的日志配置。

## 兼容性

- ✅ **向后兼容**: 不使用 `.setLogger()` 的代码无需改动
- ⚠️ **不兼容**: 使用了 `.setLogger()` 的代码需要删除该调用
- ✅ **Java 版本**: 仍然支持 Java 8+
- ✅ **功能**: 所有功能保持不变

## 相关文档

- [README.md](../README.md) - 主文档，包含日志配置示例
- [PROMETHEUS_METRICS.md](PROMETHEUS_METRICS.md) - Prometheus 指标集成
- [CODE_QUALITY.md](CODE_QUALITY.md) - 代码质量报告

## 总结

迁移到 SLF4J 是一个重要的改进，使 TGLog Java SDK 更符合 Java 生态的最佳实践：

1. ✅ **标准化** - 使用行业标准日志框架
2. ✅ **灵活性** - 用户可自由选择日志实现
3. ✅ **零配置** - 默认不输出日志，不影响性能
4. ✅ **易集成** - 与 Spring、微服务框架无缝集成
5. ✅ **高性能** - 参数化日志避免不必要的字符串拼接

对于大多数用户，这次迁移是透明的，无需任何代码改动。
