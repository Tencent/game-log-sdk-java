# TGLog Java SDK 快速开始

## 1. 添加 Maven 依赖

### 步骤 1: 在 pom.xml 中添加依赖

```xml
<dependency>
    <groupId>com.tencent.tglog</groupId>
    <artifactId>tglog-java</artifactId>
    <version>1.0.0-SNAPSHOT</version>
</dependency>
```

### 步骤 2: 配置腾讯 Maven 仓库

**推荐方式：在 `~/.m2/settings.xml` 中全局配置**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<settings xmlns="http://maven.apache.org/SETTINGS/1.0.0"
          xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
          xsi:schemaLocation="http://maven.apache.org/SETTINGS/1.0.0
                              http://maven.apache.org/xsd/settings-1.0.0.xsd">
    <profiles>
        <profile>
            <id>tencent-repos</id>
            <repositories>
                <repository>
                    <id>tencent_public</id>
                    <url>https://mirrors.tencent.com/repository/maven/tglog/releases/</url>
                    <releases><enabled>true</enabled></releases>
                    <snapshots><enabled>false</enabled></snapshots>
                </repository>
                <repository>
                    <id>tencent_public_snapshots</id>
                    <url>https://mirrors.tencent.com/repository/maven/tglog/snapshots/</url>
                    <releases><enabled>false</enabled></releases>
                    <snapshots><enabled>true</enabled></snapshots>
                </repository>
            </repositories>
        </profile>
    </profiles>
    <activeProfiles>
        <activeProfile>tencent-repos</activeProfile>
    </activeProfiles>
</settings>
```

### 步骤 3: 下载依赖

```bash
mvn clean install
```

## 2. 基本使用示例

```java
import com.tencent.tglog.*;

public class Example {
    public static void main(String[] args) throws Exception {
        // 1. 创建配置
        TglogConfig config = TglogConfig.createTcp("logs.game.com", 8080)
            .setWorkerNum(4)
            .setBatchingMaxMessages(50);

        // 2. 创建客户端
        try (TglogClient client = new TglogClient(config)) {

            // 3. 发送日志 (Lambda callback)
            client.send("battle|winner=player1|loser=player2\n", (payload, exception) -> {
                if (exception == null) {
                    System.out.println("发送成功");
                } else {
                    System.err.println("发送失败: " + exception.getMessage());
                }
            });

            // 或使用 CompletableFuture
            client.send("log\n")
                  .thenRun(() -> System.out.println("Success"))
                  .get(); // 等待完成
        }
    }
}
```

## 3. 错误处理

```java
try {
    client.send(logPayload, callback);
} catch (TglogException e) {
    // 处理同步错误（参数验证、队列满等）
    System.err.println("Error: " + e.getErrorCode());
}
```

## 4. 查看指标

```java
System.out.println(client.getMetricsString(false));
```

## 更多信息

- 详细文档：查看 `README.md`
- 部署指南：查看 `DEPLOYMENT_GUIDE.md`
- 示例代码：查看 `src/examples/java/` 目录
