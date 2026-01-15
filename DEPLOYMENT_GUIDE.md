# TGLog Java SDK 部署指南

## 发布到腾讯 Maven 仓库

### 前提条件

1. 确保有腾讯 Maven 仓库的访问权限
2. 获取 Maven 仓库的用户名和密码（如需要）

### 步骤 1: 配置 Maven 认证（如需要）

如果仓库需要认证，编辑 `~/.m2/settings.xml` 文件（如果不存在则创建），添加：

```xml
<?xml version="1.0" encoding="UTF-8"?>
<settings xmlns="http://maven.apache.org/SETTINGS/1.0.0"
          xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
          xsi:schemaLocation="http://maven.apache.org/SETTINGS/1.0.0
                              http://maven.apache.org/xsd/settings-1.0.0.xsd">
    <servers>
        <server>
            <id>tencent_public</id>
            <username>你的用户名</username>
            <password>你的密码</password>
        </server>
        <server>
            <id>tencent_public_snapshots</id>
            <username>你的用户名</username>
            <password>你的密码</password>
        </server>
    </servers>
</settings>
```

**注意**：`<server>` 的 `<id>` 必须与 pom.xml 中 `<repository>` 的 `<id>` 一致。

### 步骤 2: 发布到 Maven 仓库

```bash
# 发布快照版本（版本号带 -SNAPSHOT，如 1.0.0-SNAPSHOT）
mvn clean deploy

# 发布正式版本（需要先修改 pom.xml 中的版本号，去掉 -SNAPSHOT）
# 1. 修改 pom.xml: <version>1.0.1</version>
# 2. 执行发布
mvn clean deploy
```

### 步骤 3: 用户如何引用

发布成功后，用户在他们的项目中引用：

#### 3.1 添加依赖

在项目 `pom.xml` 中添加：

```xml
<dependency>
    <groupId>com.tencent.tglog</groupId>
    <artifactId>tglog-java</artifactId>
    <version>1.0.1</version>
</dependency>
```

#### 3.2 配置仓库

**方式一：在项目 pom.xml 中配置**

```xml
<repositories>
    <repository>
        <id>tencent_public</id>
        <name>Tencent Maven Repository</name>
        <url>https://mirrors.tencent.com/repository/maven/tglog/releases/</url>
        <releases>
            <enabled>true</enabled>
        </releases>
        <snapshots>
            <enabled>false</enabled>
        </snapshots>
    </repository>
    <repository>
        <id>tencent_public_snapshots</id>
        <name>Tencent Maven Snapshots Repository</name>
        <url>https://mirrors.tencent.com/repository/maven/tglog/snapshots/</url>
        <releases>
            <enabled>false</enabled>
        </releases>
        <snapshots>
            <enabled>true</enabled>
        </snapshots>
    </repository>
</repositories>
```

**方式二：在 ~/.m2/settings.xml 中全局配置（推荐）**

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
                    <name>Tencent Maven Repository</name>
                    <url>https://mirrors.tencent.com/repository/maven/tglog/releases/</url>
                    <releases>
                        <enabled>true</enabled>
                    </releases>
                    <snapshots>
                        <enabled>false</enabled>
                    </snapshots>
                </repository>
                <repository>
                    <id>tencent_public_snapshots</id>
                    <name>Tencent Maven Snapshots Repository</name>
                    <url>https://mirrors.tencent.com/repository/maven/tglog/snapshots/</url>
                    <releases>
                        <enabled>false</enabled>
                    </releases>
                    <snapshots>
                        <enabled>true</enabled>
                    </snapshots>
                </repository>
            </repositories>
        </profile>
    </profiles>

    <activeProfiles>
        <activeProfile>tencent-repos</activeProfile>
    </activeProfiles>
</settings>
```

#### 3.3 下载依赖

配置完成后，执行：

```bash
mvn clean install
```

Maven 会自动从腾讯仓库下载 tglog-java 依赖。

---

## 方案 2: 本地安装（临时方案）

如果暂时没有 Maven 私服，可以让同事本地安装：

### 你的操作：

1. 构建项目：
```bash
mvn clean package
```

2. 将以下文件发给同事：
   - `target/tglog-java-1.0.1.jar`
   - `target/tglog-java-1.0.1-sources.jar`
   - `pom.xml`

### 同事的操作：

1. 安装到本地 Maven 仓库：
```bash
mvn install:install-file \
  -Dfile=tglog-java-1.0.1.jar \
  -DgroupId=com.tencent.tglog \
  -DartifactId=tglog-java \
  -Dversion=1.0.1 \
  -Dpackaging=jar \
  -DgeneratePom=true
```

2. 在项目中添加依赖：
```xml
<dependency>
    <groupId>com.tencent.tglog</groupId>
    <artifactId>tglog-java</artifactId>
    <version>1.0.1</version>
</dependency>
```

---

## 方案 3: 通过公司 Git 仓库（不推荐）

同事可以直接克隆 Git 仓库并本地构建：

```bash
git clone <你的Git仓库地址>
cd sdk-java
mvn clean install
```

然后在自己的项目中引用。

**缺点**：每次更新都需要重新构建，不方便。

---

## 推荐方案

**强烈推荐使用方案 1（Maven 私服）**，这是企业级 Java 项目的标准做法：

✅ 版本管理清晰
✅ 依赖自动下载
✅ 支持多人协作
✅ 符合行业标准

如果公司没有 Maven 私服，建议申请搭建一个（Nexus 或 Artifactory）。
