# 构建说明

## 项目结构

本项目遵循标准的Maven目录布局：

```
sdk-java/
├── src/
│   ├── main/
│   │   ├── java/              # 主源代码
│   │   │   └── com/tencent/tglog/
│   │   └── resources/         # 主资源文件
│   ├── test/
│   │   ├── java/              # 单元测试 (JUnit)
│   │   │   └── com/tencent/tglog/
│   │   └── resources/         # 测试资源文件
│   ├── examples/
│   │   └── java/              # 示例程序
│   │       └── com/tencent/tglog/
│   └── integration-test/
│       └── java/              # 集成测试/性能测试
│           └── com/tencent/tglog/
├── docs/                      # 文档
├── pom.xml                    # Maven配置文件
└── target/                    # 构建输出目录（自动生成）
```

## 构建方法

### 方法一：使用Maven（推荐）

#### 编译
```bash
mvn compile
```

#### 运行测试
```bash
mvn test
```

#### 打包为JAR
```bash
mvn package
```

这将生成：
- `target/tglog-java-1.0.1.jar` - 主库JAR文件
- `target/tglog-java-1.0.1-jar-with-dependencies.jar` - 包含所有依赖的Fat JAR
- `target/tglog-java-1.0.1-sources.jar` - 源码JAR

#### 清理构建产物
```bash
mvn clean
```

### 方法二：使用Shell脚本

为了方便使用，提供了以下脚本：

#### 编译
```bash
./compile.sh
```

#### 运行示例
```bash
./run-example.sh
```

#### 运行性能测试
```bash
./run-test.sh [选项]

选项：
  -h, --host HOST         服务器地址（默认：localhost）
  -p, --port PORT         服务器端口（默认：8080）
  -m, --messages NUM      消息数量（默认：1000000）
  -w, --workers NUM       工作线程数（默认：4）
  -b, --batch-size SIZE   批次大小（默认：100）
  --tcp                   使用TCP协议
  --udp                   使用UDP协议
  -c, --callback          启用回调
  -d, --debug             启用调试日志
  --help                  显示帮助信息
```

## 环境要求

- Java 8 或更高版本
- Maven 3.6+ （用于Maven构建）

## IDE配置

### IntelliJ IDEA
1. 打开项目目录
2. IDEA会自动检测Maven项目
3. 等待Maven下载依赖
4. 右键点击 `pom.xml` → Maven → Reload Project

### Eclipse
1. File → Import → Existing Maven Projects
2. 选择项目目录
3. Eclipse会自动导入并配置项目

### VS Code
1. 安装"Java Extension Pack"插件
2. 打开项目文件夹
3. VS Code会自动检测Maven项目

## 常用Maven命令

```bash
# 清理并编译
mvn clean compile

# 运行测试
mvn test

# 打包（跳过测试）
mvn package -DskipTests

# 安装到本地Maven仓库
mvn install

# 生成JavaDoc文档
mvn javadoc:javadoc

# 显示依赖树
mvn dependency:tree

# 检查依赖更新
mvn versions:display-dependency-updates
```

## 故障排除

### Maven未找到
安装Maven：
- Ubuntu/Debian: `sudo apt-get install maven`
- macOS: `brew install maven`
- 或从 https://maven.apache.org/ 下载

### Java版本问题
检查Java版本：
```bash
java -version
javac -version
```

如需设置JAVA_HOME：
```bash
export JAVA_HOME=/path/to/jdk
```

### Shell脚本权限问题
添加执行权限：
```bash
chmod +x *.sh
```
