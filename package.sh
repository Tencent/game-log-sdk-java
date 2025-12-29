#!/bin/bash

# TGLog Java SDK 打包脚本
# 用于打包编译好的文件，方便在其他机器上执行测试

set -e

echo "=========================================="
echo "TGLog Java SDK 打包脚本"
echo "=========================================="

# 清理并编译
echo "步骤 1: 清理并编译项目..."
mvn clean package -DskipTests

# 检查编译结果
if [ ! -f target/tglog-java-1.0.0-jar-with-dependencies.jar ]; then
    echo "错误: 打包失败，找不到 jar 文件"
    exit 1
fi

# 创建发布目录
RELEASE_DIR="tglog-java-release"
echo ""
echo "步骤 2: 创建发布目录 ${RELEASE_DIR}..."
rm -rf ${RELEASE_DIR}
mkdir -p ${RELEASE_DIR}/lib
mkdir -p ${RELEASE_DIR}/bin

# 复制 jar 文件
echo "步骤 3: 复制 jar 文件..."
cp target/tglog-java-1.0.0-jar-with-dependencies.jar ${RELEASE_DIR}/lib/tglog-java.jar
cp target/tglog-java-1.0.0.jar ${RELEASE_DIR}/lib/tglog-java-slim.jar

# 打包测试类到单独的 jar
echo "步骤 4: 打包测试类和测试依赖..."
if [ -d target/test-classes ]; then
    cd target/test-classes
    jar cf ../../${RELEASE_DIR}/lib/tglog-java-tests.jar com/tencent/tglog/*.class
    cd ../..
fi

# 复制测试依赖（Guava 用于限流）
echo "步骤 4.1: 复制测试依赖..."
if [ -f ~/.m2/repository/com/google/guava/guava/33.5.0-jre/guava-33.5.0-jre.jar ]; then
    cp ~/.m2/repository/com/google/guava/guava/33.5.0-jre/guava-33.5.0-jre.jar ${RELEASE_DIR}/lib/
else
    echo "  警告: 未找到 Guava jar，测试可能需要限流功能"
fi

# 复制 SLF4J 日志实现（slf4j-simple）
echo "步骤 4.2: 复制日志实现..."
if [ -f ~/.m2/repository/org/slf4j/slf4j-simple/2.0.9/slf4j-simple-2.0.9.jar ]; then
    cp ~/.m2/repository/org/slf4j/slf4j-simple/2.0.9/slf4j-simple-2.0.9.jar ${RELEASE_DIR}/lib/
else
    echo "  警告: 未找到 slf4j-simple jar，将无法输出日志"
fi

# 创建运行脚本
echo "步骤 5: 创建运行脚本..."

# 创建测试运行脚本
cat > ${RELEASE_DIR}/bin/run-test.sh << 'EOF'
#!/bin/bash

# 获取脚本所在目录
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LIB_DIR="${SCRIPT_DIR}/../lib"

# 运行性能测试（包含测试类、主 jar 和测试依赖）
java -cp "${LIB_DIR}/tglog-java-tests.jar:${LIB_DIR}/tglog-java.jar:${LIB_DIR}/guava-33.5.0-jre.jar:${LIB_DIR}/slf4j-simple-2.0.9.jar" \
     com.tencent.tglog.TglogClientTest "$@"
EOF

chmod +x ${RELEASE_DIR}/bin/run-test.sh

# 创建示例运行脚本
cat > ${RELEASE_DIR}/bin/run-example.sh << 'EOF'
#!/bin/bash

# 获取脚本所在目录
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LIB_DIR="${SCRIPT_DIR}/../lib"

# 检查参数
if [ $# -lt 1 ]; then
    echo "用法: $0 <example-class-name> [args...]"
    echo "示例: $0 BasicUsageExample"
    exit 1
fi

EXAMPLE_CLASS=$1
shift

# 运行示例
java -cp "${LIB_DIR}/tglog-java.jar" \
     com.tencent.tglog.examples.${EXAMPLE_CLASS} "$@"
EOF

chmod +x ${RELEASE_DIR}/bin/run-example.sh

# 创建 README
cat > ${RELEASE_DIR}/README.md << 'EOF'
# TGLog Java SDK 发布包

## 目录结构

```
tglog-java-release/
├── lib/
│   ├── tglog-java.jar                    # 完整包（包含所有依赖）
│   └── tglog-java-slim.jar               # 精简包（仅项目代码）
├── bin/
│   ├── run-test.sh                       # 运行性能测试
│   └── run-example.sh                    # 运行示例
└── README.md                             # 本文件
```

## 快速开始

### 1. 运行性能测试

```bash
# 使用默认配置
./bin/run-test.sh

# 自定义配置
./bin/run-test.sh -h dev.tglog.com -p 20001 -m 100000 -w 8

# 查看帮助
./bin/run-test.sh --help
```

### 2. 性能测试参数

- `-h, --host <host>` - 服务器地址（默认: dev.tglog.com）
- `-p, --port <port>` - 服务器端口（默认: 20001）
- `-m, --messages <count>` - 发送消息数量（默认: 100000）
- `-w, --workers <count>` - Worker 线程数（默认: 8）
- `-b, --batch-size <size>` - 批次大小（默认: 50）
- `-r, --rate-limit <rate>` - 限流（默认: 500000 msg/s）
- `--tcp` - 使用 TCP 协议（默认）
- `--udp` - 使用 UDP 协议
- `-c, --callback` - 启用回调（会降低性能）
- `-d, --debug` - 启用调试日志

### 3. 在代码中使用

将 `lib/tglog-java-slim.jar` 添加到你的项目 classpath 中：

```bash
javac -cp lib/tglog-java-slim.jar YourApp.java
java -cp lib/tglog-java-slim.jar:. YourApp
```

或者在 Maven 项目中安装到本地仓库：

```bash
mvn install:install-file \
  -Dfile=lib/tglog-java-slim.jar \
  -DgroupId=com.tencent.tglog \
  -DartifactId=tglog-java \
  -Dversion=1.0.0 \
  -Dpackaging=jar
```

## 系统要求

- Java 8 或更高版本
- Linux/macOS/Windows（跨平台）

## 性能指标

- 目标吞吐量: 300,000 - 500,000 msg/s
- 低延迟: 批次延迟 10ms（可配置）
- 非阻塞: 发送操作不阻塞业务线程
EOF

# 打包成 tar.gz
echo ""
echo "步骤 6: 打包成 tar.gz..."
tar czf tglog-java-release.tar.gz ${RELEASE_DIR}

# 显示结果
echo ""
echo "=========================================="
echo "打包完成！"
echo "=========================================="
echo ""
echo "生成的文件:"
echo "  1. 发布目录: ${RELEASE_DIR}/"
echo "  2. 压缩包: tglog-java-release.tar.gz"
echo ""
echo "文件大小:"
ls -lh tglog-java-release.tar.gz | awk '{print "  压缩包: " $5}'
du -sh ${RELEASE_DIR} | awk '{print "  目录: " $1}'
echo ""
echo "使用方法:"
echo "  1. 解压: tar xzf tglog-java-release.tar.gz"
echo "  2. 进入目录: cd ${RELEASE_DIR}"
echo "  3. 运行测试: ./bin/run-test.sh"
echo ""
echo "=========================================="
