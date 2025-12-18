#!/bin/bash

# Run Performance Test

set -e

# Check if compiled
if [ ! -d "target/classes" ]; then
    echo "Not compiled yet. Running Maven compile..."
    mvn compile
fi

echo "Running Performance Test..."
echo "==========================="
echo ""

# Copy dependencies if not exists
if [ ! -d "target/lib" ]; then
    echo "Copying Maven dependencies..."
    mvn -q dependency:copy-dependencies -DoutputDirectory=target/lib -DincludeScope=compile
fi

# Compile integration tests if needed
if [ ! -d "target/integration-test-classes" ]; then
    echo "Compiling integration tests..."
    mkdir -p target/integration-test-classes

    # Build classpath for compilation
    COMPILE_CP="target/classes"
    for jar in target/lib/*.jar; do
        COMPILE_CP="$COMPILE_CP:$jar"
    done

    javac -d target/integration-test-classes \
        -cp "$COMPILE_CP" \
        -encoding UTF-8 \
        src/integration-test/java/com/tencent/tglog/*.java
fi

# Default parameters
PARAMS=""

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -h|--host)
            PARAMS="$PARAMS -h $2"
            shift 2
            ;;
        -p|--port)
            PARAMS="$PARAMS -p $2"
            shift 2
            ;;
        -m|--messages)
            PARAMS="$PARAMS -m $2"
            shift 2
            ;;
        -w|--workers)
            PARAMS="$PARAMS -w $2"
            shift 2
            ;;
        -b|--batch-size)
            PARAMS="$PARAMS -b $2"
            shift 2
            ;;
        --tcp)
            PARAMS="$PARAMS --tcp"
            shift
            ;;
        --udp)
            PARAMS="$PARAMS --udp"
            shift
            ;;
        -c|--callback)
            PARAMS="$PARAMS -c"
            shift
            ;;
        -d|--debug)
            PARAMS="$PARAMS -d"
            shift
            ;;
        --help)
            java -cp target/classes:target/integration-test-classes com.tencent.tglog.TglogClientTest --help
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

# Build classpath with Maven dependencies
CLASSPATH="target/classes:target/integration-test-classes"

# Copy dependencies if not exists
if [ ! -d "target/lib" ]; then
    echo "Copying Maven dependencies..."
    mvn -q dependency:copy-dependencies -DoutputDirectory=target/lib -DincludeScope=compile
fi

# Add all jars from target/lib to classpath
if [ -d "target/lib" ]; then
    for jar in target/lib/*.jar; do
        CLASSPATH="$CLASSPATH:$jar"
    done
fi

# Run test
java -cp "$CLASSPATH" com.tencent.tglog.TglogClientTest $PARAMS
