#!/bin/bash

# Run Concurrent Send Test

set -e

# Check if compiled
if [ ! -d "target/classes" ]; then
    echo "Error: Project not compiled. Please run ./compile.sh first"
    exit 1
fi

# Check if integration tests are compiled
if [ ! -d "target/test-classes" ]; then
    echo "Error: Integration tests not compiled. Please run ./compile.sh first"
    exit 1
fi

# Check if ConcurrentSendTest exists
if [ ! -f "target/test-classes/com/tencent/tglog/ConcurrentSendTest.class" ]; then
    echo "Error: ConcurrentSendTest.class not found. Please run ./compile.sh first"
    exit 1
fi

# Build classpath
CLASSPATH="target/classes:target/test-classes"

# Add all jars from target/lib to classpath (if exists)
if [ -d "target/lib" ]; then
    for jar in target/lib/*.jar; do
        CLASSPATH="$CLASSPATH:$jar"
    done
else
    # Copy dependencies if not exists
    echo "Copying Maven dependencies..."
    mvn -q dependency:copy-dependencies -DoutputDirectory=target/lib -DincludeScope=test
    for jar in target/lib/*.jar; do
        CLASSPATH="$CLASSPATH:$jar"
    done
fi

# Run test with all arguments passed through
java -cp "$CLASSPATH" com.tencent.tglog.ConcurrentSendTest "$@"
