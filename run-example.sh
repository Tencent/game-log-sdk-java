#!/bin/bash

# Run Simple Example

set -e

# Check if compiled
if [ ! -d "target/classes" ]; then
    echo "Not compiled yet. Running Maven compile..."
    mvn compile
fi

echo "Running Simple Example..."
echo "========================="
echo ""

# Copy dependencies if not exists
if [ ! -d "target/lib" ]; then
    echo "Copying Maven dependencies..."
    mvn -q dependency:copy-dependencies -DoutputDirectory=target/lib -DincludeScope=compile
fi

# Compile examples if needed
if [ ! -d "target/example-classes" ]; then
    echo "Compiling examples..."
    mkdir -p target/example-classes

    # Build classpath for compilation
    COMPILE_CP="target/classes"
    for jar in target/lib/*.jar; do
        COMPILE_CP="$COMPILE_CP:$jar"
    done

    javac -d target/example-classes \
        -cp "$COMPILE_CP" \
        -encoding UTF-8 \
        src/examples/java/com/tencent/tglog/*.java
fi

# Build classpath with Maven dependencies
CLASSPATH="target/classes:target/example-classes"

# Add all jars from target/lib to classpath
if [ -d "target/lib" ]; then
    for jar in target/lib/*.jar; do
        CLASSPATH="$CLASSPATH:$jar"
    done
fi

java -cp "$CLASSPATH" com.tencent.tglog.SimpleExample "$@"
