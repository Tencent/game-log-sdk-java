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

# Compile examples if needed
if [ ! -d "target/example-classes" ]; then
    echo "Compiling examples..."
    javac -d target/example-classes \
        -cp target/classes \
        -encoding UTF-8 \
        src/examples/java/com/tencent/tglog/*.java
fi

java -cp target/classes:target/example-classes com.tencent.tglog.SimpleExample "$@"
