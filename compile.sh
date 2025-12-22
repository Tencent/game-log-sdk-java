#!/bin/bash

# Compile TGLog Pure Java Client using Maven

set -e

echo "Compiling TGLog Pure Java Client with Maven..."
echo "=============================================="
echo ""

# Clean and compile (including test sources)
mvn clean test-compile

echo ""
echo "Compilation successful!"
echo "Output directories:"
echo "  - Main classes:         target/classes"
echo "  - Test classes:         target/test-classes"
echo "  - Integration tests:    target/integration-test-classes"
echo "  - Examples:             target/examples-classes"
echo ""
echo "To run the simple example:"
echo "  ./run-example.sh"
echo ""
echo "To run the performance test:"
echo "  ./run-test.sh"
echo ""
echo "To package as JAR:"
echo "  mvn package"
