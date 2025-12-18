#!/bin/bash

# Compile TGLog Pure Java Client using Maven

set -e

echo "Compiling TGLog Pure Java Client with Maven..."
echo "=============================================="
echo ""

# Clean and compile
mvn clean compile

echo ""
echo "Compilation successful!"
echo "Output directory: target/classes"
echo ""
echo "To run the simple example:"
echo "  ./run-example.sh"
echo ""
echo "To run the performance test:"
echo "  ./run-test.sh"
echo ""
echo "To package as JAR:"
echo "  mvn package"
