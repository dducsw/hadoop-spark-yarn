#!/usr/bin/env bash
set -e

echo "=== [TEST 2] Running Distributed MapReduce Pi Job on YARN ==="

EXAMPLES_JAR=$(find /opt/hadoop/share/hadoop/mapreduce -maxdepth 1 -name "hadoop-mapreduce-examples-*.jar" | head -n 1)

if [ -z "$EXAMPLES_JAR" ]; then
  echo "Error: hadoop-mapreduce-examples JAR not found!"
  exit 1
fi

echo "Submitting MapReduce Pi estimation job (2 maps, 10 samples)..."
yarn jar "$EXAMPLES_JAR" pi 2 10

echo ">>> [TEST 2 SUCCESS] MapReduce on YARN executed successfully!"
