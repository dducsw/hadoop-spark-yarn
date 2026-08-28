#!/usr/bin/env bash
set -e

echo "=== [1/4] Initializing Standard HDFS Directories ==="

# Wait for HDFS to exit SafeMode if active
echo "Checking HDFS SafeMode status..."
hdfs dfsadmin -safemode wait || true

# Create /tmp directory
echo "Creating /tmp..."
hdfs dfs -mkdir -p /tmp
hdfs dfs -chmod -R 1777 /tmp

# Create YARN log aggregation directory
echo "Creating /yarn-logs..."
hdfs dfs -mkdir -p /yarn-logs
hdfs dfs -chmod -R 1777 /yarn-logs

# Create MapReduce history directories
echo "Creating /mr-history..."
hdfs dfs -mkdir -p /mr-history/done_intermediate /mr-history/done
hdfs dfs -chmod -R 1777 /mr-history

# Create Spark event log directory
echo "Creating /spark-logs..."
hdfs dfs -mkdir -p /spark-logs
hdfs dfs -chmod -R 1777 /spark-logs

# Create Hive Warehouse directory
echo "Creating /user/hive/warehouse..."
hdfs dfs -mkdir -p /user/hive/warehouse
hdfs dfs -chmod -R 1777 /user/hive/warehouse

echo "HDFS directory tree ready:"
hdfs dfs -ls /
hdfs dfs -ls /user/hive
