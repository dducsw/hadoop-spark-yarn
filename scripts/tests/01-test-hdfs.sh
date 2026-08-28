#!/usr/bin/env bash
set -e

echo "=== [TEST 1] Verifying HDFS Read/Write on Distributed Cluster ==="

TEST_FILE="/tmp/hdfs_test_$(date +%s).txt"
echo "Data Platform Smoke Test on HDFS - $(date)" > $TEST_FILE

echo "1. Uploading test file to hdfs://master:9000/tmp/..."
hdfs dfs -put -f $TEST_FILE /tmp/hdfs_smoke_test.txt

echo "2. Verifying file in HDFS:"
hdfs dfs -ls /tmp/hdfs_smoke_test.txt

echo "3. Reading file back from HDFS:"
hdfs dfs -cat /tmp/hdfs_smoke_test.txt

echo "4. Removing temporary test file:"
hdfs dfs -rm -skipTrash /tmp/hdfs_smoke_test.txt
rm -f $TEST_FILE

echo ">>> [TEST 1 SUCCESS] HDFS Storage Read/Write verified!"
