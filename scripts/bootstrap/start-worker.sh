#!/usr/bin/env bash
set -e

echo "[Worker] Starting Big Data Platform Worker Node..."

# Wait for master NameNode & ResourceManager
echo "[Worker] Waiting for Master NameNode (master:9000)..."
while ! nc -z master 9000; do
  sleep 2
done

echo "[Worker] Waiting for Master ResourceManager (master:8032)..."
while ! nc -z master 8032; do
  sleep 2
done

# 1. Start HDFS DataNode
echo "[Worker] Starting HDFS DataNode..."
hdfs datanode > /var/log/hadoop/hadoop-datanode.log 2>&1 &

# 2. Setup Spark Shuffle & Start YARN NodeManager
cp /opt/spark/yarn/spark-*-yarn-shuffle.jar /opt/hadoop/share/hadoop/yarn/lib/ 2>/dev/null || true
echo "[Worker] Starting YARN NodeManager..."
yarn nodemanager > /var/log/hadoop/yarn-nodemanager.log 2>&1 &

echo "[Worker] All worker services started successfully!"

# Keep worker running and tail logs
tail -f /var/log/hadoop/*.log 2>/dev/null || tail -f /dev/null
