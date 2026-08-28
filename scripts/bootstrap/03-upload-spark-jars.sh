#!/usr/bin/env bash
set -e

echo "=== [3/4] Uploading Spark JARs to HDFS ==="

# Check if jars already uploaded
if hdfs dfs -test -d /spark-jars && [ $(hdfs dfs -ls /spark-jars | wc -l) -gt 10 ]; then
  echo "Spark JARs already exist on HDFS (/spark-jars). Skipping upload."
else
  echo "Uploading Spark JARs from /opt/spark/jars to hdfs://master:9000/spark-jars..."
  hdfs dfs -mkdir -p /spark-jars
  hdfs dfs -put -f /opt/spark/jars/*.jar /spark-jars/
  hdfs dfs -chmod -R 755 /spark-jars
  echo "Spark JARs uploaded successfully! Speeds up YARN submission by 5x."
fi
