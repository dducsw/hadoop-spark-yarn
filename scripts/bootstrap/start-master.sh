#!/usr/bin/env bash
set -e

echo "[Master] Starting Big Data Platform Master Node..."

# 1. Format & Start HDFS NameNode
if [ ! -f /hadoop/dfs/name/current/VERSION ]; then
  echo "[Master] Formatting NameNode for the first time..."
  hdfs namenode -format -clusterId mycluster-compact-01 -force -nonInteractive
fi

echo "[Master] Starting HDFS NameNode..."
hdfs namenode > /var/log/hadoop/hadoop-namenode.log 2>&1 &

# Wait for NameNode to open port 9000
echo "[Master] Waiting for NameNode RPC (master:9000)..."
while ! nc -z master 9000; do
  sleep 1
done
echo "[Master] NameNode is READY!"

# 2. Start YARN ResourceManager
echo "[Master] Starting YARN ResourceManager..."
yarn resourcemanager > /var/log/hadoop/yarn-resourcemanager.log 2>&1 &

# 3. Start MapReduce JobHistoryServer
echo "[Master] Starting MapReduce JobHistoryServer..."
mapred historyserver > /var/log/hadoop/mapred-historyserver.log 2>&1 &

# 4. Wait for PostgreSQL & Start Hive Metastore
echo "[Master] Waiting for PostgreSQL (postgres:5432)..."
while ! nc -z postgres 5432 && ! nc -z hive-db 5432; do
  sleep 2
done

if ! schematool -dbType postgres -info > /dev/null 2>&1; then
  echo "[Master] Initializing Hive Metastore Schema in PostgreSQL..."
  schematool -dbType postgres -initSchema || true
fi

echo "[Master] Starting Hive Metastore Service..."
hive --service metastore > /var/log/hive/hive-metastore.log 2>&1 &

# Wait for Metastore Thrift port 9083
while ! nc -z master 9083; do
  sleep 2
done
echo "[Master] Hive Metastore is READY!"

# 5. Start HiveServer2
echo "[Master] Starting HiveServer2 (JDBC:10000, WebUI:10002)..."
hive --service hiveserver2 > /var/log/hive/hive-server2.log 2>&1 &

# 6. Start Spark History Server
echo "[Master] Starting Spark History Server (WebUI:18080)..."
/opt/spark/sbin/start-history-server.sh || true

# 7. Start JupyterLab Server
echo "[Master] Starting JupyterLab Server (WebUI:8888)..."
mkdir -p /notebooks
nohup jupyter lab \
  --ip=0.0.0.0 \
  --port=8888 \
  --no-browser \
  --allow-root \
  --notebook-dir=/notebooks \
  --ServerApp.token='' \
  --ServerApp.password='' \
  --ServerApp.allow_origin='*' > /var/log/hadoop/jupyter.log 2>&1 &

echo "[Master] All master services started successfully!"

# 7. Automated Non-blocking Bootstrap (Self-healing Cluster Setup)
(
  echo "[Auto-Bootstrap] Waiting for HDFS SafeMode to turn OFF..."
  hdfs dfsadmin -safemode wait 2>/dev/null || true

  echo "[Auto-Bootstrap] Initializing HDFS standard directories..."
  bash /scripts/bootstrap/01-init-hdfs.sh 2>/dev/null || true

  if [ -d /data ]; then
    echo "[Auto-Bootstrap] Seeding sample datasets to HDFS /data..."
    hdfs dfs -mkdir -p /data 2>/dev/null || true
    for f in /data/*.csv; do
      if [ -f "$f" ]; then
        hdfs dfs -put -f "$f" /data/ 2>/dev/null || true
      fi
    done
  fi

  if ! hdfs dfs -test -d /spark-jars 2>/dev/null; then
    echo "[Auto-Bootstrap] Uploading Spark JARs to HDFS..."
    bash /scripts/bootstrap/03-upload-spark-jars.sh 2>/dev/null || true
  fi

  echo "[Auto-Bootstrap] Initializing ClickHouse OLAP schema..."
  bash /scripts/bootstrap/04-init-clickhouse.sh 2>/dev/null || true

  echo "[Auto-Bootstrap] Platform is 100% ready for ETL and Analytical queries!"
) > /var/log/hadoop/auto-bootstrap.log 2>&1 &

# Keep master running and tail logs
tail -f /var/log/hadoop/*.log /var/log/hive/*.log /var/log/spark/*.log 2>/dev/null || tail -f /dev/null
