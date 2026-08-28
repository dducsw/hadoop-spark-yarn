#!/usr/bin/env bash

echo "Big Data Platform Cluster Health Check"

echo -e "\n1. [ZooKeeper Coordinator]"
echo -n "   - zookeeper: "
nc -z zookeeper 2181 && echo "READY (Port 2181 open)" || echo "NOT READY"

echo -e "\n2. [HDFS NameNode & Live DataNodes]"
hdfs dfsadmin -report 2>/dev/null | grep -E "Live datanodes|Configured Capacity|DFS Used%|Present Capacity" || echo "HDFS not reachable"

echo -e "\n3. [YARN ResourceManager & NodeManagers]"
yarn node -list -all 2>/dev/null || echo "YARN RM not reachable"

echo -e "\n4. [Hive Metastore & HiveServer2]"
echo -n "   - Hive Metastore (master:9083): "
nc -z master 9083 && echo "READY" || echo "NOT READY"
echo -n "   - HiveServer2 (master:10000): "
nc -z master 10000 && echo "READY" || echo "NOT READY"

echo -e "\n5. [Spark History Server]"
echo -n "   - Spark History (master:18080): "
nc -z master 18080 && echo "READY" || echo "NOT READY"

echo -e "\n6. [ClickHouse OLAP Engine]"
echo -n "   - ClickHouse HTTP (clickhouse:8123): "
nc -z clickhouse 8123 && echo "READY" || echo "NOT READY"


