# Apache Hadoop (HDFS) Component Architecture

## 1. Overview
Apache Hadoop provides the foundational distributed storage layer (Hadoop Distributed File System - HDFS) for the Big Data Platform.

- **NameNode**: Manages filesystem namespace, metadata, directory tree, and block locations.
- **DataNode**: Stores physical data blocks (default block size: 128MB, replication factor: 2 across workers).

## 2. Configuration & Paths
- **Config directory**: `config/hadoop/` (`core-site.xml`, `hdfs-site.xml`, `hadoop-env.sh`)
- **Default Filesystem URI**: `hdfs://master:9000`
- **Web UI**: `http://localhost:9870`

## 3. Useful Commands
```bash
# Cluster health and storage utilization
hdfs dfsadmin -report

# Manage SafeMode
hdfs dfsadmin -safemode get
hdfs dfsadmin -safemode leave

# Filesystem operations
hdfs dfs -ls /
hdfs dfs -mkdir -p /data
hdfs dfs -put -f /data/sales_data.csv /data/
```
