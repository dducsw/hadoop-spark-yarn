# Big Data Platform Quick Reference Guide

This guide provides an overview of the core commands, environment variables, configuration parameters, and management utilities used in this platform.

---

## 1. Core Service CLI Commands

### HDFS Storage Management
```bash
# Check filesystem status & storage capacity
hdfs dfsadmin -report

# Manage SafeMode state
hdfs dfsadmin -safemode get
hdfs dfsadmin -safemode leave

# Filesystem operations
hdfs dfs -ls /
hdfs dfs -mkdir -p /data
hdfs dfs -put -f /data/sales_data.csv /data/
hdfs dfs -cat /data/sales_data.csv
```

### YARN Workload & Resource Management
```bash
# List all active NodeManagers
yarn node -list -all

# List running and completed applications
yarn application -list -appStates ALL

# Kill a running application
yarn application -kill <ApplicationId>

# Fetch aggregated container application logs
yarn logs -applicationId <ApplicationId>
```

### Apache Hive SQL & Metadata
```bash
# Connect to HiveServer2 via Beeline CLI
beeline -u "jdbc:hive2://localhost:10000/default" -n root

# Check Metastore schema integrity
schematool -dbType postgres -info
```

### ClickHouse OLAP Operations
```bash
# Execute query via HTTP API
curl -s "http://localhost:8123/" --data-binary "SHOW DATABASES;"

# Interactive Web Query Client
# Open browser at: http://localhost:8123/play
```

---

## 2. Default Configuration Parameter Reference

| Component | Property | Value | Description |
|---|---|---|---|
| **HDFS** | `fs.defaultFS` | `hdfs://master:9000` | Primary NameNode URI |
| **HDFS** | `dfs.replication` | `2` | Block replication across workers |
| **YARN** | `yarn.resourcemanager.hostname` | `master` | Resource coordination master host |
| **YARN** | `yarn.nodemanager.resource.memory-mb`| `2048` | Max RAM allocated per worker node |
| **Spark** | `spark.master` | `yarn` | Spark cluster manager |
| **Spark** | `spark.eventLog.dir` | `hdfs://master:9000/spark-logs` | HDFS location for event history |
| **Hive** | `hive.metastore.uris` | `thrift://master:9083` | Thrift Metastore endpoint |
| **Hive** | `hive.metastore.warehouse.dir` | `/user/hive/warehouse` | HDFS data warehouse root directory |
| **ClickHouse**| `HTTP Port` | `8123` | REST API and Web Query Client |
