# Big Data Platform Troubleshooting Guide

Common operational issues, diagnostic commands, and remediation strategies.

---

## 1. HDFS Storage & NameNode Issues

### Issue 1: NameNode stuck in SafeMode (`SafeMode: ON`)
- **Root Cause**: Replication threshold has not yet been satisfied during cluster startup.
- **Remediation**:
  ```bash
  docker exec -it master hdfs dfsadmin -safemode leave
  ```

### Issue 2: DataNodes not reporting to NameNode
- **Root Cause**: Network delay or hostname resolution failure between workers and master.
- **Diagnostic & Remediation**:
  ```bash
  # Check live DataNodes report
  docker exec -it master hdfs dfsadmin -report
  # Inspect worker logs
  docker logs worker1
  docker logs worker2
  ```

---

## 2. YARN & Resource Management Issues

### Issue 1: Spark application stuck in `ACCEPTED` state
- **Root Cause**: NodeManagers have insufficient allocated memory or vCores to spawn the ApplicationMaster container.
- **Remediation**:
  1. Verify running NodeManagers:
     ```bash
     docker exec -it master yarn node -list
     ```
  2. Reduce resource requests when submitting the job:
     ```bash
     spark-submit --master yarn --driver-memory 512m --executor-memory 512m ...
     ```

### Issue 2: Container killed due to Virtual Memory limits
- **Root Cause**: Docker and WSL2 kernel virtual memory accounting triggers false positives.
- **Remediation**: Handled automatically in `yarn-site.xml` by setting `yarn.nodemanager.vmem-check-enabled=false`.

---

## 3. Apache Hive & Spark SQL Interoperability

### Issue 1: `NoSuchMethodError: com.google.common.base.Preconditions.checkArgument`
- **Root Cause**: Guava version mismatch between Hive 3.1 and Hadoop 3.4.
- **Remediation**: Fixed automatically in `docker/base/Dockerfile` by replacing Hive's legacy `guava-19.0.jar` with Hadoop's modern Guava library.

### Issue 2: Hive Metastore cannot connect to PostgreSQL
- **Root Cause**: `hive-db` container initializing slower than the metastore thrift service.
- **Remediation**:
  ```bash
  # Verify database container health
  docker ps --filter "name=hive-db"
  # Test direct network connectivity from master
  docker exec -it master nc -zv hive-db 5432
  ```

---

## 4. ClickHouse OLAP Issues

### Issue 1: Connection refused on port 8123
- **Root Cause**: ClickHouse server initializing or container not running.
- **Remediation**:
  ```bash
  docker logs clickhouse
  curl -s "http://localhost:8123/ping"
  ```
