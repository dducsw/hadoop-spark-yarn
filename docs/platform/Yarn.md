# Apache YARN & MapReduce Architecture

## 1. Overview
Apache Hadoop YARN (Yet Another Resource Negotiator) manages compute cluster resources and dynamically allocates containers for applications (Spark, MapReduce).

- **ResourceManager (RM)**: Arbitrates resources among all competing applications.
- **NodeManager (NM)**: Per-node agent monitoring container resource usage (CPU, memory) and reporting to RM.
- **JobHistoryServer**: Archives completed MapReduce application metrics and logs.

## 2. Configuration & Ports
- **Config directory**: `config/yarn/` (`yarn-site.xml`, `mapred-site.xml`, `capacity-scheduler.xml`)
- **ResourceManager Web UI**: `http://localhost:8088` (RPC Port: 8032)
- **JobHistoryServer Web UI**: `http://localhost:19888` (RPC Port: 10020)

## 3. Useful Commands
```bash
# List all active NodeManagers
yarn node -list -all

# List running and finished applications
yarn application -list -appStates ALL

# Inspect application container logs
yarn logs -applicationId <ApplicationId>
```
