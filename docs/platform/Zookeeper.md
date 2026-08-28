# Apache ZooKeeper Coordinator Architecture

## 1. Overview
Apache ZooKeeper is a centralized service for maintaining configuration information, naming, providing distributed synchronization, and providing group services.

- **Role**: Cluster coordination, leader election, lock management, and service discovery across the Big Data platform.

## 2. Ports & Access
- **Client Port**: `2181`
- **Config directory**: `config/zookeeper/` (`zoo.cfg`)

## 3. Useful Commands
```bash
# Check ZooKeeper node status
nc -z zookeeper 2181 && echo "ZooKeeper is reachable"
```
