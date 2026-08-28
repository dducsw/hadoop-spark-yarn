# HDFS High Availability (HA) Architecture Reference

This document serves as an advanced architectural reference for multi-node HDFS High Availability deployments.

---

## 1. Principles of HDFS HA

In a high-availability deployment, the HDFS cluster eliminates the NameNode single point of failure (SPOF) through active/standby redundancy:

1. **Active NameNode**: Serves all client read/write operations and mutates metadata.
2. **Standby NameNode**: Maintains a synchronized copy of namespace state by continuously reading edits from JournalNodes.
3. **JournalNode Quorum (3+ nodes)**: Distributed, highly available consensus group (Paxos-like) storing edit logs. A write must succeed on a quorum (e.g., 2 out of 3) of nodes.
4. **ZooKeeper Failover Controller (ZKFC)**: Health-monitoring daemon on each NameNode. ZKFC monitors NameNode liveness and coordinates automatic leader election via ZooKeeper locks.

```
                   +------------------------+
                   |    Client Requests     |
                   +-----------+------------+
                               |
               +---------------+---------------+
               |                               |
               v                               v
      +-----------------+             +-----------------+
      | Active NameNode |             | Standby NameNode|
      |   (namenode1)   |             |   (namenode2)   |
      +--------+--------+             +--------+--------+
               |                               ^
               | 1. Writes EditLogs            | 2. Tails EditLogs
               v                               |
      +-------------------------------------------------+
      |        JournalNode Quorum (jn1, jn2, jn3)       |
      +-------------------------------------------------+
               ^                               ^
               | Active Election & Locks       |
      +--------+-------------------------------+--------+
      |        ZooKeeper Ensemble (zk1, zk2, zk3)       |
      +-------------------------------------------------+
```

---

## 2. Bootstrapping an HA Cluster (Reference Steps)

### Step 1: Initialize JournalNodes
```bash
hdfs journalnode &
```

### Step 2: Format Active NameNode
```bash
hdfs namenode -format -clusterId <ClusterID> -nonInteractive
```

### Step 3: Format ZooKeeper Metadata for ZKFC
```bash
hdfs zkfc -formatZK -force
```

### Step 4: Bootstrap Standby NameNode from Active
```bash
hdfs namenode -bootstrapStandby -nonInteractive
```

### Step 5: Start ZKFC Daemons
```bash
hdfs zkfc &
```
