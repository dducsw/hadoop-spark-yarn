# Apache Hive 3 Architecture

## 1. Overview
Apache Hive acts as the distributed Data Lakehouse catalog and SQL execution layer.

- **Hive Metastore Service (HMS)**: Central Thrift service serving table definitions, partitions, and schemas to both Hive and Spark SQL.
- **Metastore Database**: PostgreSQL 15 persistent relational storage for schema metadata.
- **HiveServer2 (HS2)**: Multi-client JDBC/ODBC server allowing BI tools (DBeaver, Tableau, PowerBI) and Beeline CLI to execute queries.

## 2. Configuration & Ports
- **Config directory**: `config/hive/` (`hive-site.xml`)
- **Thrift Metastore Port**: `9083`
- **Hive JDBC/ODBC Port**: `10000`
- **HiveServer2 Web UI**: `http://localhost:10002`

## 3. Useful Commands
```bash
# Connect to Hive via Beeline CLI
beeline -u "jdbc:hive2://master:10000/default" -n root

# Check metastore schema status
schematool -dbType postgres -info
```
