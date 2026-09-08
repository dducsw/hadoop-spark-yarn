# Apache Airflow 3 Platform Architecture

## 1. Overview

Apache Airflow 3 (version `3.2.1`) serves as the workflow orchestration and scheduling engine for the Big Data platform. It automates, coordinates, and monitors the end-to-end Medallion pipeline from Raw CSV/RDBMS landing to ClickHouse OLAP serving.

- **Orchestration Model**: TaskFlow API + classic operators, fine-grained DAG dependencies, and explicit lineage.
- **Decoupled Architecture**: Airflow 3 decouples the scheduler, user DAG parsing, and web UI/API server into distinct lightweight microservices.
- **Compute Offloading**: Airflow does not execute heavy Spark compute internally. It dispatches jobs to the YARN ResourceManager via `submit_spark_job.sh` and monitors task execution.

---

## 2. Container Topology & Roles

The Airflow cluster consists of 4 specialized containers connected via `bigdata-net`:

| Container | Host Port | Internal Port | Responsibility |
|---|---|---|---|
| **`airflow-webserver`** | `8085` | `8080` | FastAPI Execution API Server (`/execution/*`) and modern React Web UI |
| **`airflow-scheduler`** | - | - | Heartbeat management, dependency evaluation, LocalExecutor process supervisor |
| **`airflow-dag-processor`** | - | - | Dedicated background daemon for parsing DAG bundles from `pipeline/dags/` |
| **`postgres`** (`airflow-db`) | `5433` | `5432` | Shared PostgreSQL metadata store (`airflow` database) |

---

## 3. Web UI & Configuration

- **Web UI URL**: `http://localhost:8085`
- **Default Credentials**: `admin` / `admin`
- **Configuration Directory**: `config/airflow/` (`airflow.cfg`)
- **DAG Directory**: `pipeline/dags/` (mounted into containers at `/opt/airflow/dags`)
- **Logs Volume**: `airflow_logs` (mounted at `/opt/airflow/logs`)

### Key Environment Settings:
```yaml
AIRFLOW__CORE__EXECUTOR: LocalExecutor
AIRFLOW__CORE__EXECUTION_API_SERVER_URL: 'http://airflow-webserver:8080/execution/'
AIRFLOW__API__SECRET_KEY: 'tj3TRHkFNkiP/iGlq5lmxg=='
AIRFLOW__API_AUTH__JWT_SECRET: 'tj3TRHkFNkiP/iGlq5lmxg=='
AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: 'true'
AIRFLOW__CORE__LOAD_EXAMPLES: 'false'
```

---

## 4. Spark on YARN Integration Pattern

Airflow tasks use the standardized helper `build_spark_task()` to dispatch work to Spark:

```python
def build_spark_task(task_id: str, layer: str, script: str) -> BashOperator:
    return BashOperator(
        task_id=task_id,
        bash_command=f"bash /opt/airflow/scripts/ops/submit_spark_job.sh {layer} {script} '{{{{ run_id }}}}' ",
        pool="spark_yarn_pool",
        env={"BATCH_ID": "{{ run_id }}"},
    )
```

1. **Resource Protection (`spark_yarn_pool`)**: Tasks are assigned to a concurrency pool to avoid overloading the 2-worker YARN cluster.
2. **Deterministic Audit Lineage**: Injects `{{ run_id }}` as `BATCH_ID` into Spark and PostgreSQL audit tables (`pipeline_audit_log`, `pipeline_watermark`).
3. **Fail-Fast Connectivity Gate**: Pre-flight cluster health checks probe NameNode (`9870`), YARN RM (`8088`), and ClickHouse (`8123`) before dispatching batch tasks.

---

## 5. Useful Commands & Health Checks

```bash
# Check container statuses
docker ps --filter "name=airflow"

# Trigger production pipeline manually via CLI
docker exec airflow-scheduler airflow dags trigger risk_data_pipeline

# List active DAGs
docker exec airflow-scheduler airflow dags list

# Test individual task execution locally
docker exec airflow-scheduler airflow tasks test risk_data_pipeline cluster_healthcheck 2026-09-08

# Tail scheduler logs
docker logs -f airflow-scheduler

# Run Airflow verification smoke test
docker exec -it airflow-scheduler bash /opt/airflow/scripts/tests/06-test-airflow.sh
```

---

## 6. Related Documentation

- [Detailed DAG Orchestration Guide (Airflow 3)](../ORCHESTRATION.md)
- [Pipeline Architecture & Layer Design](../PIPELINE.md)
- [Spark Engine & Pipeline Optimizations](../optimization/spark-pipeline-optimization.md)
