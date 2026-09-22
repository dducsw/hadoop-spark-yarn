.PHONY: help build up down restart ps logs bootstrap test status master clean gen-secrets test-unit init-env

# Docker Compose v2 (no hyphen) — v1 deprecated
DC := docker compose

help:
	@echo "Big Data Platform CLI (Hadoop + YARN + Spark + Hive + ClickHouse + ZooKeeper)"
	@echo "Available commands:"
	@echo "  make init-env    - Initialize .env file from .env.example"
	@echo "  make build       - Build Unified Base Docker Image"
	@echo "  make up          - Start all cluster containers in background"
	@echo "  make down        - Stop cluster containers"
	@echo "  make restart     - Restart all cluster services"
	@echo "  make ps          - List running containers and statuses"
	@echo "  make logs        - Tail output logs from all cluster containers"
	@echo "  make bootstrap   - Initialize HDFS, Spark JARs, Hive DB & ClickHouse"
	@echo "  make status      - Run health checks across all platform components"
	@echo "  make test        - Run end-to-end 6-layer smoke test suite"
	@echo "  make test-unit   - Run unit tests locally (no cluster required)"
	@echo "  make gen-secrets - Generate Airflow Fernet & Secret keys for .env"
	@echo "  make master      - Open interactive bash shell inside Master node"
	@echo "  make clean       - Stop containers and purge all persistent volumes"

init-env:
	@test -f .env || cp .env.example .env
	@mkdir -p config/ssh
	@test -f config/ssh/id_rsa || ssh-keygen -t rsa -b 2048 -N "" -f config/ssh/id_rsa -C "cluster-internal"
	@echo ".env and cluster SSH keys are ready."

build:
	$(DC) build

up:
	$(DC) up -d

down:
	$(DC) down

restart: down up

ps:
	$(DC) ps

logs:
	$(DC) logs -f

bootstrap:
	@echo "Running Bootstrap Pipeline..."
	docker exec -it master bash /scripts/bootstrap/01-init-hdfs.sh
	docker exec -it master bash /scripts/bootstrap/03-upload-spark-jars.sh
	docker exec -it master bash /scripts/bootstrap/04-init-clickhouse.sh

status:
	docker exec -it master bash /scripts/ops/cluster-status.sh

test:
	@echo "Running Smoke Tests on Master & Airflow..."
	docker exec -it master bash /scripts/tests/01-test-hdfs.sh
	docker exec -it master bash /scripts/tests/02-test-yarn-mr.sh
	docker exec -it master spark-submit --master yarn /scripts/tests/03-test-spark-yarn.py
	docker exec -it master spark-submit --master yarn /scripts/tests/04-test-hive-spark.py
	docker exec -it master spark-submit --master yarn /pipeline/examples/spark_to_clickhouse_etl.py
	docker exec -it master bash /scripts/tests/05-test-clickhouse.sh
	docker exec -it airflow-scheduler bash /opt/airflow/scripts/tests/06-test-airflow.sh

test-unit:
	@echo "Running unit tests (no cluster required)..."
	cd pipeline && python -m pytest test/ -v --tb=short

gen-secrets:
	@echo "=== Copy these into your .env file ==="
	@python -c "from cryptography.fernet import Fernet; print('AIRFLOW_FERNET_KEY=' + Fernet.generate_key().decode())"
	@python -c "import secrets; print('AIRFLOW_SECRET_KEY=' + secrets.token_urlsafe(32))"

master:
	docker exec -it master bash

clean:
	$(DC) down -v
