.PHONY: help build up down restart ps logs bootstrap test status master clean

help:
	@echo "Big Data Platform CLI (Hadoop + YARN + Spark + Hive + ClickHouse + ZooKeeper)"
	@echo "Available commands:"
	@echo "  make build       - Build Unified Base Docker Image"
	@echo "  make up          - Start all 6 cluster containers in background"
	@echo "  make down        - Stop cluster containers"
	@echo "  make restart     - Restart all cluster services"
	@echo "  make ps          - List running containers and statuses"
	@echo "  make logs        - Tail output logs from all cluster containers"
	@echo "  make bootstrap   - Initialize HDFS, Spark JARs, Hive DB & ClickHouse"
	@echo "  make status      - Run health checks across all platform components"
	@echo "  make test        - Run end-to-end 5-layer smoke test suite"
	@echo "  make master      - Open interactive bash shell inside Master node"
	@echo "  make clean       - Stop containers and purge all persistent volumes"

build:
	docker-compose build

up:
	docker-compose up -d

down:
	docker-compose down

restart: down up

ps:
	docker-compose ps

logs:
	docker-compose logs -f

bootstrap:
	@echo "Running Bootstrap Pipeline..."
	docker exec -it master bash /scripts/bootstrap/01-init-hdfs.sh
	docker exec -it master bash /scripts/bootstrap/03-upload-spark-jars.sh
	docker exec -it master bash /scripts/bootstrap/04-init-clickhouse.sh

status:
	docker exec -it master bash /scripts/ops/cluster-status.sh

test:
	@echo "Running Smoke Tests on Master..."
	docker exec -it master bash /scripts/tests/01-test-hdfs.sh
	docker exec -it master bash /scripts/tests/02-test-yarn-mr.sh
	docker exec -it master spark-submit --master yarn /scripts/tests/03-test-spark-yarn.py
	docker exec -it master spark-submit --master yarn /scripts/tests/04-test-hive-spark.py
	docker exec -it master spark-submit --master yarn /jobs/spark_to_clickhouse_etl.py
	docker exec -it master bash /scripts/tests/05-test-clickhouse.sh

master:
	docker exec -it master bash

clean:
	docker-compose down -v
