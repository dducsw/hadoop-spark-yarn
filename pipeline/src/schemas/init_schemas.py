#!/usr/bin/env python3
"""CLI utility to create all databases and Spark SQL Hive tables across all layers."""
import argparse
import os
import sys

SRC_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.extend([SRC_DIR])

from src.common.logger import get_logger
from src.common.spark_session import get_spark_session
from src.schemas.curated_schemas import (
    CURATED_ALL_DDLS,
    CURATED_DB_NAME,
)
from src.schemas.raw_schemas import RAW_ALL_DDLS, RAW_DB_NAME
from src.schemas.stage_schemas import STAGE_ALL_DDLS, STAGE_DB_NAME

logger = get_logger("InitSchemas")


def init_all_schemas(layer: str = "all") -> None:
    """Executes Spark SQL DDL statements for specified layer or all layers."""
    spark = get_spark_session("InitSchemas")

    databases_and_ddls = []

    if layer in ("all", "raw"):
        databases_and_ddls.append((RAW_DB_NAME, RAW_ALL_DDLS))
    if layer in ("all", "stage"):
        databases_and_ddls.append((STAGE_DB_NAME, STAGE_ALL_DDLS))
    if layer in ("all", "curated"):
        databases_and_ddls.append((CURATED_DB_NAME, CURATED_ALL_DDLS))

    try:
        for db_name, ddls in databases_and_ddls:
            logger.info(f"Creating database if not exists: {db_name}")
            spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")

            for ddl in ddls:
                # Extract table name from DDL for clean logging
                first_line = [line.strip() for line in ddl.strip().split("\n") if line.strip()][0]
                logger.info(f"Executing: {first_line}")
                spark.sql(ddl)

        logger.info("All requested Hive schemas initialized successfully!")
    finally:
        spark.stop()


def main():
    parser = argparse.ArgumentParser(description="Initialize Hive Lakehouse DDL schemas via Spark SQL")
    parser.add_argument(
        "--layer",
        type=str,
        choices=["all", "raw", "stage", "curated"],
        default="all",
        help="Target Lakehouse layer to initialize",
    )
    args = parser.parse_args()
    init_all_schemas(layer=args.layer)


if __name__ == "__main__":
    main()
