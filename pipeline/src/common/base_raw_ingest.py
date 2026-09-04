"""
Base Raw Ingestion Job template.
Supports both CSV files and Database (RDBMS/PostgreSQL via JDBC).
Supports Full Load and Incremental Load (Watermarking).
Allows concise connection string (URI or JDBC).
"""
import os, sys
from datetime import datetime, timezone
from enum import Enum
from typing import Optional
from urllib.parse import parse_qs, urlparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
SRC_DIR = os.path.abspath(os.path.join(CURRENT_DIR, ".."))
PIPELINE_DIR = os.path.abspath(os.path.join(SRC_DIR, ".."))
sys.path.extend([PIPELINE_DIR, SRC_DIR])

from src.common.audit import log_pipeline_execution
from src.common.base_spark_job import BaseSparkJob, WriteMode
from src.common.spark_session import get_spark_session
from src.common.watermark import get_watermark, update_watermark


class SourceType(str, Enum):
    CSV = "csv"
    DB = "db"


class LoadType(str, Enum):
    FULL = "full"
    INCREMENTAL = "incremental"


def parse_connection_string(conn_str: Optional[str]) -> dict:
    """
    Parses connection strings into Spark JDBC options.
    Supports:
      - postgresql://user:password@host:port/database
      - jdbc:postgresql://host:port/database?user=...&password=...
      - postgres:5432/database
    """
    default_cfg = {
        "url": "jdbc:postgresql://postgres:5432/source_crm",
        "user": "hive",
        "password": "hivepassword",
        "driver": "org.postgresql.Driver",
    }
    if not conn_str:
        return default_cfg

    if conn_str.startswith("jdbc:"):
        parts = conn_str[5:]
        parsed = urlparse(parts)
        params = parse_qs(parsed.query)
        user = params.get("user", [default_cfg["user"]])[0]
        password = params.get("password", [default_cfg["password"]])[0]
        clean_url = f"jdbc:{parsed.scheme}://{parsed.netloc}{parsed.path}"
        driver = "org.postgresql.Driver" if "postgres" in parsed.scheme else default_cfg["driver"]
        return {"url": clean_url, "user": user, "password": password, "driver": driver}

    parsed = urlparse(conn_str)
    if parsed.scheme in ["postgresql", "postgres"]:
        host_port = parsed.netloc.split("@")[-1] if "@" in parsed.netloc else parsed.netloc
        user = parsed.username or default_cfg["user"]
        password = parsed.password or default_cfg["password"]
        dbname = parsed.path.lstrip("/") or "source_crm"
        return {
            "url": f"jdbc:postgresql://{host_port}/{dbname}",
            "user": user,
            "password": password,
            "driver": "org.postgresql.Driver",
        }

    return {
        "url": f"jdbc:postgresql://{conn_str}",
        "user": default_cfg["user"],
        "password": default_cfg["password"],
        "driver": default_cfg["driver"],
    }


class BaseRawIngestJob(BaseSparkJob):
    def __init__(
        self,
        table_name: str,
        source_type: SourceType = SourceType.DB,
        connection_string: Optional[str] = None,
        file_name: Optional[str] = None,
        source_db_table: Optional[str] = None,
        primary_key: Optional[str] = None,
        load_type: LoadType = LoadType.FULL,
        watermark_col: str = "updated_at",
        schema: Optional[StructType] = None,
        input_base_dir: str = "/data/home-credit-default-risk",
        output_base_dir: str = "/raw/credit_risk",
        hive_db: str = "raw_credit_risk",
    ):
        self.source_type = source_type
        self.load_type = load_type
        self.watermark_col = watermark_col
        self.source_db_table = source_db_table or table_name
        self.file_name = file_name or f"{table_name}.csv"
        self.input_base_dir = input_base_dir
        self.csv_path = os.path.join(input_base_dir, self.file_name)

        # Database / JDBC resolution
        self.db_params = parse_connection_string(connection_string)

        output_path = os.path.join(output_base_dir, table_name)
        target_table = f"{hive_db}.raw_{table_name}"

        source_label = (
            f"{self.db_params['url']}/{self.source_db_table}"
            if self.source_type == SourceType.DB
            else self.csv_path
        )
        source_system = "postgres_crm" if self.source_type == SourceType.DB else "csv_source"

        super().__init__(
            pipeline_layer="raw",
            table_name=table_name,
            source_table=self.source_db_table if self.source_type == SourceType.DB else self.file_name,
            target_table=target_table,
            source_path=source_label,
            target_path=output_path,
            primary_key=primary_key,
            write_mode=WriteMode.OVERWRITE if load_type == LoadType.FULL else WriteMode.APPEND,
            source_system=source_system,
        )
        self.schema = schema

    def extract(self, spark: SparkSession) -> DataFrame:
        self.logger.info(
            f"Extracting [{self.table_name}] from {self.source_type.value.upper()} "
            f"(Strategy: {self.load_type.value.upper()})"
        )

        last_wm = get_watermark(spark, self.table_name) if self.load_type == LoadType.INCREMENTAL else None

        # A. Database Ingestion via Spark JDBC
        if self.source_type == SourceType.DB:
            if last_wm:
                self.logger.info(f"Incremental query: {self.watermark_col} > '{last_wm}'")
                dbtable_expr = f"(SELECT * FROM {self.source_db_table} WHERE {self.watermark_col} > '{last_wm}') AS inc_data"
            else:
                self.logger.info(f"Initial/Full extraction from table '{self.source_db_table}'")
                dbtable_expr = self.source_db_table

            return (
                spark.read.format("jdbc")
                .option("url", self.db_params["url"])
                .option("dbtable", dbtable_expr)
                .option("user", self.db_params["user"])
                .option("password", self.db_params["password"])
                .option("driver", self.db_params["driver"])
                .option("fetchsize", "10000")
                .load()
            )

        # B. CSV Ingestion
        self.logger.info(f"Extracting CSV from {self.csv_path}")
        reader = (
            spark.read
            .option("header", "true")
            .option("nullValue", "")
            .option("nanValue", "")
        )
        df = reader.schema(self.schema).csv(self.csv_path) if self.schema else reader.option("inferSchema", "true").csv(self.csv_path)

        if last_wm and self.watermark_col in df.columns:
            self.logger.info(f"Filtering CSV incrementally with {self.watermark_col} > '{last_wm}'")
            df = df.filter(F.col(self.watermark_col) > last_wm)

        return df

    def transform(self, df: DataFrame) -> DataFrame:
        # Standardize column names to lowercase snake_case
        for col in df.columns:
            df = df.withColumnRenamed(col, col.lower())
        return self.add_audit_metadata(df)

    def load(self, spark: SparkSession, df: DataFrame) -> int:
        row_count = df.count()
        if row_count == 0:
            self.logger.warning(f"0 records extracted from source for {self.target_table}.")
            return 0

        first_run = False
        if self.load_type == LoadType.INCREMENTAL:
            last_wm = get_watermark(spark, self.table_name)
            if not last_wm:
                first_run = True

        write_mode = "overwrite" if (self.load_type == LoadType.FULL or first_run) else "append"
        self.logger.info(
            f"Writing to HDFS Parquet: {self.target_path} | Mode: {write_mode} | Records: {row_count:,}"
        )

        (
            df.write
            .mode(write_mode)
            .format("parquet")
            .save(self.target_path)
        )

        self._register_hive_table(spark)
        return row_count

    def run(self) -> None:
        """Execute Raw Ingest lifecycle with accurate watermark tracking."""
        spark = get_spark_session(f"RawIngest_{self.table_name}")
        start_time = datetime.now(timezone.utc)
        status, error_msg, row_count, col_count = "FAILED", None, 0, 0
        try:
            df_in = self.extract(spark)
            col_count = len(df_in.columns)
            self.validate(df_in)
            df_out = self.transform(df_in)
            row_count = self.load(spark, df_out)
            status = "SUCCESS"
            self.logger.info(f"SUCCESS: Ingested {row_count:,} rows ({col_count} cols) for {self.table_name}")

            # Watermark Management
            if self.load_type == LoadType.INCREMENTAL:
                if row_count > 0 and self.watermark_col in df_out.columns:
                    max_wm = df_out.select(F.max(self.watermark_col)).first()[0]
                    last_wm_val = str(max_wm) if max_wm is not None else start_time.strftime("%Y-%m-%d %H:%M:%S")
                else:
                    last_wm_val = get_watermark(spark, self.table_name) or start_time.strftime("%Y-%m-%d %H:%M:%S")
                update_watermark(
                    spark=spark,
                    table_name=self.table_name,
                    watermark_column=self.watermark_col,
                    last_watermark_value=last_wm_val,
                    status="SUCCESS",
                )
            else:
                update_watermark(
                    spark=spark,
                    table_name=self.table_name,
                    watermark_column="snapshot_timestamp",
                    last_watermark_value=start_time.strftime("%Y-%m-%d %H:%M:%S"),
                    status="SUCCESS",
                )
        except Exception as e:
            error_msg = str(e)
            self.logger.error(f"FAILED: Raw ingest error on {self.table_name}: {e}")
            raise
        finally:
            end_time = datetime.now(timezone.utc)
            log_pipeline_execution(
                spark=spark,
                pipeline_layer=self.pipeline_layer,
                table_name=self.table_name,
                source_table=self.source_table,
                target_table=self.target_table,
                source_path=self.source_path,
                target_path=self.target_path,
                start_time=start_time,
                end_time=end_time,
                status=status,
                row_count=row_count,
                column_count=col_count,
                error_message=error_msg,
            )
