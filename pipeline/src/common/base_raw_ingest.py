"""Base Raw Ingestion Job template."""
import os, sys
from typing import Optional
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
SRC_DIR = os.path.abspath(os.path.join(CURRENT_DIR, ".."))
sys.path.extend([SRC_DIR])

from src.common.base_spark_job import BaseSparkJob


class BaseRawIngestJob(BaseSparkJob):
    def __init__(
        self,
        table_name: str,
        file_name: str,
        primary_key: Optional[str] = None,
        schema: Optional[StructType] = None,
        input_base_dir: str = "/data/home-credit-default-risk",
        output_base_dir: str = "/raw/credit_risk",
        hive_db: str = "raw_credit_risk",
    ):
        input_path = os.path.join(input_base_dir, file_name)
        output_path = os.path.join(output_base_dir, table_name)
        
        target_table = f"{hive_db}.raw_{table_name}"
        super().__init__(
            pipeline_layer="raw",
            table_name=table_name,
            source_table=file_name,
            target_table=target_table,
            source_path=input_path,
            target_path=output_path,
            primary_key=primary_key,
        )
        self.schema = schema

    def extract(self, spark: SparkSession) -> DataFrame:
        self.logger.info(f"Extracting CSV from {self.source_path}")
        reader = (
            spark.read
            .option("header", "true")
            .option("nullValue", "")
            .option("nanValue", "")
        )
        if self.schema:
            return reader.schema(self.schema).csv(self.source_path)
        return reader.option("inferSchema", "true").csv(self.source_path)

    def transform(self, df: DataFrame) -> DataFrame:
        return self.add_audit_metadata(df)
