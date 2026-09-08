"""Base Stage (Silver) Cleaning, Standardizing & DLQ Quarantine Job template."""
import os, sys
from typing import List, Optional
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
SRC_DIR = os.path.abspath(os.path.join(CURRENT_DIR, ".."))
sys.path.extend([SRC_DIR])

from src.common.base_spark_job import BaseSparkJob, WriteMode


class BaseStageJob(BaseSparkJob):
    def __init__(
        self,
        table_name: str,
        primary_key: Optional[str] = None,
        dedup_cols: Optional[List[str]] = None,
        raw_base_dir: str = "/raw/credit_risk",
        stage_base_dir: str = "/stage/credit_risk",
        quarantine_base_dir: Optional[str] = None,
        raw_db: str = "raw_credit_risk",
        stage_db: str = "stage_credit_risk",
        source_table: Optional[str] = None,
        write_mode: WriteMode = WriteMode.OVERWRITE,
        partition_by: Optional[List[str]] = None,
    ):
        source_path = os.path.join(raw_base_dir, table_name)
        target_path = os.path.join(stage_base_dir, table_name)
        resolved_source_table = source_table or f"{raw_db}.raw_{table_name}"
        target_table = f"{stage_db}.stage_{table_name}"
        super().__init__(
            pipeline_layer="stage",
            table_name=table_name,
            source_table=resolved_source_table,
            target_table=target_table,
            source_path=source_path,
            target_path=target_path,
            primary_key=primary_key,
            write_mode=write_mode,
            partition_by=partition_by,
        )
        self.dedup_cols = dedup_cols or ([primary_key] if primary_key else None)
        self.quarantine_base_dir = (
            quarantine_base_dir or os.environ.get("QUARANTINE_BASE_DIR", "/quarantine/credit_risk")
        ).replace("\\", "/")
        self.quarantine_path = f"{self.quarantine_base_dir}/{table_name}"

    def extract(self, spark: SparkSession) -> DataFrame:
        self.logger.info(f"Reading Raw Parquet from {self.source_path}")
        return spark.read.parquet(self.source_path)

    def route_quarantine(self, df_rejected: DataFrame) -> None:
        """Saves malformed/duplicate records to Dead-Letter Queue (DLQ) for audit."""
        try:
            if df_rejected.take(1):
                self.rejected_count = df_rejected.count()
                df_quarantine = (
                    df_rejected
                    .withColumn("_rejected_at", F.current_timestamp())
                    .withColumn("_batch_id", F.lit(self.batch_id))
                    .withColumn("_source_table", F.lit(self.source_table))
                )
                df_quarantine.write.mode("append").format("parquet").save(self.quarantine_path)
                self.logger.warning(
                    f"DLQ Quarantine: Routed {self.rejected_count} bad records to {self.quarantine_path}"
                )
        except Exception as e:
            self.logger.error(f"DLQ Quarantine routing error: {e}")

    def transform(self, df: DataFrame) -> DataFrame:
        df_tagged = df

        # 1. Flag null primary keys
        if self.primary_key and self.primary_key in df_tagged.columns:
            df_tagged = df_tagged.withColumn(
                "_is_null_pk",
                F.when(F.col(self.primary_key).isNull(), True).otherwise(False),
            )
        else:
            df_tagged = df_tagged.withColumn("_is_null_pk", F.lit(False))

        # 2. Flag duplicates using window ranking
        if self.dedup_cols:
            valid_cols = [c for c in self.dedup_cols if c in df_tagged.columns]
            if valid_cols:
                order_col = (
                    F.col(self.primary_key).asc_nulls_last()
                    if (self.primary_key and self.primary_key in df_tagged.columns)
                    else F.lit(1)
                )
                w = Window.partitionBy(*valid_cols).orderBy(order_col)
                df_tagged = df_tagged.withColumn("_row_num", F.row_number().over(w))
            else:
                df_tagged = df_tagged.withColumn("_row_num", F.lit(1))
        else:
            df_tagged = df_tagged.withColumn("_row_num", F.lit(1))

        # 3. Classify reject reasons
        df_tagged = df_tagged.withColumn(
            "_reject_reason",
            F.when(F.col("_is_null_pk"), F.lit("NULL_PRIMARY_KEY"))
            .when(F.col("_row_num") > 1, F.lit("DUPLICATE_RECORD"))
            .otherwise(F.lit(None).cast("string")),
        )

        df_clean = (
            df_tagged
            .filter(F.col("_reject_reason").isNull())
            .drop("_is_null_pk", "_row_num", "_reject_reason")
        )
        df_rejected = (
            df_tagged
            .filter(F.col("_reject_reason").isNotNull())
            .drop("_is_null_pk", "_row_num")
        )

        # 4. Route bad records to DLQ Quarantine Zone
        self.route_quarantine(df_rejected)

        # 5. Add standardized metadata audit columns
        return self.add_audit_metadata(df_clean)
