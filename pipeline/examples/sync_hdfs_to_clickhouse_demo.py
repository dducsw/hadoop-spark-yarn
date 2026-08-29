#!/usr/bin/env python3
"""
Enterprise Demo: Sync Data from HDFS to ClickHouse OLAP
Demonstrates PySpark reading from HDFS, cleaning/enriching, and writing to ClickHouse via JDBC.
"""
import sys
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import IntegerType, StringType, DecimalType


def main():
    print("=" * 70)
    print("DEMO: SYNC HDFS -> PYSPARK -> CLICKHOUSE OLAP")
    print("=" * 70)

    start_time = time.time()

    # 1. Initialize Spark Session with ClickHouse JDBC driver
    spark = SparkSession.builder \
        .appName("Sync_HDFS_To_ClickHouse_Demo") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # 2. Extract: Read CSV / Parquet directly from HDFS
    hdfs_input_path = "hdfs://master:9000/data/products.csv"
    print(f"\n[1/3] Extracting data from HDFS: {hdfs_input_path}")
    df_raw = spark.read.option("header", "true").csv(hdfs_input_path)

    print("Raw Data from HDFS:")
    df_raw.show(truncate=False)

    # 3. Transform: Clean, cast exact types (Decimal for money) & add metadata
    print("\n[2/3] Transforming & standardizing data types...")
    df_transformed = (
        df_raw
        .withColumn("id", col("id").cast(IntegerType()))
        .withColumn("name", col("name").cast(StringType()))
        .withColumn("price", col("price").cast(DecimalType(18, 2)))
        .withColumn("synced_at", current_timestamp())
    )

    print("Transformed DataFrame Schema:")
    df_transformed.printSchema()
    df_transformed.show(truncate=False)

    # 4. Load: Write into ClickHouse OLAP table via JDBC
    clickhouse_url = "jdbc:clickhouse://clickhouse:8123/analytics?ssl=false"
    target_table = "analytics.dim_products_pyspark_sync"
    clickhouse_properties = {
        "driver": "com.clickhouse.jdbc.ClickHouseDriver",
        "user": "default",
        "password": "clickhouse123",
        "batchsize": "10000",
    }

    print(f"\n[3/3] Loading into ClickHouse table '{target_table}' via JDBC...")
    df_transformed.write \
        .mode("overwrite") \
        .jdbc(url=clickhouse_url, table=target_table, properties=clickhouse_properties)

    print(f">>> Successfully synced {df_transformed.count()} rows to ClickHouse!")
    print(f">>> Elapsed Time: {time.time() - start_time:.2f}s")
    print("=" * 70)

    spark.stop()


if __name__ == "__main__":
    main()
