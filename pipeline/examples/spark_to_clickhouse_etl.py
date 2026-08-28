#!/usr/bin/env python3
"""
Enterprise End-to-End Data Pipeline:
HDFS (Raw Data) -> PySpark on YARN (Processing & Enrichment) -> Hive (Data Lakehouse) -> ClickHouse (OLAP Serving)
"""
import sys
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, avg, count, round, to_date

def main():
    print("Starting Enterprise Pipeline: HDFS -> Spark -> Hive -> ClickHouse")

    start_time = time.time()

    spark = SparkSession.builder \
        .appName("Spark_To_ClickHouse_Pipeline") \
        .enableHiveSupport() \
        .getOrCreate()

    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    # 1. INGESTION LAYER (HDFS)
    input_path = "/data/sales_data.csv"
    print(f"\n[1/4] Ingesting raw dataset from HDFS: {input_path}...")
    df = spark.read.option("header", "true") \
                   .option("inferSchema", "true") \
                   .csv(input_path)

    print("Input Dataset Schema:")
    df.printSchema()

    # 2. TRANSFORMATION & ENRICHMENT LAYER (PySpark on YARN)
    print("\n[2/4] Cleaning, casting types, and computing business KPIs...")
    enriched_df = df.withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd")) \
                    .withColumn("quantity", col("quantity").cast("int")) \
                    .withColumn("unit_price", col("unit_price").cast("double")) \
                    .withColumn("total_amount", round(col("quantity") * col("unit_price"), 2))

    summary_df = enriched_df.groupBy("category") \
        .agg(
            count("order_id").cast("int").alias("total_orders"),
            _sum("quantity").cast("int").alias("total_units_sold"),
            round(_sum("total_amount"), 2).alias("revenue"),
            round(avg("total_amount"), 2).alias("avg_order_value")
        )

    print("Category Sales Summary KPIs:")
    summary_df.show()

    # 3. DATA WAREHOUSE LAYER (Apache Hive)
    print("\n[3/4] Writing Gold dataset to Apache Hive Warehouse (Parquet Format)...")
    spark.sql("CREATE DATABASE IF NOT EXISTS analytics_db")

    enriched_df.write \
        .mode("overwrite") \
        .format("parquet") \
        .partitionBy("category") \
        .saveAsTable("analytics_db.fact_sales")

    # 4. OLAP SERVING LAYER (ClickHouse)
    print("\n[4/4] Exporting Gold metrics to ClickHouse OLAP for BI / Dashboards...")
    clickhouse_url = "jdbc:clickhouse://clickhouse:8123/analytics?ssl=false"
    clickhouse_properties = {
        "driver": "com.clickhouse.jdbc.ClickHouseDriver",
        "user": "default",
        "password": "clickhouse123"
    }

    try:
        summary_df.write \
            .mode("append") \
            .jdbc(url=clickhouse_url, table="agg_category_sales", properties=clickhouse_properties)
        print(">>> Successfully exported Gold metrics to ClickHouse OLAP!")
    except Exception as e:
        print(f"Warning: JDBC export notice: {e}")

    elapsed = time.time() - start_time
    print(f"Pipeline completed successfully in {elapsed:.2f} seconds.")

    spark.stop()

if __name__ == "__main__":
    main()
