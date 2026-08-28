#!/usr/bin/env python3
"""
Production-like ETL Job:
Read CSV from HDFS -> Clean & Aggregate with PySpark -> Save as Partitioned Parquet Table in Hive Lakehouse
"""
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, avg, count, round

def main():
    spark = SparkSession.builder \
        .appName("SalesETL_Spark_Hive") \
        .enableHiveSupport() \
        .getOrCreate()

    print("=== 1. Ingesting Raw CSV from HDFS ===")
    input_path = "/data/sales_data.csv"

    df = spark.read.option("header", "true") \
                   .option("inferSchema", "true") \
                   .csv(input_path)

    df.printSchema()
    df.show(5)

    print("=== 2. Transforming & Enriching Data ===")
    enriched_df = df.withColumn("total_amount", round(col("quantity") * col("unit_price"), 2))

    summary_df = enriched_df.groupBy("category") \
        .agg(
            count("order_id").alias("total_orders"),
            _sum("quantity").alias("total_units_sold"),
            round(_sum("total_amount"), 2).alias("revenue"),
            round(avg("total_amount"), 2).alias("avg_order_value")
        )

    print("Category Sales Summary:")
    summary_df.show()

    print("=== 3. Writing to Hive Warehouse as Partitioned Parquet Table ===")
    spark.sql("CREATE DATABASE IF NOT EXISTS analytics_db")

    enriched_df.write \
        .mode("overwrite") \
        .format("parquet") \
        .partitionBy("category") \
        .saveAsTable("analytics_db.fact_sales")

    summary_df.write \
        .mode("overwrite") \
        .format("parquet") \
        .saveAsTable("analytics_db.agg_category_sales")

    print("=== 4. Verifying Hive Tables ===")
    spark.sql("SHOW TABLES IN analytics_db").show()
    spark.sql("SELECT * FROM analytics_db.agg_category_sales").show()

    spark.stop()
    print("ETL Job Finished Successfully!")

if __name__ == "__main__":
    main()
