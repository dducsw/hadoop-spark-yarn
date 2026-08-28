#!/usr/bin/env python3
"""
Test 3: PySpark on YARN with Distributed HDFS Read/Write
"""
import sys
import random
from pyspark.sql import SparkSession

def main():
    print("=== [TEST 3] Running PySpark Job on YARN ===")

    spark = SparkSession.builder \
        .appName("SparkYarnSmokeTest") \
        .getOrCreate()

    sc = spark.sparkContext
    print(f"Spark Master: {sc.master}")
    print(f"Application ID: {sc.applicationId}")
    print(f"Spark Version: {sc.version}")

    # 1. Parallelize computation: Monte Carlo Pi estimation
    n = 100000
    def inside(p):
        x, y = random.random(), random.random()
        return x*x + y*y < 1

    count = sc.parallelize(range(0, n)).filter(inside).count()
    pi_est = 4.0 * count / n
    print(f"Estimated Pi value: {pi_est}")

    # 2. DataFrame test & HDFS Parquet Write/Read
    data = [("DataPlatform", 100), ("Hadoop", 200), ("Spark", 300), ("Hive", 400), ("YARN", 500)]
    df = spark.createDataFrame(data, ["technology", "score"])

    hdfs_path = "/tmp/spark_smoke_test.parquet"
    print(f"Writing DataFrame to HDFS: {hdfs_path}...")
    df.write.mode("overwrite").parquet(hdfs_path)

    print(f"Reading back DataFrame from HDFS:")
    read_df = spark.read.parquet(hdfs_path)
    read_df.show()

    assert read_df.count() == 5, "Count mismatch!"

    spark.stop()
    print(">>> [TEST 3 SUCCESS] PySpark on YARN + HDFS execution passed!")

if __name__ == "__main__":
    main()
