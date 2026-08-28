#!/usr/bin/env python3
"""
Test 4: Hive Metastore & Spark SQL Interoperability (Shared Catalog & Warehouse)
"""
import sys
from pyspark.sql import SparkSession

def main():
    print("=== [TEST 4] Verifying Spark SQL and Hive Catalog Integration ===")

    spark = SparkSession.builder \
        .appName("SparkHiveInteropTest") \
        .enableHiveSupport() \
        .getOrCreate()

    print("1. Creating Database and Table in Hive Metastore via Spark SQL...")
    spark.sql("CREATE DATABASE IF NOT EXISTS test_db")
    spark.sql("USE test_db")
    spark.sql("DROP TABLE IF EXISTS sales_demo")

    spark.sql("""
        CREATE TABLE sales_demo (
            id INT,
            product STRING,
            amount DOUBLE,
            category STRING
        )
        STORED AS PARQUET
    """)

    print("2. Inserting records into Hive Table via Spark SQL...")
    spark.sql("""
        INSERT INTO sales_demo VALUES
        (1, 'MacBook Pro', 2500.0, 'Electronics'),
        (2, 'Dell XPS', 1800.0, 'Electronics'),
        (3, 'Standing Desk', 450.0, 'Furniture'),
        (4, 'Ergonomic Chair', 350.0, 'Furniture')
    """)

    print("3. Querying Hive Table & performing aggregation with Spark SQL:")
    result_df = spark.sql("""
        SELECT category, COUNT(*) as total_items, SUM(amount) as total_revenue
        FROM sales_demo
        GROUP BY category
    """)
    result_df.show()

    # Verify tables in catalog
    tables = [t.name for t in spark.catalog.listTables("test_db")]
    print(f"Tables in test_db: {tables}")
    assert "sales_demo" in tables, "Table not found in Hive Catalog!"

    spark.stop()
    print(">>> [TEST 4 SUCCESS] Spark SQL and Hive Metastore integration verified!")

if __name__ == "__main__":
    main()
