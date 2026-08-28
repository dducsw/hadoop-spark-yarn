#!/usr/bin/env python3
"""
Simple PySpark WordCount on YARN
"""
import sys
from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder \
        .appName("PySparkWordCount") \
        .getOrCreate()

    sc = spark.sparkContext

    text_data = [
        "Apache Hadoop is a collection of open-source software utilities",
        "Apache Spark is a multi-language engine for executing data engineering",
        "Apache Hive is a distributed fault-tolerant data warehouse system",
        "YARN stands for Yet Another Resource Negotiator",
        "ZooKeeper is a centralized service for maintaining configuration information"
    ]

    rdd = sc.parallelize(text_data)
    counts = rdd.flatMap(lambda line: line.split(" ")) \
                .map(lambda word: (word.lower().strip(",.-"), 1)) \
                .filter(lambda pair: len(pair[0]) > 0) \
                .reduceByKey(lambda a, b: a + b) \
                .sortBy(lambda pair: pair[1], ascending=False)

    print("\n--- TOP WORD COUNTS ---")
    for word, count in counts.take(10):
        print(f"{word}: {count}")

    spark.stop()

if __name__ == "__main__":
    main()
