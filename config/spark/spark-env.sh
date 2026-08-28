#!/usr/bin/env bash

export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export HADOOP_HOME=/opt/hadoop
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export YARN_CONF_DIR=$HADOOP_HOME/etc/hadoop
export SPARK_HOME=/opt/spark
export SPARK_CONF_DIR=$SPARK_HOME/conf
export SPARK_LOG_DIR=/var/log/spark

# Ensure PySpark uses Python3
export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=python3
