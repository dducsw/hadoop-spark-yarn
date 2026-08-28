#!/usr/bin/env bash

# Java Environment
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# Hadoop Configuration
export HADOOP_HOME=/opt/hadoop
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export HADOOP_LOG_DIR=/var/log/hadoop

# Process user overrides
export HDFS_NAMENODE_USER=root
export HDFS_DATANODE_USER=root
export HDFS_SECONDARYNAMENODE_USER=root
export HDFS_JOURNALNODE_USER=root
export HDFS_ZKFC_USER=root

export YARN_RESOURCEMANAGER_USER=root
export YARN_NODEMANAGER_USER=root
