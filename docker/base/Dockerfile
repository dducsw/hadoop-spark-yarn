FROM ubuntu:22.04

LABEL maintainer="Data Platform Team"
LABEL description="Unified Base Image for Hadoop HA, YARN, Spark, Hive on Docker"

# Avoid interactive prompts
ENV DEBIAN_FRONTEND=noninteractive

# Install core utilities, Java 11, Python 3
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-11-jdk \
    wget \
    curl \
    ssh \
    netcat \
    procps \
    python3 \
    python3-pip \
    python3-dev \
    sudo \
    ca-certificates \
    iputils-ping \
    vim \
    && rm -rf /var/lib/apt/lists/*

# Install PySpark & Python dependencies
RUN pip3 install --no-cache-dir pyspark==3.5.4 pandas pyarrow jupyterlab findspark

# Versions
ARG HADOOP_VERSION=3.4.3
ARG SPARK_VERSION=3.5.4
ARG HIVE_VERSION=3.1.3
ARG PG_JDBC_VERSION=42.7.3
ARG CLICKHOUSE_JDBC_VERSION=0.6.3

# Environment Variables
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV HADOOP_HOME=/opt/hadoop
ENV SPARK_HOME=/opt/spark
ENV HIVE_HOME=/opt/hive

ENV HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
ENV YARN_CONF_DIR=$HADOOP_HOME/etc/hadoop
ENV SPARK_CONF_DIR=$SPARK_HOME/conf
ENV HIVE_CONF_DIR=$HIVE_HOME/conf

ENV PATH=$PATH:$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SPARK_HOME/bin:$SPARK_HOME/sbin:$HIVE_HOME/bin

# 1. Install Hadoop
COPY docker/base/downloads/hadoop-${HADOOP_VERSION}.tar.gz /tmp/hadoop.tar.gz
RUN tar -xzf /tmp/hadoop.tar.gz -C /opt/ \
    && mv /opt/hadoop-${HADOOP_VERSION} $HADOOP_HOME \
    && rm /tmp/hadoop.tar.gz

# 2. Install Apache Spark
COPY docker/base/downloads/spark-${SPARK_VERSION}-bin-hadoop3.tgz /tmp/spark.tgz
RUN tar -xzf /tmp/spark.tgz -C /opt/ \
    && mv /opt/spark-${SPARK_VERSION}-bin-hadoop3 $SPARK_HOME \
    && rm /tmp/spark.tgz

# 3. Install Apache Hive
COPY docker/base/downloads/apache-hive-${HIVE_VERSION}-bin.tar.gz /tmp/hive.tar.gz
RUN tar -xzf /tmp/hive.tar.gz -C /opt/ \
    && mv /opt/apache-hive-${HIVE_VERSION}-bin $HIVE_HOME \
    && rm /tmp/hive.tar.gz

# 4. Fix Hive 3 Guava Conflict (Replace Hive guava-19.0.jar with Hadoop's Guava)
RUN rm -f $HIVE_HOME/lib/guava-19.0.jar \
    && cp $HADOOP_HOME/share/hadoop/common/lib/guava-*.jar $HIVE_HOME/lib/

# 5. Copy PostgreSQL & ClickHouse JDBC Drivers, Hive fix & Spark YARN Shuffle
COPY docker/base/downloads/commons-collections-3.2.2.jar $HIVE_HOME/lib/
COPY docker/base/downloads/postgresql-${PG_JDBC_VERSION}.jar $HIVE_HOME/lib/
COPY docker/base/downloads/postgresql-${PG_JDBC_VERSION}.jar $SPARK_HOME/jars/
COPY docker/base/downloads/clickhouse-jdbc-${CLICKHOUSE_JDBC_VERSION}-all.jar $SPARK_HOME/jars/
RUN cp $SPARK_HOME/yarn/spark-*-yarn-shuffle.jar $HADOOP_HOME/share/hadoop/yarn/lib/ 2>/dev/null || true

# 6. Setup Passwordless SSH
RUN ssh-keygen -t rsa -P '' -f /root/.ssh/id_rsa \
    && cat /root/.ssh/id_rsa.pub >> /root/.ssh/authorized_keys \
    && chmod 0600 /root/.ssh/authorized_keys

# 7. Create needed storage & log directories
RUN mkdir -p \
    /hadoop/dfs/name \
    /hadoop/dfs/data \
    /hadoop/dfs/journal \
    /hadoop/tmp \
    /var/log/hadoop \
    /var/log/spark \
    /var/log/hive

# 8. Copy Configurations
COPY config/hadoop/* $HADOOP_HOME/etc/hadoop/
COPY config/yarn/*   $HADOOP_HOME/etc/hadoop/
COPY config/spark/*  $SPARK_HOME/conf/
COPY config/hive/*   $HIVE_HOME/conf/

# Symlink hive-site.xml to Spark conf for unified Hive Catalog
RUN ln -sf $HIVE_HOME/conf/hive-site.xml $SPARK_HOME/conf/hive-site.xml

# Copy Entrypoint
COPY docker/base/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

WORKDIR /root

ENTRYPOINT ["/entrypoint.sh"]
CMD ["bash"]
