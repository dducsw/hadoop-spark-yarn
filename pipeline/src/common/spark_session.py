import socketserver

# Windows compatibility patch for PySpark on Python 3.12+
if not hasattr(socketserver, "UnixStreamServer"):
    class DummyUnixServer:
        pass
    socketserver.UnixStreamServer = DummyUnixServer

from pyspark.sql import SparkSession


def get_spark_session(app_name: str = "HomeCredit_Raw_Ingestion") -> SparkSession:
    """Create or retrieve a standard SparkSession configured for the cluster."""
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .enableHiveSupport()
        .getOrCreate()
    )
