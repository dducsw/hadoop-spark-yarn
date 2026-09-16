FROM apache/superset:latest

USER root

# Bake in required database drivers at build time -- no runtime pip install
RUN pip install --no-cache-dir clickhouse-connect>=0.7.0 psycopg2-binary>=2.9

USER superset

