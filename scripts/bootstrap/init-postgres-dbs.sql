-- Bootstrap script for PostgreSQL Container
-- Automatically executed on first startup via /docker-entrypoint-initdb.d/

-- 1. Create Airflow user & database
DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'airflow') THEN
    CREATE ROLE airflow WITH LOGIN PASSWORD 'airflowpassword' SUPERUSER CREATEDB;
  END IF;
END
$$;

SELECT 'CREATE DATABASE airflow OWNER airflow'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow')\gexec

-- 2. Create OLTP Source Database for Fintech CRM
SELECT 'CREATE DATABASE source_crm OWNER hive'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'source_crm')\gexec

-- 3. Grant privileges
GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;
GRANT ALL PRIVILEGES ON DATABASE source_crm TO hive;
