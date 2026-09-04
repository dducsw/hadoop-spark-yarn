#!/usr/bin/env python3
"""
Seed PostgreSQL OLTP Database (`source_crm`) from CSV datasets.
Built with Pydantic, SQLAlchemy, Pandas, and ThreadPoolExecutor.

Features:
- Validated configuration via Pydantic.
- High-throughput streaming via PostgreSQL COPY protocol.
- Automatic synthetic `created_at` and `updated_at` generation for incremental tables.
- B-Tree index on `updated_at` for sub-second Spark JDBC watermark queries.
- Multi-threaded table ingestion.
"""
import argparse
import io
import logging
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Optional

import pandas as pd
from pydantic import BaseModel, Field
import sqlalchemy
from sqlalchemy import create_engine, text

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("seed_postgres")


class DatabaseConfig(BaseModel):
    """PostgreSQL Connection Settings."""
    host: str = Field(default="localhost", description="PostgreSQL Host")
    port: int = Field(default=5433, description="PostgreSQL Host Port")
    user: str = Field(default="hive", description="Username")
    password: str = Field(default="hivepassword", description="Password")
    database: str = Field(default="source_crm", description="Database Name")
    pool_size: int = Field(default=10, description="Connection pool size")

    @property
    def connection_url(self) -> str:
        return f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"


class TableConfig(BaseModel):
    """Configuration for individual source table."""
    csv_file: str
    table_name: str
    is_incremental: bool = False
    date_col: Optional[str] = None
    update_col: Optional[str] = None
    date_type: str = "days"  # "days" or "months"


# Standard configuration for the 8 Home Credit tables
TABLE_CONFIGS: List[TableConfig] = [
    TableConfig(
        csv_file="application_train.csv",
        table_name="application_train",
        is_incremental=False,
        date_col="days_registration",
        date_type="days",
    ),
    TableConfig(
        csv_file="application_test.csv",
        table_name="application_test",
        is_incremental=False,
        date_col="days_registration",
        date_type="days",
    ),
    TableConfig(
        csv_file="bureau.csv",
        table_name="bureau",
        is_incremental=True,
        date_col="days_credit",
        update_col="days_credit_update",
        date_type="days",
    ),
    TableConfig(
        csv_file="bureau_balance.csv",
        table_name="bureau_balance",
        is_incremental=True,
        date_col="months_balance",
        date_type="months",
    ),
    TableConfig(
        csv_file="POS_CASH_balance.csv",
        table_name="pos_cash_balance",
        is_incremental=True,
        date_col="months_balance",
        date_type="months",
    ),
    TableConfig(
        csv_file="credit_card_balance.csv",
        table_name="credit_card_balance",
        is_incremental=True,
        date_col="months_balance",
        date_type="months",
    ),
    TableConfig(
        csv_file="previous_application.csv",
        table_name="previous_application",
        is_incremental=True,
        date_col="days_decision",
        date_type="days",
    ),
    TableConfig(
        csv_file="installments_payments.csv",
        table_name="installments_payments",
        is_incremental=True,
        date_col="days_instalment",
        update_col="days_entry_payment",
        date_type="days",
    ),
]


def add_timestamp_columns(df: pd.DataFrame, config: TableConfig) -> pd.DataFrame:
    """Adds realistic created_at and updated_at timestamps based on relative days/months."""
    base_date = pd.to_datetime("2024-01-01 00:00:00")

    if config.date_col and config.date_col in df.columns:
        if config.date_type == "months":
            # months_balance is negative (-96 to 0) -> convert to days approx 30.4375 days/month
            delta_days = df[config.date_col].fillna(0).astype(float) * 30.4375
            created_at = base_date + pd.to_timedelta(delta_days, unit="D")
        else:
            # days_* is negative relative days (-3000 to 0)
            delta_days = df[config.date_col].fillna(0).astype(float)
            created_at = base_date + pd.to_timedelta(delta_days, unit="D")
    else:
        created_at = pd.Timestamp.now()

    if config.update_col and config.update_col in df.columns:
        delta_update = df[config.update_col].fillna(df[config.date_col] if config.date_col else 0).astype(float)
        updated_at = base_date + pd.to_timedelta(delta_update, unit="D")
    else:
        updated_at = created_at

    df["created_at"] = created_at
    df["updated_at"] = updated_at
    return df


def seed_single_table(
    engine: sqlalchemy.engine.Engine,
    config: TableConfig,
    csv_dir: str,
    limit: Optional[int] = None,
    chunksize: int = 100_000,
) -> int:
    """Streams CSV into PostgreSQL table using COPY protocol."""
    csv_path = os.path.join(csv_dir, config.csv_file)
    if not os.path.exists(csv_path):
        logger.warning(f"File {csv_path} does not exist. Skipping.")
        return 0

    table_name = config.table_name
    logger.info(f"[{table_name}] Starting ingestion from {config.csv_file}...")
    start_time = time.time()
    total_rows = 0

    # Read the first chunk to create the table structure in PostgreSQL
    first_chunk = pd.read_csv(csv_path, nrows=100)
    first_chunk.columns = [c.lower() for c in first_chunk.columns]
    first_chunk = add_timestamp_columns(first_chunk, config)

    # Recreate table with DDL inferred from pandas
    schema_sql = pd.io.sql.get_schema(first_chunk, table_name, con=engine)
    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {table_name} CASCADE;"))
        conn.execute(text(schema_sql))

    # Stream chunks using PostgreSQL COPY via raw connection
    raw_conn = engine.raw_connection()
    try:
        reader = pd.read_csv(csv_path, chunksize=chunksize, nrows=limit)
        for chunk_idx, chunk in enumerate(reader, start=1):
            chunk.columns = [c.lower() for c in chunk.columns]
            chunk = add_timestamp_columns(chunk, config)

            # High-speed COPY via in-memory tab-separated buffer
            buffer = io.StringIO()
            chunk.to_csv(buffer, index=False, header=False, sep="\t", na_rep="\\N")
            buffer.seek(0)

            cursor = raw_conn.cursor()
            cursor.copy_from(
                buffer,
                table_name,
                sep="\t",
                null="\\N",
                columns=list(chunk.columns),
            )
            raw_conn.commit()
            cursor.close()

            total_rows += len(chunk)
            if chunk_idx % 5 == 0 or total_rows < chunksize:
                logger.info(f"[{table_name}] Ingested {total_rows:,} rows so far...")

        # Create Index on updated_at for fast incremental watermark queries
        with engine.begin() as conn:
            conn.execute(text(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_updated_at ON {table_name} (updated_at);"))
            if config.is_incremental:
                conn.execute(text(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_created_at ON {table_name} (created_at);"))

        elapsed = round(time.time() - start_time, 2)
        logger.info(f"[{table_name}] SUCCESS: Ingested {total_rows:,} rows with indexes in {elapsed}s.")
    finally:
        raw_conn.close()

    return total_rows


def main():
    parser = argparse.ArgumentParser(description="Seed PostgreSQL OLTP Database from CSVs")
    parser.add_argument("--host", type=str, default="localhost")
    parser.add_argument("--port", type=int, default=5433)
    parser.add_argument("--user", type=str, default="hive")
    parser.add_argument("--password", type=str, default="hivepassword")
    parser.add_argument("--database", type=str, default="source_crm")
    parser.add_argument("--input-dir", type=str, default="data/home-credit-default-risk")
    parser.add_argument("--tables", type=str, default=None, help="Comma-separated table names to seed")
    parser.add_argument("--limit", type=int, default=None, help="Max rows per table (for quick testing)")
    parser.add_argument("--threads", type=int, default=2, help="Concurrency workers")
    parser.add_argument("--chunksize", type=int, default=100_000, help="Pandas chunk size for COPY")

    args = parser.parse_args()

    db_config = DatabaseConfig(
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
        database=args.database,
    )

    logger.info("=" * 60)
    logger.info(" SEEDING POSTGRESQL OLTP DATABASE (source_crm)")
    logger.info(f" Target Database: {db_config.host}:{db_config.port}/{db_config.database}")
    logger.info(f" Input Directory: {args.input_dir}")
    logger.info(f" Concurrency:     {args.threads} threads")
    logger.info("=" * 60)

    engine = create_engine(
        db_config.connection_url,
        pool_size=db_config.pool_size,
        max_overflow=5,
    )

    # Filter tables if specified
    targets = TABLE_CONFIGS
    if args.tables:
        selected = [t.strip() for t in args.tables.split(",")]
        targets = [t for t in TABLE_CONFIGS if t.table_name in selected]

    start_total = time.time()
    results = {}

    with ThreadPoolExecutor(max_workers=args.threads) as executor:
        future_to_table = {
            executor.submit(
                seed_single_table,
                engine,
                cfg,
                args.input_dir,
                args.limit,
                args.chunksize,
            ): cfg.table_name
            for cfg in targets
        }

        for future in as_completed(future_to_table):
            table_name = future_to_table[future]
            try:
                rows = future.result()
                results[table_name] = rows
            except Exception as e:
                logger.error(f"[{table_name}] Failed: {e}", exc_info=True)
                results[table_name] = -1

    total_time = round(time.time() - start_total, 2)
    logger.info("=" * 60)
    logger.info(" SEEDING COMPLETED IN %ss", total_time)
    for tbl, count in results.items():
        status = f"{count:,} rows" if count >= 0 else "FAILED"
        logger.info(f"  - {tbl:30s}: {status}")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
