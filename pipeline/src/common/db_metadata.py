"""Centralized Metadata & State Management using PostgreSQL with SQLite fallback."""
import os
import sqlite3
from contextlib import contextmanager
from typing import Generator, Tuple

try:
    import psycopg2
    HAS_PSYCOPG2 = True
except ImportError:
    HAS_PSYCOPG2 = False


def _get_pg_config() -> dict:
    is_windows = os.name == "nt"
    default_host = "localhost" if is_windows else "postgres"
    default_port = 5433 if is_windows else 5432
    return {
        "host": os.environ.get("METADATA_PG_HOST", default_host),
        "port": int(os.environ.get("METADATA_PG_PORT", default_port)),
        "database": os.environ.get("METADATA_PG_DB", os.environ.get("POSTGRES_DB", "metastore")),
        "user": os.environ.get("METADATA_PG_USER", os.environ.get("POSTGRES_USER", "hive")),
        "password": os.environ.get("METADATA_PG_PASSWORD", os.environ.get("POSTGRES_PASSWORD", "hivepassword")),
        "connect_timeout": int(os.environ.get("METADATA_PG_TIMEOUT", "2")),
    }


def _get_sqlite_path() -> str:
    base_dir = os.environ.get("METADATA_LOCAL_DIR")
    if not base_dir:
        base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".metadata"))
    os.makedirs(base_dir, exist_ok=True)
    return os.path.join(base_dir, "pipeline_metadata.db")


import socket

def _is_pg_reachable(host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=0.3):
            return True
    except Exception:
        return False


@contextmanager
def get_metadata_cursor() -> Generator[Tuple[any, str], None, None]:
    """
    Yields (cursor, backend_type) where backend_type is 'postgres' or 'sqlite'.
    Automatically commits and closes connections.
    """
    conn = None
    backend = None
    if HAS_PSYCOPG2:
        cfg = _get_pg_config()
        if _is_pg_reachable(cfg["host"], cfg["port"]):
            try:
                conn = psycopg2.connect(**cfg)
                backend = "postgres"
            except Exception:
                conn = None

    if conn is None:
        sqlite_path = _get_sqlite_path()
        conn = sqlite3.connect(sqlite_path)
        conn.row_factory = sqlite3.Row
        backend = "sqlite"

    try:
        cursor = conn.cursor()
        _init_schema(cursor, backend)
        yield cursor, backend
        conn.commit()
    except Exception:
        if conn:
            conn.rollback()
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def _init_schema(cursor, backend: str) -> None:
    """Bootstraps audit and watermark tables."""
    if backend == "postgres":
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS pipeline_audit_log (
                job_id VARCHAR(64) PRIMARY KEY,
                pipeline_layer VARCHAR(32) NOT NULL,
                table_name VARCHAR(128) NOT NULL,
                source_table VARCHAR(256),
                target_table VARCHAR(256),
                source_path TEXT,
                target_path TEXT,
                start_time TIMESTAMP NOT NULL,
                end_time TIMESTAMP NOT NULL,
                duration_sec DOUBLE PRECISION NOT NULL,
                row_count BIGINT,
                column_count INT,
                rejected_count BIGINT DEFAULT 0,
                status VARCHAR(32) NOT NULL,
                error_message TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        cursor.execute("ALTER TABLE pipeline_audit_log ADD COLUMN IF NOT EXISTS rejected_count BIGINT DEFAULT 0;")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS pipeline_watermark (
                table_name VARCHAR(128) PRIMARY KEY,
                watermark_column VARCHAR(64),
                last_watermark_value VARCHAR(128),
                last_updated_at TIMESTAMP NOT NULL,
                status VARCHAR(32) NOT NULL
            );
        """)
    else:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS pipeline_audit_log (
                job_id TEXT PRIMARY KEY,
                pipeline_layer TEXT NOT NULL,
                table_name TEXT NOT NULL,
                source_table TEXT,
                target_table TEXT,
                source_path TEXT,
                target_path TEXT,
                start_time TEXT NOT NULL,
                end_time TEXT NOT NULL,
                duration_sec REAL NOT NULL,
                row_count INTEGER,
                column_count INTEGER,
                rejected_count INTEGER DEFAULT 0,
                status TEXT NOT NULL,
                error_message TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        try:
            cursor.execute("ALTER TABLE pipeline_audit_log ADD COLUMN rejected_count INTEGER DEFAULT 0;")
        except Exception:
            pass
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS pipeline_watermark (
                table_name TEXT PRIMARY KEY,
                watermark_column TEXT,
                last_watermark_value TEXT,
                last_updated_at TEXT NOT NULL,
                status TEXT NOT NULL
            );
        """)
