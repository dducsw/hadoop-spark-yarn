"""Centralized Metadata & State Management using PostgreSQL."""
import os
from collections.abc import Generator
from contextlib import contextmanager

try:
    import psycopg2
    HAS_PSYCOPG2 = True
except ImportError:
    psycopg2 = None
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
        "password": os.environ.get("METADATA_PG_PASSWORD", os.environ.get("POSTGRES_PASSWORD", "")),
        "connect_timeout": int(os.environ.get("METADATA_PG_TIMEOUT", "3")),
    }


# Module-level flag: schema initialized once per process
_SCHEMA_INITIALIZED: bool = False


class _NoOpCursor:
    """Fallback dummy cursor when psycopg2 is not installed (e.g. lightweight unit test runners)."""
    def execute(self, *args, **kwargs): pass
    def fetchone(self): return None
    def fetchall(self): return []
    def close(self): pass


@contextmanager
def get_metadata_cursor() -> Generator[tuple[any, str], None, None]:
    """
    Yields (cursor, 'postgres') connected to PostgreSQL metadata database.
    If psycopg2 is unavailable, yields a no-op cursor to preserve pipeline execution.
    """
    global _SCHEMA_INITIALIZED
    if not HAS_PSYCOPG2:
        yield _NoOpCursor(), "postgres"
        return

    conn = psycopg2.connect(**_get_pg_config())
    cursor = conn.cursor()
    try:
        if not _SCHEMA_INITIALIZED:
            _init_schema(cursor)
            _SCHEMA_INITIALIZED = True
        yield cursor, "postgres"
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()


def _init_schema(cursor) -> None:
    """Bootstraps audit and watermark tables in PostgreSQL."""
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
