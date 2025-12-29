"""
Database connection utilities for tag manager.
"""

from pathlib import Path
import duckdb

DEFAULT_DB_PATH = Path(r"C:\Users\User\Desktop\VibeCoding\PolyMarketData\silver.duckdb")


def init_schema(conn: duckdb.DuckDBPyConnection) -> None:
    """
    Initialize the tag manager schema if tables don't exist.

    Creates Tags and TagExamples tables.
    """
    conn.execute("""
        CREATE TABLE IF NOT EXISTS Tags (
            tag_id INTEGER PRIMARY KEY,
            name VARCHAR NOT NULL UNIQUE,
            description VARCHAR,
            is_active BOOLEAN DEFAULT TRUE,
            all_checked BOOLEAN DEFAULT FALSE,
            last_checked_market_id INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS TagExamples (
            example_id INTEGER PRIMARY KEY,
            tag_id INTEGER NOT NULL,
            market_id INTEGER NOT NULL,
            is_positive BOOLEAN NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (tag_id, market_id)
        )
    """)


def get_connection(db_path: Path = DEFAULT_DB_PATH) -> duckdb.DuckDBPyConnection:
    """
    Get a DuckDB connection to the silver database.

    Args:
        db_path: Path to the DuckDB file

    Returns:
        DuckDB connection
    """
    conn = duckdb.connect(str(db_path))
    init_schema(conn)
    return conn
