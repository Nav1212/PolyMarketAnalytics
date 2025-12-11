"""
Database Statistics Utilities
Functions to inspect DuckDB table sizes, row counts, and metadata
"""

import duckdb
import os
from pathlib import Path
from typing import Dict, List, Any, Optional

from create_database import DEFAULT_DB_PATH


def get_connection(db_path: str = None) -> duckdb.DuckDBPyConnection:
    """Get database connection"""
    if db_path is None:
        db_path = DEFAULT_DB_PATH
    return duckdb.connect(db_path)


def get_table_row_counts(conn: duckdb.DuckDBPyConnection) -> Dict[str, int]:
    """
    Get row counts for all tables.
    
    Returns:
        Dict mapping table_name -> row_count
    """
    tables = conn.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'main'
        ORDER BY table_name
    """).fetchall()
    
    counts = {}
    for (table,) in tables:
        count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        counts[table] = count
    
    return counts


def get_database_file_size(db_path: str = None) -> int:
    """
    Get database file size in bytes.
    
    Returns:
        File size in bytes
    """
    if db_path is None:
        db_path = DEFAULT_DB_PATH
    if os.path.exists(db_path):
        return os.path.getsize(db_path)
    return 0


def get_database_size_mb(db_path: str = None) -> float:
    """Get database file size in megabytes"""
    return get_database_file_size(db_path) / 1024 / 1024


def get_staging_summary(conn: duckdb.DuckDBPyConnection) -> Dict[str, Any]:
    """
    Get staging layer summary stats.
    
    Returns:
        Dict with staging table stats
    """
    summary = {}
    
    # Markets
    summary['markets'] = {
        'count': conn.execute("SELECT COUNT(*) FROM stg_markets_raw").fetchone()[0],
        'latest_fetch': conn.execute("SELECT MAX(fetched_at) FROM stg_markets_raw").fetchone()[0]
    }
    
    # Prices
    summary['prices'] = {
        'count': conn.execute("SELECT COUNT(*) FROM stg_prices_raw").fetchone()[0],
        'latest_fetch': conn.execute("SELECT MAX(fetched_at) FROM stg_prices_raw").fetchone()[0],
        'date_range': conn.execute("SELECT MIN(start_ts), MAX(end_ts) FROM stg_prices_raw").fetchone()
    }
    
    # Trades
    summary['trades'] = {
        'count': conn.execute("SELECT COUNT(*) FROM stg_trades_raw").fetchone()[0],
        'latest_fetch': conn.execute("SELECT MAX(fetched_at) FROM stg_trades_raw").fetchone()[0],
        'date_range': conn.execute("SELECT MIN(trade_timestamp), MAX(trade_timestamp) FROM stg_trades_raw").fetchone()
    }
    
    return summary


def get_gold_summary(conn: duckdb.DuckDBPyConnection) -> Dict[str, Any]:
    """
    Get gold layer summary stats.
    
    Returns:
        Dict with gold table stats
    """
    summary = {}
    
    # Lookup tables (snowflake schema)
    summary['CategoryDim'] = conn.execute("SELECT COUNT(*) FROM CategoryDim").fetchone()[0]
    
    # Dimensions
    summary['EventDim'] = conn.execute("SELECT COUNT(*) FROM EventDim").fetchone()[0]
    summary['MarketDim'] = conn.execute("SELECT COUNT(*) FROM MarketDim").fetchone()[0]
    summary['TokenDim'] = conn.execute("SELECT COUNT(*) FROM TokenDim").fetchone()[0]
    
    # Facts
    summary['HourlyPriceFact'] = conn.execute("SELECT COUNT(*) FROM HourlyPriceFact").fetchone()[0]
    summary['TradeFact'] = conn.execute("SELECT COUNT(*) FROM TradeFact").fetchone()[0]
    
    # Price snapshot date range (using snapshot_hour)
    price_range = conn.execute("SELECT MIN(snapshot_hour), MAX(snapshot_hour) FROM HourlyPriceFact").fetchone()
    summary['price_date_range'] = price_range
    
    return summary


def get_pipeline_metadata(conn: duckdb.DuckDBPyConnection) -> Dict[str, Any]:
    """
    Get pipeline metadata.
    
    Returns:
        Dict of metadata key-value pairs
    """
    rows = conn.execute("SELECT key, value, updated_at FROM _pipeline_metadata").fetchall()
    return {row[0]: {'value': row[1], 'updated_at': row[2]} for row in rows}


def get_all_stats(db_path: str = None) -> Dict[str, Any]:
    """
    Get all database statistics in one call.
    
    Returns:
        Dict with all stats
    """
    if db_path is None:
        db_path = DEFAULT_DB_PATH
    conn = get_connection(db_path)
    
    stats = {
        'db_file_size_mb': get_database_size_mb(db_path),
        'table_row_counts': get_table_row_counts(conn),
        'staging': get_staging_summary(conn),
        'gold': get_gold_summary(conn),
        'metadata': get_pipeline_metadata(conn)
    }
    
    conn.close()
    return stats


def print_summary(db_path: str = None):
    """Print formatted database summary"""
    if db_path is None:
        db_path = DEFAULT_DB_PATH
    stats = get_all_stats(db_path)
    
    print("=" * 60)
    print("POLYMARKET DATABASE SUMMARY")
    print("=" * 60)
    
    print(f"\nDatabase file: {stats['db_file_size_mb']:.2f} MB")
    
    print("\nTable Row Counts:")
    for table, count in stats['table_row_counts'].items():
        print(f"  {table:30} {count:>12,}")
    
    print("\nStaging Layer:")
    for key, val in stats['staging'].items():
        print(f"  {key}: {val}")
    
    print("\nGold Layer:")
    for key, val in stats['gold'].items():
        print(f"  {key}: {val}")
    
    print("\nPipeline Metadata:")
    for key, val in stats['metadata'].items():
        print(f"  {key}: {val['value']} ({val['updated_at']})")


if __name__ == "__main__":
    print_summary()
