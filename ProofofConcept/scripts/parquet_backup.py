"""
Parquet Backup Script
Monthly export of gold layer tables to Parquet files
"""

import duckdb
from datetime import datetime
from pathlib import Path

from create_database import DEFAULT_DB_PATH, DEFAULT_ARCHIVE_DIR


def export_to_parquet(conn: duckdb.DuckDBPyConnection,
                      table: str,
                      output_path: str,
                      partition_by: str = None) -> int:
    """
    Export a table to Parquet format.
    
    Args:
        conn: DuckDB connection
        table: Table name to export
        output_path: Output file/directory path
        partition_by: Optional column to partition by
        
    Returns:
        Number of rows exported
    """
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    
    if partition_by:
        conn.execute(f"""
            COPY (SELECT * FROM {table})
            TO '{output_path}'
            (FORMAT PARQUET, PARTITION_BY ({partition_by}), OVERWRITE_OR_IGNORE)
        """)
    else:
        conn.execute(f"""
            COPY {table} TO '{output_path}' (FORMAT PARQUET)
        """)
    
    count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    return count


def run_monthly_backup(db_path: str = None,
                       archive_dir: str = None,
                       year_month: str = None):
    """
    Export gold layer tables to Parquet for monthly backup.
    
    Args:
        db_path: Path to DuckDB database (default: ../PolyMarketData/polymarket.duckdb)
        archive_dir: Directory for Parquet archives (default: ../PolyMarketData/archive)
        year_month: Override year-month (YYYY-MM), default: current
    """
    if db_path is None:
        db_path = DEFAULT_DB_PATH
    if archive_dir is None:
        archive_dir = DEFAULT_ARCHIVE_DIR
    if year_month is None:
        year_month = datetime.now().strftime("%Y-%m")
    
    print("="*60)
    print("PARQUET BACKUP")
    print(f"Database: {db_path}")
    print(f"Archive: {archive_dir}/gold/{year_month}/")
    print(f"Started: {datetime.now().isoformat()}")
    print("="*60)
    
    conn = duckdb.connect(db_path)
    
    archive_path = Path(archive_dir) / "gold" / year_month
    archive_path.mkdir(parents=True, exist_ok=True)
    
    # Export dimension tables (small, full export)
    # Snowflake schema: CategoryDim is the lookup table
    dim_tables = ["CategoryDim", "EventDim", "MarketDim", "TokenDim"]
    
    for table in dim_tables:
        output = archive_path / f"{table}.parquet"
        count = export_to_parquet(conn, table, str(output))
        print(f"✓ {table}: {count:,} rows → {output.name}")
    
    # Export fact tables (large, full export or partitioned)
    # HourlyPriceFact - partition by year
    output = archive_path / "HourlyPriceFact.parquet"
    count = export_to_parquet(conn, "HourlyPriceFact", str(output))
    print(f"✓ HourlyPriceFact: {count:,} rows → {output.name}")
    
    # TradeFact
    output = archive_path / "TradeFact.parquet"
    count = export_to_parquet(conn, "TradeFact", str(output))
    print(f"✓ TradeFact: {count:,} rows → {output.name}")
    
    # Calculate total size
    total_size = sum(f.stat().st_size for f in archive_path.glob("*.parquet"))
    print(f"\nTotal archive size: {total_size / 1024 / 1024:.2f} MB")
    
    # Record backup in metadata
    conn.execute("""
        INSERT OR REPLACE INTO _pipeline_metadata (key, value, updated_at)
        VALUES ('last_parquet_backup', ?, CURRENT_TIMESTAMP)
    """, [year_month])
    
    conn.close()
    print(f"\n✓ Backup complete: {archive_path}")


def restore_from_parquet(db_path: str,
                         archive_dir: str,
                         year_month: str):
    """
    Restore gold layer tables from Parquet backup.
    
    WARNING: This will truncate existing data!
    
    Args:
        db_path: Path to DuckDB database
        archive_dir: Directory containing Parquet archives
        year_month: Year-month to restore (YYYY-MM)
    """
    print("="*60)
    print("PARQUET RESTORE")
    print(f"Source: {archive_dir}/gold/{year_month}/")
    print(f"Database: {db_path}")
    print("="*60)
    
    archive_path = Path(archive_dir) / "gold" / year_month
    
    if not archive_path.exists():
        print(f"ERROR: Archive not found: {archive_path}")
        return
    
    conn = duckdb.connect(db_path)
    
    # Restore in reverse dependency order (snowflake schema)
    tables = [
        "TradeFact",
        "HourlyPriceFact", 
        "TokenDim",
        "MarketDim",
        "EventDim",
        "CategoryDim"
    ]
    
    for table in tables:
        parquet_file = archive_path / f"{table}.parquet"
        if not parquet_file.exists():
            print(f"  Skipping {table} (no parquet file)")
            continue
        
        # Truncate and reload
        conn.execute(f"DELETE FROM {table}")
        conn.execute(f"""
            INSERT INTO {table}
            SELECT * FROM read_parquet('{parquet_file}')
        """)
        
        count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        print(f"✓ Restored {table}: {count:,} rows")
    
    conn.close()
    print("\n✓ Restore complete")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Parquet Backup/Restore")
    parser.add_argument("action", choices=["backup", "restore"], help="Action to perform")
    parser.add_argument("--db", default=None, help="Database path (default: ../PolyMarketData/polymarket.duckdb)")
    parser.add_argument("--archive", default=None, help="Archive directory (default: ../PolyMarketData/archive)")
    parser.add_argument("--month", help="Year-month (YYYY-MM)")
    
    args = parser.parse_args()
    
    if args.action == "backup":
        run_monthly_backup(
            db_path=args.db,
            archive_dir=args.archive,
            year_month=args.month
        )
    else:
        if not args.month:
            print("ERROR: --month required for restore")
        else:
            restore_from_parquet(
                db_path=args.db,
                archive_dir=args.archive,
                year_month=args.month
            )
