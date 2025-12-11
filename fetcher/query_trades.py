"""
Query trade parquet files using DuckDB.

Usage:
    python query_trades.py                    # Interactive mode
    python query_trades.py --sql "SELECT ..."  # Run single query
    python query_trades.py --stats            # Show dataset statistics
"""

import argparse
from pathlib import Path

import duckdb


def get_connection(data_dir: str) -> duckdb.DuckDBPyConnection:
    """Create DuckDB connection with parquet files registered as a view."""
    conn = duckdb.connect(":memory:")
    
    parquet_path = Path(data_dir)
    if not parquet_path.exists():
        raise FileNotFoundError(f"Data directory not found: {data_dir}")
    
    # Register all parquet files as a single view (supports Hive partitioning)
    glob_pattern = str(parquet_path / "**" / "*.parquet")
    conn.execute(f"""
        CREATE VIEW trades AS 
        SELECT * FROM read_parquet('{glob_pattern}', hive_partitioning=true)
    """)
    
    return conn


def show_schema(conn: duckdb.DuckDBPyConnection) -> None:
    """Display the schema of the trades table."""
    print("\n=== Schema ===")
    result = conn.execute("DESCRIBE trades").fetchall()
    for row in result:
        print(f"  {row[0]}: {row[1]}")


def show_stats(conn: duckdb.DuckDBPyConnection) -> None:
    """Display basic statistics about the dataset."""
    print("\n=== Dataset Statistics ===")
    
    count = conn.execute("SELECT COUNT(*) FROM trades").fetchone()[0]
    print(f"Total records: {count:,}")
    
    if count > 0:
        # Partition stats
        partitions = conn.execute("""
            SELECT dt, COUNT(*) as count 
            FROM trades 
            GROUP BY dt 
            ORDER BY dt
        """).fetchall()
        
        print(f"\nRecords by date:")
        for dt, cnt in partitions:
            print(f"  {dt}: {cnt:,}")


def run_query(conn: duckdb.DuckDBPyConnection, sql: str) -> None:
    """Execute a SQL query and print results."""
    try:
        result = conn.execute(sql)
        df = result.fetchdf()
        print(df.to_string())
    except Exception as e:
        print(f"Error: {e}")


def interactive_mode(conn: duckdb.DuckDBPyConnection) -> None:
    """Run interactive query mode."""
    print("\n=== DuckDB Trade Query Interface ===")
    print("Type 'help' for example queries, 'exit' to quit\n")
    
    examples = """
Example queries:
  SELECT * FROM trades LIMIT 10;
  SELECT COUNT(*) FROM trades;
  SELECT dt, COUNT(*) FROM trades GROUP BY dt;
  SELECT * FROM trades WHERE market_id = 'xxx';
  DESCRIBE trades;
"""
    
    while True:
        try:
            sql = input("duckdb> ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nGoodbye!")
            break
        
        if not sql:
            continue
        if sql.lower() == "exit":
            break
        if sql.lower() == "help":
            print(examples)
            continue
        
        run_query(conn, sql)
        print()


def main():
    parser = argparse.ArgumentParser(description="Query trade parquet files with DuckDB")
    parser.add_argument(
        "--data-dir",
        default="data/trades",
        help="Path to parquet data directory (default: data/trades)"
    )
    parser.add_argument(
        "--sql",
        help="Run a single SQL query and exit"
    )
    parser.add_argument(
        "--stats",
        action="store_true",
        help="Show dataset statistics"
    )
    parser.add_argument(
        "--schema",
        action="store_true",
        help="Show table schema"
    )
    
    args = parser.parse_args()
    
    try:
        conn = get_connection(args.data_dir)
    except FileNotFoundError as e:
        print(f"Error: {e}")
        return 1
    
    if args.schema:
        show_schema(conn)
    elif args.stats:
        show_stats(conn)
    elif args.sql:
        run_query(conn, args.sql)
    else:
        show_schema(conn)
        show_stats(conn)
        interactive_mode(conn)
    
    conn.close()
    return 0


if __name__ == "__main__":
    exit(main())
