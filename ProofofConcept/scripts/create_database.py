"""
Polymarket Database Schema Creation
Creates DuckDB database with staging + gold layer tables

SCHEMA: Snowflake (normalized for storage efficiency)
- Staging: Raw JSON from API (temporary)
- Gold: Normalized dimensions + facts

Run this script to initialize a fresh database:
    python create_database.py

Database file will be created at: ../PolyMarketData/polymarket.duckdb
This is outside the repo to keep data separate from code.
"""

import duckdb
from pathlib import Path
from datetime import datetime, timedelta

# Default data directory (outside repo)
DEFAULT_DATA_DIR = Path(__file__).parent.parent.parent / "PolyMarketData"
DEFAULT_DB_PATH = str(DEFAULT_DATA_DIR / "polymarket.duckdb")
DEFAULT_ARCHIVE_DIR = str(DEFAULT_DATA_DIR / "archive")


def create_database(db_path: str = None) -> duckdb.DuckDBPyConnection:
    """
    Create DuckDB database with snowflake schema for storage efficiency.
    
    Snowflake design:
    - dim_category: Normalized event categories (few rows)
    - dim_outcome: Normalized YES/NO outcomes (2 rows)
    - dim_event: Events with FK to category
    - dim_market: Markets with FK to event
    - dim_token: Tokens with FK to market + outcome
    - fact_price_snapshot: Hourly prices (date computed, not joined)
    - fact_trade: Individual trades
    
    Args:
        db_path: Path to create database file (default: ../PolyMarketData/polymarket.duckdb)
        
    Returns:
        DuckDB connection
    """
    if db_path is None:
        db_path = DEFAULT_DB_PATH
    # Ensure directory exists
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    
    # Connect (creates file if doesn't exist)
    conn = duckdb.connect(db_path)
    
    print(f"Creating database at: {db_path}")
    
    # ==================== STAGING LAYER ====================
    print("Creating staging layer tables...")
    
    # stg_markets_raw - Raw market metadata from API
    # Use condition_id as primary key since it's already unique
    conn.execute("""
        CREATE TABLE IF NOT EXISTS stg_markets_raw (
            condition_id    TEXT PRIMARY KEY,
            raw_json        JSON NOT NULL,
            fetched_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # stg_prices_raw - Raw hourly price data (scraped daily)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS stg_prices_raw (
            condition_id    TEXT NOT NULL,
            token_id        TEXT NOT NULL,
            raw_json        JSON NOT NULL,
            fetched_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (condition_id, token_id, start_ts, end_ts)
        )
    """)
    
    # stg_trades_raw - Raw trade data (Nov 2024 initially, expand later)
    # Uses auto-generated rowid
    conn.execute("""
        CREATE TABLE IF NOT EXISTS stg_trades_raw (
            condition_id    TEXT NOT NULL,
            raw_json        JSON NOT NULL,
            fetched_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    print("✓ Staging layer created")
    
    # ==================== GOLD LAYER (Snowflake Schema) ====================
    # Normalized for storage efficiency - lookup tables first
    print("Creating gold layer tables (snowflake schema)...")
    
    # CategoryDim - Normalized category lookup (small, ~20 rows)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS CategoryDim (
            category_id     TINYINT PRIMARY KEY,
            category_name   TEXT NOT NULL UNIQUE
        )
    """)
    
    # dim_outcome - Normalized outcome lookup (2 rows: YES/NO)    
    # Pre-populate outcomes

    
    # EventDim - Event dimension (FK to category)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS EventDim (
            event_id        INTEGER PRIMARY KEY,
            slug            TEXT NOT NULL UNIQUE,
            title           TEXT NOT NULL,
            description     TEXT,
            category_id     TINYINT REFERENCES CategoryDim(category_id),
            end_date        TIMESTAMP,
            created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # MarketDim - Market dimension (FK to event)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS MarketDim (
            market_id       INTEGER PRIMARY KEY,
            external_id    TEXT NOT NULL UNIQUE,
            event_id        INTEGER REFERENCES EventDim(event_id),
            question        TEXT NOT NULL,
            active          BOOLEAN DEFAULT TRUE,
            end_date_iso    TIMESTAMP,
            start_date_iso  TIMESTAMP,
            VolumeNum       TEXT,  
            created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    # Note: Removed description from dim_market (use event description)
    
    # TokenDim - Token dimension (FK to market)
    # outcome: bit (1=YES, 0=NO) for storage efficiency
    conn.execute("""
        CREATE TABLE IF NOT EXISTS TokenDim (
            token_id        INTEGER PRIMARY KEY,
            token_address   TEXT NOT NULL UNIQUE,
            market_id       INTEGER REFERENCES MarketDim(market_id),
            outcome      bit,
            created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # TraderDim - Trader/wallet dimension (normalized to save storage)
    # Stores wallet address as BLOB(20) - raw bytes, not hex string
    # 20 bytes vs 42 bytes for TEXT hex representation (52% savings)
    # Saves ~80 bytes per trade vs storing maker+taker addresses directly
    conn.execute("""
        CREATE TABLE IF NOT EXISTS TraderDim (
            trader_id       INTEGER PRIMARY KEY,
            wallet_address  BLOB NOT NULL UNIQUE,
            first_seen      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # HourlyPriceFact - Hourly price facts
    # Store full timestamp (date_trunc to hour) - simpler and efficient
    # DuckDB can extract date parts on-the-fly very efficiently
    conn.execute("""
        CREATE TABLE IF NOT EXISTS HourlyPriceFact (
            token_id        INTEGER NOT NULL REFERENCES TokenDim(token_id),
            snapshot_ts     TIMESTAMP NOT NULL,
            price           REAL NOT NULL,
            volume          REAL,
            PRIMARY KEY (token_id, snapshot_ts)
        )
    """)
    # Use REAL (4 bytes) instead of DOUBLE (8 bytes) - price precision is fine
    # volume = hourly volume (can sum to get 24h/daily/weekly as needed)
    # ~20 bytes per row (removed snapshot_id, combined date+hour into timestamp)
    
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_price_ts 
        ON HourlyPriceFact(snapshot_ts)
    """)
    
    # TradeFact - Trade facts (no synthetic trade_id for storage efficiency)
    # Composite primary key: same token + timestamp + price + size = same trade
    # maker_id/taker_id reference TraderDim (4 bytes each vs 42 bytes for address)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS TradeFact (
            token_id        INTEGER NOT NULL REFERENCES TokenDim(token_id),
            trade_ts        TIMESTAMP NOT NULL,
            price           REAL NOT NULL,
            size            REAL NOT NULL,
            side            BIT NOT NULL,
            maker_id        INTEGER REFERENCES TraderDim(trader_id),
            taker_id        INTEGER REFERENCES TraderDim(trader_id),
            PRIMARY KEY (token_id, trade_ts, price, size)
        )
    """)
    # side: bit (1=buy, 0=sell) for storage efficiency
    # ~29 bytes per row (was ~21 without trader tracking)
    
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_trade_ts 
        ON TradeFact(trade_ts)
    """)
    
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_trade_maker 
        ON TradeFact(maker_id)
    """)
    
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_trade_taker 
        ON TradeFact(taker_id)
    """)

    print("✓ Gold layer created")
    
    # ==================== METADATA TABLE ====================
    conn.execute("""
        CREATE TABLE IF NOT EXISTS _pipeline_metadata (
            key             TEXT PRIMARY KEY,
            value           TEXT,
            updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Record creation time
    conn.execute("""
        INSERT OR REPLACE INTO _pipeline_metadata (key, value, updated_at)
        VALUES ('db_created', ?, CURRENT_TIMESTAMP)
    """, [datetime.now().isoformat()])
    
    print("✓ Metadata table created")
    
    return conn


def populate_categories(conn: duckdb.DuckDBPyConnection) -> int:
    """
    Populate dim_category from staging data.
    Called during transform, not at DB creation.
    
    Returns:
        Number of categories
    """
    conn.execute("""
        INSERT OR IGNORE INTO dim_category (category_id, category_name)
        SELECT 
            ROW_NUMBER() OVER (ORDER BY category) as category_id,
            category as category_name
        FROM (
            SELECT DISTINCT 
                COALESCE(json_extract_string(raw_json, '$.event.category'), 'Unknown') as category
            FROM stg_markets_raw
            WHERE category IS NOT NULL
        )
    """)
    return conn.execute("SELECT COUNT(*) FROM dim_category").fetchone()[0]


def get_table_stats(conn: duckdb.DuckDBPyConnection) -> dict:
    """Get row counts for all tables"""
    tables = [
        'stg_markets_raw', 'stg_prices_raw', 'stg_trades_raw',
        'CategoryDim', 'EventDim', 'MarketDim', 'TokenDim', 'TraderDim',
        'HourlyPriceFact', 'TradeFact'
    ]
    
    stats = {}
    for table in tables:
        try:
            count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            stats[table] = count
        except:
            stats[table] = -1
    
    return stats


def print_schema_summary(conn: duckdb.DuckDBPyConnection):
    """Print summary of database schema"""
    print("\n" + "="*60)
    print("DATABASE SCHEMA SUMMARY (Snowflake)")
    print("="*60)
    
    stats = get_table_stats(conn)
    
    print("\nStaging Layer:")
    print(f"  stg_markets_raw:    {stats['stg_markets_raw']:>10,} rows")
    print(f"  stg_prices_raw:     {stats['stg_prices_raw']:>10,} rows")
    print(f"  stg_trades_raw:     {stats['stg_trades_raw']:>10,} rows")
    
    print("\nGold Layer (Lookup Dimensions):")
    print(f"  CategoryDim:        {stats['CategoryDim']:>10,} rows")
    print(f"  TraderDim:          {stats['TraderDim']:>10,} rows")
    
    print("\nGold Layer (Main Dimensions):")
    print(f"  EventDim:           {stats['EventDim']:>10,} rows")
    print(f"  MarketDim:          {stats['MarketDim']:>10,} rows")
    print(f"  TokenDim:           {stats['TokenDim']:>10,} rows")
    
    print("\nGold Layer (Facts):")
    print(f"  HourlyPriceFact:    {stats['HourlyPriceFact']:>10,} rows")
    print(f"  TradeFact:          {stats['TradeFact']:>10,} rows")
    
    # Estimate storage savings
    print("\n" + "-"*60)
    print("Storage Optimizations:")
    print("  ✓ CategoryDim: Normalized (vs repeated TEXT)")
    print("  ✓ TraderDim: BLOB addresses (20 bytes vs 42 TEXT, 8 byte FK per trade)")
    print("  ✓ outcome: bit (1 bit vs 'Yes'/'No' TEXT ~3 bytes)")
    print("  ✓ side: bit (1 bit vs 'buy'/'sell' TEXT ~4 bytes)")
    print("  ✓ No dim_date: Timestamps computed on-the-fly")
    print("  ✓ REAL prices: 4 bytes (vs DOUBLE 8 bytes)")
    print("="*60)


def estimate_storage_savings():
    """Print estimated storage savings from snowflake design"""
    print("\nEstimated Storage per TradeFact row:")
    print("  Star schema (naive):")
    print("    trade_id (INT): 4, token_id (INT): 4, trade_ts (TS): 8")
    print("    price (DOUBLE): 8, size (DOUBLE): 8, side (TEXT): 5")
    print("    outcome (TEXT): 3, date_id (INT): 4")
    print("    Total: ~44 bytes/row")
    print("")
    print("  Snowflake schema (optimized):")
    print("    trade_id (INT): 4, token_id (INT): 4, trade_ts (TS): 8")
    print("    price (REAL): 4, size (REAL): 4, side (bit): <1")
    print("    Total: ~25 bytes/row")
    print("")
    print("  Savings per 1M trades: ~44 MB → ~25 MB (43% reduction)")


if __name__ == "__main__":
    # Create database
    conn = create_database()
    
    # Show summary
    print_schema_summary(conn)
    estimate_storage_savings()
    
    conn.close()
    print(f"\n✓ Database ready at: {DEFAULT_DB_PATH}")
