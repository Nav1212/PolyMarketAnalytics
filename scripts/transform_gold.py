"""
Gold Layer Transformation
SQL scripts to transform staging JSON into snowflake schema

This module provides transformation functions that YOU can customize.
The staging layer stores raw JSON - you parse it into normalized dimensions.

Snowflake Schema Flow:
  stg_markets_raw -> CategoryDim -> EventDim -> MarketDim -> TokenDim
  stg_prices_raw  -> HourlyPriceFact (with timestamp, no date FK)
  stg_trades_raw  -> TraderDim -> TradeFact (with maker/taker FKs)

Storage Optimizations:
  - outcome: bit (1=YES, 0=NO) instead of TEXT or FK
  - side: bit (1=buy, 0=sell) instead of TEXT
  - maker/taker: INTEGER FK to TraderDim (8 bytes vs 84 bytes for addresses)
  - price/size: REAL (4 bytes) instead of DOUBLE (8 bytes)
  - No dim_date: timestamps stored directly
"""

import duckdb
import json
from datetime import datetime
from typing import Optional

from create_database import DEFAULT_DB_PATH


def transform_categories(conn: duckdb.DuckDBPyConnection) -> int:
    """
    Transform stg_markets_raw → CategoryDim
    
    Extracts unique categories from market metadata.
    Categories are normalized to save storage (FK instead of repeated text).
    """
    print("Transforming categories...")
    
    result = conn.execute("""
        INSERT INTO CategoryDim (category_id, category_name)
        SELECT DISTINCT
            ROW_NUMBER() OVER (ORDER BY category) as category_id,
            category
        FROM (
            SELECT DISTINCT COALESCE(
                json_extract_string(raw_json, '$.event.category'),
                json_extract_string(raw_json, '$.category'),
                'Unknown'
            ) as category
            FROM stg_markets_raw
        )
        WHERE category NOT IN (SELECT category_name FROM CategoryDim)
    """)
    
    count = conn.execute("SELECT changes()").fetchone()[0]
    print(f"✓ Inserted {count} categories into CategoryDim")
    return count


def transform_events(conn: duckdb.DuckDBPyConnection) -> int:
    """
    Transform stg_markets_raw → EventDim
    
    Extracts unique events from market metadata.
    Links to CategoryDim via category_id (not category text).
    """
    print("Transforming events...")
    
    # DuckDB can query JSON directly - now uses category_id FK
    result = conn.execute("""
        INSERT INTO EventDim (slug, title, description, category_id, end_date)
        SELECT DISTINCT
            COALESCE(
                json_extract_string(raw_json, '$.event.slug'),
                json_extract_string(raw_json, '$.eventSlug'),
                'unknown-' || condition_id
            ) as slug,
            COALESCE(
                json_extract_string(raw_json, '$.event.title'),
                json_extract_string(raw_json, '$.eventTitle'),
                json_extract_string(raw_json, '$.question')
            ) as title,
            json_extract_string(raw_json, '$.event.description') as description,
            dc.category_id,
            TRY_CAST(
                json_extract_string(raw_json, '$.event.endDate') AS TIMESTAMP
            ) as end_date
        FROM stg_markets_raw s
        LEFT JOIN CategoryDim dc ON dc.category_name = COALESCE(
            json_extract_string(s.raw_json, '$.event.category'),
            json_extract_string(s.raw_json, '$.category'),
            'Unknown'
        )
        WHERE NOT EXISTS (
            SELECT 1 FROM EventDim e 
            WHERE e.slug = COALESCE(
                json_extract_string(s.raw_json, '$.event.slug'),
                json_extract_string(s.raw_json, '$.eventSlug'),
                'unknown-' || s.condition_id
            )
        )
    """)
    
    count = conn.execute("SELECT changes()").fetchone()[0]
    print(f"✓ Inserted {count} events into EventDim")
    return count


def transform_markets(conn: duckdb.DuckDBPyConnection) -> int:
    """
    Transform stg_markets_raw → MarketDim
    
    Links markets to events, extracts market-level attributes.
    """
    print("Transforming markets...")
    
    result = conn.execute("""
        INSERT INTO MarketDim (condition_id, event_id, question, active, end_date_iso)
        SELECT 
            s.condition_id,
            e.event_id,
            COALESCE(
                json_extract_string(s.raw_json, '$.question'),
                json_extract_string(s.raw_json, '$.title'),
                'Unknown'
            ) as question,
            COALESCE(
                json_extract(s.raw_json, '$.active')::BOOLEAN,
                NOT json_extract(s.raw_json, '$.closed')::BOOLEAN,
                TRUE
            ) as active,
            TRY_CAST(
                json_extract_string(s.raw_json, '$.endDate') AS TIMESTAMP
            ) as end_date_iso
        FROM stg_markets_raw s
        LEFT JOIN EventDim e ON e.slug = COALESCE(
            json_extract_string(s.raw_json, '$.event.slug'),
            json_extract_string(s.raw_json, '$.eventSlug'),
            'unknown-' || s.condition_id
        )
        WHERE NOT EXISTS (
            SELECT 1 FROM MarketDim m WHERE m.condition_id = s.condition_id
        )
    """)
    
    count = conn.execute("SELECT changes()").fetchone()[0]
    print(f"✓ Inserted {count} markets into MarketDim")
    return count


def transform_tokens(conn: duckdb.DuckDBPyConnection) -> int:
    """
    Transform stg_markets_raw → TokenDim
    
    Extracts YES/NO tokens from market tokens array.
    Uses outcome as bit (1=YES, 0=NO) for storage efficiency.
    """
    print("Transforming tokens...")
    
    # Unnest JSON array, convert outcome to bit (1=YES, 0=NO)
    result = conn.execute("""
        INSERT INTO TokenDim (token_address, market_id, outcome)
        SELECT DISTINCT
            t.token_address,
            m.market_id,
            CASE UPPER(t.outcome)
                WHEN 'YES' THEN 1
                WHEN 'NO' THEN 0
                ELSE NULL
            END as outcome
        FROM stg_markets_raw s
        JOIN MarketDim m ON m.condition_id = s.condition_id
        CROSS JOIN LATERAL (
            SELECT 
                json_extract_string(token.value, '$.token_id') as token_address,
                json_extract_string(token.value, '$.outcome') as outcome
            FROM json_each(json_extract(s.raw_json, '$.tokens')) as token
            WHERE json_extract_string(token.value, '$.token_id') IS NOT NULL
        ) t
        WHERE t.token_address IS NOT NULL
          AND NOT EXISTS (
              SELECT 1 FROM TokenDim dt WHERE dt.token_address = t.token_address
          )
    """)
    
    count = conn.execute("SELECT changes()").fetchone()[0]
    print(f"✓ Inserted {count} tokens into TokenDim")
    return count


def transform_prices(conn: duckdb.DuckDBPyConnection) -> int:
    """
    Transform stg_prices_raw → HourlyPriceFact
    
    Unnests JSON price arrays into hourly fact records.
    Uses snapshot_hour TIMESTAMP directly (no dim_date FK for efficiency).
    """
    print("Transforming prices...")
    
    result = conn.execute("""
        INSERT INTO HourlyPriceFact (token_id, snapshot_hour, price)
        SELECT DISTINCT
            dt.token_id,
            date_trunc('hour', to_timestamp(p.ts)) as snapshot_hour,
            p.price::REAL
        FROM stg_prices_raw s
        JOIN TokenDim dt ON dt.token_address = s.token_id
        CROSS JOIN LATERAL (
            SELECT 
                json_extract(point.value, '$.t')::BIGINT as ts,
                json_extract(point.value, '$.p')::DOUBLE as price
            FROM json_each(s.raw_json) as point
            WHERE json_extract(point.value, '$.t') IS NOT NULL
        ) p
        WHERE p.price IS NOT NULL
          AND NOT EXISTS (
              SELECT 1 FROM HourlyPriceFact fps 
              WHERE fps.token_id = dt.token_id 
                AND fps.snapshot_hour = date_trunc('hour', to_timestamp(p.ts))
          )
    """)
    
    count = conn.execute("SELECT changes()").fetchone()[0]
    print(f"✓ Inserted {count} price snapshots into HourlyPriceFact")
    return count


def transform_traders(conn: duckdb.DuckDBPyConnection) -> int:
    """
    Transform stg_trades_raw → TraderDim
    
    Extracts unique wallet addresses (maker/taker) from trades.
    Converts hex addresses to BLOB (20 bytes vs 42 bytes TEXT).
    Must run before transform_trades().
    """
    print("Transforming traders...")
    
    # Extract unique maker/taker addresses, convert hex to BLOB
    # decode(substring(addr, 3), 'hex') removes '0x' prefix and converts to bytes
    conn.execute("""
        INSERT INTO TraderDim (wallet_address)
        SELECT DISTINCT decode(substring(wallet, 3), 'hex') FROM (
            SELECT json_extract_string(raw_json, '$.maker') as wallet
            FROM stg_trades_raw
            WHERE json_extract_string(raw_json, '$.maker') IS NOT NULL
              AND length(json_extract_string(raw_json, '$.maker')) = 42
            UNION
            SELECT json_extract_string(raw_json, '$.taker') as wallet
            FROM stg_trades_raw
            WHERE json_extract_string(raw_json, '$.taker') IS NOT NULL
              AND length(json_extract_string(raw_json, '$.taker')) = 42
        )
        WHERE wallet IS NOT NULL
          AND decode(substring(wallet, 3), 'hex') NOT IN (SELECT wallet_address FROM TraderDim)
    """)
    
    count = conn.execute("SELECT changes()").fetchone()[0]
    print(f"✓ Inserted {count} traders into TraderDim")
    return count


def transform_trades(conn: duckdb.DuckDBPyConnection) -> int:
    """
    Transform stg_trades_raw → TradeFact
    
    Parses trade JSON into fact records.
    Uses trade_ts TIMESTAMP, side as bit (1=buy, 0=sell).
    Links maker/taker via TraderDim FKs (addresses stored as BLOB).
    """
    print("Transforming trades...")
    
    result = conn.execute("""
        INSERT INTO TradeFact (token_id, trade_ts, price, size, side, maker_id, taker_id)
        SELECT 
            dt.token_id,
            s.trade_timestamp as trade_ts,
            json_extract(s.raw_json, '$.price')::REAL as price,
            json_extract(s.raw_json, '$.size')::REAL as size,
            CASE UPPER(COALESCE(json_extract_string(s.raw_json, '$.side'), ''))
                WHEN 'BUY' THEN 1
                WHEN 'SELL' THEN 0
                ELSE 0
            END as side,
            maker.trader_id as maker_id,
            taker.trader_id as taker_id
        FROM stg_trades_raw s
        JOIN MarketDim m ON m.external_id = s.condition_id
        JOIN TokenDim dt ON dt.market_id = m.market_id
            AND dt.outcome = CASE 
                WHEN UPPER(json_extract_string(s.raw_json, '$.outcome')) = 'YES' THEN 1
                WHEN UPPER(json_extract_string(s.raw_json, '$.outcome')) = 'NO' THEN 0
                ELSE dt.outcome
            END
        LEFT JOIN TraderDim maker ON maker.wallet_address = decode(substring(json_extract_string(s.raw_json, '$.maker'), 3), 'hex')
        LEFT JOIN TraderDim taker ON taker.wallet_address = decode(substring(json_extract_string(s.raw_json, '$.taker'), 3), 'hex')
        WHERE json_extract(s.raw_json, '$.price') IS NOT NULL
    """)
    
    count = conn.execute("SELECT changes()").fetchone()[0]
    print(f"✓ Inserted {count} trades into TradeFact")
    return count


def cleanup_staging(conn: duckdb.DuckDBPyConnection,
                    older_than_days: int = 30) -> dict:
    """
    Delete staging records that have been transformed.
    
    Only deletes records older than specified days that have
    corresponding gold layer records.
    
    Args:
        conn: DuckDB connection
        older_than_days: Delete staging records older than this
        
    Returns:
        Dict with counts of deleted records per table
    """
    print(f"\nCleaning up staging (records > {older_than_days} days old)...")
    
    cutoff = datetime.now().isoformat()
    
    deleted = {}
    
    # Clean prices staging (where we have corresponding fact records)
    conn.execute("""
        DELETE FROM stg_prices_raw
        WHERE fetched_at < CURRENT_TIMESTAMP - INTERVAL ? DAY
          AND EXISTS (
              SELECT 1 FROM TokenDim dt 
              WHERE dt.token_address = stg_prices_raw.token_id
          )
    """, [older_than_days])
    deleted['stg_prices_raw'] = conn.execute("SELECT changes()").fetchone()[0]
    
    # Clean trades staging
    conn.execute("""
        DELETE FROM stg_trades_raw
        WHERE fetched_at < CURRENT_TIMESTAMP - INTERVAL ? DAY
          AND EXISTS (
              SELECT 1 FROM MarketDim m 
              WHERE m.condition_id = stg_trades_raw.condition_id
          )
    """, [older_than_days])
    deleted['stg_trades_raw'] = conn.execute("SELECT changes()").fetchone()[0]
    
    # Markets staging we keep (reference data)
    deleted['stg_markets_raw'] = 0
    
    for table, count in deleted.items():
        if count > 0:
            print(f"  Deleted {count} rows from {table}")
    
    return deleted


def run_full_transform(db_path: str = None,
                       cleanup: bool = True,
                       cleanup_days: int = 30):
    """
    Run full transformation pipeline: staging → gold (snowflake schema).
    
    Args:
        db_path: Path to DuckDB database (default: ../PolyMarketData/polymarket.duckdb)
        cleanup: Delete old staging records after transform
        cleanup_days: Days to keep staging records
    """
    if db_path is None:
        db_path = DEFAULT_DB_PATH
    
    print("="*60)
    print("GOLD LAYER TRANSFORMATION (Snowflake Schema)")
    print(f"Database: {db_path}")
    print(f"Started: {datetime.now().isoformat()}")
    print("="*60)
    
    conn = duckdb.connect(db_path)
    
    # Transform in dependency order (snowflake: lookup tables first)
    transform_categories(conn)  # Must be before events
    transform_events(conn)
    transform_markets(conn)
    transform_tokens(conn)
    transform_prices(conn)
    transform_traders(conn)  # Must be before trades
    transform_trades(conn)
    
    # Cleanup staging
    if cleanup:
        cleanup_staging(conn, cleanup_days)
    
    # Print summary
    print("\n" + "="*60)
    print("GOLD LAYER SUMMARY")
    print("="*60)
    
    for table in ["CategoryDim", "EventDim", "MarketDim", 
                  "TokenDim", "TraderDim", "HourlyPriceFact", "TradeFact"]:
        count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        print(f"  {table}: {count:,} rows")
    
    conn.close()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Gold Layer Transformation")
    parser.add_argument("--db", default=None, help="Database path (default: ../PolyMarketData/polymarket.duckdb)")
    parser.add_argument("--no-cleanup", action="store_true", help="Skip staging cleanup")
    parser.add_argument("--cleanup-days", type=int, default=30, help="Days to retain staging")
    
    args = parser.parse_args()
    
    run_full_transform(
        db_path=args.db,
        cleanup=not args.no_cleanup,
        cleanup_days=args.cleanup_days
    )
