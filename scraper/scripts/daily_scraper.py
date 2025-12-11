"""
Daily Scraper
Appends yesterday's hourly prices to staging tables
Run via cron/scheduler daily
"""

import duckdb
import json
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from api_client import PolymarketClient, timestamp_to_unix
from create_database import DEFAULT_DB_PATH


def scrape_daily_prices(conn: duckdb.DuckDBPyConnection,
                        client: PolymarketClient,
                        target_date: datetime = None) -> int:
    """
    Scrape hourly prices for a specific date (default: yesterday).
    Skips tokens that already have price data for this date.
    
    Args:
        conn: DuckDB connection
        client: Polymarket API client
        target_date: Date to scrape (default: yesterday UTC)
        
    Returns:
        Number of price records loaded
    """
    # Default to yesterday
    if target_date is None:
        target_date = datetime.now(timezone.utc).replace(
            hour=0, minute=0, second=0, microsecond=0
        ) - timedelta(days=1)
    
    start_ts = timestamp_to_unix(target_date)
    end_ts = timestamp_to_unix(target_date + timedelta(days=1) - timedelta(seconds=1))
    start_dt = target_date
    end_dt = target_date + timedelta(days=1) - timedelta(seconds=1)
    
    print(f"Scraping prices for: {target_date.date()}")
    print(f"  Start: {start_ts} ({target_date.isoformat()})")
    print(f"  End: {end_ts}")
    
    # Get tokens that already have data for this date range
    existing_tokens = set(row[0] for row in conn.execute("""
        SELECT DISTINCT token_id FROM stg_prices_raw
        WHERE start_ts <= ? AND end_ts >= ?
    """, [end_dt, start_dt]).fetchall())
    
    # Also check gold layer
    try:
        gold_tokens = set(row[0] for row in conn.execute("""
            SELECT DISTINCT t.token_address FROM HourlyPriceFact f
            JOIN TokenDim t ON f.token_id = t.token_id
            WHERE f.snapshot_ts >= ? AND f.snapshot_ts <= ?
        """, [start_dt, end_dt]).fetchall())
        existing_tokens |= gold_tokens
    except:
        pass
    
    print(f"  Found {len(existing_tokens)} tokens already have data for this date")
    
    # Get active markets from staging
    markets = conn.execute("""
        SELECT condition_id, raw_json 
        FROM stg_markets_raw
        WHERE json_extract(raw_json, '$.active') = true
           OR json_extract(raw_json, '$.closed') = false
    """).fetchall()
    
    # Fallback: if no active filter, get all recent
    if not markets:
        markets = conn.execute("""
            SELECT condition_id, raw_json 
            FROM stg_markets_raw
            ORDER BY fetched_at DESC
            LIMIT 500
        """).fetchall()
    
    print(f"Processing {len(markets)} markets...")
    
    total_prices = 0
    skipped_tokens = 0
    
    for i, (condition_id, raw_json) in enumerate(markets):
        market = json.loads(raw_json)
        
        # Extract tokens - API returns clobTokenIds array
        clob_token_ids = market.get("clobTokenIds") or []
        
        for token_id in clob_token_ids:
            if not token_id:
                continue
            
            # Skip if already have data for this token
            if token_id in existing_tokens:
                skipped_tokens += 1
                continue
            
            # Fetch hourly prices for this day
            # Note: interval and start_ts/end_ts are mutually exclusive
            # Use start_ts/end_ts for specific date ranges
            history = client.get_prices_history(
                token_id=token_id,
                start_ts=start_ts,
                end_ts=end_ts,
                fidelity=60  # Hourly
            )
            
            if history:
                # Insert into staging
                conn.execute("""
                    INSERT INTO stg_prices_raw 
                    (condition_id, token_id, raw_json, start_ts, end_ts)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT (condition_id, token_id, start_ts, end_ts) 
                    DO UPDATE SET
                        raw_json = EXCLUDED.raw_json,
                        fetched_at = CURRENT_TIMESTAMP
                """, [
                    condition_id, 
                    token_id, 
                    json.dumps(history), 
                    target_date,
                    target_date + timedelta(days=1)
                ])
                
                total_prices += len(history)
                existing_tokens.add(token_id)  # Track to avoid re-fetching
        
        if (i + 1) % 100 == 0:
            print(f"  Processed {i + 1}/{len(markets)} markets...")
    
    print(f"✓ Loaded {total_prices:,} price points for {target_date.date()} (skipped {skipped_tokens} tokens with existing data)")
    return total_prices


def update_market_metadata(conn: duckdb.DuckDBPyConnection,
                           client: PolymarketClient) -> int:
    """
    Refresh market metadata (catch new markets, update status).
    
    Args:
        conn: DuckDB connection
        client: Polymarket API client
        
    Returns:
        Number of markets updated
    """
    print("\nRefreshing market metadata...")
    
    count = 0
    batch = []
    
    for market in client.get_all_markets(batch_size=100):
        condition_id = market.get("condition_id") or market.get("conditionId")
        if not condition_id:
            continue
        
        batch.append((condition_id, json.dumps(market), "gamma_api"))
        count += 1
        
        if len(batch) >= 50:
            conn.executemany("""
                INSERT INTO stg_markets_raw (condition_id, raw_json, source)
                VALUES (?, ?, ?)
                ON CONFLICT (condition_id) DO UPDATE SET
                    raw_json = EXCLUDED.raw_json,
                    fetched_at = CURRENT_TIMESTAMP
            """, batch)
            batch = []
    
    if batch:
        conn.executemany("""
            INSERT INTO stg_markets_raw (condition_id, raw_json, source)
            VALUES (?, ?, ?)
            ON CONFLICT (condition_id) DO UPDATE SET
                raw_json = EXCLUDED.raw_json,
                fetched_at = CURRENT_TIMESTAMP
        """, batch)
    
    print(f"✓ Refreshed {count} markets")
    return count


def run_daily_scrape(db_path: str = None,
                     target_date: datetime = None,
                     refresh_markets: bool = True):
    """
    Run daily scraping job.
    
    Args:
        db_path: Path to DuckDB database (default: ../PolyMarketData/polymarket.duckdb)
        target_date: Date to scrape (default: yesterday)
        refresh_markets: Also refresh market metadata
    """
    if db_path is None:
        db_path = DEFAULT_DB_PATH
    
    print("="*60)
    print("POLYMARKET DAILY SCRAPER")
    print(f"Database: {db_path}")
    print(f"Started: {datetime.now().isoformat()}")
    print("="*60)
    
    start_time = time.time()
    
    conn = duckdb.connect(db_path)
    
    with PolymarketClient() as client:
        # Refresh market metadata first
        if refresh_markets:
            update_market_metadata(conn, client)
        
        # Scrape yesterday's prices
        scrape_daily_prices(conn, client, target_date)
        
        print(f"\nAPI Stats: {client.get_stats()}")
    
    # Record last run
    conn.execute("""
        INSERT OR REPLACE INTO _pipeline_metadata (key, value, updated_at)
        VALUES ('last_daily_scrape', ?, CURRENT_TIMESTAMP)
    """, [datetime.now(timezone.utc).isoformat()])
    
    elapsed = time.time() - start_time
    print(f"\nTotal time: {elapsed:.1f} seconds")
    
    conn.close()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Polymarket Daily Scraper")
    parser.add_argument("--db", default=None, help="Database path (default: ../PolyMarketData/polymarket.duckdb)")
    parser.add_argument("--date", help="Target date (YYYY-MM-DD), default: yesterday")
    parser.add_argument("--no-refresh", action="store_true", help="Skip market metadata refresh")
    
    args = parser.parse_args()
    
    target = None
    if args.date:
        target = datetime.strptime(args.date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    
    run_daily_scrape(
        db_path=args.db,
        target_date=target,
        refresh_markets=not args.no_refresh
    )
