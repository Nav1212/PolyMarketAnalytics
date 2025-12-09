"""
Historical Backfill Script
Loads all markets and hourly price history into staging tables
"""

import duckdb
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
import re 
from api_client import PolymarketClient, timestamp_to_unix
from create_database import DEFAULT_DB_PATH


def backfill_markets(conn: duckdb.DuckDBPyConnection, 
                     client: PolymarketClient,
                     limit: Optional[int] = None) -> int:
    """
    Backfill markets into stg_markets_raw.
    Skips markets already in staging or gold layer.
    
    Args:
        conn: DuckDB connection
        client: Polymarket API client
        limit: Maximum number of NEW markets to load (None = all)
        
    Returns:
        Number of NEW markets loaded
    """
    print("\n" + "="*60)
    print("BACKFILLING MARKETS")
    if limit:
        print(f"  (Limited to {limit} new markets)")
    print("="*60)
    
    # Get existing market IDs from staging and gold layers
    existing_staging = set(row[0] for row in conn.execute(
        "SELECT condition_id FROM stg_markets_raw"
    ).fetchall())
    
    existing_gold = set(row[0] for row in conn.execute(
            "SELECT external_id FROM MarketDim"
        ).fetchall())
    
    existing_ids = existing_staging | existing_gold
    if len(existing_ids)>=limit:
        return 0
    print(f"  Found {len(existing_ids):,} markets already in database")
    
    batch = []
    
    for market in client.get_all_markets(batch_size=100):
        condition_id = market.get("condition_id") or market.get("conditionId")
        if not condition_id:
            continue
        
        # Skip if already exists
        if condition_id in existing_ids:
            continue
        
        batch.append((condition_id, json.dumps(market), "gamma_api"))
        existing_ids.add(condition_id)  # Track for deduplication within batch
        
        # Check limit

        
        if len(batch) >= 100:
            _insert_markets_batch(conn, batch)
            print(f"  Loaded new markets (skipped existing)...")
            batch = []
    
    # Insert remaining
    if batch:
        _insert_markets_batch(conn, batch)
    
    return 


def _insert_markets_batch(conn: duckdb.DuckDBPyConnection, batch: list):
    """Insert batch of markets with upsert logic"""
    for item in batch:
        conn.execute("""
            INSERT INTO stg_markets_raw (condition_id, raw_json, source)
            VALUES (?, ?, ?)
            ON CONFLICT (condition_id) DO UPDATE SET
                raw_json = excluded.raw_json
        """, item)


def clean_clob_token_ids(raw_ids):
    cleaned = []

    # If raw_ids is a JSON string like "[\"abc\", \"def\"]"
    if isinstance(raw_ids, str):
        try:
            parsed = json.loads(raw_ids)
            raw_ids = parsed
        except Exception:
            # If parsing fails, treat as list of characters (bad data)
            raw_ids = []

    # Ensure we have a list
    if not isinstance(raw_ids, list):
        return []

    for item in raw_ids:
        if not item:
            continue

        # Clean the item
        token = str(item).strip("[](){}\"' \n\r\t")

        # Allow alphanumeric, dash, underscore, comma
        token = re.sub(r"[^A-Za-z0-9\-,_]", "", token)

        # Skip short or empty tokens
        if len(token) < 3:
            continue

        cleaned.append(token)

    return cleaned


def backfill_prices(conn: duckdb.DuckDBPyConnection,
                    client: PolymarketClient,
                    fidelity_minutes: int = 60,
                    limit_markets: Optional[int] = None) -> int:
    """
    Backfill hourly price history for markets without price data.
    Skips markets that already have prices in staging or gold layer.
    
    Args:
        conn: DuckDB connection
        client: Polymarket API client
        fidelity_minutes: Price resolution (60 = hourly)
        limit_markets: Optional limit for testing
        
    Returns:
        Number of price records loaded
    """
    print("\n" + "="*60)
    print("BACKFILLING HOURLY PRICES")
    print("="*60)
    
    # Get markets that already have price data
    markets_with_prices_staging = set(row[0] for row in conn.execute(
        "SELECT DISTINCT condition_id FROM stg_prices_raw"
    ).fetchall())
    
    # Check gold layer
    markets_with_prices_gold = set()
    try:
        # Get condition_ids from markets that have price facts
        markets_with_prices_gold = set(row[0] for row in conn.execute("""
            SELECT DISTINCT m.external_id 
            FROM HourlyPriceFact f
            JOIN TokenDim t ON f.token_id = t.token_id
            JOIN MarketDim m ON t.market_id = m.market_id
        """).fetchall())
    except:
        pass  # Gold tables may not exist yet
    
    markets_with_prices = markets_with_prices_staging | markets_with_prices_gold
    print(f"  Found {len(markets_with_prices):,} markets already have price data")
    
    # Get all markets from staging, excluding those with prices
    markets = conn.execute("""
        SELECT condition_id, raw_json 
        FROM stg_markets_raw
        WHERE condition_id NOT IN (SELECT DISTINCT condition_id FROM stg_prices_raw)
        order by condition_id desc
    """).fetchall()
    
    if limit_markets:
        markets = markets[:limit_markets]
    
    print(f"  Processing {len(markets)} markets needing price data...")
    
    total_prices = 0
    
    for i, (condition_id, raw_json) in enumerate(markets):
        market = json.loads(raw_json)
        
        # Extract tokens - API returns parallel arrays: clobTokenIds and outcomes
        clob_token_ids = market.get("clobTokenIds") or []
        print("got clob_token ids:", clob_token_ids)
        clob_token_ids = clean_clob_token_ids(clob_token_ids)
        print("cleaned clob_token ids:", clob_token_ids)

        # Pair them up (first token = first outcome, etc.)
        for  token_id in enumerate(clob_token_ids):
            if not token_id:
                continue
            
            
            # Fetch price history
            # Use interval="max" to get all historical data
            # Note: don't use start_ts/end_ts with interval - they're mutually exclusive
            history = client.get_prices_history(
                token_id=token_id,
                interval="max",
                fidelity=fidelity_minutes
            )
            
            if history:
                # Determine time range
                timestamps = [h.get("t", 0) for h in history if h.get("t")]
                if timestamps:
                    start_ts = datetime.fromtimestamp(min(timestamps), tz=timezone.utc)
                    end_ts = datetime.fromtimestamp(max(timestamps), tz=timezone.utc)
                    
                    # Insert into staging
                    conn.execute("""
                        INSERT INTO stg_prices_raw 
                        (condition_id, token_id, raw_json, start_ts, end_ts)
                        VALUES (?, ?, ?, ?, ?)
                        ON CONFLICT (condition_id, token_id, start_ts, end_ts) 
                        DO UPDATE SET
                            raw_json = EXCLUDED.raw_json,
                            fetched_at = CURRENT_TIMESTAMP
                    """, [condition_id, token_id, json.dumps(history), start_ts, end_ts])
                    
                    total_prices += len(history)
        
        if (i + 1) % 100 == 0:
            print(f"  Processed {i + 1}/{len(markets)} markets, {total_prices:,} price points...")
    
    print(f"✓ Loaded {total_prices:,} price points into stg_prices_raw")
    return total_prices


def backfill_trades(conn: duckdb.DuckDBPyConnection,
                                   client: PolymarketClient) -> int:
    """
    Backfill trades for November 2024 only.
    Skips trades that already exist in staging or gold layer.
    
    Args:
        conn: DuckDB connection
        client: Polymarket API client
        
    Returns:
        Number of NEW trades loaded
    """
    print("\n" + "="*60)
    print("BACKFILLING NOVEMBER 2024 TRADES")
    print("="*60)
    
    start_dt = datetime(2024, 11, 1, 0, 0, 0, tzinfo=timezone.utc)
    end_dt =  datetime(2024, 11, 30, 0, 0, 0, tzinfo=timezone.utc)
    #end_dt =  datetime.now(timezone.utc)
    start_ts = timestamp_to_unix(start_dt)
    end_ts = timestamp_to_unix(end_dt)
    
    print(f"Date range: 2024-11-01 to 2024-11-30")
    
    # Build set of existing trade signatures from staging
    # Use (condition_id, timestamp) as a quick lookup key
    print("  Loading existing trade timestamps from staging...")
    existing_trades = set()
    try:
        rows = conn.execute("""
            SELECT condition_id, trade_timestamp 
            FROM stg_trades_raw 
            WHERE trade_timestamp >= ? AND trade_timestamp <= ?
        """, [start_dt, end_dt]).fetchall()
        for cid, ts in rows:
            existing_trades.add((cid, ts))
    except:
        pass
    
    # Also check gold layer - use (token_id, trade_ts, price, size) composite key
    # But we need condition_id for API matching, so build a timestamp set
    print("  Loading existing trade timestamps from gold layer...")
    try:
        rows = conn.execute("""
            SELECT m.external_id, f.trade_ts
            FROM TradeFact f
            JOIN TokenDim t ON f.token_id = t.token_id
            JOIN MarketDim m ON t.market_id = m.market_id
            WHERE f.trade_ts >= ? AND f.trade_ts <= ?
        """, [start_dt, end_dt]).fetchall()
        for cid, ts in rows:
            existing_trades.add((cid, ts))
    except:
        pass
    
    print(f"  Found {len(existing_trades):,} existing trades for this period")
    
    count = 0
    skipped = 0
    batch = []
    batch_size = 100
    
    for trade in client.get_trades_for_period(start_ts=start_ts, end_ts=end_ts):
        condition_id = trade.get("conditionId")
        print("got trade:", condition_id)
        trade_ts = trade.get("timestamp")
        
        if not condition_id or not trade_ts:
            continue
        
        # Parse timestamp
        if isinstance(trade_ts, (int, float)):
            trade_dt = datetime.fromtimestamp(trade_ts, tz=timezone.utc)
        else:
            trade_dt = datetime.fromisoformat(trade_ts.replace("Z", "+00:00"))
        
        # Skip if already exists
        trade_key = (condition_id, trade_dt)
        if trade_key in existing_trades:
            skipped += 1
            continue
        
        batch.append((condition_id, json.dumps(trade), trade_dt))
        existing_trades.add(trade_key)  # Track to avoid dupes within batch
        count += 1
        
        if len(batch) >= batch_size:
            _insert_trades_batch(conn, batch)
            batch = []
            
            if count % 10000 == 0:
                print(f"  Loaded {count:,} new trades (skipped {skipped:,} existing)...")
    
    # Insert remaining
    if batch:
        _insert_trades_batch(conn, batch)
    
    print(f"✓ Loaded {count:,} new trades (skipped {skipped:,} existing)")
    return count


def _insert_trades_batch(conn: duckdb.DuckDBPyConnection, batch: list):
    """Insert batch of trades"""
    conn.executemany("""
        INSERT INTO stg_trades_raw (condition_id, raw_json, trade_timestamp)
        VALUES (?, ?, ?)
    """, batch)


def run_backfill(db_path: str = None,
                 skip_markets: bool = False,
                 skip_prices: bool = False,
                 skip_trades: bool = False,
                 limit_markets: Optional[int] = None):
    """
    Run full historical backfill.
    
    Args:
        db_path: Path to DuckDB database (default: ../PolyMarketData/polymarket.duckdb)
        skip_markets: Skip markets backfill
        skip_prices: Skip prices backfill
        skip_trades: Skip trades backfill
        limit_markets: Limit number of markets for testing
    """
    if db_path is None:
        db_path = DEFAULT_DB_PATH
    
    print("="*60)
    print("POLYMARKET HISTORICAL BACKFILL")
    print(f"Database: {db_path}")
    print(f"Started: {datetime.now().isoformat()}")
    print("="*60)
    
    start_time = time.time()
    
    # Connect to database
    conn = duckdb.connect(db_path)
    
    # Initialize API client
    with PolymarketClient() as client:
        # Backfill markets
        if not skip_markets:
            backfill_markets(conn, client, limit=limit_markets)
        
        # Backfill prices
        if not skip_prices:
            backfill_prices(conn, client, limit_markets=limit_markets)
        
        # Backfill November 2024 trades
        if not skip_trades:
            backfill_trades(conn, client)
        
        # Print stats
        api_stats = client.get_stats()
        print(f"\nAPI Stats: {api_stats}")
    
    # Print table stats
    print("\n" + "="*60)
    print("STAGING TABLE SUMMARY")
    print("="*60)
    
    for table in ["stg_markets_raw", "stg_prices_raw", "stg_trades_raw"]:
        count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        print(f"  {table}: {count:,} rows")
    
    elapsed = time.time() - start_time
    print(f"\nTotal time: {elapsed:.1f} seconds")
    
    conn.close()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Polymarket Historical Backfill")
    parser.add_argument("--db", default=None, help="Database path (default: ../PolyMarketData/polymarket.duckdb)")
    parser.add_argument("--skip-markets", action="store_true", help="Skip markets backfill")
    parser.add_argument("--skip-prices", action="store_true", help="Skip prices backfill")
    parser.add_argument("--skip-trades", action="store_true", help="Skip trades backfill")
    parser.add_argument("--limit", type=int, help="Limit markets for testing")
    
    args = parser.parse_args()
    
    run_backfill(
        db_path=args.db,
        skip_markets=args.skip_markets,
        skip_prices=args.skip_prices,
        skip_trades=args.skip_trades,
        limit_markets=args.limit
    )
