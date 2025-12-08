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

from api_client import PolymarketClient, timestamp_to_unix
from create_database import DEFAULT_DB_PATH


def backfill_markets(conn: duckdb.DuckDBPyConnection, 
                     client: PolymarketClient) -> int:
    """
    Backfill all markets into stg_markets_raw.
    
    Args:
        conn: DuckDB connection
        client: Polymarket API client
        
    Returns:
        Number of markets loaded
    """
    print("\n" + "="*60)
    print("BACKFILLING MARKETS")
    print("="*60)
    
    count = 0
    batch = []
    batch_size = 50
    
    for market in client.get_all_markets(batch_size=100):
        condition_id = market.get("condition_id") or market.get("conditionId")
        if not condition_id:
            continue
        
        batch.append((condition_id, json.dumps(market), "gamma_api"))
        count += 1
        
        if len(batch) >= batch_size:
            _insert_markets_batch(conn, batch)
            print(f"  Loaded {count} markets...")
            batch = []
    
    # Insert remaining
    if batch:
        _insert_markets_batch(conn, batch)
    
    print(f"✓ Loaded {count} markets into stg_markets_raw")
    return count


def _insert_markets_batch(conn: duckdb.DuckDBPyConnection, batch: list):
    """Insert batch of markets with upsert logic"""
    for item in batch:
        conn.execute("""
            INSERT INTO stg_markets_raw (condition_id, raw_json, source)
            VALUES (?, ?, ?)
            ON CONFLICT (condition_id) DO UPDATE SET
                raw_json = excluded.raw_json
        """, item)


def backfill_prices(conn: duckdb.DuckDBPyConnection,
                    client: PolymarketClient,
                    fidelity_minutes: int = 60,
                    limit_markets: Optional[int] = None) -> int:
    """
    Backfill hourly price history for all markets.
    
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
    
    # Get all markets from staging
    markets = conn.execute("""
        SELECT condition_id, raw_json 
        FROM stg_markets_raw
    """).fetchall()
    
    if limit_markets:
        markets = markets[:limit_markets]
    
    print(f"Processing {len(markets)} markets...")
    
    total_prices = 0
    
    for i, (condition_id, raw_json) in enumerate(markets):
        market = json.loads(raw_json)
        
        # Extract tokens (YES/NO outcomes)
        tokens = market.get("tokens") or market.get("clobTokenIds") or []
        
        for token in tokens:
            if isinstance(token, dict):
                token_id = token.get("token_id") or token.get("tokenId")
                outcome = token.get("outcome", "Unknown")
            else:
                token_id = str(token)
                outcome = "Unknown"
            
            if not token_id:
                continue
            
            # Fetch price history
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


def backfill_trades_november_2024(conn: duckdb.DuckDBPyConnection,
                                   client: PolymarketClient) -> int:
    """
    Backfill trades for November 2024 only.
    
    Args:
        conn: DuckDB connection
        client: Polymarket API client
        
    Returns:
        Number of trades loaded
    """
    print("\n" + "="*60)
    print("BACKFILLING NOVEMBER 2024 TRADES")
    print("="*60)
    
    # November 2024 range
    start_ts = timestamp_to_unix(datetime(2024, 11, 1, 0, 0, 0, tzinfo=timezone.utc))
    end_ts = timestamp_to_unix(datetime(2024, 11, 30, 23, 59, 59, tzinfo=timezone.utc))
    
    print(f"Date range: 2024-11-01 to 2024-11-30")
    
    count = 0
    batch = []
    batch_size = 100
    
    for trade in client.get_trades_for_period(start_ts=start_ts, end_ts=end_ts):
        condition_id = trade.get("market") or trade.get("condition_id")
        trade_ts = trade.get("timestamp") or trade.get("matchTime")
        
        if not condition_id or not trade_ts:
            continue
        
        # Parse timestamp
        if isinstance(trade_ts, (int, float)):
            trade_dt = datetime.fromtimestamp(trade_ts, tz=timezone.utc)
        else:
            trade_dt = datetime.fromisoformat(trade_ts.replace("Z", "+00:00"))
        
        batch.append((condition_id, json.dumps(trade), trade_dt))
        count += 1
        
        if len(batch) >= batch_size:
            _insert_trades_batch(conn, batch)
            batch = []
            
            if count % 1000 == 0:
                print(f"  Loaded {count:,} trades...")
    
    # Insert remaining
    if batch:
        _insert_trades_batch(conn, batch)
    
    print(f"✓ Loaded {count:,} trades for November 2024")
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
            backfill_markets(conn, client)
        
        # Backfill prices
        if not skip_prices:
            backfill_prices(conn, client, limit_markets=limit_markets)
        
        # Backfill November 2024 trades
        if not skip_trades:
            backfill_trades_november_2024(conn, client)
        
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
