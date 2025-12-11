"""
Main entry point for the Polymarket Trade Fetcher
"""

from datetime import datetime
from queue import Queue
from trade_fetcher import TradeFetcher
from concurrent.futures import ThreadPoolExecutor
import duckdb
from pathlib import Path

# Database path
DEFAULT_DATA_DIR = Path(__file__).parent.parent.parent / "PolyMarketData"
DEFAULT_DB_PATH = str(DEFAULT_DATA_DIR / "polymarket.duckdb")


def get_inactive_markets_from_db(time: datetime, limit: int = None ):
    """
    Query inactive market IDs from DuckDB silver layer (MarketDim).
    
    Args:
        db_path: Path to DuckDB database (default uses DEFAULT_DB_PATH)
        limit: Optional limit on number of markets to return
    
    Returns:
        List of market external_ids (condition_ids)
    """
    db_path = DEFAULT_DB_PATH
    
    
    query = """
        SELECT external_id 
        FROM MarketDim 
        where end_date_iso < ?
        ORDER BY end_date_iso desc
    """
    params = [time]

    if limit:
        query += " LIMIT ?"
        params.append(limit)
    with duckdb.connect(db_path, read_only=True) as conn:
        result = conn.execute(query, params).fetchall()
    
    market_ids = [row[0] for row in result]
    return market_ids


def fetch_trades(market_id: str, start_time: int, end_time: int):
    """
    Fetch trades for a single market and time range.
    
    Args:
        market_id: Market condition_id
        start_time: Start timestamp in Unix seconds
        end_time: End timestamp in Unix seconds
    
    Returns:
        Queue containing all fetched trades
    """
    with TradeFetcher() as fetcher:
        print(f"Fetching trades for market: {market_id}")
        print(f"Time range: {start_time} to {end_time}")
        
        trade_queue = fetcher.fetch_all_trades(
            market=market_id,
            start_time=start_time,
            end_time=end_time
        )
        
        print(f"✓ Fetched {trade_queue.qsize()} trades")
        return trade_queue


def fetch_trades_multimarket(market_ids: list, start_time: int, end_time: int, num_workers: int = 5):
    """
    Fetch trades for multiple markets using worker threads.
    
    Args:
        market_ids: List of market condition_ids
        start_time: Start timestamp in Unix seconds
        end_time: End timestamp in Unix seconds
        num_workers: Number of worker threads (default 5)
    
    Returns:
        Queue containing all fetched trades from all markets
    """
    # Enqueue all markets
    market_queue = Queue()
    for market_id in market_ids:
        market_queue.put(market_id)
    
    print(f"Enqueued {len(market_ids)} markets for processing")
    print(f"Using {num_workers} workers")
    print(f"Time range: {start_time} to {end_time}")
    print("=" * 60)
    
    # Fetch trades using multiple workers
    with TradeFetcher() as fetcher:
        trade_queue = fetcher.fetch_trades_multithreaded_testing(
            market_queue=market_queue,
            start_time=start_time,
            end_time=end_time,
            num_workers=num_workers
        )
    
    return trade_queue


def main():
    """
    Main function to demonstrate multi-market trade fetching
    """

        # Set time range
        #i use a time range that covers all of polymarket history
    end_time = datetime.now()
    start_time = datetime(2000, 1, 1)
    # Query active market IDs from DuckDB silver layer limiting to 5 for testing 
    print("Querying inactive markets from DuckDB...")
    market_ids = get_inactive_markets_from_db(end_time, limit=5)
    
    if not market_ids:
        print("No inactive markets found in database!")
        return
    
    print(f"Found {len(market_ids)} active markets")
    

    
    # Convert to Unix timestamps
    start_ts = int(start_time.timestamp())
    end_ts = int(end_time.timestamp())
    
    print("=" * 60)
    print("Polymarket Multi-Market Trade Fetcher")
    print("=" * 60)
    print(f"Markets: {len(market_ids)}")
    print(f"Start Time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"End Time: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    print()
    
    # Fetch trades using multiple workers
    trade_queue = fetch_trades_multimarket(
        market_ids=market_ids,
        start_time=start_ts,
        end_time=end_ts,
        num_workers=5
    )
    
    # Display sample trades if available
    if not trade_queue.empty():
        print("\n" + "=" * 60)
        print("Sample Trades (first 3):")
        print("=" * 60)
        
        sample_count = min(3, trade_queue.qsize())
        for i in range(sample_count):
            trade = trade_queue.get()
            print(f"\nTrade {i + 1}:")
            for key, value in trade.items():
                print(f"  {key}: {value}")
    else:
        print("\nNo trades found for the specified time range.")


if __name__ == "__main__":
    main()
