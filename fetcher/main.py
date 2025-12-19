"""
Main entry point for the Polymarket Trade Fetcher

Supports cursor persistence for resumable fetching:
- On interrupt/shutdown, cursors are saved to cursor.json
- On startup, if cursor.json exists, fetching resumes from last position
"""

import argparse
import signal
import sys
from datetime import datetime
from queue import Queue
from typing import Optional
from trade_fetcher import TradeFetcher
from worker_manager import WorkerManager
from coordinator import FetcherCoordinator, LoadOrder
from config import get_config
from cursor_manager import get_cursor_manager
import duckdb
from pathlib import Path
import time

# Database path
DEFAULT_DATA_DIR = Path(__file__).parent.parent.parent / "PolyMarketData"
DEFAULT_DB_PATH = str(DEFAULT_DATA_DIR / "polymarket.duckdb")


def get_inactive_markets_from_db(time: datetime, limit: Optional[int] = None):
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
        where end_date_iso < ? and VolumeNum !='0'
        ORDER BY end_date_iso desc
    """
    params: list = [time]

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
        List of trades
    """
    with TradeFetcher() as fetcher:
        print(f"Fetching trades for market: {market_id}")
        print(f"Time range: {start_time} to {end_time}")
        
        trades = fetcher.fetch_trades(
            market=market_id,
            limit=500
        )
        
        print(f"✓ Fetched {len(trades)} trades")
        return trades


def fetch_trades_multimarket(market_ids: list, start_time: int, end_time: int, num_workers: Optional[int] = None):
    """
    Fetch trades for multiple markets using the coordinator.
    
    Args:
        market_ids: List of market condition_ids
        start_time: Start timestamp in Unix seconds
        end_time: End timestamp in Unix seconds
        num_workers: Number of worker threads (uses config if None)
    
    Returns:
        SwappableQueue containing all fetched trades from all markets
    """
    config = get_config()
    coordinator = FetcherCoordinator(config=config)
    
    return coordinator.run_trades(
        market_ids=market_ids,
        num_workers=num_workers
    )


def main():
    """
    Main function to run the Polymarket fetcher coordinator.
    
    Modes:
        all     - Run all fetchers (markets → trades/prices/leaderboard)
        trades  - Run only trade fetcher for inactive markets
        markets - Run only market fetcher
    
    Cursor Persistence:
        - Automatically resumes from last position if cursor.json exists
        - Use --fresh to ignore existing cursors and start fresh
        - On interrupt (Ctrl+C), cursors are saved for later resume
    """
    parser = argparse.ArgumentParser(description="Polymarket Data Fetcher")
    parser.add_argument(
        "--mode",
        choices=["all", "trades", "markets", "prices", "leaderboard"],
        default="all",
        help="Fetch mode: all (full pipeline), trades, markets, prices, or leaderboard"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit number of markets to process (for testing)"
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=None,
        help="Timeout in seconds to wait for completion"
    )
    parser.add_argument(
        "--fresh",
        action="store_true",
        help="Ignore existing cursors and start a fresh fetch"
    )
    args = parser.parse_args()
    
    config = get_config()
    
    # Handle --fresh flag: clear cursors before starting
    if args.fresh:
        cursor_manager = get_cursor_manager()
        cursor_manager.clear_cursors()
        print("Cleared existing cursors, starting fresh fetch.")
    
    # Check for existing cursors
    cursor_manager = get_cursor_manager()
    cursor_manager.load_cursors()
    if cursor_manager.has_progress:
        print("\n" + "=" * 60)
        print("RESUMING FROM PREVIOUS PROGRESS")
        print("=" * 60)
        cursors = cursor_manager.cursors
        if not cursors.trades.is_empty():
            print(f"  Trades: {len(cursors.trades.pending_markets)} markets pending")
            if cursors.trades.market:
                print(f"    Current: {cursors.trades.market[:16]}... (offset={cursors.trades.offset})")
        if not cursors.prices.is_empty():
            print(f"  Prices: {len(cursors.prices.pending_tokens)} tokens pending")
        if not cursors.leaderboard.is_empty():
            print(f"  Leaderboard: category_idx={cursors.leaderboard.current_category_index}, period_idx={cursors.leaderboard.current_time_period_index}")
            print(f"    Offset: {cursors.leaderboard.current_offset}, Completed: {cursors.leaderboard.completed}")
        if not cursors.markets.is_empty():
            print(f"  Markets: cursor={cursors.markets.next_cursor[:20] if cursors.markets.next_cursor else 'None'}..., completed={cursors.markets.completed}")
        print("=" * 60 + "\n")
    
    config = get_config()
    coordinator = FetcherCoordinator(config=config)
    
    # Set time range
    end_time = datetime.now()
    start_time = datetime(2000, 1, 1)
    start_ts = int(start_time.timestamp())
    end_ts = int(end_time.timestamp())
    
    print("=" * 60)
    print("Polymarket Data Fetcher - Coordinator Mode")
    print("=" * 60)
    print(f"Mode: {args.mode}")
    print(f"Load Order: {coordinator.load_order}")
    print(f"Start Time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"End Time: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    print()
    
    try:
        if args.mode == "all":
            # Run full pipeline: Markets → Trades/Prices/Leaderboard
            print("Starting full fetch pipeline...")
            print(f"  Market Workers: {config.workers.market}")
            print(f"  Trade Workers: {config.workers.trade}")
            print(f"  Price Workers: {config.workers.price}")
            print(f"  Leaderboard Workers: {config.workers.leaderboard}")
            print()
            
            queues = coordinator.run_all(
                start_time=start_ts,
                end_time=end_ts,
                use_swappable=True
            )
            
            print("All fetchers started. Waiting for completion...")
            completed = coordinator.wait_for_completion(timeout=args.timeout)
            
            if completed:
                print("\n✓ All fetchers completed successfully")
            else:
                print("\n⚠ Timeout reached, some fetchers may still be running")
            
            # Print queue sizes
            print("\nQueue Summary:")
            for name, queue in queues.items():
                print(f"  {name}: {queue.qsize()} items")
        
        elif args.mode == "trades":
            # Query inactive markets and fetch trades
            print("Querying inactive markets from DuckDB...")
            market_ids = get_inactive_markets_from_db(end_time, limit=args.limit)
            
            if not market_ids:
                print("No inactive markets found in database!")
                return
            
            print(f"Found {len(market_ids)} inactive markets")
            print(f"Trade Workers: {config.workers.trade}")
            print()
            
            trade_queue = coordinator.run_trades(
                market_ids=market_ids,
            )
            
            print("Trade fetcher started. Waiting for completion...")
            completed = coordinator.wait_for_completion(timeout=args.timeout)
            
            if completed:
                print(f"\n✓ Fetched {trade_queue.qsize()} trades")
            else:
                print(f"\n⚠ Timeout reached, {trade_queue.qsize()} trades fetched so far")
            
            # Display sample trades
            if trade_queue.qsize() > 0:
                print("\n" + "=" * 60)
                print("Sample Trades (first 3):")
                print("=" * 60)
                
                sample_count = min(3, trade_queue.qsize())
                for i in range(sample_count):
                    trade = trade_queue.get()
                    print(f"\nTrade {i + 1}:")
                    for key, value in list(trade.items())[:5]:
                        print(f"  {key}: {value}")
        
        elif args.mode == "markets":
            print(f"Market Workers: {config.workers.market}")
            print()
            
            market_queue = coordinator.run_markets()
            
            print("Market fetcher started. Waiting for completion...")
            completed = coordinator.wait_for_completion(timeout=args.timeout)
            
            if completed:
                print(f"\n✓ Fetched {market_queue.qsize()} markets")
            else:
                print(f"\n⚠ Timeout reached, {market_queue.qsize()} markets fetched so far")
        
        elif args.mode == "prices":
            # Need token IDs - get from markets first or DB
            print("Querying markets to get token IDs...")
            # For now, demonstrate with a placeholder
            print("Price fetching requires token IDs. Use --mode=all for full pipeline.")
            return
        
        elif args.mode == "leaderboard":
            print("Querying inactive markets from DuckDB...")
            market_ids = get_inactive_markets_from_db(end_time, limit=args.limit)
            
            if not market_ids:
                print("No inactive markets found in database!")
                return
            
            print(f"Found {len(market_ids)} markets for leaderboard fetch")
            print(f"Leaderboard Workers: {config.workers.leaderboard}")
            print()
            
            leaderboard_queue = coordinator.run_leaderboard()
            
            print("Leaderboard fetcher started. Waiting for completion...")
            completed = coordinator.wait_for_completion(timeout=args.timeout)
            
            if completed:
                print(f"\n✓ Fetched {leaderboard_queue.qsize()} leaderboard entries")
            else:
                print(f"\n⚠ Timeout reached, {leaderboard_queue.qsize()} entries fetched so far")
        
        # Print rate limit timing statistics
        print("\n" + "=" * 60)
        print("Rate Limit Statistics:")
        print("=" * 60)
        coordinator.print_statistics()
    
    except KeyboardInterrupt:
        print("\n\nInterrupted by user. Cursors have been saved.")
        print("Run the program again to resume from the last position.")
        print("Use --fresh flag to start a new fetch instead.")
        sys.exit(1)


if __name__ == "__main__":
    main()