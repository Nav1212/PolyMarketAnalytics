"""
Multithreaded Trade Scraper for Polymarket
Uses 5 workers with shared rate limiter, writes to DB as trades arrive
Triggers batch transform when staging reaches 10,000 trades
"""

import duckdb
import argparse
import json
import subprocess
import sys
from pathlib import Path
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock, Thread
from typing import List, Dict, Any, Optional
from api_client import PolymarketClient, TokenBucketLimiter, timestamp_to_unix

# Default database path
DEFAULT_DB_PATH = Path(__file__).parent.parent.parent / "PolyMarketData" / "polymarket.duckdb"


class BatchDBWriter:
    """Thread-safe batch writer with 10K threshold and async transform"""
    
    def __init__(self, db_path: str, batch_size: int = 10000):
        self.db_path = db_path
        self.batch_size = batch_size
        self.buffer = []
        self.lock = Lock()
        self.transform_lock = Lock()  # Prevents writes during transform
        self.total_written = 0
        self.total_trades = 0
        self.transform_count = 0
        
    def add_trades(self, condition_id: str, trades: List[Dict[str, Any]]):
        """Add trades to buffer and flush if threshold reached"""
        with self.lock:
            for trade in trades:
                self.buffer.append({
                    'condition_id': condition_id,
                    'raw_json': json.dumps(trade),
                    'fetched_at': datetime.now(timezone.utc)
                })
                self.total_trades += 1
            
            # Check if we should flush
            if len(self.buffer) >= self.batch_size:
                self._flush()
    
    def _flush(self):
        """Flush buffer to database (must hold lock)"""
        if not self.buffer:
            return
        
        # Wait if transform is running
        with self.transform_lock:
            conn = duckdb.connect(self.db_path)
            try:
                # Check if staging has existing data (transform may be pending)
                result = conn.execute("SELECT COUNT(*) FROM stg_trades_raw").fetchone()
                existing_count = result[0] if result else 0
                if existing_count > 0:
                    print(f"⏸ Staging has {existing_count:,} records, waiting for transform to clear...")
                    # Release lock temporarily to let transform complete
                    return  # Will retry on next flush
                
                # Insert batch
                conn.executemany("""
                    INSERT INTO stg_trades_raw (condition_id, raw_json, fetched_at)
                    VALUES (?, ?, ?)
                """, [(t['condition_id'], t['raw_json'], t['fetched_at']) for t in self.buffer])
                
                self.total_written += len(self.buffer)
                buffer_size = len(self.buffer)
                print(f"✓ Flushed {buffer_size:,} trades to staging (total: {self.total_written:,})")
                self.buffer.clear()
                
                # Trigger async transform
                Thread(target=self._run_transform, daemon=True).start()
                
            finally:
                conn.close()
    
    def _run_transform(self):
        """Run transform script in background thread"""
        with self.transform_lock:
            try:
                self.transform_count += 1
                print(f"🔄 Starting transform #{self.transform_count}...")
                
                # Run transform_gold.py
                script_path = Path(__file__).parent / "transform_gold.py"
                result = subprocess.run(
                    [sys.executable, str(script_path), "--db", self.db_path],
                    capture_output=True,
                    text=True,
                    encoding='utf-8',
                    errors='replace',
                    timeout=300  # 5 minute timeout
                )
                
                if result.returncode == 0:
                    print(f"✓ Transform #{self.transform_count} complete")
                    
                    # Truncate staging table
                    conn = duckdb.connect(self.db_path)
                    try:
                        conn.execute("DELETE FROM stg_trades_raw")
                        print(f"✓ Staging table truncated")
                    finally:
                        conn.close()
                else:
                    print(f"✗ Transform #{self.transform_count} failed: {result.stderr}")
                    
            except Exception as e:
                print(f"✗ Transform error: {e}")
    
    def flush(self):
        """Public flush method (thread-safe)"""
        with self.lock:
            self._flush()
    
    def get_stats(self) -> dict:
        """Get writer statistics"""
        with self.lock:
            return {
                'total_trades_received': self.total_trades,
                'total_written': self.total_written,
                'buffer_size': len(self.buffer)
            }


def fetch_markets(db_path: str, start_ts: Optional[int] = None, limit: Optional[int] = None) -> List[Dict[str, Any]]:
    """
    Fetch markets from database that were active during the scrape period.
    
    Args:
        db_path: Path to database
        start_ts: Start timestamp of scrape period (Unix seconds)
        limit: Optional limit on number of markets
        
    Returns:
        List of market dicts with condition_id, question, end_date_iso
    """
    conn = duckdb.connect(db_path, read_only=True)
    try:
        query = """
            SELECT 
                m.external_id as condition_id,
                m.question,
                m.end_date_iso
            FROM MarketDim m
            WHERE m.active = true
        """
        
        # If scraping historical data, filter out markets that closed before the period
        if start_ts:
            query += f"""
                AND (
                    m.end_date_iso IS NULL
                    OR m.end_date_iso >= to_timestamp({start_ts})
                )
            """
        
        if limit:
            query += f" LIMIT {limit}"
        
        markets = []
        for row in conn.execute(query).fetchall():
            markets.append({
                'condition_id': row[0],
                'question': row[1],
                'end_date_iso': row[2]
            })
        
        return markets
    finally:
        conn.close()


def scrape_market_trades(
    market: Dict[str, Any],
    client: PolymarketClient,
    writer: BatchDBWriter,
    start_ts: Optional[int] = None,
    end_ts: Optional[int] = None
) -> int:
    """
    Scrape all trades for a single market using timestamp windowing pagination.
    Writes raw JSON + condition_id to DB immediately (parsing happens in transform).
    
    Args:
        market: Dict with 'condition_id', 'question', 'end_date_iso'
        client: PolymarketClient with shared rate limiter
        writer: BatchDBWriter for thread-safe DB writes
        start_ts: Start timestamp (Unix seconds)
        end_ts: End timestamp (Unix seconds)
    
    Returns:
        Number of trades fetched
    """
    condition_id = market['condition_id']
    question = market['question'][:60]
    
    total_trades = 0
    current_start = start_ts
    page = 0
    
    try:
        while current_start is None or (end_ts is None or current_start < end_ts):
            page += 1
            
            # Fetch trades using timestamp window
            trades = client.get_trades(
                market=condition_id,
                start_ts=current_start,
                end_ts=end_ts,
                limit=500
            )
            
            if not trades or not isinstance(trades, list):
                break
            
            # Write to DB immediately (condition_id already known from market dict)
            writer.add_trades(condition_id, trades)
            total_trades += len(trades)
            
            print(f"  [{condition_id[:8]}] Page {page}: {len(trades)} trades | {question}...")
            
            # Check if more pages exist
            if len(trades) < 500:
                break
            
            # Move window forward: last timestamp + 1 second
            last_trade = trades[-1]
            if not isinstance(last_trade, dict):
                break
            last_timestamp = last_trade.get('timestamp')
            if not last_timestamp:
                break
            current_start = last_timestamp + 1
        
        return total_trades
        
    except Exception as e:
        print(f"✗ Error scraping market {condition_id}: {e}")
        return total_trades


def scrape_trades_multithreaded(
    db_path: str,
    start_ts: Optional[int] = None,
    end_ts: Optional[int] = None,
    num_workers: int = 5,
    limit_markets: Optional[int] = None
):
    """
    Scrape trades using multiple worker threads.
    
    Args:
        db_path: Path to DuckDB database
        start_ts: Start timestamp (Unix seconds)
        end_ts: End timestamp (Unix seconds)
        num_workers: Number of worker threads (default 5)
        limit_markets: Optional limit on number of markets to process
    """
    print("="*70)
    print("MULTITHREADED TRADE SCRAPER")
    print("="*70)
    print(f"Workers: {num_workers}")
    print(f"Rate limit: 7.5 req/s (shared across all workers)")
    print(f"Batch size: 10,000 trades")
    print(f"Database: {db_path}")
    
    # Clear staging table at start
    print("\nClearing staging table...")
    conn = duckdb.connect(db_path)
    try:
        conn.execute("DELETE FROM stg_trades_raw")
        print("✓ Staging table cleared")
    finally:
        conn.close()
    
    # Fetch markets
    print("\nFetching markets from database...")
    if start_ts:
        print(f"Filtering markets that were active after {datetime.fromtimestamp(start_ts, tz=timezone.utc)}")
    markets = fetch_markets(db_path, start_ts=start_ts, limit=limit_markets)
    print(f"✓ Found {len(markets):,} markets to scrape")
    
    if not markets:
        print("No markets to scrape!")
        return
    
    # Create shared rate limiter (7.5 req/s across all threads)
    shared_limiter = TokenBucketLimiter(requests_per_second=7.5)
    
    # Create batch writer
    writer = BatchDBWriter(db_path, batch_size=10000)
    
    # Start time
    start_time = datetime.now()
    total_trades = 0
    
    print(f"\nStarting scrape with {num_workers} workers...")
    print("-"*70)
    
    # Create thread pool
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        # Create client for each worker with shared rate limiter
        clients = [
            PolymarketClient(shared_data_limiter=shared_limiter) 
            for _ in range(num_workers)
        ]
        
        # Submit all market scraping tasks
        futures = []
        for i, market in enumerate(markets):
            client = clients[i % num_workers]  # Distribute markets across clients
            future = executor.submit(
                scrape_market_trades,
                market,
                client,
                writer,
                start_ts,
                end_ts
            )
            futures.append(future)
        
        # Wait for completion
        for future in as_completed(futures):
            try:
                trades_count = future.result()
                total_trades += trades_count
            except Exception as e:
                print(f"✗ Task failed: {e}")
        
        # Close all clients
        for client in clients:
            client.close()
    
    # Final flush
    print("\nFlushing remaining trades...")
    writer.flush()
    
    # Print statistics
    elapsed = (datetime.now() - start_time).total_seconds()
    writer_stats = writer.get_stats()
    limiter_stats = shared_limiter.get_stats()
    
    print("\n" + "="*70)
    print("SCRAPE COMPLETE")
    print("="*70)
    print(f"Markets processed:     {len(markets):,}")
    print(f"Total trades scraped:  {total_trades:,}")
    print(f"Trades written to DB:  {writer_stats['total_written']:,}")
    print(f"Buffer remaining:      {writer_stats['buffer_size']:,}")
    print(f"Time elapsed:          {elapsed:.1f}s")
    print(f"Avg throughput:        {total_trades/elapsed:.1f} trades/s")
    print(f"\nRate limiter stats:")
    print(f"  Current requests:    {limiter_stats['current_requests']}")
    print(f"  Max requests:        {limiter_stats['max_requests']}")
    print(f"  Window:              {limiter_stats['window_seconds']}s")
    print(f"  Rate limit:          {limiter_stats['rate_limit']} req/s")
    print("="*70)


def main():
    parser = argparse.ArgumentParser(
        description="Scrape Polymarket trades with multithreading"
    )
    parser.add_argument(
        "--db",
        default=str(DEFAULT_DB_PATH),
        help="Path to DuckDB database"
    )
    parser.add_argument(
        "--start",
        type=int,
        help="Start timestamp (Unix seconds)"
    )
    parser.add_argument(
        "--end",
        type=int,
        help="End timestamp (Unix seconds)"
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=5,
        help="Number of worker threads (default: 5)"
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Limit number of markets to process (for testing)"
    )
    parser.add_argument(
        "--november",
        action="store_true",
        help="Scrape November 2024 data"
    )
    
    args = parser.parse_args()
    
    # Handle November shortcut
    if args.november:
        start = datetime(2024, 11, 1, 0, 0, 0, tzinfo=timezone.utc)
        end = datetime(2024, 11, 30, 23, 59, 59, tzinfo=timezone.utc)
        args.start = timestamp_to_unix(start)
        args.end = timestamp_to_unix(end)
        print(f"Using November 2024 range: {start} to {end}")
    
    scrape_trades_multithreaded(
        db_path=args.db,
        start_ts=args.start,
        end_ts=args.end,
        num_workers=args.workers,
        limit_markets=args.limit
    )


if __name__ == "__main__":
    main()
