"""
Simple Trade Fetcher for Polymarket
Fetches trades for a given market and time range
"""

import httpx
from typing import List, Dict, Any, Union, Optional, Generator
from datetime import datetime
from queue import Queue, Empty
import threading
import time

from swappable_queue import SwappableQueue
from parquet_persister import (
    ParquetPersister, 
    create_trade_persisted_queue,
    load_market_parquet,
    save_cursor,
    load_cursor
)
from worker_manager import WorkerManager, get_worker_manager
from config import get_config, Config


class TradeFetcher:
    """
    Simple class to fetch trades from Polymarket Data API
    """
    
    def __init__(
        self,
        timeout: float = None,
        worker_manager: WorkerManager = None,
        config: Config = None
    ):
        """
        Initialize the trade fetcher.
        
        Args:
            timeout: Request timeout in seconds (uses config if None)
            worker_manager: WorkerManager instance for rate limiting (uses default if None)
            config: Config object (uses global config if None)
        """
        self._config = config or get_config()
        
        if timeout is None:
            timeout = self._config.api.timeout
        
        self.client = httpx.Client(
            timeout=httpx.Timeout(timeout, connect=self._config.api.connect_timeout),
            headers={
                "User-Agent": "PolymarketTradeFetcher/1.0",
                "Accept": "application/json"
            }
        )
        self._manager = worker_manager or get_worker_manager()
        self._data_api_base = self._config.api.data_api_base
    
    def close(self):
        """Close HTTP client"""
        self.client.close()
    
    def __enter__(self):
        return self
    
    def __exit__(self, *args):
        self.close()

    def fetch_price_history(market: str)->List[Dict[str, Any]]:
        """
        Fetch price history for a specific market.
        
        Args:
            market: Market condition_id (e.g., "0x123abc...")
        
        Returns:
            List of price history entries
        
        Example:
            >>> price_history = TradeFetcher.fetch_price_history("0x123abc...")
            >>> for entry in price_history:
            ...     print(entry)
        """
        start_ts = 0
        end_ts = datetime.now().timestamp()   
        while start_ts < end_ts:
            params = {
                "market": market,
                "startTs": start_ts,
                "endTs": end_ts,
                "resolution": "1m"
            }
            
            try:
                response = httpx.get(
                    f"{TradeFetcher.DATA_API_BASE}/price-history",
                    params=params,
                    timeout=30.0
                )
        
            
            except httpx.HTTPStatusError as e:
                print(f"HTTP error fetching price history: {e.response.status_code} - {e.response.text}")
                return []
            
            except httpx.RequestError as e:
                print(f"Request error fetching price history: {e}")
                return []
            
            except Exception as e:
                print(f"Unexpected error fetching price history: {e}")
                return []
        return response


    def fetch_trades(
        self,
        market: str,
        filtertype: str ="",
        filteramount: int =0,
        offset: int =0,
        limit: int = 500,
        user_id: str ="",
        loop_start: float = None
    ) -> List[Dict[str, Any]]:
        """
        Fetch trades for a specific market within a time range.
        
        Args:
            market: Market condition_id (e.g., "0x123abc...")
            start_time: Start timestamp in Unix seconds
            end_time: End timestamp in Unix seconds
            limit: Maximum number of trades per request (max 500)
            loop_start: Timestamp for rate limit timing tracking
        
        Returns:
            List of trade dictionaries
        
        Example:
            >>> fetcher = TradeFetcher()
            >>> trades = fetcher.fetch_trades(
            ...     market="0x123abc...",
            ...     start_time=1702300800,
            ...     end_time=1702387200,
            ...     limit=500
            ... )
            >>> print(f"Fetched {len(trades)} trades")
        """
        if loop_start is None:
            loop_start = time.time()
        
        params = {
            "market": market,
            "limit": limit,
            "offset": offset,
            "filterType": filtertype,
            "filterAmount": filteramount,
        }
        
        # Acquire rate limit token before making request
        self._manager.acquire_trade(loop_start)
        
        try:
            response = self.client.get(
                f"{self._data_api_base}/trades",
                params=params
            )
            response.raise_for_status()
            return response.json()
        
        except httpx.HTTPStatusError as e:
            print(f"HTTP error fetching trades: {e.response.status_code} - {e.response.text}")
            return []
        
        except httpx.RequestError as e:
            print(f"Request error fetching trades: {e}")
            return []
        
        except Exception as e:
            print(f"Unexpected error fetching trades: {e}")
            return []
    
    def _worker(
        self,
        worker_id: int,
        market_queue: Queue,
        trade_queue: Union[Queue, SwappableQueue],
        start_time: int,
        end_time: int
    ):
        """
        Worker thread that fetches trades for markets from the market queue.
        
        Args:
            worker_id: ID of this worker
            market_queue: Queue containing market IDs to process
            trade_queue: Queue to add fetched trades to (Queue or SwappableQueue)
            start_time: Start timestamp in Unix seconds
            end_time: End timestamp in Unix seconds
        """
        # Determine which put method to use based on queue type
        is_swappable = isinstance(trade_queue, SwappableQueue)
        
        while True:
            try:
                # Get market from queue (non-blocking with timeout)
                market = market_queue.get(timeout=1)
                if market is None:
                    market_queue.task_done()
                    if not is_swappable:
                        trade_queue.put(None)
                    return
                
                print(f"Worker {worker_id}: Processing market {market[:10]}...")
                
                # Fetch all trades for this market
                trade_count = 0
                offset =0 
                filteramount = 0 
                while True:
                    loop_start = time.time()
                    trades = self.fetch_trades(
                        market=market,
                        limit=500,
                        offset=offset,
                        filtertype ="CASH",
                        filteramount=filteramount,
                        loop_start=loop_start
                    )
                    
                    if not trades:
                        break
                    
                    # Add trades to the output queue
                    if is_swappable:
                        # Batch add for efficiency
                        trade_queue.put_many(trades)
                        trade_count += len(trades)
                    else:
                        for trade in trades:
                            trade_queue.put(trade)
                            trade_count += 1
                    
                    # Set floor to 50th percentile of trade sizes
                    sizes = sorted([t['size'] for t in trades])
                    median_size = sizes[len(sizes) // 2]
                    # Get the timestamp of the last trade to continue from there
                    # If we got less than the limit, we've fetched everything
                    if len(trades) < 500:
                        break
                    if offset >=1000:
                        filteramount += int(median_size)
                        offset =0
                    offset+=500

                    print(f"Worker {worker_id}: Fetched {trade_count} ")
                    # Move to the next batch (start after the last trade)
                print(f"Worker {worker_id}: Finished market {market[:10]}, total trades: {trade_count}")                
                market_queue.task_done()
            except Empty:
                # Timeout on get() → loop again, don't exit
                continue
               
            except Exception as e:
                print(f"Worker {worker_id}: Error - {e}")
                if market is not None:
                    market_queue.task_done()
                if not is_swappable:
                    trade_queue.put(None)
                return
    
    def fetch_trades_multithreaded_testing(
        self,
        market_queue: Queue = None,
        market_parquet_path: str = None,
        start_time: int = None,
        end_time: int = None,
        num_workers: int = None,
        batch_threshold: int = None,
        output_dir: str = None
    ) -> Queue:
        """
        Fetch trades for multiple markets using multiple worker threads.
        
        Args:
            market_queue: Queue containing market IDs to process (mutually exclusive with market_parquet_path)
            market_parquet_path: Path to market parquet files to load market IDs from
            start_time: Start timestamp in Unix seconds
            end_time: End timestamp in Unix seconds
            num_workers: Number of worker threads (uses config if None)
            batch_threshold: Queue threshold for parquet writes (uses config if None)
            output_dir: Output directory for trade parquets (uses config if None)
        
        Returns:
            Queue containing all fetched trades from all markets
        """
        # Use config defaults
        if num_workers is None:
            num_workers = self._config.workers.num_workers
        if batch_threshold is None:
            batch_threshold = self._config.queues.trade_threshold
        if output_dir is None:
            output_dir = self._config.output_dirs.trade
        
        # Load markets from parquet if path provided
        if market_parquet_path is not None:
            market_ids = load_market_parquet(market_parquet_path)
            market_queue = Queue()
            for market_id in market_ids:
                market_queue.put(market_id)
            print(f"Loaded {len(market_ids)} markets from parquet")
        elif market_queue is None:
            raise ValueError("Either market_queue or market_parquet_path must be provided")
        
        # Load cursor to resume from last run
        cursor = load_cursor(output_dir, self._config.cursors.filename)
        if cursor and self._config.cursors.enabled:
            print(f"[TradeFetcher] Resuming from cursor: {cursor}")
        
        # Create persisted queue for trades
        trade_queue, persister = create_trade_persisted_queue(
            threshold=batch_threshold,
            output_dir=output_dir,
            auto_start=True
        )
        threads = []        
        # Create and start worker threads
        for i in range(num_workers):
            thread = threading.Thread(
                target=self._worker,
                args=(i + 1, market_queue, trade_queue, start_time, end_time)
            )
            thread.start()
            threads.append(thread)
        # Add sentinel values to stop workers

            

        # Wait for all markets to be processed
        market_queue.join()
        for _ in range(num_workers):
            market_queue.put(None)        
        
        # Wait for all workers to finish
        for thread in threads:
            thread.join()
        
        print(f"All workers finished.")
        persister.stop()
        
        # Save cursor for next run
        if self._config.cursors.enabled:
            cursor_data = {
                "last_run": datetime.now().isoformat(),
                "start_time": start_time,
                "end_time": end_time,
                "markets_processed": market_queue.qsize() if hasattr(market_queue, 'qsize') else 0,
                "records_written": persister.stats.get("total_records_written", 0)
            }
            save_cursor(output_dir, cursor_data, self._config.cursors.filename)
        
        return trade_queue


# Example usage
if __name__ == "__main__":
    # Example: Fetch trades for a market
    with TradeFetcher() as fetcher:

        market_id = "0x1234567890abcdef1234567890abcdef12345678"
        start_ts = int(datetime(2024, 12, 1).timestamp())
        end_ts = int(datetime(2024, 12, 30).timestamp())
        
        print(f"Fetching trades for market {market_id[:10]}...")
        print(f"Time range: {start_ts} to {end_ts}")
        
        trades = fetcher.fetch_all_trades(
            market=market_id,
            start_time=start_ts,
            end_time=end_ts
        )
        
        print(f"\nFetched {trades.qsize()} total trades")
        
        if trades.qsize() == 500:
            print(f"\n500th trade:")
            # Retrieve the 500th trade (index 499) from the queue without losing data
            trades_list = []
            for _ in range(trades.qsize()):
                trades_list.append(trades.get())
            print(trades_list[499])