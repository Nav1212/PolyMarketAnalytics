"""
Price Fetcher for Polymarket
Fetches price history for markets from the Data API
"""

import httpx
from typing import List, Dict, Any, Union, Optional
from datetime import datetime
from queue import Queue, Empty
import threading
import time

from swappable_queue import SwappableQueue
from parquet_persister import (
    ParquetPersister,
    create_price_persisted_queue,
    load_market_parquet,
    save_cursor,
    load_cursor
)
from worker_manager import WorkerManager, get_worker_manager
from config import get_config, Config


class PriceFetcher:
    """
    Fetches price history from Polymarket Data API
    """
    
    def __init__(
        self,
        timeout: float = None,
        worker_manager: WorkerManager = None,
        config: Config = None
    ):
        """
        Initialize the price fetcher.
        
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
                "User-Agent": "PolymarketPriceFetcher/1.0",
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
    
    def fetch_price_history(
        self,
        token_id: str,
        start_ts: int = None,
        end_ts: int = None,
        resolution: str = "1m",
        loop_start: float = None
    ) -> List[Dict[str, Any]]:
        """
        Fetch price history for a specific token.
        
        Args:
            token_id: Token ID (clob_token_id)
            start_ts: Start timestamp in Unix seconds
            end_ts: End timestamp in Unix seconds
            resolution: Time resolution (e.g., "1m", "5m", "1h", "1d")
            loop_start: Timestamp for rate limit timing tracking
        
        Returns:
            List of price history entries
        
        Example:
            >>> fetcher = PriceFetcher()
            >>> prices = fetcher.fetch_price_history(
            ...     token_id="12345",
            ...     start_ts=1702300800,
            ...     end_ts=1702387200,
            ...     resolution="1h"
            ... )
            >>> print(f"Fetched {len(prices)} price points")
        """
        if loop_start is None:
            loop_start = time.time()
        
        if end_ts is None:
            end_ts = int(datetime.now().timestamp())
        if start_ts is None:
            start_ts = 0
        
        params = {
            "market": token_id,
            "startTs": start_ts,
            "endTs": end_ts,
            "resolution": resolution
        }
        
        # Acquire rate limit token before making request
        self._manager.acquire_price(loop_start)
        
        try:
            response = self.client.get(
                f"{self._data_api_base}/price-history",
                params=params
            )
            response.raise_for_status()
            data = response.json()
            
            # Add token_id to each record for parquet storage
            for record in data:
                record['token_id'] = token_id
            
            return data
        
        except httpx.HTTPStatusError as e:
            print(f"HTTP error fetching price history: {e.response.status_code} - {e.response.text}")
            return []
        
        except httpx.RequestError as e:
            print(f"Request error fetching price history: {e}")
            return []
        
        except Exception as e:
            print(f"Unexpected error fetching price history: {e}")
            return []
    
    def _worker(
        self,
        worker_id: int,
        token_queue: Queue,
        price_queue: Union[Queue, SwappableQueue],
        start_time: int,
        end_time: int,
        resolution: str = "1m"
    ):
        """
        Worker thread that fetches price history for tokens from the queue.
        
        Args:
            worker_id: ID of this worker
            token_queue: Queue containing token IDs to process
            price_queue: Queue to add fetched prices to (Queue or SwappableQueue)
            start_time: Start timestamp in Unix seconds
            end_time: End timestamp in Unix seconds
            resolution: Price resolution
        """
        is_swappable = isinstance(price_queue, SwappableQueue)
        
        while True:
            try:
                # Get token from queue (non-blocking with timeout)
                token_id = token_queue.get(timeout=1)
                if token_id is None:
                    token_queue.task_done()
                    if not is_swappable:
                        price_queue.put(None)
                    return
                
                print(f"Worker {worker_id}: Processing token {token_id[:10] if len(token_id) > 10 else token_id}...")
                
                loop_start = time.time()
                prices = self.fetch_price_history(
                    token_id=token_id,
                    start_ts=start_time,
                    end_ts=end_time,
                    resolution="1m",
                    loop_start=loop_start
                )
                
                if prices:
                    if is_swappable:
                        price_queue.put_many(prices)
                    else:
                        for price in prices:
                            price_queue.put(price)
                    
                    print(f"Worker {worker_id}: Fetched {len(prices)} prices for token {token_id[:10] if len(token_id) > 10 else token_id}")
                
                token_queue.task_done()
                
            except Empty:
                continue
            
            except Exception as e:
                print(f"Worker {worker_id}: Error - {e}")
                if token_id is not None:
                    token_queue.task_done()
                if not is_swappable:
                    price_queue.put(None)
                return
    
    def fetch_prices_multithreaded(
        self,
        token_queue: Queue = None,
        market_parquet_path: str = None,
        start_time: int = None,
        end_time: int = None,
        resolution: str = "1m",
        num_workers: int = None,
        batch_threshold: int = None,
        output_dir: str = None
    ) -> SwappableQueue:
        """
        Fetch price history for multiple tokens using multiple worker threads.
        
        Args:
            token_queue: Queue containing token IDs to process (mutually exclusive with market_parquet_path)
            market_parquet_path: Path to market parquet files to load token IDs from
            start_time: Start timestamp in Unix seconds
            end_time: End timestamp in Unix seconds
            resolution: Price resolution (e.g., "1m", "1h", "1d")
            num_workers: Number of worker threads (uses config if None)
            batch_threshold: Queue threshold for parquet writes (uses config if None)
            output_dir: Output directory for price parquets (uses config if None)
        
        Returns:
            SwappableQueue containing fetched prices
        """
        # Use config defaults
        if num_workers is None:
            num_workers = self._config.workers.num_workers
        if batch_threshold is None:
            batch_threshold = self._config.queues.price_threshold
        if output_dir is None:
            output_dir = self._config.output_dirs.price
        
        # Load tokens from parquet if path provided
        if market_parquet_path is not None:
            # You'll need to implement loading token IDs from market parquet
            # For now, assume the parquet has a token_id column
            from parquet_persister import load_parquet_data
            data = load_parquet_data(
                market_parquet_path,
                query="SELECT DISTINCT token_id FROM data WHERE token_id IS NOT NULL"
            )
            token_ids = [row['token_id'] for row in data]
            token_queue = Queue()
            for token_id in token_ids:
                token_queue.put(token_id)
            print(f"Loaded {len(token_ids)} tokens from parquet")
        elif token_queue is None:
            raise ValueError("Either token_queue or market_parquet_path must be provided")
        
        # Load cursor to resume from last run
        cursor = load_cursor(output_dir, self._config.cursors.filename)
        if cursor and self._config.cursors.enabled:
            print(f"[PriceFetcher] Resuming from cursor: {cursor}")
        
        # Create persisted queue for prices
        price_queue, persister = create_price_persisted_queue(
            threshold=batch_threshold,
            output_dir=output_dir,
            auto_start=True
        )
        
        threads = []
        # Create and start worker threads
        for i in range(num_workers):
            thread = threading.Thread(
                target=self._worker,
                args=(i + 1, token_queue, price_queue, start_time, end_time, resolution)
            )
            thread.start()
            threads.append(thread)
        
        # Wait for all tokens to be processed
        token_queue.join()
        for _ in range(num_workers):
            token_queue.put(None)
        
        # Wait for all workers to finish
        for thread in threads:
            thread.join()
        
        print(f"All price workers finished.")
        persister.stop()
        
        # Save cursor for next run
        if self._config.cursors.enabled:
            cursor_data = {
                "last_run": datetime.now().isoformat(),
                "start_time": start_time,
                "end_time": end_time,
                "resolution": resolution,
                "records_written": persister.stats.get("total_records_written", 0)
            }
            save_cursor(output_dir, cursor_data, self._config.cursors.filename)
        
        return price_queue


# Example usage
if __name__ == "__main__":
    with PriceFetcher() as fetcher:
        # Example: Fetch price history for a token
        token_id = "12345678901234567890"
        start_ts = int(datetime(2024, 12, 1).timestamp())
        end_ts = int(datetime(2024, 12, 30).timestamp())
        
        print(f"Fetching price history for token {token_id[:10]}...")
        print(f"Time range: {start_ts} to {end_ts}")
        
        prices = fetcher.fetch_price_history(
            token_id=token_id,
            start_ts=start_ts,
            end_ts=end_ts,
            resolution="1h"
        )
        
        print(f"\nFetched {len(prices)} price points")
