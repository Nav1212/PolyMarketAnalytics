"""
Price Fetcher for Polymarket
Fetches price history for markets from the Data API
"""

import httpx
from typing import List, Dict, Any, Union, Optional
from datetime import datetime
from queue import Queue, Empty
import random
import threading
import time

from swappable_queue import SwappableQueue
from worker_manager import WorkerManager, get_worker_manager
from config import get_config, Config
from cursor_manager import CursorManager, get_cursor_manager


class PriceFetcher:
    """
    Fetches price history from Polymarket Data API
    """
    
    def __init__(
        self,
        timeout: Optional[float] = None,
        worker_manager: Optional[WorkerManager] = None,
        config: Optional[Config] = None,
        market_queue: Optional[Queue] = None,
        cursor_manager: Optional[CursorManager] = None,
    ):
        """
        Initialize the price fetcher.
        
        Args:
            timeout: Request timeout in seconds (uses config if None)
            worker_manager: WorkerManager instance for rate limiting (uses default if None)
            config: Config object (uses global config if None)
            cursor_manager: CursorManager for progress persistence (uses global if None)
        """
        self._config = config or get_config()
        self._market_queue = market_queue
        self._cursor_manager = cursor_manager or get_cursor_manager()
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
        self._clob_api = self._config.api.clob_api_base 
    
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
        start_ts: Optional[int] = None,
        end_ts: Optional[int] = None,
        fidelity: int = 1,
        loop_start: Optional[float] = None
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
            # Default to 30 days ago if no start time provided
            start_ts = end_ts - (30 * 24 * 60 * 60)
        
        # Fetch in daily increments to avoid "interval too long" error
        DAY_SECONDS = 24 * 60 * 60
        all_results = []
        
        current_start = start_ts
        while current_start < end_ts:
            current_end = min(current_start + DAY_SECONDS, end_ts)
            
            params = {
                "market": token_id,
                "startTs": current_start,
                "endTs": current_end,
                "fidelity": fidelity
            }
            
            chunk_results = self._fetch_price_chunk(token_id, params, loop_start)
            all_results.extend(chunk_results)
            
            current_start = current_end
            loop_start = None  # Only use loop_start for first request
        
        return all_results
    
    def _fetch_price_chunk(
        self,
        token_id: str,
        params: Dict[str, Any],
        loop_start: Optional[float] = None
    ) -> List[Dict[str, Any]]:
        """Fetch a single chunk of price history."""
        if loop_start is not None:
            self._manager.acquire_price(loop_start)
        else:
            self._manager.acquire_price(time.time())
        
        try:
            response = self.client.get(
                f"{self._clob_api}/prices-history",
                params=params
            )
            response.raise_for_status()
            data = response.json()
            
            # Handle different response formats
            # API may return: {"history": [...]} or a list directly
            if isinstance(data, dict):
                # Extract the price history array from the dict
                history = data.get('history', data.get('prices', data.get('sample_prices', [])))
            elif isinstance(data, list):
                history = data
            else:
                history = []
            
            # Normalize field names to match PRICE_SCHEMA: timestamp, token_id, price
            result = []
            for record in history:
                if isinstance(record, dict):
                    # Handle different API response formats:
                    # - {t: timestamp, p: price}
                    # - {timestamp: ..., price: ...}
                    # - {ts: ..., price: ...}
                    normalized = {
                        'token_id': token_id,
                        'timestamp': record.get('t') or record.get('timestamp') or record.get('ts') or 0,
                        'price': record.get('p') or record.get('price') or 0.0
                    }
                    # Ensure timestamp is int and price is float
                    normalized['timestamp'] = int(normalized['timestamp']) if normalized['timestamp'] else 0
                    normalized['price'] = float(normalized['price']) if normalized['price'] else 0.0
                    result.append(normalized)
                elif isinstance(record, (list, tuple)) and len(record) >= 2:
                    # Handle [timestamp, price] tuple format
                    result.append({
                        'token_id': token_id,
                        'timestamp': int(record[0]) if record[0] else 0,
                        'price': float(record[1]) if record[1] else 0.0
                    })
            
            return result
        
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
        price_queue: Union[Queue, SwappableQueue],
        start_time: int,
        end_time: int,
        fidelity: int = 10000,
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
        
        # Assert queue is not None - caller must provide it
        assert self._market_queue is not None, "market_queue must be provided for worker"
        
        # Retry configuration
        max_attempts = self._config.retry.max_attempts
        base_delay = self._config.retry.base_delay
        max_delay = self._config.retry.max_delay
        exponential_base = self._config.retry.exponential_base
        
        while True:
            token_id = None
            queue_item = None
            try:
                # Get token from queue (non-blocking with timeout)
                # Queue now contains tuples of (token_id, market_start_ts)
                queue_item = self._market_queue.get(timeout=1)
                if queue_item is None:
                    self._market_queue.task_done()
                    if not is_swappable:
                        price_queue.put(None)
                    return
                
                # Handle both tuple format and legacy string format
                if isinstance(queue_item, tuple):
                    token_id, market_start_ts = queue_item
                else:
                    token_id = queue_item
                    market_start_ts = None
                
                # Use market start time if available, otherwise use provided start_time
                effective_start = market_start_ts if market_start_ts is not None else start_time
                
                print(f"Worker {worker_id}: Processing token {token_id[:10] if len(token_id) > 10 else token_id}...")
                
                # Retry loop for processing this token
                attempt = 0
                success = False
                last_exception = None
                
                while attempt < max_attempts and not success:
                    attempt += 1
                    try:
                        # Update cursor with current progress
                        self._cursor_manager.update_price_cursor(
                            token_id=token_id,
                            start_ts=effective_start,
                            end_ts=end_time
                        )
                        
                        loop_start = time.time()
                        prices = self.fetch_price_history(
                            token_id=token_id,
                            start_ts=effective_start,
                            end_ts=end_time,
                            fidelity=fidelity,
                            loop_start=loop_start
                        )
                        
                        if prices:
                            if is_swappable:
                                price_queue.put_many(prices)
                            else:
                                for price in prices:
                                    price_queue.put(price)
                            
                            print(f"Worker {worker_id}: Fetched {len(prices)} prices for token {token_id[:10] if len(token_id) > 10 else token_id}")
                        
                        # Token completed successfully
                        success = True
                        
                        # Remove from pending list
                        current_cursor = self._cursor_manager.get_price_cursor()
                        pending = [t for t in current_cursor.pending_tokens 
                                  if (t[0] if isinstance(t, tuple) else t) != token_id]
                        self._cursor_manager.update_price_cursor(
                            token_id="",  # Clear current token
                            start_ts=start_time or 0,
                            end_ts=end_time or 0,
                            pending_tokens=pending
                        )
                        
                    except Exception as e:
                        last_exception = e
                        if attempt < max_attempts:
                            # Calculate delay with exponential backoff and jitter
                            delay = min(base_delay * (exponential_base ** (attempt - 1)), max_delay)
                            jitter = delay * 0.25 * (2 * random.random() - 1)  # ±25% jitter
                            sleep_time = delay + jitter
                            
                            print(f"Worker {worker_id}: Attempt {attempt}/{max_attempts} failed for token {token_id[:10] if len(token_id) > 10 else token_id}: {e}. Retrying in {sleep_time:.1f}s...")
                            time.sleep(sleep_time)
                        else:
                            print(f"Worker {worker_id}: All {max_attempts} attempts failed for token {token_id[:10] if len(token_id) > 10 else token_id}: {e}")
                
                # Mark task as done regardless of success/failure
                self._market_queue.task_done()
                
            except Empty:
                continue
    


# Example usage
if __name__ == "__main__":
    with PriceFetcher() as fetcher:
        # Example: Fetch price history for a token
        token_id = "12345678901234567890"
        start_ts = int(datetime(2024, 12, 29).timestamp())
        end_ts = int(datetime(2024, 12, 30).timestamp())
        
        print(f"Fetching price history for token {token_id[:10]}...")
        print(f"Time range: {start_ts} to {end_ts}")
        
        prices = fetcher.fetch_price_history(
            token_id=token_id,
            start_ts=start_ts,
            end_ts=end_ts,
            fidelity=1
        )
        
        print(f"\nFetched {len(prices)} price points")
