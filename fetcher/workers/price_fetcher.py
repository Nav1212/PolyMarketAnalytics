"""
Price Fetcher for Polymarket
Fetches price history for markets from the Data API
"""

import httpx
from typing import List, Dict, Any, Union, Optional
from datetime import datetime
from queue import Queue, Empty
import random
import time

from fetcher.persistence.swappable_queue import SwappableQueue
from fetcher.workers.worker_manager import WorkerManager, get_worker_manager
from fetcher.workers.params_provider import (
    PriceParamsProvider,
    HistoricalPriceParamsProvider,
    get_price_params_provider,
)
from fetcher.config import get_config, Config
from fetcher.cursors.manager import CursorManager, get_cursor_manager


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
        params_provider: Optional[PriceParamsProvider] = None,
    ):
        """
        Initialize the price fetcher.
        
        Args:
            timeout: Request timeout in seconds (uses config if None)
            worker_manager: WorkerManager instance for rate limiting (uses default if None)
            config: Config object (uses global config if None)
            cursor_manager: CursorManager for progress persistence (uses global if None)
            params_provider: PriceParamsProvider for parameter injection (uses default if None)
        """
        self._config = config or get_config()
        self._market_queue = market_queue
        self._cursor_manager = cursor_manager or get_cursor_manager()
        self._params_provider = params_provider  # Can be None, will create per-token providers in worker
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
        self._price_api = self._config.api.price_api_base 
    
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
        fidelity: int = 60,
        loop_start: Optional[float] = None
    ) -> List[Dict[str, Any]]:
        """
        Fetch price history for a specific token (single chunk).
        
        Args:
            token_id: Token ID (clob_token_id)
            start_ts: Start timestamp in Unix seconds
            end_ts: End timestamp in Unix seconds
            fidelity: Resolution in minutes (default 60 = hourly)
            loop_start: Timestamp for rate limit timing tracking
        
        Returns:
            List of price history entries
        
        Example:
            >>> fetcher = PriceFetcher()
            >>> prices = fetcher.fetch_price_history(
            ...     token_id="12345",
            ...     start_ts=1702300800,
            ...     end_ts=1702387200,
            ...     fidelity=60
            ... )
            >>> print(f"Fetched {len(prices)} price points")
        """
        if loop_start is None:
            loop_start = time.time()
        
        now = int(datetime.now().timestamp())
        
        if end_ts is None:
            end_ts = now
        if start_ts is None:
            start_ts = end_ts - (30 * 24 * 60 * 60)
        
        if end_ts > now:
            print(f"[PriceFetcher] Warning: end_ts ({end_ts}) is in the future, capping to now ({now})")
            end_ts = now
        
        if start_ts >= end_ts:
            print(f"[PriceFetcher] Warning: start_ts ({start_ts}) >= end_ts ({end_ts}), returning empty")
            return []
        
        params = {
            "market": token_id,
            "startTs": start_ts,
            "endTs": end_ts,
            "fidelity": fidelity
        }
        
        return self._fetch_price_chunk(token_id, params, loop_start)
    
    def fetch_all_historical_prices(
        self,
        token_id: str,
        params_provider: Optional[PriceParamsProvider] = None,
        start_ts: Optional[int] = None,
        end_ts: Optional[int] = None,
        fidelity: int = 60,
    ) -> List[Dict[str, Any]]:
        """
        Fetch ALL historical price data for a token using the params provider.
        
        Continues fetching until:
        - The API returns empty history AND
        - end_ts exceeds current time
        
        Args:
            token_id: Token ID (clob_token_id)
            params_provider: Provider for parameters (creates new one if None)
            start_ts: Optional start timestamp (used if creating new provider)
            end_ts: Optional end timestamp (used if creating new provider)
            fidelity: Resolution in minutes (default 60 = hourly)
        
        Returns:
            List of all historical price entries
        """
        # Create a provider if not given
        if params_provider is None:
            params_provider = HistoricalPriceParamsProvider(
                start_ts=start_ts,
                end_ts=end_ts,
                fidelity=fidelity,
            )
        else:
            # Reset the provider for this token
            params_provider.reset(start_ts=start_ts)
        
        all_results: List[Dict[str, Any]] = []
        chunk_count = 0
        
        while not params_provider.is_complete:
            price_params = params_provider.get_params()
            
            # Fetch this chunk
            loop_start = time.time()
            chunk_results = self.fetch_price_history(
                token_id=token_id,
                start_ts=price_params.start_ts,
                end_ts=price_params.end_ts,
                fidelity=price_params.fidelity,
                loop_start=loop_start,
            )
            
            all_results.extend(chunk_results)
            chunk_count += 1
            
            # Update provider state based on response
            params_provider.update_params(chunk_results)
            
            if chunk_count % 10 == 0:
                print(f"[PriceFetcher] Token {token_id[:10]}...: Fetched {chunk_count} chunks, {len(all_results)} total records")
        
        print(f"[PriceFetcher] Token {token_id[:10]}...: Completed with {chunk_count} chunks, {len(all_results)} total records")
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
                f"{self._price_api}/prices-history",
                params=params
            )
            response.raise_for_status()
            data = response.json()
            
            if isinstance(data, dict):
                history = data.get('history', data.get('prices', data.get('sample_prices', [])))
            elif isinstance(data, list):
                history = data
            else:
                history = []            
            result = []
            for record in history:
                if isinstance(record, dict):
                    normalized = {
                        'token_id': token_id,
                        'timestamp': record.get('t') or record.get('timestamp') or record.get('ts') or 0,
                        'price': record.get('p') or record.get('price') or 0.0
                    }
                    normalized['timestamp'] = int(normalized['timestamp']) if normalized['timestamp'] else 0
                    normalized['price'] = float(normalized['price']) if normalized['price'] else 0.0
                    result.append(normalized)
                elif isinstance(record, (list, tuple)) and len(record) >= 2:
                    result.append({
                        'token_id': token_id,
                        'timestamp': int(record[0]) if record[0] else 0,
                        'price': float(record[1]) if record[1] else 0.0
                    })
            
            return result
        
        except httpx.HTTPStatusError as e:
            print(f"[PriceFetcher] HTTP error for token {token_id[:20]}...: {e.response.status_code} - {e.response.text}")
            print(f"[PriceFetcher] Request params were: {params}")
            return []
        
        except httpx.RequestError as e:
            print(f"[PriceFetcher] Request error for token {token_id[:20]}...: {e}")
            print(f"[PriceFetcher] Request params were: {params}")
            return []
        
        except Exception as e:
            print(f"[PriceFetcher] Unexpected error for token {token_id[:20]}...: {e}")
            print(f"[PriceFetcher] Request params were: {params}")
            return []
    
    def _worker(
        self,
        worker_id: int,
        price_queue: Union[Queue, SwappableQueue],
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        fidelity: int = 60,
        params_provider: Optional[PriceParamsProvider] = None,
    ):
        """
        Worker thread that fetches ALL historical price data for tokens from the queue.
        
        Uses the params provider to fetch chunks until completion (empty response
        AND end_ts exceeds current time).
        
        Args:
            worker_id: ID of this worker
            price_queue: Queue to add fetched prices to (Queue or SwappableQueue)
            start_time: Optional start timestamp (overridden by provider if given)
            end_time: Optional end timestamp (overridden by provider if given)
            fidelity: Resolution in minutes (default 60 = hourly)
            params_provider: Provider for parameters (creates per-token providers if None)
        """
        is_swappable = isinstance(price_queue, SwappableQueue)
        
        # Assert queue is not None - caller must provide it
        assert self._market_queue is not None, "market_queue must be provided for worker"
        
        # Retry configuration
        max_attempts = self._config.retry.max_attempts
        base_delay = self._config.retry.base_delay
        max_delay = self._config.retry.max_delay
        exponential_base = self._config.retry.exponential_base
        
        # Use injected provider or instance provider
        base_provider = params_provider or self._params_provider
        
        while True:
            token_id = None
            queue_item = None
            try:
                # Get token from queue (non-blocking with timeout)
                # Queue now contains tuples of (token_id, start_ts, end_ts, is_closed)
                queue_item = self._market_queue.get(timeout=1)
                if queue_item is None:
                    self._market_queue.task_done()
                    if not is_swappable:
                        price_queue.put(None)
                    return
                
                # Handle different tuple formats and legacy string format
                if isinstance(queue_item, tuple):
                    if len(queue_item) == 4:
                        # New format: (token_id, start_ts, end_ts, is_closed)
                        token_id, market_start_ts, market_end_ts, is_closed = queue_item
                    elif len(queue_item) == 2:
                        # Legacy format: (token_id, market_start_ts)
                        token_id, market_start_ts = queue_item
                        market_end_ts = None
                        is_closed = False
                    else:
                        token_id = queue_item[0]
                        market_start_ts = None
                        market_end_ts = None
                        is_closed = False
                else:
                    token_id = queue_item
                    market_start_ts = None
                    market_end_ts = None
                    is_closed = False
                
                # Use market start time if available, otherwise use provided start_time
                effective_start = market_start_ts if market_start_ts is not None else start_time
                
                # For closed markets, use the historical end time; for open markets, use current time
                effective_end = market_end_ts if market_end_ts is not None else end_time
                
                market_type = "closed" if is_closed else "active"
                print(f"Worker {worker_id}: Processing {market_type} market token {token_id[:10] if len(token_id) > 10 else token_id}...")
                
                # Create a per-token params provider using settings from base_provider if given
                if base_provider is not None:
                    # Clone provider settings for this token
                    token_provider = HistoricalPriceParamsProvider(
                        start_ts=effective_start,
                        end_ts=effective_end,
                        fidelity=base_provider.fidelity,
                        chunk_seconds=base_provider.chunk_seconds,
                    )
                else:
                    token_provider = HistoricalPriceParamsProvider(
                        start_ts=effective_start,
                        end_ts=effective_end,
                        fidelity=fidelity,
                    )
                
                # Retry loop for processing this token
                attempt = 0
                success = False
                last_exception = None
                total_prices = 0
                
                while attempt < max_attempts and not success:
                    attempt += 1
                    try:
                        # Fetch ALL historical data using the provider
                        while not token_provider.is_complete:
                            price_params = token_provider.get_params()
                            
                            # Update cursor with current progress
                            self._cursor_manager.update_price_cursor(
                                token_id=token_id,
                                start_ts=price_params.start_ts,
                                end_ts=price_params.end_ts
                            )
                            
                            loop_start = time.time()
                            prices = self.fetch_price_history(
                                token_id=token_id,
                                start_ts=price_params.start_ts,
                                end_ts=price_params.end_ts,
                                fidelity=price_params.fidelity,
                                loop_start=loop_start
                            )
                            
                            # Update provider with response
                            token_provider.update_params(prices)
                            
                            if prices:
                                if is_swappable:
                                    price_queue.put_many(prices)
                                else:
                                    for price in prices:
                                        price_queue.put(price)
                                total_prices += len(prices)
                        
                        print(f"Worker {worker_id}: Fetched {total_prices} total prices for token {token_id[:10] if len(token_id) > 10 else token_id}")
                        success = True
                        
                        current_cursor = self._cursor_manager.get_price_cursor()
                        pending = [t for t in current_cursor.pending_tokens 
                                  if (t[0] if isinstance(t, tuple) else t) != token_id]
                        self._cursor_manager.update_price_cursor(
                            token_id="",  
                            start_ts=effective_start or 0,
                            end_ts=end_time or 0,
                            pending_tokens=pending
                        )
                        
                    except Exception as e:
                        last_exception = e
                        if attempt < max_attempts:
                            delay = min(base_delay * (exponential_base ** (attempt - 1)), max_delay)
                            jitter = delay * 0.25 * (2 * random.random() - 1)  # ±25% jitter
                            sleep_time = delay + jitter
                            
                            print(f"Worker {worker_id}: Attempt {attempt}/{max_attempts} failed for token {token_id[:10] if len(token_id) > 10 else token_id}: {e}. Retrying in {sleep_time:.1f}s...")
                            time.sleep(sleep_time)
                            # Reset provider to retry from current position
                            # (don't reset to beginning, continue from where we left off)
                        else:
                            print(f"Worker {worker_id}: All {max_attempts} attempts failed for token {token_id[:10] if len(token_id) > 10 else token_id}: {e}")
                
                self._market_queue.task_done()
                
            except Empty:
                continue
    


if __name__ == "__main__":
    # Example: Fetch all historical prices with hourly granularity
    with PriceFetcher() as fetcher:
        token_id = "12345678901234567890"
        
        # Create a custom params provider with hourly granularity
        from fetcher.workers.params_provider import HistoricalPriceParamsProvider
        
        provider = HistoricalPriceParamsProvider(
            start_ts=int(datetime(2024, 12, 29).timestamp()),
            end_ts=int(datetime(2024, 12, 30).timestamp()),
            fidelity=60,  # Hourly granularity
            chunk_seconds=24 * 60 * 60,  # 1 day chunks
        )
        
        print(f"Fetching ALL historical prices for token {token_id[:10]}...")
        print(f"Using hourly granularity (fidelity=60)")
        
        # Option 1: Use fetch_all_historical_prices with provider
        prices = fetcher.fetch_all_historical_prices(
            token_id=token_id,
            params_provider=provider,
        )
        
        print(f"\nFetched {len(prices)} total price points")
        
        # Example: Direct injection via constructor
        print("\n--- Alternative: Constructor injection ---")
        provider2 = HistoricalPriceParamsProvider(
            start_ts=int(datetime(2024, 12, 28).timestamp()),
            end_ts=None,  # Up to current time
            fidelity=60,
        )
        
        with PriceFetcher(params_provider=provider2) as fetcher2:
            prices2 = fetcher2.fetch_all_historical_prices(
                token_id=token_id,
                start_ts=int(datetime(2024, 12, 28).timestamp()),
            )
            print(f"Fetched {len(prices2)} price points with injected provider")
