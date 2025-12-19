"""
Simple Trade Fetcher for Polymarket
Fetches trades for a given market and time range
"""

import httpx
from typing import List, Dict, Any, Union, Optional, Generator
from datetime import datetime
from queue import Queue, Empty
import random
import threading
import time

from swappable_queue import SwappableQueue
from utils.logging_config import get_logger
from utils.exceptions import (
    PolymarketAPIError,
    RateLimitExceededError,
    NetworkTimeoutError,
)
from utils.retry import retry

logger = get_logger("trade_fetcher")

from parquet_persister import (
    ParquetPersister 
)
from worker_manager import WorkerManager, get_worker_manager
from config import get_config, Config
from cursor_manager import CursorManager, get_cursor_manager


class TradeFetcher:
    """
    Simple class to fetch trades from Polymarket Data API
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
        Initialize the trade fetcher.
        
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


    def fetch_trades(
        self,
        market: str,
        filtertype: str ="",
        filteramount: int =0,
        offset: int =0,
        limit: int = 500,
        user_id: str ="",
        loop_start: Optional[float] = None
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
            if e.response.status_code == 429:
                logger.warning(
                    "Rate limit exceeded fetching trades",
                    extra={"market": market, "status_code": 429}
                )
                raise RateLimitExceededError(
                    endpoint="/trades",
                    response_body=e.response.text
                )
            logger.error(
                f"HTTP error fetching trades: {e.response.status_code}",
                extra={"market": market, "status_code": e.response.status_code}
            )
            return []
        
        except httpx.TimeoutException as e:
            logger.error(
                f"Timeout fetching trades for market {market[:16]}...",
                extra={"market": market}
            )
            raise NetworkTimeoutError(
                endpoint="/trades",
                timeout_seconds=self._config.api.timeout
            )
        
        except httpx.RequestError as e:
            logger.error(
                f"Request error fetching trades: {e}",
                extra={"market": market, "error_type": type(e).__name__}
            )
            return []
        
        except Exception as e:
            logger.exception(
                f"Unexpected error fetching trades: {e}",
                extra={"market": market}
            )
            return []
    
    def _worker(
        self,
        worker_id: int,
        trade_queue: Union[Queue, SwappableQueue],
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
        
        # Assert queue is not None - caller must provide it
        assert self._market_queue is not None, "market_queue must be provided for worker"
        
        # Retry configuration
        max_attempts = self._config.retry.max_attempts
        base_delay = self._config.retry.base_delay
        max_delay = self._config.retry.max_delay
        exponential_base = self._config.retry.exponential_base
        
        while True:
            market = None
            try:
                # Get market from queue (non-blocking with timeout)
                market = self._market_queue.get(timeout=1)
                if market is None:
                    self._market_queue.task_done()
                    if not is_swappable:
                        trade_queue.put(None)
                    return
                
                logger.info(
                    f"Worker {worker_id}: Processing market {market[:16]}...",
                    extra={"worker_id": worker_id, "market": market[:16]}
                )
                
                # Retry loop for processing this market
                attempt = 0
                success = False
                last_exception = None
                
                while attempt < max_attempts and not success:
                    attempt += 1
                    try:
                        # Check if we should resume from a cursor for this market
                        trade_cursor = self._cursor_manager.get_trade_cursor()
                        if trade_cursor.market == market and trade_cursor.offset > 0:
                            offset = trade_cursor.offset
                            filteramount = trade_cursor.filter_amount
                            logger.info(
                                f"Worker {worker_id}: Resuming market {market[:16]} from offset={offset}, filter={filteramount}",
                                extra={"worker_id": worker_id, "market": market[:16]}
                            )
                        else:
                            offset = 0
                            filteramount = 0
                        
                        # Fetch all trades for this market
                        trade_count = 0
                        while True:
                            # Update cursor with current progress and save immediately
                            self._cursor_manager.update_trade_cursor(
                                market=market,
                                offset=offset,
                                filter_amount=filteramount
                            )
                            # Save cursor to disk after every batch for crash recovery
                            self._cursor_manager.save_cursors()
                            
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

                            logger.debug(
                                f"Worker {worker_id}: Fetched {trade_count} trades so far",
                                extra={"worker_id": worker_id, "trade_count": trade_count}
                            )
                            # Move to the next batch (start after the last trade)
                        
                        # Market completed successfully
                        success = True
                        
                        # Remove from pending list
                        current_cursor = self._cursor_manager.get_trade_cursor()
                        pending = [m for m in current_cursor.pending_markets if m != market]
                        self._cursor_manager.update_trade_cursor(
                            market="",  # Clear current market
                            offset=0,
                            filter_amount=0,
                            pending_markets=pending
                        )
                        
                        logger.info(
                            f"Worker {worker_id}: Finished market {market[:16]}, total trades: {trade_count}",
                            extra={"worker_id": worker_id, "market": market[:16], "trade_count": trade_count}
                        )
                        
                    except Exception as e:
                        last_exception = e
                        if attempt < max_attempts:
                            # Calculate delay with exponential backoff and jitter
                            delay = min(base_delay * (exponential_base ** (attempt - 1)), max_delay)
                            jitter = delay * 0.25 * (2 * random.random() - 1)  # ±25% jitter
                            sleep_time = delay + jitter
                            
                            logger.warning(
                                f"Worker {worker_id}: Attempt {attempt}/{max_attempts} failed for market {market[:16]}: {e}. Retrying in {sleep_time:.1f}s...",
                                extra={"worker_id": worker_id, "market": market[:16], "attempt": attempt}
                            )
                            time.sleep(sleep_time)
                        else:
                            logger.error(
                                f"Worker {worker_id}: All {max_attempts} attempts failed for market {market[:16]}: {e}",
                                extra={"worker_id": worker_id, "market": market[:16], "error_type": type(e).__name__}
                            )
                
                # Mark task as done regardless of success/failure
                self._market_queue.task_done()
                
            except Empty:
                # Timeout on get() → loop again, don't exit
                continue
    


# Example usage
if __name__ == "__main__":
    # Example: Fetch trades for a market
    with TradeFetcher() as fetcher:
        market_id = "0x1234567890abcdef1234567890abcdef12345678"
        
        logger.info(f"Fetching trades for market {market_id[:16]}...")
        
        trades = fetcher.fetch_trades(
            market=market_id,
            limit=100
        )
        
        logger.info(f"Fetched {len(trades)} trades")