"""
Orderbook Fetcher for Kalshi
Fetches orderbook snapshots for markets
"""

import httpx
from typing import List, Dict, Any, Optional, Union
from queue import Queue, Empty
import time

from kalshi_fetcher.persistence.swappable_queue import SwappableQueue
from kalshi_fetcher.utils.logging_config import get_logger
from kalshi_fetcher.utils.exceptions import (
    KalshiAPIError,
    RateLimitExceededError,
    NetworkTimeoutError,
)
from kalshi_fetcher.utils.retry import retry
from kalshi_fetcher.workers.worker_manager import WorkerManager, get_worker_manager
from kalshi_fetcher.config import get_config, Config
from kalshi_fetcher.cursors.manager import CursorManager, get_cursor_manager

logger = get_logger("orderbook_fetcher")


class OrderbookFetcher:
    """
    Fetches orderbook snapshots from Kalshi API.
    """
    
    def __init__(
        self,
        timeout: Optional[float] = None,
        worker_manager: Optional[WorkerManager] = None,
        config: Optional[Config] = None,
        output_queue: Optional[Union[Queue, SwappableQueue]] = None,
        ticker_queue: Optional[Queue] = None,
        cursor_manager: Optional[CursorManager] = None,
    ):
        """
        Initialize the orderbook fetcher.
        
        Args:
            timeout: Request timeout in seconds
            worker_manager: WorkerManager for rate limiting
            config: Config object
            output_queue: Queue to write orderbook data to
            ticker_queue: Queue to receive market tickers from market fetcher
            cursor_manager: CursorManager for progress persistence
        """
        self._config = config or get_config()
        self._cursor_manager = cursor_manager or get_cursor_manager()
        
        if timeout is None:
            timeout = self._config.api.timeout
        
        self.client = httpx.Client(
            timeout=httpx.Timeout(timeout, connect=self._config.api.connect_timeout),
            headers={
                "User-Agent": "KalshiFetcher/1.0",
                "Accept": "application/json",
            }
        )
        
        self._manager = worker_manager or get_worker_manager()
        self._base_url = self._config.api.base_url
        self._output_queue = output_queue
        self._ticker_queue = ticker_queue
    
    def close(self):
        """Close HTTP client."""
        self.client.close()
    
    def __enter__(self):
        return self
    
    def __exit__(self, *args):
        self.close()
    
    @retry(max_attempts=3, base_delay=1.0)
    def fetch_orderbook(
        self,
        ticker: str,
        depth: int = 100,
        loop_start: Optional[float] = None,
    ) -> Dict[str, Any]:
        """
        Fetch orderbook for a market.
        
        Args:
            ticker: Market ticker
            depth: Depth of orderbook (default 100)
            loop_start: Timestamp for rate limit tracking
        
        Returns:
            Dict with orderbook data
        """
        if loop_start is None:
            loop_start = time.time()
        
        params = {
            "depth": depth,
        }
        
        # Acquire rate limit token
        self._manager.acquire_orderbook(loop_start)
        
        try:
            response = self.client.get(
                f"{self._base_url}/markets/{ticker}/orderbook",
                params=params
            )
            response.raise_for_status()
            return response.json()
        
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 429:
                logger.warning(f"Rate limit exceeded fetching orderbook for {ticker}")
                raise RateLimitExceededError(
                    endpoint=f"/markets/{ticker}/orderbook",
                    response_body=e.response.text
                )
            logger.error(f"HTTP error fetching orderbook: {e.response.status_code}")
            raise KalshiAPIError(
                f"Failed to fetch orderbook: {e.response.status_code}",
                status_code=e.response.status_code,
                endpoint=f"/markets/{ticker}/orderbook"
            )
        
        except httpx.TimeoutException as e:
            logger.error(f"Timeout fetching orderbook for {ticker}: {e}")
            raise NetworkTimeoutError(
                endpoint=f"/markets/{ticker}/orderbook",
                timeout=self._config.api.timeout
            )
    
    def fetch_and_enqueue_orderbook(self, ticker: str) -> int:
        """
        Fetch orderbook and enqueue all levels for persistence.
        
        Args:
            ticker: Market ticker
        
        Returns:
            Number of orderbook entries enqueued
        """
        loop_start = time.time()
        snapshot_time = int(loop_start * 1000)  # Unix ms
        
        try:
            result = self.fetch_orderbook(ticker, loop_start=loop_start)
        except Exception as e:
            logger.error(f"Failed to fetch orderbook for {ticker}: {e}")
            return 0
        
        orderbook = result.get("orderbook", {})
        entries = 0
        
        # Process yes side
        yes_levels = orderbook.get("yes", [])
        for level in yes_levels:
            record = {
                "ticker": ticker,
                "snapshot_time": snapshot_time,
                "side": "yes",
                "price": level.get("price", 0.0),
                "quantity": level.get("quantity", 0),
            }
            self._enqueue_record(record)
            entries += 1
        
        # Process no side
        no_levels = orderbook.get("no", [])
        for level in no_levels:
            record = {
                "ticker": ticker,
                "snapshot_time": snapshot_time,
                "side": "no",
                "price": level.get("price", 0.0),
                "quantity": level.get("quantity", 0),
            }
            self._enqueue_record(record)
            entries += 1
        
        logger.debug(f"Enqueued {entries} orderbook entries for {ticker}")
        return entries
    
    def run_from_ticker_queue(self, timeout: float = 30.0) -> None:
        """
        Run orderbook fetcher consuming tickers from queue.
        
        Args:
            timeout: How long to wait for new tickers before stopping
        """
        if self._ticker_queue is None:
            logger.warning("No ticker queue configured")
            return
        
        total_entries = 0
        markets_processed = 0
        
        while True:
            try:
                ticker = self._ticker_queue.get(timeout=timeout)
                if ticker is None:  # Sentinel value
                    break
                
                entries = self.fetch_and_enqueue_orderbook(ticker)
                total_entries += entries
                markets_processed += 1
                
                # Update cursor
                self._cursor_manager.update_orderbook_cursor(ticker=ticker)
                
            except Empty:
                logger.info("Ticker queue empty, stopping orderbook fetcher")
                break
        
        logger.info(
            f"Orderbook fetcher complete: {total_entries} entries from {markets_processed} markets"
        )
        self._cursor_manager.update_orderbook_cursor(completed=True)
    
    def _enqueue_record(self, record: Dict[str, Any]) -> None:
        """Enqueue a single orderbook record."""
        if self._output_queue is not None:
            if isinstance(self._output_queue, SwappableQueue):
                self._output_queue.put(record)
            else:
                self._output_queue.put(record)
