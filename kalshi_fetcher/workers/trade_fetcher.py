"""
Trade Fetcher for Kalshi
Fetches trade history for markets
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

logger = get_logger("trade_fetcher")


class TradeFetcher:
    """
    Fetches trades from Kalshi API.
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
        Initialize the trade fetcher.
        
        Args:
            timeout: Request timeout in seconds
            worker_manager: WorkerManager for rate limiting
            config: Config object
            output_queue: Queue to write trade data to
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
    def fetch_trades(
        self,
        ticker: Optional[str] = None,
        cursor: Optional[str] = None,
        limit: int = 100,
        min_ts: Optional[int] = None,
        max_ts: Optional[int] = None,
        loop_start: Optional[float] = None,
    ) -> Dict[str, Any]:
        """
        Fetch trades from Kalshi API.
        
        Args:
            ticker: Market ticker to filter by
            cursor: Pagination cursor from previous response
            limit: Number of trades per page (max 1000)
            min_ts: Minimum timestamp filter (Unix ms)
            max_ts: Maximum timestamp filter (Unix ms)
            loop_start: Timestamp for rate limit tracking
        
        Returns:
            Dict with 'trades' list and optional 'cursor' for pagination
        """
        if loop_start is None:
            loop_start = time.time()
        
        params = {
            "limit": min(limit, 1000),
        }
        
        if ticker:
            params["ticker"] = ticker
        if cursor:
            params["cursor"] = cursor
        if min_ts:
            params["min_ts"] = min_ts
        if max_ts:
            params["max_ts"] = max_ts
        
        # Acquire rate limit token
        self._manager.acquire_trade(loop_start)
        
        try:
            response = self.client.get(
                f"{self._base_url}/markets/trades",
                params=params
            )
            response.raise_for_status()
            return response.json()
        
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 429:
                logger.warning("Rate limit exceeded fetching trades")
                raise RateLimitExceededError(
                    endpoint="/markets/trades",
                    response_body=e.response.text
                )
            logger.error(f"HTTP error fetching trades: {e.response.status_code}")
            raise KalshiAPIError(
                f"Failed to fetch trades: {e.response.status_code}",
                status_code=e.response.status_code,
                endpoint="/markets/trades"
            )
        
        except httpx.TimeoutException as e:
            logger.error(f"Timeout fetching trades: {e}")
            raise NetworkTimeoutError(endpoint="/markets/trades", timeout=self._config.api.timeout)
    
    def fetch_all_trades_for_ticker(
        self,
        ticker: str,
        min_ts: Optional[int] = None,
        max_ts: Optional[int] = None,
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        Fetch all trades for a market with pagination.
        
        Args:
            ticker: Market ticker
            min_ts: Minimum timestamp filter
            max_ts: Maximum timestamp filter
            limit: Maximum total trades to fetch
        
        Returns:
            List of all trade dictionaries
        """
        all_trades = []
        cursor = None
        
        while True:
            loop_start = time.time()
            
            result = self.fetch_trades(
                ticker=ticker,
                cursor=cursor,
                limit=1000,
                min_ts=min_ts,
                max_ts=max_ts,
                loop_start=loop_start,
            )
            
            trades = result.get("trades", [])
            all_trades.extend(trades)
            
            # Enqueue for persistence
            self._enqueue_trades(trades)
            
            # Check pagination
            cursor = result.get("cursor")
            
            if not cursor:
                break
            
            if limit and len(all_trades) >= limit:
                break
        
        logger.info(f"Fetched {len(all_trades)} trades for {ticker}")
        return all_trades
    
    def run_from_ticker_queue(self, timeout: float = 30.0) -> None:
        """
        Run trade fetcher consuming tickers from queue.
        
        Args:
            timeout: How long to wait for new tickers before stopping
        """
        if self._ticker_queue is None:
            logger.warning("No ticker queue configured")
            return
        
        trades_fetched = 0
        markets_processed = 0
        
        while True:
            try:
                ticker = self._ticker_queue.get(timeout=timeout)
                if ticker is None:  # Sentinel value
                    break
                
                logger.info(f"Fetching trades for: {ticker}")
                trades = self.fetch_all_trades_for_ticker(ticker)
                trades_fetched += len(trades)
                markets_processed += 1
                
                # Update cursor
                self._cursor_manager.update_trade_cursor(ticker=ticker)
                
            except Empty:
                logger.info("Ticker queue empty, stopping trade fetcher")
                break
        
        logger.info(f"Trade fetcher complete: {trades_fetched} trades from {markets_processed} markets")
        self._cursor_manager.update_trade_cursor(completed=True)
    
    def _enqueue_trades(self, trades: List[Dict[str, Any]]) -> None:
        """
        Enqueue trades for persistence.
        
        Args:
            trades: List of trade dictionaries from API
        """
        for trade in trades:
            # Transform to schema format
            trade_record = {
                "trade_id": trade.get("trade_id", ""),
                "ticker": trade.get("ticker", ""),
                "side": trade.get("side", ""),
                "count": trade.get("count", 0),
                "price": trade.get("price", 0.0),
                "taker_side": trade.get("taker_side", ""),
                "created_time": trade.get("created_time", ""),
                "created_time_ts": trade.get("created_time_ts", 0),
            }
            
            if self._output_queue is not None:
                if isinstance(self._output_queue, SwappableQueue):
                    self._output_queue.put(trade_record)
                else:
                    self._output_queue.put(trade_record)
