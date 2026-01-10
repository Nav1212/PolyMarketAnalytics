"""
Market Fetcher for Kalshi
Fetches markets (individual contracts within events)
"""

import httpx
from typing import List, Dict, Any, Optional, Union
from queue import Queue
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

logger = get_logger("market_fetcher")


class MarketFetcher:
    """
    Fetches markets from Kalshi API.
    Markets are individual contracts within events.
    """
    
    def __init__(
        self,
        timeout: Optional[float] = None,
        worker_manager: Optional[WorkerManager] = None,
        config: Optional[Config] = None,
        output_queue: Optional[Union[Queue, SwappableQueue]] = None,
        trade_ticker_queue: Optional[Queue] = None,
        orderbook_ticker_queue: Optional[Queue] = None,
        event_queue: Optional[Queue] = None,
        cursor_manager: Optional[CursorManager] = None,
    ):
        """
        Initialize the market fetcher.
        
        Args:
            timeout: Request timeout in seconds
            worker_manager: WorkerManager for rate limiting
            config: Config object
            output_queue: Queue to write market data to
            trade_ticker_queue: Queue to write tickers for trade fetcher
            orderbook_ticker_queue: Queue to write tickers for orderbook fetcher
            event_queue: Queue to receive event tickers from event fetcher
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
        self._trade_ticker_queue = trade_ticker_queue
        self._orderbook_ticker_queue = orderbook_ticker_queue
        self._event_queue = event_queue
    
    def close(self):
        """Close HTTP client."""
        self.client.close()
    
    def __enter__(self):
        return self
    
    def __exit__(self, *args):
        self.close()
    
    @retry(max_attempts=3, base_delay=1.0)
    def fetch_markets(
        self,
        cursor: Optional[str] = None,
        limit: int = 100,
        event_ticker: Optional[str] = None,
        series_ticker: Optional[str] = None,
        status: Optional[str] = None,
        tickers: Optional[List[str]] = None,
        loop_start: Optional[float] = None,
    ) -> Dict[str, Any]:
        """
        Fetch markets from Kalshi API.
        
        Args:
            cursor: Pagination cursor from previous response
            limit: Number of markets per page (max 200)
            event_ticker: Filter by parent event
            series_ticker: Filter by series
            status: Filter by status (open, closed, settled)
            tickers: Filter by specific market tickers
            loop_start: Timestamp for rate limit tracking
        
        Returns:
            Dict with 'markets' list and optional 'cursor' for pagination
        """
        if loop_start is None:
            loop_start = time.time()
        
        params = {
            "limit": min(limit, 200),
        }
        
        if cursor:
            params["cursor"] = cursor
        if event_ticker:
            params["event_ticker"] = event_ticker
        if series_ticker:
            params["series_ticker"] = series_ticker
        if status:
            params["status"] = status
        if tickers:
            params["tickers"] = ",".join(tickers)
        
        # Acquire rate limit token
        self._manager.acquire_market(loop_start)
        
        try:
            response = self.client.get(
                f"{self._base_url}/markets",
                params=params
            )
            response.raise_for_status()
            return response.json()
        
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 429:
                logger.warning("Rate limit exceeded fetching markets")
                raise RateLimitExceededError(
                    endpoint="/markets",
                    response_body=e.response.text
                )
            logger.error(f"HTTP error fetching markets: {e.response.status_code}")
            raise KalshiAPIError(
                f"Failed to fetch markets: {e.response.status_code}",
                status_code=e.response.status_code,
                endpoint="/markets"
            )
        
        except httpx.TimeoutException as e:
            logger.error(f"Timeout fetching markets: {e}")
            raise NetworkTimeoutError(endpoint="/markets", timeout=self._config.api.timeout)
    
    @retry(max_attempts=3, base_delay=1.0)
    def fetch_market(
        self,
        ticker: str,
        loop_start: Optional[float] = None,
    ) -> Dict[str, Any]:
        """
        Fetch a single market by ticker.
        
        Args:
            ticker: Market ticker
            loop_start: Timestamp for rate limit tracking
        
        Returns:
            Market dictionary
        """
        if loop_start is None:
            loop_start = time.time()
        
        self._manager.acquire_market(loop_start)
        
        try:
            response = self.client.get(f"{self._base_url}/markets/{ticker}")
            response.raise_for_status()
            return response.json().get("market", {})
        
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 429:
                raise RateLimitExceededError(endpoint=f"/markets/{ticker}")
            logger.error(f"HTTP error fetching market {ticker}: {e.response.status_code}")
            return {}
        
        except httpx.TimeoutException:
            logger.error(f"Timeout fetching market {ticker}")
            return {}
    
    def fetch_all_markets(
        self,
        event_ticker: Optional[str] = None,
        status: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        Fetch all markets with pagination.
        
        Args:
            event_ticker: Filter by parent event
            status: Filter by status
            limit: Maximum total markets to fetch
        
        Returns:
            List of all market dictionaries
        """
        all_markets = []
        cursor = None
        
        # Check for resume cursor
        cursor_state = self._cursor_manager.get_market_cursor()
        if cursor_state.cursor and not cursor_state.completed:
            cursor = cursor_state.cursor
            logger.info(f"Resuming from cursor: {cursor}")
        
        while True:
            loop_start = time.time()
            
            result = self.fetch_markets(
                cursor=cursor,
                limit=100,
                event_ticker=event_ticker,
                status=status,
                loop_start=loop_start,
            )
            
            markets = result.get("markets", [])
            all_markets.extend(markets)
            
            # Enqueue for downstream processing
            self._enqueue_markets(markets)
            
            # Check pagination
            cursor = result.get("cursor")
            
            # Update cursor for resume
            self._cursor_manager.update_market_cursor(cursor=cursor or "")
            
            if not cursor:
                self._cursor_manager.update_market_cursor(completed=True)
                break
            
            if limit and len(all_markets) >= limit:
                break
        
        logger.info(f"Fetched {len(all_markets)} total markets")
        return all_markets
    
    def run_from_event_queue(self) -> None:
        """
        Run market fetcher consuming event tickers from queue.
        Fetches all markets for each event.
        """
        if self._event_queue is None:
            logger.warning("No event queue configured")
            return
        
        while True:
            try:
                event_ticker = self._event_queue.get(timeout=5.0)
                if event_ticker is None:  # Sentinel value
                    break
                
                logger.info(f"Fetching markets for event: {event_ticker}")
                self.fetch_all_markets(event_ticker=event_ticker)
                
            except Exception:
                # Queue empty timeout
                break
    
    def _enqueue_markets(self, markets: List[Dict[str, Any]]) -> None:
        """
        Enqueue markets for persistence and downstream fetchers.
        
        Args:
            markets: List of market dictionaries from API
        """
        for market in markets:
            # Transform to schema format
            market_record = {
                "ticker": market.get("ticker", ""),
                "event_ticker": market.get("event_ticker", ""),
                "title": market.get("title", ""),
                "subtitle": market.get("subtitle", ""),
                "status": market.get("status", ""),
                "yes_bid": market.get("yes_bid", 0.0),
                "yes_ask": market.get("yes_ask", 0.0),
                "no_bid": market.get("no_bid", 0.0),
                "no_ask": market.get("no_ask", 0.0),
                "last_price": market.get("last_price", 0.0),
                "volume": market.get("volume", 0),
                "volume_24h": market.get("volume_24h", 0),
                "open_interest": market.get("open_interest", 0),
                "open_time": market.get("open_time", ""),
                "close_time": market.get("close_time", ""),
                "expiration_time": market.get("expiration_time", ""),
                "result": market.get("result", ""),
                "strike_type": market.get("strike_type", ""),
                "floor_strike": market.get("floor_strike"),
                "cap_strike": market.get("cap_strike"),
            }
            
            # Add to output queue for persistence
            if self._output_queue is not None:
                if isinstance(self._output_queue, SwappableQueue):
                    self._output_queue.put(market_record)
                else:
                    self._output_queue.put(market_record)
            
            ticker = market.get("ticker", "")
            
            # Add ticker to trade fetcher queue
            if self._trade_ticker_queue is not None and ticker:
                self._trade_ticker_queue.put(ticker)
            
            # Add ticker to orderbook fetcher queue
            if self._orderbook_ticker_queue is not None and ticker:
                self._orderbook_ticker_queue.put(ticker)
