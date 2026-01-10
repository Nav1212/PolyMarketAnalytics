"""
Event Fetcher for Kalshi
Fetches events (top-level groupings of markets)
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

logger = get_logger("event_fetcher")


class EventFetcher:
    """
    Fetches events from Kalshi API.
    Events are top-level groupings that contain multiple markets.
    """
    
    def __init__(
        self,
        timeout: Optional[float] = None,
        worker_manager: Optional[WorkerManager] = None,
        config: Optional[Config] = None,
        output_queue: Optional[Union[Queue, SwappableQueue]] = None,
        market_event_queue: Optional[Queue] = None,
        cursor_manager: Optional[CursorManager] = None,
    ):
        """
        Initialize the event fetcher.
        
        Args:
            timeout: Request timeout in seconds
            worker_manager: WorkerManager for rate limiting
            config: Config object
            output_queue: Queue to write event data to
            market_event_queue: Queue to write event tickers for market fetcher
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
        self._market_event_queue = market_event_queue
    
    def close(self):
        """Close HTTP client."""
        self.client.close()
    
    def __enter__(self):
        return self
    
    def __exit__(self, *args):
        self.close()
    
    @retry(max_attempts=3, base_delay=1.0)
    def fetch_events(
        self,
        cursor: Optional[str] = None,
        limit: int = 100,
        status: Optional[str] = None,
        series_ticker: Optional[str] = None,
        with_nested_markets: bool = False,
        loop_start: Optional[float] = None,
    ) -> Dict[str, Any]:
        """
        Fetch events from Kalshi API.
        
        Args:
            cursor: Pagination cursor from previous response
            limit: Number of events per page (max 200)
            status: Filter by status (open, closed, settled)
            series_ticker: Filter by series
            with_nested_markets: Include nested market data
            loop_start: Timestamp for rate limit tracking
        
        Returns:
            Dict with 'events' list and optional 'cursor' for pagination
        """
        if loop_start is None:
            loop_start = time.time()
        
        params = {
            "limit": min(limit, 200),
        }
        
        if cursor:
            params["cursor"] = cursor
        if status:
            params["status"] = status
        if series_ticker:
            params["series_ticker"] = series_ticker
        if with_nested_markets:
            params["with_nested_markets"] = "true"
        
        # Acquire rate limit token
        self._manager.acquire_event(loop_start)
        
        try:
            response = self.client.get(
                f"{self._base_url}/events",
                params=params
            )
            response.raise_for_status()
            return response.json()
        
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 429:
                logger.warning("Rate limit exceeded fetching events")
                raise RateLimitExceededError(
                    endpoint="/events",
                    response_body=e.response.text
                )
            logger.error(f"HTTP error fetching events: {e.response.status_code}")
            raise KalshiAPIError(
                f"Failed to fetch events: {e.response.status_code}",
                status_code=e.response.status_code,
                endpoint="/events"
            )
        
        except httpx.TimeoutException as e:
            logger.error(f"Timeout fetching events: {e}")
            raise NetworkTimeoutError(endpoint="/events", timeout=self._config.api.timeout)
    
    def fetch_all_events(
        self,
        status: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        Fetch all events with pagination.
        
        Args:
            status: Filter by status
            limit: Maximum total events to fetch (None for all)
        
        Returns:
            List of all event dictionaries
        """
        all_events = []
        cursor = None
        
        # Check for resume cursor
        cursor_state = self._cursor_manager.get_event_cursor()
        if cursor_state.cursor and not cursor_state.completed:
            cursor = cursor_state.cursor
            logger.info(f"Resuming from cursor: {cursor}")
        
        while True:
            loop_start = time.time()
            
            result = self.fetch_events(
                cursor=cursor,
                limit=100,
                status=status,
                loop_start=loop_start,
            )
            
            events = result.get("events", [])
            all_events.extend(events)
            
            # Enqueue for downstream processing
            self._enqueue_events(events)
            
            # Check pagination
            cursor = result.get("cursor")
            
            # Update cursor for resume
            self._cursor_manager.update_event_cursor(cursor=cursor or "")
            
            if not cursor:
                self._cursor_manager.update_event_cursor(completed=True)
                break
            
            if limit and len(all_events) >= limit:
                break
        
        logger.info(f"Fetched {len(all_events)} total events")
        return all_events
    
    def _enqueue_events(self, events: List[Dict[str, Any]]) -> None:
        """
        Enqueue events for persistence and downstream fetchers.
        
        Args:
            events: List of event dictionaries from API
        """
        for event in events:
            # Transform to schema format
            event_record = {
                "event_ticker": event.get("event_ticker", ""),
                "series_ticker": event.get("series_ticker", ""),
                "title": event.get("title", ""),
                "sub_title": event.get("sub_title", ""),
                "category": event.get("category", ""),
                "mutually_exclusive": event.get("mutually_exclusive", False),
                "strike_date": event.get("strike_date", ""),
                "strike_period": event.get("strike_period", ""),
                "status": event.get("status", ""),
            }
            
            # Add to output queue for persistence
            if self._output_queue is not None:
                if isinstance(self._output_queue, SwappableQueue):
                    self._output_queue.put(event_record)
                else:
                    self._output_queue.put(event_record)
            
            # Add event ticker to market fetcher queue
            if self._market_event_queue is not None:
                self._market_event_queue.put(event.get("event_ticker", ""))
