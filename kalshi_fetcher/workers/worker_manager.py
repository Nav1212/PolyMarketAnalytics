"""
Centralized Worker Manager for rate limiting and timing statistics.
Manages separate token buckets for events, markets, trades, and orderbooks.
"""

import threading
import time
import statistics
from collections import deque
from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from kalshi_fetcher.config import Config


class TokenBucket:
    """
    Thread-safe token bucket rate limiter.
    Blocking acquire() that waits until a token is available.
    """
    
    def __init__(self, rate: int, window_seconds: float = 10.0):
        """
        Args:
            rate: Number of requests allowed per window
            window_seconds: Time window in seconds (default 10s)
        """
        self.rate = rate
        self.window = window_seconds
        self.tokens = rate
        self.last_refill = time.time()
        self.lock = threading.Lock()
    
    def acquire(self) -> bool:
        """
        Acquire a token, blocking until one is available.
        
        Returns:
            True if had to wait (rate limit was hit), False if token was immediately available
        """
        waited = False
        while True:
            with self.lock:
                now = time.time()
                elapsed = now - self.last_refill
                
                # Refill tokens if window has passed
                if elapsed >= self.window:
                    self.tokens = self.rate
                    self.last_refill = now
                
                # If token available, consume and return
                if self.tokens > 0:
                    self.tokens -= 1
                    return waited
            
            # No token available - wait and retry
            waited = True
            time.sleep(0.01)


class WorkerManager:
    """
    Centralized manager for rate limiting across event, market, trade, and orderbook fetchers.
    Tracks timing statistics for when workers first hit rate limits per loop.
    """
    
    def __init__(
        self,
        event_rate: int = 100,
        market_rate: int = 100,
        trade_rate: int = 70,
        orderbook_rate: int = 100,
        window_seconds: float = 10.0,
        config: Optional["Config"] = None
    ):
        """
        Args:
            event_rate: Requests per window for event API (default 100)
            market_rate: Requests per window for market API (default 100)
            trade_rate: Requests per window for trade API (default 70)
            orderbook_rate: Requests per window for orderbook API (default 100)
            window_seconds: Time window in seconds (default 10s)
            config: Optional Config object to load settings from
        """
        # Use config if provided, otherwise use explicit parameters
        if config is not None:
            event_rate = config.rate_limits.event
            market_rate = config.rate_limits.market
            trade_rate = config.rate_limits.trade
            orderbook_rate = config.rate_limits.orderbook
            window_seconds = config.rate_limits.window_seconds
        
        # Separate token buckets
        self._event_bucket = TokenBucket(event_rate, window_seconds)
        self._market_bucket = TokenBucket(market_rate, window_seconds)
        self._trade_bucket = TokenBucket(trade_rate, window_seconds)
        self._orderbook_bucket = TokenBucket(orderbook_rate, window_seconds)
        
        # Timing stats - one deque per job type
        self._event_hit_times: deque = deque()
        self._market_hit_times: deque = deque()
        self._trade_hit_times: deque = deque()
        self._orderbook_hit_times: deque = deque()
    
    def acquire_event(self, loop_start: Optional[float] = None) -> None:
        """
        Acquire a token for event API requests.
        
        Args:
            loop_start: Timestamp when current loop iteration started
        """
        waited = self._event_bucket.acquire()
        if waited and loop_start is not None:
            elapsed = time.time() - loop_start
            self._event_hit_times.append(elapsed)
    
    def acquire_market(self, loop_start: Optional[float] = None) -> None:
        """
        Acquire a token for market API requests.
        
        Args:
            loop_start: Timestamp when current loop iteration started
        """
        waited = self._market_bucket.acquire()
        if waited and loop_start is not None:
            elapsed = time.time() - loop_start
            self._market_hit_times.append(elapsed)
    
    def acquire_trade(self, loop_start: Optional[float] = None) -> None:
        """
        Acquire a token for trade API requests.
        
        Args:
            loop_start: Timestamp when current loop iteration started
        """
        waited = self._trade_bucket.acquire()
        if waited and loop_start is not None:
            elapsed = time.time() - loop_start
            self._trade_hit_times.append(elapsed)
    
    def acquire_orderbook(self, loop_start: Optional[float] = None) -> None:
        """
        Acquire a token for orderbook API requests.
        
        Args:
            loop_start: Timestamp when current loop iteration started
        """
        waited = self._orderbook_bucket.acquire()
        if waited and loop_start is not None:
            elapsed = time.time() - loop_start
            self._orderbook_hit_times.append(elapsed)
    
    def get_stats(self) -> dict:
        """
        Get timing statistics for rate limit hits.
        
        Returns:
            Dict with mean/median/count stats per worker type
        """
        stats = {}
        
        for name, times in [
            ("event", list(self._event_hit_times)),
            ("market", list(self._market_hit_times)),
            ("trade", list(self._trade_hit_times)),
            ("orderbook", list(self._orderbook_hit_times)),
        ]:
            if times:
                stats[name] = {
                    "count": len(times),
                    "mean": statistics.mean(times),
                    "median": statistics.median(times),
                    "min": min(times),
                    "max": max(times),
                }
            else:
                stats[name] = {"count": 0}
        
        return stats
    
    def clear_stats(self) -> None:
        """Clear all timing statistics."""
        self._event_hit_times.clear()
        self._market_hit_times.clear()
        self._trade_hit_times.clear()
        self._orderbook_hit_times.clear()


# =============================================================================
# Global Singleton
# =============================================================================

_worker_manager: Optional[WorkerManager] = None


def get_worker_manager() -> WorkerManager:
    """Get global worker manager singleton."""
    global _worker_manager
    if _worker_manager is None:
        _worker_manager = WorkerManager()
    return _worker_manager


def set_worker_manager(manager: WorkerManager) -> None:
    """Set global worker manager singleton (for testing)."""
    global _worker_manager
    _worker_manager = manager
