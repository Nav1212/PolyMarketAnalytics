"""
Centralized Worker Manager for rate limiting and timing statistics.
Manages separate token buckets for trades and markets, tracks time-to-first-limit-hit per loop.
"""

import threading
import time
import statistics
from collections import deque
from typing import Optional


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
    Centralized manager for rate limiting across trade and market fetchers.
    Tracks timing statistics for when workers first hit rate limits per loop.
    """
    
    def __init__(self, trade_rate: int = 70, market_rate: int = 100, window_seconds: float = 10.0):
        """
        Args:
            trade_rate: Requests per window for trade API (default 70)
            market_rate: Requests per window for market API (default 100)
            window_seconds: Time window in seconds (default 10s)
        """
        # Separate token buckets
        self._trade_bucket = TokenBucket(trade_rate, window_seconds)
        self._market_bucket = TokenBucket(market_rate, window_seconds)
        
        # Timing stats - one deque per job type (lock-free appends)
        self._trade_hit_times: deque = deque()
        self._market_hit_times: deque = deque()
    
    def acquire_trade(self, loop_start: Optional[float] = None) -> None:
        """
        Acquire a token for trade API requests.
        If rate limit is hit and loop_start provided, records time-to-first-hit.
        
        Args:
            loop_start: Timestamp when current loop iteration started (from time.time())
        """
        waited = self._trade_bucket.acquire()
        if waited and loop_start is not None:
            elapsed = time.time() - loop_start
            self._trade_hit_times.append(elapsed)
    
    def acquire_market(self, loop_start: Optional[float] = None) -> None:
        """
        Acquire a token for market API requests.
        If rate limit is hit and loop_start provided, records time-to-first-hit.
        
        Args:
            loop_start: Timestamp when current loop iteration started (from time.time())
        """
        waited = self._market_bucket.acquire()
        if waited and loop_start is not None:
            elapsed = time.time() - loop_start
            self._market_hit_times.append(elapsed)
    
    def _compute_stats(self, times: deque) -> dict:
        """Compute average, median, and fastest from a deque of times."""
        if not times:
            return None
        
        times_list = list(times)
        return {
            "count": len(times_list),
            "average": statistics.mean(times_list),
            "median": statistics.median(times_list),
            "fastest": min(times_list),
            "slowest": max(times_list)
        }
    
    def get_trade_stats(self) -> Optional[dict]:
        """Get timing statistics for trade rate limit hits."""
        return self._compute_stats(self._trade_hit_times)
    
    def get_market_stats(self) -> Optional[dict]:
        """Get timing statistics for market rate limit hits."""
        return self._compute_stats(self._market_hit_times)
    
    def print_statistics(self) -> None:
        """Print timing statistics for both job types."""
        print("\n" + "=" * 60)
        print("RATE LIMIT TIMING STATISTICS")
        print("=" * 60)
        
        # Trade stats
        print("\n[TRADE API]")
        trade_stats = self.get_trade_stats()
        if trade_stats:
            print(f"  Total rate limit hits: {trade_stats['count']}")
            print(f"  Average time to hit:   {trade_stats['average']:.4f}s")
            print(f"  Median time to hit:    {trade_stats['median']:.4f}s")
            print(f"  Fastest time to hit:   {trade_stats['fastest']:.4f}s")
            print(f"  Slowest time to hit:   {trade_stats['slowest']:.4f}s")
        else:
            print("  No rate limit hits recorded")
        
        # Market stats
        print("\n[MARKET API]")
        market_stats = self.get_market_stats()
        if market_stats:
            print(f"  Total rate limit hits: {market_stats['count']}")
            print(f"  Average time to hit:   {market_stats['average']:.4f}s")
            print(f"  Median time to hit:    {market_stats['median']:.4f}s")
            print(f"  Fastest time to hit:   {market_stats['fastest']:.4f}s")
            print(f"  Slowest time to hit:   {market_stats['slowest']:.4f}s")
        else:
            print("  No rate limit hits recorded")
        
        print("\n" + "=" * 60)
    
    def reset_statistics(self) -> None:
        """Clear all timing statistics."""
        self._trade_hit_times.clear()
        self._market_hit_times.clear()


# Global singleton for easy access (optional pattern)
_default_manager: Optional[WorkerManager] = None


def get_worker_manager() -> WorkerManager:
    """Get or create the default WorkerManager instance."""
    global _default_manager
    if _default_manager is None:
        _default_manager = WorkerManager()
    return _default_manager


def set_worker_manager(manager: WorkerManager) -> None:
    """Set a custom WorkerManager as the default."""
    global _default_manager
    _default_manager = manager
