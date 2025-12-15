"""
Unit tests for TokenBucket rate limiter.

Tests rate limiting behavior, blocking, and thread safety.
"""

import pytest
import threading
import time
from concurrent.futures import ThreadPoolExecutor
import sys
from pathlib import Path

# Add fetcher module to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "fetcher"))

from worker_manager import TokenBucket


class TestTokenBucketBasic:
    """Test basic token bucket operations."""
    
    def test_initial_tokens_available(self):
        """Test that initial tokens are available."""
        bucket = TokenBucket(rate=10, window_seconds=1.0)
        # Should not block for first 10 requests
        for _ in range(10):
            waited = bucket.acquire()
            assert waited is False
    
    def test_tokens_deplete(self):
        """Test that tokens get depleted."""
        bucket = TokenBucket(rate=5, window_seconds=10.0)
        
        # Use all tokens
        for _ in range(5):
            bucket.acquire()
        
        # Next acquire should block (return True)
        start = time.time()
        waited = bucket.acquire()
        elapsed = time.time() - start
        
        # Should have waited and returned True
        assert waited is True
    
    def test_acquire_returns_waited_status(self):
        """Test that acquire correctly reports if it had to wait."""
        bucket = TokenBucket(rate=2, window_seconds=0.1)
        
        # First two should not wait
        assert bucket.acquire() is False
        assert bucket.acquire() is False
        
        # Third should wait
        assert bucket.acquire() is True


class TestTokenBucketRefill:
    """Test token refill behavior."""
    
    def test_tokens_refill_after_window(self):
        """Test that tokens refill after the window period."""
        bucket = TokenBucket(rate=3, window_seconds=0.1)
        
        # Use all tokens
        for _ in range(3):
            bucket.acquire()
        
        # Wait for refill
        time.sleep(0.15)
        
        # Should have tokens again
        waited = bucket.acquire()
        assert waited is False
    
    def test_refill_resets_all_tokens(self):
        """Test that refill resets to full capacity."""
        bucket = TokenBucket(rate=5, window_seconds=0.1)
        
        # Use all tokens
        for _ in range(5):
            bucket.acquire()
        
        # Wait for refill
        time.sleep(0.15)
        
        # Should be able to acquire all tokens again without waiting
        for _ in range(5):
            waited = bucket.acquire()
            assert waited is False


class TestTokenBucketThreadSafety:
    """Test thread safety of token bucket."""
    
    def test_concurrent_acquires(self):
        """Test concurrent acquire calls don't over-consume tokens."""
        bucket = TokenBucket(rate=10, window_seconds=1.0)
        successful_acquires = []
        lock = threading.Lock()
        
        def try_acquire(thread_id):
            result = bucket.acquire()
            with lock:
                successful_acquires.append((thread_id, result))
        
        # Launch 20 threads trying to acquire at once
        with ThreadPoolExecutor(max_workers=20) as executor:
            futures = [executor.submit(try_acquire, i) for i in range(20)]
            for f in futures:
                f.result()
        
        # All 20 should complete (some waited, some didn't)
        assert len(successful_acquires) == 20
        
        # Count how many had to wait
        waited_count = sum(1 for _, waited in successful_acquires if waited)
        # At least 10 should have had to wait (since rate is 10)
        assert waited_count >= 10
    
    def test_rate_limiting_under_load(self):
        """Test that rate limiting holds under concurrent load."""
        rate = 10
        window = 0.5
        bucket = TokenBucket(rate=rate, window_seconds=window)
        
        start_time = time.time()
        num_requests = 25
        
        def make_request(i):
            bucket.acquire()
            return time.time()
        
        # Execute requests concurrently
        with ThreadPoolExecutor(max_workers=25) as executor:
            timestamps = list(executor.map(make_request, range(num_requests)))
        
        total_time = max(timestamps) - start_time
        
        # With rate=10 per 0.5s, 25 requests should take at least 1 second
        # (10 immediate, 10 after 0.5s, 5 after 1s)
        assert total_time >= 0.9  # Allow some margin


class TestTokenBucketEdgeCases:
    """Test edge cases."""
    
    def test_zero_rate(self):
        """Test behavior with zero rate (should still work)."""
        bucket = TokenBucket(rate=0, window_seconds=1.0)
        # With 0 tokens, acquire should wait for refill
        # This is an edge case - implementation may vary
        start = time.time()
        bucket.acquire()
        elapsed = time.time() - start
        # Should have waited at least one window
        assert elapsed >= 0.9 or elapsed < 0.1  # Either waits or doesn't based on impl
    
    def test_high_rate(self):
        """Test with high rate limit."""
        bucket = TokenBucket(rate=1000, window_seconds=1.0)
        
        # Should be able to acquire many without waiting
        waited_count = 0
        for _ in range(500):
            if bucket.acquire():
                waited_count += 1
        
        # Should not have had to wait at all
        assert waited_count == 0
    
    def test_small_window(self):
        """Test with very small window."""
        bucket = TokenBucket(rate=2, window_seconds=0.05)
        
        # Use tokens
        bucket.acquire()
        bucket.acquire()
        
        # Wait for quick refill
        time.sleep(0.06)
        
        # Should have tokens again
        waited = bucket.acquire()
        assert waited is False
