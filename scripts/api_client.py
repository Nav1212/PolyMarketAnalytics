"""
Polymarket API Client
Handles requests to Gamma API and CLOB API with rate limiting
"""

import httpx
import time
import json
import threading
from collections import deque
from typing import Optional, List, Dict, Any, Generator
from datetime import datetime, timezone


class TokenBucketLimiter:
    """
    Thread-safe token bucket rate limiter.
    Tracks requests in a time window to enforce rate limits across multiple threads.
    """
    def __init__(self, requests_per_second: float, window_seconds: float = 1.0):
        """
        Args:
            requests_per_second: Maximum requests allowed per second
            window_seconds: Time window to track requests (default 1 second)
        """
        self.rate = requests_per_second
        self.window = window_seconds
        self.lock = threading.Lock()
        self.requests = deque()  # Stores timestamps of recent requests
    
    def acquire(self) -> None:
        """
        Acquire permission to make a request.
        Blocks if rate limit would be exceeded.
        """
        with self.lock:
            now = time.time()
            
            # Remove old requests outside the time window
            cutoff = now - self.window
            while self.requests and self.requests[0] < cutoff:
                self.requests.popleft()
            
            # If at capacity, wait until oldest request expires
            if len(self.requests) >= self.rate * self.window:
                sleep_time = self.requests[0] + self.window - now
                if sleep_time > 0:
                    time.sleep(sleep_time)
                    # Re-check after sleeping
                    now = time.time()
                    cutoff = now - self.window
                    while self.requests and self.requests[0] < cutoff:
                        self.requests.popleft()
            
            # Record this request
            self.requests.append(now)
    
    def get_stats(self) -> dict:
        """Get current limiter statistics"""
        with self.lock:
            now = time.time()
            cutoff = now - self.window
            # Clean old requests
            while self.requests and self.requests[0] < cutoff:
                self.requests.popleft()
            
            return {
                'current_requests': len(self.requests),
                'max_requests': int(self.rate * self.window),
                'window_seconds': self.window,
                'rate_limit': self.rate
            }


class PolymarketClient:
    """
    Polymarket API client for Gamma and CLOB endpoints.
    
    API Endpoints:
        - Gamma API: https://gamma-api.polymarket.com (markets metadata)
        - CLOB API: https://clob.polymarket.com (prices, orderbook)
        - Data API: https://data-api.polymarket.com (trades)
    
    Rate Limits (documented):
        - Gamma /markets: 12.5 req/s
        - CLOB /prices-history: 10 req/s
        - Data /trades: 7.5 req/s
    """
    
    GAMMA_BASE = "https://gamma-api.polymarket.com"
    CLOB_BASE = "https://clob.polymarket.com"
    DATA_API_BASE = "https://data-api.polymarket.com"
    
    def __init__(self, 
                 gamma_rps: float = 10.0,
                 clob_rps: float = 8.0,
                 data_rps: float = 7.5,
                 timeout: float = 30.0,
                 shared_data_limiter: Optional[TokenBucketLimiter] = None):
        """
        Initialize client with rate limiters.
        
        Args:
            gamma_rps: Requests per second for Gamma API
            clob_rps: Requests per second for CLOB API
            data_rps: Requests per second for Data API (ignored if shared_data_limiter provided)
            timeout: Request timeout in seconds
            shared_data_limiter: Optional shared rate limiter for Data API (for multithreading)
        """
        self.gamma_limiter = TokenBucketLimiter(gamma_rps)
        self.clob_limiter = TokenBucketLimiter(clob_rps)
        self.data_limiter = shared_data_limiter or TokenBucketLimiter(data_rps)
        
        self.client = httpx.Client(
            timeout=httpx.Timeout(timeout, connect=10.0),
            headers={
                "User-Agent": "PolymarketDataPipeline/1.0",
                "Accept": "application/json"
            }
        )
        
        self._request_count = 0
        self._error_count = 0
        self._max_retries = 3
    
    def close(self):
        """Close HTTP client"""
        self.client.close()
    
    def __enter__(self):
        return self
    
    def __exit__(self, *args):
        self.close()
    
    # ==================== GAMMA API (Markets) ====================
    
    def get_markets(self, 
                    limit: int = 100,
                    offset: int = 0,
                    active: Optional[bool] = None,
                    closed: Optional[bool] = None) -> List[Dict[str, Any]]:
        """
        Fetch markets from Gamma API.
        
        Args:
            limit: Number of markets per page (max 100)
            offset: Pagination offset
            active: Filter by active status
            closed: Filter by closed status
            
        Returns:
            List of market dicts
        """
        self.gamma_limiter.acquire()
        
        params = {"limit": limit, "offset": offset}
        if active is not None:
            params["active"] = str(active).lower()
        if closed is not None:
            params["closed"] = str(closed).lower()
        
        try:
            response = self.client.get(f"{self.GAMMA_BASE}/markets", params=params)
            response.raise_for_status()
            self._request_count += 1
            return response.json()
        except Exception as e:
            self._error_count += 1
            print(f"Error fetching markets: {e}")
            return []
    
    def get_all_markets(self, batch_size: int = 100) -> Generator[Dict[str, Any], None, None]:
        """
        Iterate through all markets with pagination.
        
        Yields:
            Individual market dicts
        """
        offset = 0
        seen_ids = set()
        
        while True:
            markets = self.get_markets(limit=batch_size, offset=offset)
            if not markets:
                break
            
            new_count = 0
            for market in markets:
                # Check for duplicates (API might wrap around)
                condition_id = market.get("condition_id") or market.get("conditionId")
                if condition_id and condition_id not in seen_ids:
                    seen_ids.add(condition_id)
                    new_count += 1
                    yield market
            
            # If no new markets, we've looped around
            if new_count == 0:
                break
            
            if len(markets) < batch_size:
                break
            
            offset += batch_size
    
    def get_market(self, condition_id: str) -> Optional[Dict[str, Any]]:
        """Fetch single market by condition_id"""
        self.gamma_limiter.acquire()
        
        try:
            response = self.client.get(f"{self.GAMMA_BASE}/markets/{condition_id}")
            response.raise_for_status()
            self._request_count += 1
            return response.json()
        except Exception as e:
            self._error_count += 1
            print(f"Error fetching market {condition_id}: {e}")
            return None
    
    def get_events(self, limit: int = 100, offset: int = 0) -> List[Dict[str, Any]]:
        """Fetch events from Gamma API"""
        self.gamma_limiter.acquire()
        
        try:
            response = self.client.get(
                f"{self.GAMMA_BASE}/events",
                params={"limit": limit, "offset": offset}
            )
            response.raise_for_status()
            self._request_count += 1
            return response.json()
        except Exception as e:
            self._error_count += 1
            print(f"Error fetching events: {e}")
            return []
    
    # ==================== CLOB API (Prices) ====================
    
    def get_prices_history(
        self,
        token_id: str,
        start_ts: Optional[int] = None,
        end_ts: Optional[int] = None,
        interval: Optional[str] = None,
        fidelity: int = 60
    ) -> List[Dict[str, Any]]:

        self.clob_limiter.acquire()

        # Validate mutual exclusivity
        if interval is not None and (start_ts is not None or end_ts is not None):
            raise ValueError("Use either interval OR start_ts/end_ts, not both.")

        params = {
            "market": token_id,
            "fidelity": fidelity,
        }

        # If date range
        if start_ts is not None or end_ts is not None:
            if start_ts is None or end_ts is None:
                raise ValueError("Both start_ts and end_ts must be provided.")
            params["startTs"] = start_ts
            params["endTs"] = end_ts
        else:
            params["interval"] = interval or "max"

        try:
            response = self.client.get(f"{self.CLOB_BASE}/prices-history", params=params)
            response.raise_for_status()
            self._request_count += 1

            data = response.json()

            # Handle different formats cleanly
            if isinstance(data, dict) and "history" in data:
                return data["history"]
            if isinstance(data, list):
                return data

            return []   

        except Exception as e:
            self._error_count += 1
            print(f"Error fetching price history for {token_id}: {e}")
            return []

    def get_current_prices(self, token_ids: List[str]) -> Dict[str, float]:
        """
        Fetch current prices for multiple tokens.
        
        Args:
            token_ids: List of token IDs
            
        Returns:
            Dict mapping token_id -> current price
        """
        self.clob_limiter.acquire()
        
        try:
            # CLOB API accepts comma-separated token IDs
            response = self.client.get(
                f"{self.CLOB_BASE}/prices",
                params={"token_ids": ",".join(token_ids)}
            )
            response.raise_for_status()
            self._request_count += 1
            return response.json()
        except Exception as e:
            self._error_count += 1
            print(f"Error fetching current prices: {e}")
            return {}
    
    # ==================== DATA API (Trades) ====================
    
    def get_trades(self,
                   market: Optional[str] = None,
                   start_ts: Optional[int] = None,
                   end_ts: Optional[int] = None,
                   limit: int = 500) -> List[Dict[str, Any]]:
        """
        Fetch trades from Data API.
        Returns a flat list of trade dicts (API does not support cursor pagination).
        
        Args:
            market: Filter by market condition_id
            start_ts: Start timestamp (Unix seconds)
            end_ts: End timestamp (Unix seconds)
            limit: Number of trades per page (max 500)
            
        Returns:
            List of trade dicts
        """
        self.data_limiter.acquire()
        
        params = {"limit": limit}
        if market:
            params["market"] = market
        if start_ts:
            params["startTs"] = start_ts
        if end_ts:
            params["endTs"] = end_ts
        
        try:
            response = self.client.get(f"{self.DATA_API_BASE}/trades", params=params)
            response.raise_for_status()
            self._request_count += 1
            return response.json()
        except Exception as e:
            self._error_count += 1
            print(f"Error fetching trades: {e}")
            return []
    
    def get_trades_for_period(self,
                              start_ts: int,
                              end_ts: int,
                              market: Optional[str] = None,
                              page_size: int = 500) -> Generator[Dict[str, Any], None, None]:
        """
        Iterate through all trades for a time period using timestamp windowing.
        
        Args:
            start_ts: Start timestamp (Unix seconds)
            end_ts: End timestamp (Unix seconds)
            market: Optional market filter (condition_id)
            page_size: Trades per API call (default 500, API max)
            
        Yields:
            Individual trade dicts
        """
        current_start = start_ts
        
        while current_start < end_ts:
            trades = self.get_trades(
                market=market,
                start_ts=current_start,
                end_ts=end_ts,
                limit=page_size
            )
            
            if not trades:
                break
            
            yield from trades
            
            # If we got less than page_size, we're done
            if len(trades) < page_size:
                break
            
            # Move window forward: last timestamp + 1 second
            last_timestamp = trades[-1].get('timestamp')
            if not last_timestamp:
                break
            current_start = last_timestamp + 1
    # ==================== UTILITY ====================
    
    def get_stats(self) -> Dict[str, int]:
        """Get request statistics"""
        return {
            "total_requests": self._request_count,
            "errors": self._error_count
        }


# ==================== HELPER FUNCTIONS ====================

def timestamp_to_unix(dt: datetime) -> int:
    """Convert datetime to Unix timestamp (seconds)"""
    return int(dt.replace(tzinfo=timezone.utc).timestamp())


def unix_to_timestamp(ts: int) -> datetime:
    """Convert Unix timestamp to datetime"""
    return datetime.fromtimestamp(ts, tz=timezone.utc)


def get_november_2024_range() -> tuple:
    """Get Unix timestamps for November 2024"""
    start = datetime(2024, 11, 1, 0, 0, 0, tzinfo=timezone.utc)
    end = datetime(2024, 11, 30, 23, 59, 59, tzinfo=timezone.utc)
    return timestamp_to_unix(start), timestamp_to_unix(end)


if __name__ == "__main__":
    # Quick test
    print("Testing Polymarket API client...")
    
    with PolymarketClient() as client:
        # Test fetching markets
        print("\nFetching first 5 markets...")
        markets = client.get_markets(limit=5)
        for m in markets:
            print(f"  - {m.get('question', 'N/A')[:60]}...")
        
        # Print stats
        stats = client.get_stats()
        print(f"\nStats: {stats}")
