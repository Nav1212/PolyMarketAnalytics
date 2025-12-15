"""
Leaderboard Fetcher for Polymarket
Fetches leaderboard data for markets from the Data API
"""

import httpx
from typing import List, Dict, Any, Generator
import time

from worker_manager import WorkerManager, get_worker_manager
from config import get_config, Config


class LeaderboardFetcher:
    """
    Fetches leaderboard data from Polymarket Data API
    """
    
    def __init__(
        self,
        timeout: float = None,
        worker_manager: WorkerManager = None,
        config: Config = None
    ):
        """
        Initialize the leaderboard fetcher.
        
        Args:
            timeout: Request timeout in seconds (uses config if None)
            worker_manager: WorkerManager instance for rate limiting (uses default if None)
            config: Config object (uses global config if None)
        """
        self._config = config or get_config()
        
        if timeout is None:
            timeout = self._config.api.timeout
        
        self.client = httpx.Client(
            timeout=httpx.Timeout(timeout, connect=self._config.api.connect_timeout),
            headers={
                "User-Agent": "PolymarketLeaderboardFetcher/1.0",
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
    
    def fetch_leaderboard(
        self,
        market: str,
        category: str = "all",
        timePeriod: str = "all",
        orderBy: str = "VOL",
        limit: int = 50,
        max_offset: int = 10000,
        loop_start: float = None
    ) -> Generator[Any, None, None]:
        """
        Fetch leaderboard for a specific market.
        
        Args:
            market: Market condition_id (e.g., "0x123abc...")
            category: Category filter (default "all")
            timePeriod: Time period filter (default "all")
            orderBy: Order by field (default "VOL")
            limit: Number of entries per request (default 50)
            max_offset: Maximum offset to fetch (default 10000)
            loop_start: Timestamp for rate limit timing tracking
        
        Yields:
            Response objects containing leaderboard entries
        
        Example:
            >>> fetcher = LeaderboardFetcher()
            >>> for response in fetcher.fetch_leaderboard("0x123abc..."):
            ...     data = response.json()
            ...     print(data)
        """
        offset = 0
        
        while offset < max_offset:
            params = {
                "category": category,
                "market": market,
                "timePeriod": timePeriod,
                "orderBy": orderBy,
                "limit": limit,
                "offset": offset
            }
            
            if loop_start is None:
                loop_start = time.time()
            
            self._manager.acquire_leaderboard(loop_start)
            
            try:
                response = self.client.get(
                    f"{self._data_api_base}/v1/leaderboard",
                    params=params
                )
                response.raise_for_status()
                yield response
                offset += limit
                
                # Reset loop_start for next iteration
                loop_start = None
                
            except httpx.HTTPStatusError as e:
                print(f"HTTP error fetching leaderboard: {e.response.status_code} - {e.response.text}")
                return
            
            except httpx.RequestError as e:
                print(f"Request error fetching leaderboard: {e}")
                return
            
            except Exception as e:
                print(f"Unexpected error fetching leaderboard: {e}")
                return
    
    def fetch_leaderboard_all(
        self,
        market: str,
        category: str = "all",
        timePeriod: str = "all",
        orderBy: str = "VOL",
        limit: int = 50,
        max_offset: int = 10000
    ) -> List[Dict[str, Any]]:
        """
        Fetch all leaderboard entries for a specific market.
        
        Args:
            market: Market condition_id (e.g., "0x123abc...")
            category: Category filter (default "all")
            timePeriod: Time period filter (default "all")
            orderBy: Order by field (default "VOL")
            limit: Number of entries per request (default 50)
            max_offset: Maximum offset to fetch (default 10000)
        
        Returns:
            List of all leaderboard entries
        
        Example:
            >>> fetcher = LeaderboardFetcher()
            >>> entries = fetcher.fetch_leaderboard_all("0x123abc...")
            >>> print(f"Total entries: {len(entries)}")
        """
        all_entries = []
        
        for response in self.fetch_leaderboard(
            market=market,
            category=category,
            timePeriod=timePeriod,
            orderBy=orderBy,
            limit=limit,
            max_offset=max_offset
        ):
            data = response.json()
            if isinstance(data, list):
                all_entries.extend(data)
            elif isinstance(data, dict) and "data" in data:
                all_entries.extend(data["data"])
            else:
                all_entries.append(data)
        
        return all_entries
