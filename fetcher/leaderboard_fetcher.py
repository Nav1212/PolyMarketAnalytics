"""
Leaderboard Fetcher for Polymarket
Fetches leaderboard data for markets from the Data API
"""

import httpx
import threading
from enum import Enum
from typing import List, Dict, Any, Generator, Union
from queue import Queue, Empty
import time

from swappable_queue import SwappableQueue
from worker_manager import WorkerManager, get_worker_manager
from config import get_config, Config


class LeaderboardCategory(str, Enum):
    """Market category for the leaderboard."""
    OVERALL = "OVERALL"
    POLITICS = "POLITICS"
    SPORTS = "SPORTS"
    CRYPTO = "CRYPTO"
    CULTURE = "CULTURE"
    MENTIONS = "MENTIONS"
    WEATHER = "WEATHER"
    ECONOMICS = "ECONOMICS"
    TECH = "TECH"
    FINANCE = "FINANCE"


class LeaderboardTimePeriod(str, Enum):
    """Time period for leaderboard results."""
    DAY = "DAY"
    WEEK = "WEEK"
    MONTH = "MONTH"
    ALL = "ALL"


class LeaderboardOrderBy(str, Enum):
    """Leaderboard ordering criteria."""
    PNL = "PNL"
    VOL = "VOL"


class LeaderboardFetcher:
    """
    Fetches leaderboard data from Polymarket Data API
    """
    
    def __init__(
        self,
        timeout: float = None,
        worker_manager: WorkerManager = None,
        config: Config = None,
    ):
        """
        Initialize the leaderboard fetcher.
        
        Args:
            timeout: Request timeout in seconds (uses config if None)
            worker_manager: WorkerManager instance for rate limiting (uses default if None)
            config: Config object (uses global config if None)
            market_queue: Queue containing market IDs to process
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
        category: Union[LeaderboardCategory, str] = LeaderboardCategory.OVERALL,
        timePeriod: Union[LeaderboardTimePeriod, str] = LeaderboardTimePeriod.DAY,
        orderBy: Union[LeaderboardOrderBy, str] = LeaderboardOrderBy.PNL,
        limit: int = 50,
        max_offset: int = 10000,
        loop_start: float = None
    ) -> Generator[Any, None, None]:
        """
        Fetch leaderboard for a specific market.
        
        Args:
            market: Market condition_id (e.g., "0x123abc...")
            category: Category filter (default OVERALL)
            timePeriod: Time period filter (default DAY)
            orderBy: Order by field (default PNL)
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
        # Convert enums to string values
        category_str = category.value if isinstance(category, LeaderboardCategory) else category
        time_period_str = timePeriod.value if isinstance(timePeriod, LeaderboardTimePeriod) else timePeriod
        order_by_str = orderBy.value if isinstance(orderBy, LeaderboardOrderBy) else orderBy
        
        offset = 0
        
        while offset < max_offset:
            params = {
                "category": category_str,
                "market": market,
                "timePeriod": time_period_str,
                "OrderBy": order_by_str,
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
        category: Union[LeaderboardCategory, str] = LeaderboardCategory.OVERALL,
        timePeriod: Union[LeaderboardTimePeriod, str] = LeaderboardTimePeriod.DAY,
        orderBy: Union[LeaderboardOrderBy, str] = LeaderboardOrderBy.PNL,
        limit: int = 50,
        max_offset: int = 10000
    ) -> List[Dict[str, Any]]:
        """
        Fetch all leaderboard entries for a specific market.
        
        Args:
            market: Market condition_id (e.g., "0x123abc...")
            category: Category filter (default OVERALL)
            timePeriod: Time period filter (default DAY)
            orderBy: Order by field (default PNL)
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
    
    def _worker(
        self,
        worker_id: int,
        market_queue: Queue,
        output_queue: Union[Queue, SwappableQueue],
        category: Union[LeaderboardCategory, str] = LeaderboardCategory.OVERALL,
        timePeriod: Union[LeaderboardTimePeriod, str] = LeaderboardTimePeriod.DAY,
        OrderBy: Union[LeaderboardOrderBy, str] = LeaderboardOrderBy.PNL
    ):
        """
        Worker thread that fetches leaderboard data from the market queue.
        
        Args:
            worker_id: ID of this worker
            market_queue: Queue containing market IDs to process
            output_queue: Queue to add fetched leaderboard entries to
            category: Category filter (default OVERALL)
            timePeriod: Time period filter (default DAY)
            OrderBy: Order by field (default PNL)
        """
        is_swappable = isinstance(output_queue, SwappableQueue)
        
        while True:
            try:
                # Get a market from the queue (blocks until available, with timeout)
                market = market_queue.get(timeout=1.0)
                
                # Handle poison pill for graceful shutdown
                if market is None:
                    market_queue.task_done()
                    break
                
                display_id = market[:16] + "..." if len(market) > 16 else market
                print(f"[Leaderboard Worker {worker_id}] Processing market: {display_id}")
                
                entries = self.fetch_leaderboard_all(
                    market=market,
                    category=category,
                    timePeriod=timePeriod, 
                    orderBy=OrderBy,
                )
                
                if entries:
                    if is_swappable:
                        output_queue.put_many(entries)
                    else:
                        for entry in entries:
                            output_queue.put(entry)
                    
                    print(f"[Leaderboard Worker {worker_id}] Fetched {len(entries)} entries for {display_id}")
                
                # Mark the task as done
                market_queue.task_done()
                                
            except Empty:
                # Queue is empty, continue waiting
                continue
            
            except Exception as e:
                print(f"[Leaderboard Worker {worker_id}] Error: {e}")    
    def run_workers(
        self,
        market_queue: Queue,
        output_queue: Union[Queue, SwappableQueue],
        category: Union[LeaderboardCategory, str] = LeaderboardCategory.OVERALL,
        timePeriod: Union[LeaderboardTimePeriod, str] = LeaderboardTimePeriod.DAY,
        num_workers: int = None
    ) -> List[threading.Thread]:
        """
        Start worker threads to fetch leaderboard data from the market queue.
        
        Args:
            market_queue: Queue containing market IDs to process (workers pull from this)
            output_queue: Queue to add fetched leaderboard entries to
            category: Category filter (default OVERALL)
            timePeriod: Time period filter (default DAY)
            num_workers: Number of workers (uses config if None)
        
        Returns:
            List of started threads (caller should join them)
        """
        if num_workers is None:
            num_workers = self._config.workers.leaderboard
        
        threads = []
        for i in range(num_workers):
            t = threading.Thread(
                target=self._worker,
                args=(i, market_queue, output_queue, category, timePeriod),
                daemon=True
            )
            t.start()
            threads.append(t)
        
        return threads
