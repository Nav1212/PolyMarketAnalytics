"""
Leaderboard Fetcher for Polymarket
Fetches leaderboard data for markets from the Data API
"""

import httpx
import threading
from enum import Enum
from typing import List, Dict, Any, Generator, Union, Optional
from queue import Queue, Empty
import time

from swappable_queue import SwappableQueue
from worker_manager import WorkerManager, get_worker_manager
from config import get_config, Config
from cursor_manager import CursorManager, get_cursor_manager


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
        timeout: Optional[float] = None,
        worker_manager: Optional[WorkerManager] = None,
        config: Optional[Config] = None,
        cursor_manager: Optional[CursorManager] = None,
    ):
        """
        Initialize the leaderboard fetcher.
        
        Args:
            timeout: Request timeout in seconds (uses config if None)
            worker_manager: WorkerManager instance for rate limiting (uses default if None)
            config: Config object (uses global config if None)
            cursor_manager: CursorManager for progress persistence (uses global if None)
        """
        self._config = config or get_config()
        self._cursor_manager = cursor_manager or get_cursor_manager()
        
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
    
    @staticmethod
    def get_all_categories() -> List[LeaderboardCategory]:
        """Return all leaderboard categories in order."""
        return list(LeaderboardCategory)
    
    @staticmethod
    def get_all_time_periods() -> List[LeaderboardTimePeriod]:
        """Return all leaderboard time periods in order."""
        return list(LeaderboardTimePeriod)
    
    def fetch_leaderboard_page(
        self,
        category: Union[LeaderboardCategory, str],
        timePeriod: Union[LeaderboardTimePeriod, str],
        orderBy: Union[LeaderboardOrderBy, str] = LeaderboardOrderBy.PNL,
        limit: int = 50,
        offset: int = 0,
        loop_start: Optional[float] = None
    ) -> List[Dict[str, Any]]:
        """
        Fetch a single page of leaderboard data.
        
        Args:
            category: Category filter
            timePeriod: Time period filter
            orderBy: Order by field (default PNL)
            limit: Number of entries per request (default 50)
            offset: Offset for pagination
            loop_start: Timestamp for rate limit timing tracking
        
        Returns:
            List of leaderboard entries for this page
        """
        category_str = category.value if isinstance(category, LeaderboardCategory) else category
        time_period_str = timePeriod.value if isinstance(timePeriod, LeaderboardTimePeriod) else timePeriod
        order_by_str = orderBy.value if isinstance(orderBy, LeaderboardOrderBy) else orderBy
        
        params = {
            "category": category_str,
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
            data = response.json()
            
            # Handle different response formats
            if isinstance(data, list):
                return data
            elif isinstance(data, dict) and "data" in data:
                return data["data"]
            else:
                return [data] if data else []
                
        except httpx.HTTPStatusError as e:
            print(f"HTTP error fetching leaderboard: {e.response.status_code} - {e.response.text}")
            return []
        
        except httpx.RequestError as e:
            print(f"Request error fetching leaderboard: {e}")
            return []
        
        except Exception as e:
            print(f"Unexpected error fetching leaderboard: {e}")
            return []
    
    def fetch_leaderboard(
        self,
        category: Union[LeaderboardCategory, str] = LeaderboardCategory.OVERALL,
        timePeriod: Union[LeaderboardTimePeriod, str] = LeaderboardTimePeriod.DAY,
        orderBy: Union[LeaderboardOrderBy, str] = LeaderboardOrderBy.PNL,
        limit: int = 50,
        max_offset: int = 10000,
        loop_start: Optional[float] = None
    ) -> Generator[Any, None, None]:
        """
        Fetch leaderboard for a category/time period combination.
        
        Args:
            category: Category filter (default OVERALL)
            timePeriod: Time period filter (default DAY)
            orderBy: Order by field (default PNL)
            limit: Number of entries per request (default 50)
            max_offset: Maximum offset to fetch (default 10000)
            loop_start: Timestamp for rate limit timing tracking
        
        Yields:
            Response objects containing leaderboard entries
        """
        # Convert enums to string values
        category_str = category.value if isinstance(category, LeaderboardCategory) else category
        time_period_str = timePeriod.value if isinstance(timePeriod, LeaderboardTimePeriod) else timePeriod
        order_by_str = orderBy.value if isinstance(orderBy, LeaderboardOrderBy) else orderBy
        
        offset = 0
        
        while offset < max_offset:
            params = {
                "category": category_str,
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
                
                # Check if we got empty results (end of data)
                data = response.json()
                if isinstance(data, list) and len(data) == 0:
                    return
                if isinstance(data, dict) and len(data.get("data", [])) == 0:
                    return
                
                yield response
                offset += limit
                
                # If we got fewer results than limit, we're done
                result_count = len(data) if isinstance(data, list) else len(data.get("data", []))
                if result_count < limit:
                    return
                
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
    
    def fetch_all_combinations(
        self,
        output_queue: Union[Queue, SwappableQueue],
        trader_queue: Optional[Union[Queue, SwappableQueue]] = None,
        orderBy: Union[LeaderboardOrderBy, str] = LeaderboardOrderBy.PNL,
        limit: int = 50,
        max_offset: int = 10000
    ) -> int:
        """
        Fetch leaderboard for all category/time_period combinations.
        Saves cursor after each page for crash recovery.
        
        Args:
            output_queue: Queue to add fetched leaderboard entries to
            trader_queue: Optional queue to add trader wallet addresses for trade fetching
            orderBy: Order by field (default PNL)
            limit: Number of entries per request (default 50)
            max_offset: Maximum offset per combination
        
        Returns:
            Total number of entries fetched
        """
        is_swappable = isinstance(output_queue, SwappableQueue)
        trader_queue_is_swappable = isinstance(trader_queue, SwappableQueue) if trader_queue else False
        categories = self.get_all_categories()
        time_periods = self.get_all_time_periods()
        
        # Load cursor to resume from last position
        cursor = self._cursor_manager.get_leaderboard_cursor()
        
        if cursor.completed:
            print("[Leaderboard] Already completed all combinations")
            return 0
        
        start_cat_idx = cursor.current_category_index
        start_tp_idx = cursor.current_time_period_index
        start_offset = cursor.current_offset
        
        total_entries = 0
        
        for cat_idx, category in enumerate(categories):
            # Skip categories before resume point
            if cat_idx < start_cat_idx:
                continue
                
            for tp_idx, time_period in enumerate(time_periods):
                # Skip time periods before resume point
                if cat_idx == start_cat_idx and tp_idx < start_tp_idx:
                    continue
                
                # Determine starting offset
                if cat_idx == start_cat_idx and tp_idx == start_tp_idx:
                    offset = start_offset
                else:
                    offset = 0
                
                print(f"[Leaderboard] Fetching {category.value}/{time_period.value} from offset {offset}")
                
                while offset < max_offset:
                    # Update cursor before each request
                    self._cursor_manager.update_leaderboard_cursor(
                        current_category_index=cat_idx,
                        current_time_period_index=tp_idx,
                        current_offset=offset,
                        completed=False
                    )
                    self._cursor_manager.save_cursors()
                    
                    entries = self.fetch_leaderboard_page(
                        category=category,
                        timePeriod=time_period,
                        orderBy=orderBy,
                        limit=limit,
                        offset=offset
                    )
                    
                    if not entries:
                        break
                    
                    # Add category and time_period to each entry for tracking
                    for entry in entries:
                        entry['_category'] = category.value
                        entry['_timePeriod'] = time_period.value
                    
                    if is_swappable:
                        output_queue.put_many(entries)
                    else:
                        for entry in entries:
                            output_queue.put(entry)
                    
                    # Add trader wallets to trader queue for downstream trade fetching
                    if trader_queue:
                        wallets = [entry.get('proxyWallet') for entry in entries if entry.get('proxyWallet')]
                        if wallets:
                            if isinstance(trader_queue, SwappableQueue):
                                trader_queue.put_many(wallets)
                            else:
                                for wallet in wallets:
                                    trader_queue.put(wallet)
                    
                    total_entries += len(entries)
                    offset += limit
                    
                    # If we got fewer results than limit, we're done with this combination
                    if len(entries) < limit:
                        break
                
                print(f"[Leaderboard] Completed {category.value}/{time_period.value}, total so far: {total_entries}")
        
        # Mark as completed
        self._cursor_manager.update_leaderboard_cursor(
            current_category_index=len(categories) - 1,
            current_time_period_index=len(time_periods) - 1,
            current_offset=0,
            completed=True
        )
        self._cursor_manager.save_cursors()
        
        print(f"[Leaderboard] Completed all combinations, total entries: {total_entries}")
        return total_entries
    
    def _worker(
        self,
        worker_id: int,
        output_queue: Union[Queue, SwappableQueue],
        trader_queue: Optional[Union[Queue, SwappableQueue]] = None,
    ):
        """
        Worker thread that fetches leaderboard data for all enum combinations.
        
        Args:
            worker_id: ID of this worker
            output_queue: Queue to add fetched leaderboard entries to
            trader_queue: Optional queue to add trader wallet addresses for trade fetching
            stop_event: Optional event to signal stop
        """
        print(f"[Leaderboard Worker {worker_id}] Starting enum iteration...")
        
        try:
            total = self.fetch_all_combinations(output_queue, trader_queue=trader_queue)
            print(f"[Leaderboard Worker {worker_id}] Finished, total entries: {total}")
        except Exception as e:
            print(f"[Leaderboard Worker {worker_id}] Error: {e}")
    
    def run_workers(
        self,
        output_queue: Union[Queue, SwappableQueue],
        trader_queue: Optional[Union[Queue, SwappableQueue]] = None,
        stop_event: Optional[threading.Event] = None
    ) -> List[threading.Thread]:
        """
        Start a worker thread to fetch leaderboard data for all enum combinations.
        
        Note: Only one worker is used since we're iterating through combinations,
        not consuming from a queue.
        
        Args:
            output_queue: Queue to add fetched leaderboard entries to
            trader_queue: Optional queue to add trader wallet addresses for trade fetching
            num_workers: Number of workers (only 1 is used for enum iteration)
            stop_event: Optional event to signal stop
        
        Returns:
            List of started threads (caller should join them)
        """
        # Only use 1 worker for enum iteration
        t = threading.Thread(
            target=self._worker,
            args=(0, output_queue, trader_queue, stop_event),
            daemon=True
        )
        t.start()
        return [t]
