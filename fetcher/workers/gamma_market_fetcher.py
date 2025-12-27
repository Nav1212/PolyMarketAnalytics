"""
Gamma Market Fetcher for Polymarket

Fetches markets from the Polymarket Gamma API endpoint:
https://gamma-api.polymarket.com/markets

Extracts nested events and categories from each market and outputs
to separate parquet queues.

Uses offset/limit pagination (not cursor-based).
Completion is tracked via a manual-reset cursor.
"""

import httpx
import logging
import threading
import time
from typing import List, Dict, Any, Union, Optional
from queue import Queue

from fetcher.persistence.swappable_queue import SwappableQueue
from fetcher.persistence.parquet_persister import (
    create_gamma_market_persisted_queue,
    create_gamma_event_persisted_queue,
    create_gamma_category_persisted_queue,
)
from fetcher.workers.worker_manager import WorkerManager, get_worker_manager
from fetcher.config import get_config, Config
from fetcher.cursors.manager import CursorManager, get_cursor_manager


# Set up logging
logger = logging.getLogger(__name__)


class GammaMarketFetcher:
    """
    Gamma Market Fetcher for Polymarket.
    
    Fetches markets from the Gamma API and extracts nested events and categories
    into separate output queues for parquet persistence.
    
    Uses offset/limit pagination and tracks completion via a manual-reset cursor.
    """
    GAMMA_API_BASE = "https://gamma-api.polymarket.com"
    DEFAULT_LIMIT = 100  # Max items per page
    
    def __init__(
        self,
        timeout: Optional[float] = None,
        worker_manager: Optional[WorkerManager] = None,
        config: Optional[Config] = None,
        market_output_queue: Optional[Union[Queue, SwappableQueue]] = None,
        event_output_queue: Optional[Union[Queue, SwappableQueue]] = None,
        category_output_queue: Optional[Union[Queue, SwappableQueue]] = None,
        cursor_manager: Optional[CursorManager] = None,
    ):
        """
        Initialize the Gamma Market Fetcher.
        
        Args:
            timeout: Request timeout in seconds (uses config if None)
            worker_manager: WorkerManager instance for rate limiting (uses default if None)
            config: Config object (uses global config if None)
            market_output_queue: Queue to write market data to
            event_output_queue: Queue to write extracted event data to
            category_output_queue: Queue to write extracted category data to
            cursor_manager: CursorManager for progress persistence (uses global if None)
        """
        self._config = config or get_config()
        self._cursor_manager = cursor_manager or get_cursor_manager()
        self._manager = worker_manager or get_worker_manager()
        
        # HTTP client
        self.client = httpx.Client(
            timeout=httpx.Timeout(timeout or self._config.api.timeout),
            headers={
                "User-Agent": "GammaMarketFetcher/1.0",
                "Accept": "application/json"
            }
        )
        
        # Output queues
        self._market_output_queue = market_output_queue
        self._event_output_queue = event_output_queue
        self._category_output_queue = category_output_queue
    
    def close(self) -> None:
        """Close HTTP client resources."""
        self.client.close()
    
    def __enter__(self):
        return self
    
    def __exit__(self, *args):
        self.close()
    
    def fetch_markets_page(
        self,
        limit: int = DEFAULT_LIMIT,
        offset: int = 0,
        **filters
    ) -> List[Dict[str, Any]]:
        """
        Fetch a single page of markets from the Gamma API.
        
        Args:
            limit: Number of markets per page (max 100)
            offset: Offset for pagination
            **filters: Additional query parameters (closed, active, etc.)
        
        Returns:
            List of market dictionaries
        """
        loop_start = time.time()
        self._manager.acquire_gamma_market(loop_start)
        
        params = {
            "limit": limit,
            "offset": offset,
            **filters
        }
        
        response = self.client.get(
            f"{self.GAMMA_API_BASE}/markets",
            params=params
        )
        response.raise_for_status()
        
        # Gamma API returns a list directly (not wrapped in {"data": ...})
        return response.json()
    
    def _extract_market_record(self, market: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract market fields matching GAMMA_MARKET_SCHEMA.
        
        Args:
            market: Raw market dict from API
            
        Returns:
            Dict with only schema-relevant fields
        """
        return {
            'id': market.get('id', ''),
            'conditionId': market.get('conditionId', ''),
            'question': market.get('question', ''),
            'slug': market.get('slug', ''),
            'category': market.get('category', ''),
            'description': market.get('description', ''),
            'liquidity': market.get('liquidity', ''),
            'volume': market.get('volume', ''),
            'active': bool(market.get('active', False)),
            'closed': bool(market.get('closed', False)),
            'startDate': market.get('startDate', ''),
            'endDate': market.get('endDate', ''),
            'outcomes': market.get('outcomes', ''),
            'outcomePrices': market.get('outcomePrices', ''),
            'clobTokenIds': market.get('clobTokenIds', ''),
            'volumeNum': float(market.get('volumeNum', 0) or 0),
            'liquidityNum': float(market.get('liquidityNum', 0) or 0),
            'marketGroup': int(market.get('marketGroup', 0) or 0),
        }
    
    def _extract_events(self, market_id: str, market: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Extract event records from a market.
        
        Args:
            market_id: Parent market ID
            market: Raw market dict from API
            
        Returns:
            List of event dicts matching GAMMA_EVENT_SCHEMA
        """
        events = market.get('events', []) or []
        extracted = []
        
        for event in events:
            extracted.append({
                'marketId': market_id,
                'eventId': str(event.get('id', '')),
                'ticker': event.get('ticker', ''),
                'slug': event.get('slug', ''),
                'title': event.get('title', ''),
                'description': event.get('description', ''),
                'category': event.get('category', ''),
                'subcategory': event.get('subcategory', ''),
                'liquidity': float(event.get('liquidity', 0) or 0),
                'volume': float(event.get('volume', 0) or 0),
                'active': bool(event.get('active', False)),
                'closed': bool(event.get('closed', False)),
            })
        
        return extracted
    
    def _extract_categories(self, market_id: str, market: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Extract category records from a market.
        
        Args:
            market_id: Parent market ID
            market: Raw market dict from API
            
        Returns:
            List of category dicts matching GAMMA_CATEGORY_SCHEMA
        """
        categories = market.get('categories', []) or []
        extracted = []
        
        for category in categories:
            extracted.append({
                'marketId': market_id,
                'categoryId': str(category.get('id', '')),
                'label': category.get('label', ''),
                'parentCategory': category.get('parentCategory', ''),
                'slug': category.get('slug', ''),
            })
        
        return extracted
    
    def _process_batch(
        self,
        markets: List[Dict[str, Any]],
        market_queue: Optional[Union[Queue, SwappableQueue]],
        event_queue: Optional[Union[Queue, SwappableQueue]],
        category_queue: Optional[Union[Queue, SwappableQueue]],
    ) -> tuple[int, int, int]:
        """
        Process a batch of markets, extracting nested objects to their queues.
        
        Args:
            markets: List of market dicts from API
            market_queue: Queue for market records
            event_queue: Queue for event records
            category_queue: Queue for category records
            
        Returns:
            Tuple of (market_count, event_count, category_count)
        """
        market_records = []
        event_records = []
        category_records = []
        
        for market in markets:
            market_id = market.get('id', '')
            
            # Extract market record
            market_records.append(self._extract_market_record(market))
            
            # Extract nested events
            event_records.extend(self._extract_events(market_id, market))
            
            # Extract nested categories
            category_records.extend(self._extract_categories(market_id, market))
        
        # Write to queues
        is_market_swappable = isinstance(market_queue, SwappableQueue)
        is_event_swappable = isinstance(event_queue, SwappableQueue)
        is_category_swappable = isinstance(category_queue, SwappableQueue)
        
        if market_queue is not None and market_records:
            if is_market_swappable:
                market_queue.put_many(market_records)
            else:
                for record in market_records:
                    market_queue.put(record)
        
        if event_queue is not None and event_records:
            if is_event_swappable:
                event_queue.put_many(event_records)
            else:
                for record in event_records:
                    event_queue.put(record)
        
        if category_queue is not None and category_records:
            if is_category_swappable:
                category_queue.put_many(category_records)
            else:
                for record in category_records:
                    category_queue.put(record)
        
        return len(market_records), len(event_records), len(category_records)
    
    def fetch_all_markets(
        self,
        limit: int = DEFAULT_LIMIT,
        **filters
    ) -> List[Dict[str, Any]]:
        """
        Fetch all markets from the Gamma API (simple mode, no persistence).
        
        Args:
            limit: Items per page
            **filters: Additional query parameters
            
        Returns:
            List of all market dictionaries
        """
        all_markets = []
        offset = 0
        
        while True:
            batch = self.fetch_markets_page(limit=limit, offset=offset, **filters)
            
            if not batch:
                break
            
            all_markets.extend(batch)
            print(f"[GammaMarketFetcher] Fetched {len(batch)} markets (total: {len(all_markets)})")
            
            if len(batch) < limit:
                break
            
            offset += limit
        
        return all_markets
    
    def fetch_all_markets_to_parquet(
        self,
        market_output_dir: str = "data/gamma_markets",
        event_output_dir: str = "data/gamma_events",
        category_output_dir: str = "data/gamma_categories",
        market_threshold: int = 1000,
        event_threshold: int = 1000,
        category_threshold: int = 1000,
        limit: int = DEFAULT_LIMIT,
        **filters
    ) -> Dict[str, int]:
        """
        Fetch all markets from Gamma API and persist to parquet files.
        
        Args:
            market_output_dir: Directory for market parquet files
            event_output_dir: Directory for event parquet files
            category_output_dir: Directory for category parquet files
            market_threshold: Batch threshold for market queue
            event_threshold: Batch threshold for event queue
            category_threshold: Batch threshold for category queue
            limit: Items per API page
            **filters: Additional query parameters
            
        Returns:
            Dict with counts: {"markets": n, "events": n, "categories": n}
        """
        # Check cursor - skip if already completed
        gamma_cursor = self._cursor_manager.get_gamma_market_cursor()
        if gamma_cursor.completed:
            print("[GammaMarketFetcher] Already completed. Reset cursor to run again.")
            return {"markets": 0, "events": 0, "categories": 0}
        
        # Create persisted queues
        market_queue, market_persister = create_gamma_market_persisted_queue(
            threshold=market_threshold,
            output_dir=market_output_dir,
            auto_start=True
        )
        event_queue, event_persister = create_gamma_event_persisted_queue(
            threshold=event_threshold,
            output_dir=event_output_dir,
            auto_start=True
        )
        category_queue, category_persister = create_gamma_category_persisted_queue(
            threshold=category_threshold,
            output_dir=category_output_dir,
            auto_start=True
        )
        
        total_markets = 0
        total_events = 0
        total_categories = 0
        offset = 0
        
        try:
            while True:
                batch = self.fetch_markets_page(limit=limit, offset=offset, **filters)
                
                if not batch:
                    break
                
                # Process and extract nested objects
                m_count, e_count, c_count = self._process_batch(
                    batch, market_queue, event_queue, category_queue
                )
                
                total_markets += m_count
                total_events += e_count
                total_categories += c_count
                
                print(
                    f"[GammaMarketFetcher] Fetched {len(batch)} markets "
                    f"(total: {total_markets} markets, {total_events} events, {total_categories} categories)"
                )
                
                if len(batch) < limit:
                    break
                
                offset += limit
            
            # Mark as completed
            self._cursor_manager.update_gamma_market_cursor(completed=True)
            self._cursor_manager.save_cursors()
            
        finally:
            # Stop persisters and flush
            market_persister.stop()
            event_persister.stop()
            category_persister.stop()
        
        print(
            f"[GammaMarketFetcher] Complete: {total_markets} markets, "
            f"{total_events} events, {total_categories} categories"
        )
        
        return {
            "markets": total_markets,
            "events": total_events,
            "categories": total_categories
        }
    
    def _worker(
        self,
        worker_id: int,
        market_queue: Union[Queue, SwappableQueue],
        event_queue: Union[Queue, SwappableQueue],
        category_queue: Union[Queue, SwappableQueue],
        stop_event: Optional[threading.Event] = None
    ) -> None:
        """
        Worker thread that fetches all Gamma markets.
        
        Args:
            worker_id: ID of this worker
            market_queue: Queue for market output
            event_queue: Queue for event output
            category_queue: Queue for category output
            stop_event: Optional event to signal stop
        """
        # Check cursor - skip if already completed
        gamma_cursor = self._cursor_manager.get_gamma_market_cursor()
        if gamma_cursor.completed:
            print(f"[GammaMarket Worker {worker_id}] Already completed. Skipping.")
            return
        
        print(f"[GammaMarket Worker {worker_id}] Starting Gamma market fetch...")
        
        total_markets = 0
        total_events = 0
        total_categories = 0
        offset = 0
        limit = self.DEFAULT_LIMIT
        
        # Retry configuration
        max_attempts = self._config.retry.max_attempts
        base_delay = self._config.retry.base_delay
        max_delay = self._config.retry.max_delay
        exponential_base = self._config.retry.exponential_base
        
        while stop_event is None or not stop_event.is_set():
            # Retry loop for each API call
            attempt = 0
            success = False
            batch = []
            
            while attempt < max_attempts and not success:
                attempt += 1
                try:
                    batch = self.fetch_markets_page(limit=limit, offset=offset)
                    success = True
                except httpx.HTTPStatusError as e:
                    if e.response.status_code == 429:
                        # Rate limited - wait and retry
                        delay = min(base_delay * (exponential_base ** attempt), max_delay)
                        logger.warning(
                            f"[GammaMarket Worker {worker_id}] Rate limited (429), "
                            f"waiting {delay:.1f}s (attempt {attempt}/{max_attempts})"
                        )
                        time.sleep(delay)
                    else:
                        logger.error(
                            f"[GammaMarket Worker {worker_id}] HTTP error: {e}"
                        )
                        raise
                except Exception as e:
                    logger.error(
                        f"[GammaMarket Worker {worker_id}] Error on attempt {attempt}: {e}"
                    )
                    if attempt >= max_attempts:
                        raise
                    delay = min(base_delay * (exponential_base ** attempt), max_delay)
                    time.sleep(delay)
            
            if not success:
                logger.error(f"[GammaMarket Worker {worker_id}] Max retries exceeded")
                break
            
            if not batch:
                # No more data
                break
            
            # Process batch
            m_count, e_count, c_count = self._process_batch(
                batch, market_queue, event_queue, category_queue
            )
            
            total_markets += m_count
            total_events += e_count
            total_categories += c_count
            
            print(
                f"[GammaMarket Worker {worker_id}] Fetched {len(batch)} markets "
                f"(total: {total_markets} markets, {total_events} events, {total_categories} categories)"
            )
            
            if len(batch) < limit:
                # Last page
                break
            
            offset += limit
        
        # Mark as completed (only if not stopped early)
        if stop_event is None or not stop_event.is_set():
            self._cursor_manager.update_gamma_market_cursor(completed=True)
            self._cursor_manager.save_cursors()
            print(f"[GammaMarket Worker {worker_id}] Completed successfully.")
        else:
            print(f"[GammaMarket Worker {worker_id}] Stopped early.")
    
    def run_workers(
        self,
        market_queue: Optional[Union[Queue, SwappableQueue]] = None,
        event_queue: Optional[Union[Queue, SwappableQueue]] = None,
        category_queue: Optional[Union[Queue, SwappableQueue]] = None,
        num_workers: int = 1,
        stop_event: Optional[threading.Event] = None
    ) -> List[threading.Thread]:
        """
        Start worker threads for fetching Gamma markets.
        
        Note: Only 1 worker is recommended since we do a full refresh.
        
        Args:
            market_queue: Queue for market output (uses instance queue if None)
            event_queue: Queue for event output (uses instance queue if None)
            category_queue: Queue for category output (uses instance queue if None)
            num_workers: Number of worker threads (default 1)
            stop_event: Optional event to signal stop
            
        Returns:
            List of started worker threads
        """
        market_queue = market_queue or self._market_output_queue
        event_queue = event_queue or self._event_output_queue
        category_queue = category_queue or self._category_output_queue
        
        if market_queue is None:
            raise ValueError("market_queue is required")
        if event_queue is None:
            raise ValueError("event_queue is required")
        if category_queue is None:
            raise ValueError("category_queue is required")
        
        threads = []
        for worker_id in range(num_workers):
            thread = threading.Thread(
                target=self._worker,
                args=(worker_id, market_queue, event_queue, category_queue, stop_event),
                name=f"GammaMarketFetcher-Worker-{worker_id}",
                daemon=True
            )
            thread.start()
            threads.append(thread)
        
        return threads
