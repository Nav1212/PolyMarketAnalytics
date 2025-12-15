"""
Simple Market Fetcher for Polymarket
Fetches markets, all markets
"""

import base64
import httpx
from typing import List, Dict, Any, Union, Optional
from datetime import datetime
from queue import Queue, Empty
import threading
import time
from py_clob_client.client import ClobClient
from swappable_queue import SwappableQueue
from parquet_persister import ParquetPersister, create_market_persisted_queue
from worker_manager import WorkerManager, get_worker_manager
from config import get_config, Config


class MarketFetcher:
    """
    Simple Market Fetcher for Polymarket
    """
    CLOB_API_BASE = "https://data-api.polymarket.com"
    
    def __init__(
        self,
        timeout: float = 30.0,
        worker_manager: WorkerManager = None,
        config: Config = None,
        trade_market_queue: Queue = None,
        price_token_queue: Queue = None,
        leaderboard_market_queue: Queue = None,
        output_queue: Union[Queue, SwappableQueue] = None
    ):
        """
        Initialize the market fetcher.
        
        Args:
            timeout: Request timeout in seconds
            worker_manager: WorkerManager instance for rate limiting (uses default if None)
            config: Config object (uses global config if None)
            trade_market_queue: Queue to write market condition IDs for trade fetcher
            price_token_queue: Queue to write token IDs for price fetcher
            leaderboard_market_queue: Queue to write market condition IDs for leaderboard fetcher
            output_queue: Queue to write market data to (used by coordinator)
        """
        self._config = config or get_config()
        self.client = ClobClient(
            host="https://clob.polymarket.com",
            chain_id=137
        )
        self._manager = worker_manager or get_worker_manager()
        self._trade_market_queue = trade_market_queue
        self._price_token_queue = price_token_queue
        self._leaderboard_market_queue = leaderboard_market_queue
        self._output_queue = output_queue
    
    def close(self):
        """Close resources"""
        pass

    def __enter__(self):
        return self
    
    def __exit__(self, *args):
        self.close()
    
    def _enqueue_markets_for_fetchers(self, markets: List[Dict[str, Any]]) -> None:
        """
        Write market condition IDs and token IDs to the trade, price, and leaderboard queues.
        
        Args:
            markets: List of market dictionaries from API
        """
        for market in markets:
            condition_id = market.get("condition_id")
            
            # Write to trade fetcher queue
            if self._trade_market_queue is not None and condition_id:
                self._trade_market_queue.put(condition_id)
            
            # Write to leaderboard fetcher queue
            if self._leaderboard_market_queue is not None and condition_id:
                self._leaderboard_market_queue.put(condition_id)
            
            # Write token IDs to price fetcher queue
            if self._price_token_queue is not None:
                tokens = market.get("tokens", [])
                for token in tokens:
                    token_id = token.get("token_id")
                    if token_id:
                        self._price_token_queue.put(token_id)
    
    @staticmethod
    def int_to_base64_urlsafe(n: int) -> str:
        if n < 0:
            raise ValueError("Only non-negative integers are supported")

        byte_length = max(1, (n.bit_length() + 7) // 8)
        b = n.to_bytes(byte_length, "big")
        return base64.urlsafe_b64encode(b).rstrip(b"=").decode("ascii")

    def fetch_all_markets(self) -> List[Dict[str, Any]]:
        """
        Fetch all markets from Polymarket API.
        
        Returns:
            List of market dictionaries
        """
        markets = []
        page = 1
        next_cursor = None
        while True:
            loop_start = time.time()
            # Rate limiting via worker manager
            self._manager.acquire_market(loop_start)
            
            response = self.client.get_markets(next_cursor=next_cursor) if next_cursor else self.client.get_markets()
            data = response
            next_cursor = data.get("next_cursor")
            batch = data.get("data", [])
            if not batch:
                break  # No more markets
            
            # Enqueue for trade and price fetchers
            self._enqueue_markets_for_fetchers(batch)
            
            markets.extend(batch)
            print(f"Fetched {len(batch)} markets (total: {len(markets)})")
            page += 1
        
        return markets

    def fetch_all_markets_to_parquet(
        self,
        output_dir: str = "data/markets",
        batch_threshold: int = 1000
    ) -> SwappableQueue:
        """
        Fetch all markets from Polymarket API and persist to parquet files.
        
        Args:
            output_dir: Directory for parquet files
            batch_threshold: Number of items that triggers a parquet write
        
        Returns:
            SwappableQueue containing fetched markets
        """
        # Create persisted queue for markets
        market_queue, persister = create_market_persisted_queue(
            threshold=batch_threshold,
            output_dir=output_dir,
            auto_start=True
        )
        
        next_cursor = None
        total_markets = 0
        
        while True:
            loop_start = time.time()
            # Rate limiting via worker manager
            self._manager.acquire_market(loop_start)
            
            response = self.client.get_markets(next_cursor=next_cursor) if next_cursor else self.client.get_markets()
            data = response
            next_cursor = data.get("next_cursor")
            batch = data.get("data", [])
            
            if not batch:
                break  # No more markets
            
            # Enqueue for trade and price fetchers
            self._enqueue_markets_for_fetchers(batch)
            
            # Add to persisted queue
            market_queue.put_many(batch)
            total_markets += len(batch)
            print(f"Fetched {len(batch)} markets (total: {total_markets})")
        
        # Stop persister and flush remaining
        persister.stop()
        
        print(f"Total markets fetched and persisted: {total_markets}")
        return market_queue
    
    def _worker(
        self,
        worker_id: int,
        output_queue: Union[Queue, SwappableQueue],
        stop_event: threading.Event = None
    ):
        """
        Worker thread that fetches all markets.
        
        Args:
            worker_id: ID of this worker
            output_queue: Queue to add fetched markets to
            stop_event: Optional event to signal stop
        """
        is_swappable = isinstance(output_queue, SwappableQueue)
        next_cursor = None
        total_markets = 0
        
        print(f"[Market Worker {worker_id}] Starting market fetch...")
        
        while stop_event is None or not stop_event.is_set():
            loop_start = time.time()
            self._manager.acquire_market(loop_start)
            
            try:
                response = self.client.get_markets(next_cursor=next_cursor) if next_cursor else self.client.get_markets()
                batch = response.get("data", [])
                next_cursor = response.get("next_cursor")
                
                if not batch:
                    break
                
                # Enqueue for downstream fetchers
                self._enqueue_markets_for_fetchers(batch)
                
                # Add to output queue
                if is_swappable:
                    output_queue.put_many(batch)
                else:
                    for market in batch:
                        output_queue.put(market)
                
                total_markets += len(batch)
                print(f"[Market Worker {worker_id}] Fetched {len(batch)} markets (total: {total_markets})")
                
            except Exception as e:
                print(f"[Market Worker {worker_id}] Error: {e}")
                break
        
        print(f"[Market Worker {worker_id}] Finished, total markets: {total_markets}")
    
    def run_workers(
        self,
        output_queue: Union[Queue, SwappableQueue] = None,
        num_workers: int = None,
        stop_event: threading.Event = None
    ) -> List[threading.Thread]:
        """
        Start worker threads to fetch markets.
        
        Args:
            output_queue: Queue to add fetched markets to (uses instance output_queue if None)
            num_workers: Number of workers (uses config if None, typically 1 for markets)
            stop_event: Optional event to signal stop
        
        Returns:
            List of started threads (caller should join them)
        """
        # Use instance output queue if not provided
        output_queue = output_queue or self._output_queue
        if output_queue is None:
            raise ValueError("output_queue must be provided either in constructor or run_workers call")
        
        if num_workers is None:
            num_workers = self._config.workers.market_workers
        
        threads = []
        for i in range(num_workers):
            t = threading.Thread(
                target=self._worker,
                args=(i, output_queue, stop_event),
                daemon=True
            )
            t.start()
            threads.append(t)
        
        return threads


##testing implementation
def main():
    # Create centralized worker manager
    worker_manager = WorkerManager(trade_rate=70, market_rate=100)
    
    with MarketFetcher(worker_manager=worker_manager) as fetcher:
        markets = fetcher.fetch_all_markets()
        print(f"Total markets fetched: {len(markets)}")
        # Here you can process or store the markets as needed
    
    # Print rate limit timing statistics
    worker_manager.print_statistics()