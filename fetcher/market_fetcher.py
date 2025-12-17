"""
Simple Market Fetcher for Polymarket
Fetches markets, all markets
"""

import base64
import httpx
import json
from typing import List, Dict, Any, Union, Optional
from datetime import datetime
from pathlib import Path
from queue import Queue, Empty
import threading
import time
from py_clob_client.client import ClobClient
from swappable_queue import SwappableQueue
from parquet_persister import ParquetPersister, create_market_persisted_queue
from worker_manager import WorkerManager, get_worker_manager
from config import get_config, Config
from cursor_manager import CursorManager, get_cursor_manager


class MarketFetcher:
    """
    Simple Market Fetcher for Polymarket
    """
    CLOB_API_BASE = "https://data-api.polymarket.com"
    
    def __init__(
        self,
        timeout: float = 30.0,
        worker_manager: Optional[WorkerManager] = None,
        config: Optional[Config] = None,
        trade_market_queue: Optional[Queue] = None,
        price_token_queue: Optional[Queue] = None,
        leaderboard_market_queue: Optional[Queue] = None,
        output_queue: Optional[Union[Queue, SwappableQueue]] = None,
        market_token_queue: Optional[Union[Queue, SwappableQueue]] = None,
        cursor_manager: Optional[CursorManager] = None
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
            market_token_queue: Queue to write extracted token data for parquet persistence
            cursor_manager: CursorManager for progress persistence (uses global if None)
        """
        self._config = config or get_config()
        self._cursor_manager = cursor_manager or get_cursor_manager()
        self.client = ClobClient(
            host="https://clob.polymarket.com",
            chain_id=137
        )
        self._manager = worker_manager or get_worker_manager()
        self._trade_market_queue = trade_market_queue
        self._price_token_queue = price_token_queue
        self._leaderboard_market_queue = leaderboard_market_queue
        self._output_queue = output_queue
        self._market_token_queue = market_token_queue
    
    def close(self):
        """Close resources"""
        pass

    def __enter__(self):
        return self
    
    def __exit__(self, *args):
        self.close()
    
    def _enqueue_markets_for_fetchers(self, markets: List[Dict[str, Any]]) -> None:
        """
        Write market condition IDs and token IDs to the trade, price, leaderboard, and token queues.
        
        Args:
            markets: List of market dictionaries from API
        """
        is_token_queue_swappable = isinstance(self._market_token_queue, SwappableQueue)
        token_batch = []  # Collect tokens for batch insert
        
        for market in markets:
            condition_id = market.get("condition_id")
            
            # Write to trade fetcher queue
            if self._trade_market_queue is not None and condition_id:
                self._trade_market_queue.put(condition_id)
            
            # Write to leaderboard fetcher queue
            if self._leaderboard_market_queue is not None and condition_id:
                self._leaderboard_market_queue.put(condition_id)
            
            # Extract tokens for parquet persistence and price fetcher
            tokens = market.get("tokens", [])
            # Get market start time from game_start_time or end_date_iso
            market_start = market.get("game_start_time") or market.get("end_date_iso")
            market_start_ts = None
            if market_start:
                try:
                    # Parse ISO format datetime
                    dt = datetime.fromisoformat(market_start.replace('Z', '+00:00'))
                    market_start_ts = int(dt.timestamp())
                except (ValueError, AttributeError):
                    pass
            
            for token in tokens:
                token_id = token.get("token_id")
                if token_id:
                    # Write to price fetcher queue
                    if self._price_token_queue is not None:
                        self._price_token_queue.put((token_id, market_start_ts))
                    
                    # Extract token data for parquet (matches MARKET_TOKEN_SCHEMA)
                    if self._market_token_queue is not None:
                        token_record = {
                            'condition_Id': condition_id,
                            'token_id': token_id,
                            'price': float(token.get('price', 0.0)) if token.get('price') else 0.0,
                            'winner': bool(token.get('winner', False))
                        }
                        token_batch.append(token_record)
        
        # Write tokens to queue
        if self._market_token_queue is not None and token_batch:
            if is_token_queue_swappable:
                # Type is SwappableQueue, use put_many
                swappable_queue: SwappableQueue = self._market_token_queue  # type: ignore
                swappable_queue.put_many(token_batch)
            else:
                for token_record in token_batch:
                    self._market_token_queue.put(token_record)
    
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
            data: Dict[str, Any] = response  # type: ignore[assignment]
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
        stop_event: Optional[threading.Event] = None
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
                response: Dict[str, Any] = self.client.get_markets(next_cursor=next_cursor) if next_cursor else self.client.get_markets()  # type: ignore[assignment]
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
        
        # Send sentinel values to downstream fetchers to signal completion
        self._send_shutdown_signals()
        
        print(f"[Market Worker {worker_id}] Finished, total markets: {total_markets}")
    
    def _send_shutdown_signals(self) -> None:
        """Send None sentinels to downstream queues and save queue state to disk."""
        from config import get_config
        config = get_config()
        
        # Save downstream queue contents before sending sentinels
        self._save_downstream_queues()
        
        # Send sentinel to trade queue for each trade worker
        if self._trade_market_queue is not None:
            for _ in range(config.workers.trade):
                self._trade_market_queue.put(None)
        
        # Send sentinel to price queue for each price worker
        if self._price_token_queue is not None:
            for _ in range(config.workers.price):
                self._price_token_queue.put(None)
        
        # Send sentinel to leaderboard queue for each leaderboard worker
        if self._leaderboard_market_queue is not None:
            for _ in range(config.workers.leaderboard):
                self._leaderboard_market_queue.put(None)
    
    def _save_downstream_queues(self) -> None:
        """
        Save the current contents of downstream queues to disk for recovery.
        This allows resuming from where we left off if the process is interrupted.
        """
        queue_state = {
            "trade_markets": [],
            "price_tokens": []
        }
        
        # Copy trade queue contents (thread-safe)
        if self._trade_market_queue is not None:
            with self._trade_market_queue.mutex:
                trade_markets = list(self._trade_market_queue.queue)
            queue_state["trade_markets"] = [m for m in trade_markets if m is not None]
        
        # Copy price queue contents
        if self._price_token_queue is not None:
            with self._price_token_queue.mutex:
                price_tokens = list(self._price_token_queue.queue)
            queue_state["price_tokens"] = [
                list(t) if isinstance(t, tuple) else t 
                for t in price_tokens if t is not None
            ]
        
        # Save to file
        queue_file = Path(__file__).parent / "downstream_queues.json"
        try:
            with open(queue_file, 'w') as f:
                json.dump(queue_state, f, indent=2)
            print(f"Saved downstream queue state: {len(queue_state['trade_markets'])} trades, {len(queue_state['price_tokens'])} prices")
        except Exception as e:
            print(f"Error saving downstream queues: {e}")
    
    def restore_downstream_queues(self) -> bool:
        """
        Restore downstream queues from saved state file.
        
        Returns:
            True if queues were restored, False if no saved state
        """
        queue_file = Path(__file__).parent / "downstream_queues.json"
        if not queue_file.exists():
            return False
        
        try:
            with open(queue_file, 'r') as f:
                queue_state = json.load(f)
            
            restored = False
            
            if self._trade_market_queue is not None:
                for market in queue_state.get("trade_markets", []):
                    self._trade_market_queue.put(market)
                if queue_state.get("trade_markets"):
                    print(f"Restored {len(queue_state['trade_markets'])} markets to trade queue")
                    restored = True
            
            if self._price_token_queue is not None:
                for token in queue_state.get("price_tokens", []):
                    if isinstance(token, list):
                        self._price_token_queue.put(tuple(token))
                    else:
                        self._price_token_queue.put(token)
                if queue_state.get("price_tokens"):
                    print(f"Restored {len(queue_state['price_tokens'])} tokens to price queue")
                    restored = True
            
            if restored:
                queue_file.unlink()
                print("Deleted downstream_queues.json after restore")
            
            return restored
            
        except Exception as e:
            print(f"Error restoring downstream queues: {e}")
            return False
    
    def run_workers(
        self,
        output_queue: Optional[Union[Queue, SwappableQueue]] = None,
        num_workers: Optional[int] = None,
        stop_event: Optional[threading.Event] = None
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
            num_workers = self._config.workers.market
        
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