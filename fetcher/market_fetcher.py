"""
Simple Market Fetcher for Polymarket
Fetches markets, all markets
"""

import base64
import httpx
from typing import List, Dict, Any, Union
from datetime import datetime
from queue import Queue, Empty
import threading
import time
from py_clob_client.client import ClobClient
from swappable_queue import SwappableQueue
from parquet_persister import ParquetPersister, create_persisted_queue
from worker_manager import WorkerManager, get_worker_manager


class MarketFetcher:
    """
    Simple Market Fetcher for Polymarket
    """
    CLOB_API_BASE = "https://data-api.polymarket.com"
    
    def __init__(self, timeout: float = 30.0, worker_manager: WorkerManager = None):
        """
        Initialize the market fetcher.
        
        Args:
            timeout: Request timeout in seconds
            worker_manager: WorkerManager instance for rate limiting (uses default if None)
        """
        self.client = ClobClient(
            host="https://clob.polymarket.com",
            chain_id=137
        )
        self._manager = worker_manager or get_worker_manager()
    

    def close(self):
        """Close HTTP client"""
        self.client.close()
    def __enter__(self):
        return self
    
    def __exit__(self, *args):
        self.close()
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
        cursor = 0
        while True:
            loop_start = time.time()
            # Rate limiting via worker manager
            self._manager.acquire_market(loop_start)
            
            response = self.client.get_markets(next_cursor=self.int_to_base64_urlsafe(cursor))
            data = response
            cursor += 1
            batch = data.get("data", [])
            if not batch:
                break  # No more markets
            
            markets.extend(batch)
            print(f"Fetched {len(batch)} markets (total: {len(markets)})")
            page += 1
        
        return markets

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