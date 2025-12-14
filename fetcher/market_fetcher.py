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
class MarketFetcher:
    """
    Keeping this hear in case the rate limit matters endpoint documentation unclear
    """
    RATE = 100  # requests per 10 second
    tokens = RATE
    last_refill = time.time()
    lock = threading.Lock()

    @classmethod
    def acquire_token(cls):
        with cls.lock:
            current_time = time.time()
            elapsed = current_time - cls.last_refill
            if elapsed >= 10:
                # Refill tokens
                cls.tokens = cls.RATE
                cls.last_refill = current_time

            if cls.tokens > 0:
                cls.tokens -= 1
                return True
            else:
                return False
    CLOB_API_BASE = "https://data-api.polymarket.com"
    
    def __init__(self, timeout: float = 30.0):
        """
        Initialize the trade fetcher.
        
        Args:
            timeout: Request timeout in seconds
        """
        self.client = ClobClient(
        host="https://clob.polymarket.com",
        chain_id=137
        )
    

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
            # Rate limiting
            while not self.acquire_token():
                time.sleep(0.1)  # Wait for token
            
            response = self.client.get_markets(next_cursor=self.int_to_base64_urlsafe(cursor))
            )
            response.raise_for_status()
            data = response.json()
            cursor+= 1
            batch = data.get("markets", [])
            if not batch:
                break  # No more markets
            
            markets.extend(batch)
            print(f"Fetched {len(batch)} markets (total: {len(markets)})")
            page += 1
        
        return markets

##testing implementation
def main():
    with MarketFetcher() as fetcher:
        markets = fetcher.fetch_all_markets()
        print(f"Total markets fetched: {len(markets)}")
        # Here you can process or store the markets as needed