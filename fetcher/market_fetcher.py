"""
Simple Market Fetcher for Polymarket
Fetches markets, all markets
"""

import httpx
from typing import List, Dict, Any, Union
from datetime import datetime
from queue import Queue, Empty
import threading
import time

from swappable_queue import SwappableQueue
from parquet_persister import ParquetPersister, create_persisted_queue
class TradeFetcher:
    """
    Simple class to fetch trades from Polymarket Data API
    """
    RATE = 70  # requests per 10 second
    tokens = RATE
    last_refill = time.time()
    lock = threading.Lock()
