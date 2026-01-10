"""
Workers module - Contains all data fetcher workers.

This module isolates business logic for fetching data from Kalshi API.
Each fetcher is responsible for a single data type.

Exports:
    - EventFetcher: Fetches event data
    - MarketFetcher: Fetches market data
    - TradeFetcher: Fetches trade data
    - OrderbookFetcher: Fetches orderbook snapshots
    - WorkerManager: Centralized rate limiting and timing statistics
    - get_worker_manager, set_worker_manager: Global singleton accessors
"""

from kalshi_fetcher.workers.event_fetcher import EventFetcher
from kalshi_fetcher.workers.market_fetcher import MarketFetcher
from kalshi_fetcher.workers.trade_fetcher import TradeFetcher
from kalshi_fetcher.workers.orderbook_fetcher import OrderbookFetcher
from kalshi_fetcher.workers.worker_manager import (
    WorkerManager,
    TokenBucket,
    get_worker_manager,
    set_worker_manager,
)

__all__ = [
    "EventFetcher",
    "MarketFetcher",
    "TradeFetcher",
    "OrderbookFetcher",
    "WorkerManager",
    "TokenBucket",
    "get_worker_manager",
    "set_worker_manager",
]
