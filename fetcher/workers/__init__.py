"""
Workers module - Contains all data fetcher workers.

This module isolates business logic for fetching data from various 
Polymarket APIs. Each fetcher is responsible for a single data type.

Exports:
    - TradeFetcher: Fetches trade data
    - MarketFetcher: Fetches market data
    - PriceFetcher: Fetches price history data
    - LeaderboardFetcher: Fetches leaderboard data
    - WorkerManager: Centralized rate limiting and timing statistics
    - get_worker_manager, set_worker_manager: Global singleton accessors
"""

from fetcher.workers.trade_fetcher import TradeFetcher
from fetcher.workers.market_fetcher import MarketFetcher
from fetcher.workers.price_fetcher import PriceFetcher
from fetcher.workers.leaderboard_fetcher import LeaderboardFetcher
from fetcher.workers.worker_manager import (
    WorkerManager,
    get_worker_manager,
    set_worker_manager,
    TokenBucket,
)

__all__ = [
    "TradeFetcher",
    "MarketFetcher",
    "PriceFetcher",
    "LeaderboardFetcher",
    "WorkerManager",
    "get_worker_manager",
    "set_worker_manager",
    "TokenBucket",
]
