"""
Kalshi Data Pipeline - Bronze Layer Fetcher

A multi-threaded data pipeline for fetching Kalshi prediction market data
with cursor-based resume capability.

Usage:
    # Full pipeline
    python -m kalshi_fetcher.main --mode=all
    
    # Events only
    python -m kalshi_fetcher.main --mode=events
    
    # Markets only  
    python -m kalshi_fetcher.main --mode=markets
    
    # Resume from last position (default behavior)
    python -m kalshi_fetcher.main --mode=all
    
    # Fresh start (ignore cursors)
    python -m kalshi_fetcher.main --mode=all --fresh

Architecture:
    FetcherCoordinator → Workers (EventFetcher, MarketFetcher, TradeFetcher, OrderbookFetcher)
                              ↓
                       SwappableQueue (thread-safe batching)
                              ↓
                       ParquetPersister → data/kalshi/{type}/dt=YYYY-MM-DD/*.parquet

Data Flow:
    EventFetcher → event_ticker → MarketFetcher
    MarketFetcher → ticker → TradeFetcher, OrderbookFetcher
"""

from kalshi_fetcher.config import get_config, Config
from kalshi_fetcher.coordination import FetcherCoordinator, LoadOrder
from kalshi_fetcher.workers import (
    EventFetcher,
    MarketFetcher,
    TradeFetcher,
    OrderbookFetcher,
    WorkerManager,
    get_worker_manager,
    set_worker_manager,
)
from kalshi_fetcher.persistence import (
    SwappableQueue,
    ParquetPersister,
    DataType,
    create_event_persisted_queue,
    create_market_persisted_queue,
    create_trade_persisted_queue,
    create_orderbook_persisted_queue,
)
from kalshi_fetcher.cursors import (
    CursorManager,
    get_cursor_manager,
    set_cursor_manager,
)

__version__ = "0.1.0"

__all__ = [
    # Config
    "get_config",
    "Config",
    
    # Coordination
    "FetcherCoordinator",
    "LoadOrder",
    
    # Workers
    "EventFetcher",
    "MarketFetcher",
    "TradeFetcher",
    "OrderbookFetcher",
    "WorkerManager",
    "get_worker_manager",
    "set_worker_manager",
    
    # Persistence
    "SwappableQueue",
    "ParquetPersister",
    "DataType",
    "create_event_persisted_queue",
    "create_market_persisted_queue",
    "create_trade_persisted_queue",
    "create_orderbook_persisted_queue",
    
    # Cursors
    "CursorManager",
    "get_cursor_manager",
    "set_cursor_manager",
]
