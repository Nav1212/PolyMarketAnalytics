"""
Polymarket Data Fetcher Package

This package provides a comprehensive data fetching system for Polymarket,
supporting parallel fetching of markets, trades, prices, and leaderboard data
with automatic cursor persistence for resumable operations.

Package Structure:
    fetcher/
    ├── __init__.py          # This file - package entry point
    ├── config.py            # Configuration management
    ├── main.py              # CLI entry point
    ├── cursors/             # Cursor persistence for resumable fetching
    │   ├── __init__.py
    │   └── manager.py       # CursorManager and cursor dataclasses
    ├── workers/             # Data fetching workers
    │   ├── __init__.py
    │   ├── worker_manager.py    # Rate limiting and worker coordination
    │   ├── trade_fetcher.py     # Trade data fetcher
    │   ├── market_fetcher.py    # Market data fetcher
    │   ├── price_fetcher.py     # Price history fetcher
    │   └── leaderboard_fetcher.py  # Leaderboard data fetcher
    ├── persistence/         # Data persistence layer
    │   ├── __init__.py
    │   ├── swappable_queue.py   # Thread-safe queue with atomic swap
    │   └── parquet_persister.py # Non-blocking parquet writer
    ├── coordination/        # Orchestration layer
    │   ├── __init__.py
    │   └── coordinator.py   # FetcherCoordinator - main orchestrator
    └── utils/               # Shared utilities
        ├── __init__.py
        ├── logging_config.py
        ├── exceptions.py
        └── retry.py

Usage:
    # From command line:
    python -m fetcher --mode all
    python -m fetcher --mode trades --limit 100
    
    # Programmatic usage:
    from fetcher import run, FetcherCoordinator
    
    # Quick start - run all fetchers
    run()
    
    # Or use coordinator directly
    from fetcher import FetcherCoordinator
    coordinator = FetcherCoordinator()
    queues = coordinator.run_all()
    coordinator.wait_for_completion()
"""

from fetcher.config import Config, get_config, set_config
from fetcher.coordination import FetcherCoordinator, LoadOrder
from fetcher.cursors import (
    CursorManager,
    get_cursor_manager,
    set_cursor_manager,
    TradeCursor,
    PriceCursor,
    LeaderboardCursor,
    MarketCursor,
    Cursors,
)
from fetcher.workers import (
    WorkerManager,
    get_worker_manager,
    set_worker_manager,
    TradeFetcher,
    MarketFetcher,
    PriceFetcher,
    LeaderboardFetcher,
)
from fetcher.persistence import (
    SwappableQueue,
    ParquetPersister,
    DataType,
    create_persisted_queue,
    create_trade_persisted_queue,
    create_market_persisted_queue,
    create_market_token_persisted_queue,
    create_price_persisted_queue,
    create_leaderboard_persisted_queue,
)


def run(
    mode: str = "all",
    limit: int | None = None,
    timeout: int | None = None,
    fresh: bool = False,
) -> dict:
    """
    Run the Polymarket data fetcher.
    
    This is a convenience function that creates a coordinator and runs
    the specified fetch mode. For more control, use FetcherCoordinator directly.
    
    Args:
        mode: Fetch mode - "all", "trades", "markets", "prices", or "leaderboard"
        limit: Optional limit on number of items to process
        timeout: Optional timeout in seconds
        fresh: If True, ignore existing cursors and start fresh
    
    Returns:
        Dictionary containing output queues and statistics
    
    Example:
        from fetcher import run
        
        # Run all fetchers
        result = run()
        
        # Run only trades with a limit
        result = run(mode="trades", limit=100)
    """
    config = get_config()
    
    # Handle fresh start
    if fresh:
        cursor_manager = get_cursor_manager()
        cursor_manager.clear_cursors()
    
    coordinator = FetcherCoordinator(config=config)
    
    if mode == "all":
        queues = coordinator.run_all()
        coordinator.wait_for_completion(timeout=timeout)
        return queues
    elif mode == "trades":
        queue = coordinator.run_trades(market_ids=[], num_workers=limit)
        coordinator.wait_for_completion(timeout=timeout)
        return {"trade_queue": queue}
    elif mode == "markets":
        queue = coordinator.run_markets()
        coordinator.wait_for_completion(timeout=timeout)
        return {"market_queue": queue}
    elif mode == "leaderboard":
        queue = coordinator.run_leaderboard()
        coordinator.wait_for_completion(timeout=timeout)
        return {"leaderboard_queue": queue}
    else:
        raise ValueError(f"Unknown mode: {mode}")


__all__ = [
    # Main entry point
    "run",
    
    # Configuration
    "Config",
    "get_config",
    "set_config",
    
    # Coordination
    "FetcherCoordinator",
    "LoadOrder",
    
    # Cursors
    "CursorManager",
    "get_cursor_manager",
    "set_cursor_manager",
    "TradeCursor",
    "PriceCursor",
    "LeaderboardCursor",
    "MarketCursor",
    "Cursors",
    
    # Workers
    "WorkerManager",
    "get_worker_manager",
    "set_worker_manager",
    "TradeFetcher",
    "MarketFetcher",
    "PriceFetcher",
    "LeaderboardFetcher",
    
    # Persistence
    "SwappableQueue",
    "ParquetPersister",
    "DataType",
    "create_persisted_queue",
    "create_trade_persisted_queue",
    "create_market_persisted_queue",
    "create_market_token_persisted_queue",
    "create_price_persisted_queue",
    "create_leaderboard_persisted_queue",
]
