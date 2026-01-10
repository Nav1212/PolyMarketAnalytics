"""
FetcherCoordinator - Orchestrates all Kalshi fetchers with proper load ordering.

Load Order:
    1. EventFetcher - Runs first, populates event tickers for market fetcher
    2. MarketFetcher - Runs after events, populates tickers for trade/orderbook fetchers
    3. TradeFetcher, OrderbookFetcher - Run in parallel after markets

Dependencies:
    EventFetcher → MarketFetcher (via event_ticker)
    MarketFetcher → TradeFetcher (via ticker)
    MarketFetcher → OrderbookFetcher (via ticker)

Cursor Persistence:
    On shutdown, cursors are saved to kalshi_cursor.json for resuming.
"""

from enum import IntEnum
from queue import Queue
from threading import Thread
from typing import Optional, List

import time

from kalshi_fetcher.config import Config, get_config
from kalshi_fetcher.workers import (
    WorkerManager,
    get_worker_manager,
    set_worker_manager,
    EventFetcher,
    MarketFetcher,
    TradeFetcher,
    OrderbookFetcher,
)
from kalshi_fetcher.persistence import (
    SwappableQueue,
    ParquetPersister,
    create_event_persisted_queue,
    create_market_persisted_queue,
    create_trade_persisted_queue,
    create_orderbook_persisted_queue,
    DataType,
)
from kalshi_fetcher.cursors import (
    CursorManager,
    get_cursor_manager,
    set_cursor_manager,
)
from kalshi_fetcher.utils.logging_config import get_logger

logger = get_logger("coordinator")


class LoadOrder(IntEnum):
    """Hardcoded load order for fetchers."""
    EVENT = 1      # Runs first - populates event tickers
    MARKET = 2     # Runs after events
    TRADE = 3      # Runs after markets (parallel with orderbook)
    ORDERBOOK = 3  # Runs after markets (parallel with trade)


class FetcherCoordinator:
    """
    Coordinates all fetchers with proper load ordering and queue management.
    
    EventFetcher runs first and feeds event_tickers to MarketFetcher.
    MarketFetcher feeds tickers to TradeFetcher and OrderbookFetcher.
    
    Supports cursor persistence for resuming from interruptions.
    """
    
    def __init__(self, config: Optional[Config] = None):
        """
        Initialize the coordinator with configuration.
        
        Args:
            config: Configuration object. If None, uses global config.
        """
        self._config = config or get_config()
        
        # Initialize WorkerManager with rate limits from config
        self._manager = WorkerManager(
            event_rate=self._config.rate_limits.event,
            market_rate=self._config.rate_limits.market,
            trade_rate=self._config.rate_limits.trade,
            orderbook_rate=self._config.rate_limits.orderbook,
            window_seconds=self._config.rate_limits.window_seconds,
            config=self._config
        )
        set_worker_manager(self._manager)
        
        # Initialize cursor manager for persistence
        self._cursor_manager = CursorManager(
            cursor_file=self._config.cursors.filename,
            auto_save=True,
            enabled=self._config.cursors.enabled
        )
        set_cursor_manager(self._cursor_manager)
        
        # Queues and persisters (initialized per run)
        self._event_queue: Optional[SwappableQueue] = None
        self._event_persister: Optional[ParquetPersister] = None
        self._market_queue: Optional[SwappableQueue] = None
        self._market_persister: Optional[ParquetPersister] = None
        self._trade_queue: Optional[SwappableQueue] = None
        self._trade_persister: Optional[ParquetPersister] = None
        self._orderbook_queue: Optional[SwappableQueue] = None
        self._orderbook_persister: Optional[ParquetPersister] = None
        
        # Inter-fetcher queues
        self._event_to_market_queue: Optional[Queue] = None
        self._market_to_trade_queue: Optional[Queue] = None
        self._market_to_orderbook_queue: Optional[Queue] = None
    
    def run_all(
        self,
        fresh: bool = False,
        event_limit: Optional[int] = None,
        market_limit: Optional[int] = None,
    ) -> dict:
        """
        Run full pipeline: events → markets → trades + orderbooks.
        
        Args:
            fresh: If True, ignore existing cursors and start fresh
            event_limit: Maximum events to fetch
            market_limit: Maximum markets to fetch
        
        Returns:
            Dict with statistics from the run
        """
        if fresh:
            self._cursor_manager.clear_cursors()
        else:
            self._cursor_manager.load_cursors()
        
        start_time = time.time()
        
        # Initialize queues and persisters
        self._init_queues_and_persisters()
        
        try:
            # Start all persisters
            self._start_persisters()
            
            # Phase 1: Fetch events
            logger.info("Phase 1: Fetching events...")
            event_count = self._run_event_fetcher(limit=event_limit)
            
            # Signal end of events
            if self._event_to_market_queue:
                self._event_to_market_queue.put(None)
            
            # Phase 2: Fetch markets
            logger.info("Phase 2: Fetching markets...")
            market_count = self._run_market_fetcher(limit=market_limit)
            
            # Signal end of markets
            if self._market_to_trade_queue:
                self._market_to_trade_queue.put(None)
            if self._market_to_orderbook_queue:
                self._market_to_orderbook_queue.put(None)
            
            # Phase 3: Fetch trades and orderbooks in parallel
            logger.info("Phase 3: Fetching trades and orderbooks...")
            trade_thread = Thread(target=self._run_trade_fetcher, name="TradeFetcher")
            orderbook_thread = Thread(target=self._run_orderbook_fetcher, name="OrderbookFetcher")
            
            trade_thread.start()
            orderbook_thread.start()
            
            trade_thread.join()
            orderbook_thread.join()
            
            elapsed = time.time() - start_time
            
            # Get final stats
            stats = {
                "events": event_count,
                "markets": market_count,
                "elapsed_seconds": elapsed,
                "rate_limit_stats": self._manager.get_stats(),
            }
            
            logger.info(f"Pipeline complete in {elapsed:.2f}s")
            logger.info(f"Stats: {stats}")
            
            return stats
            
        finally:
            # Stop persisters and flush remaining data
            self._stop_persisters()
            
            # Save cursors
            self._cursor_manager.save_cursors()
    
    def run_events(self, limit: Optional[int] = None, fresh: bool = False) -> int:
        """
        Run only event fetching.
        
        Args:
            limit: Maximum events to fetch
            fresh: Start fresh ignoring cursors
        
        Returns:
            Number of events fetched
        """
        if fresh:
            self._cursor_manager.clear_cursors()
        else:
            self._cursor_manager.load_cursors()
        
        # Initialize event queue and persister
        self._event_queue, self._event_persister = create_event_persisted_queue(
            output_dir=self._config.output_dirs.event,
            threshold=self._config.queues.event_threshold
        )
        
        try:
            self._event_persister.start()
            count = self._run_event_fetcher(limit=limit)
            return count
        finally:
            self._event_persister.stop(flush=True)
            self._cursor_manager.save_cursors()
    
    def run_markets(self, limit: Optional[int] = None, fresh: bool = False) -> int:
        """
        Run only market fetching.
        
        Args:
            limit: Maximum markets to fetch
            fresh: Start fresh ignoring cursors
        
        Returns:
            Number of markets fetched
        """
        if fresh:
            self._cursor_manager.clear_cursors()
        else:
            self._cursor_manager.load_cursors()
        
        # Initialize market queue and persister
        self._market_queue, self._market_persister = create_market_persisted_queue(
            output_dir=self._config.output_dirs.market,
            threshold=self._config.queues.market_threshold
        )
        
        try:
            self._market_persister.start()
            count = self._run_market_fetcher(limit=limit)
            return count
        finally:
            self._market_persister.stop(flush=True)
            self._cursor_manager.save_cursors()
    
    def run_trades(self, tickers: List[str], fresh: bool = False) -> int:
        """
        Run trade fetching for specific market tickers.
        
        Args:
            tickers: List of market tickers to fetch trades for
            fresh: Start fresh ignoring cursors
        
        Returns:
            Number of trades fetched (approximate)
        """
        if fresh:
            self._cursor_manager.clear_cursors()
        else:
            self._cursor_manager.load_cursors()
        
        # Initialize trade queue and persister
        self._trade_queue, self._trade_persister = create_trade_persisted_queue(
            output_dir=self._config.output_dirs.trade,
            threshold=self._config.queues.trade_threshold
        )
        
        # Create ticker queue and populate
        self._market_to_trade_queue = Queue()
        for ticker in tickers:
            self._market_to_trade_queue.put(ticker)
        self._market_to_trade_queue.put(None)  # Sentinel
        
        try:
            self._trade_persister.start()
            self._run_trade_fetcher()
            return self._trade_persister.items_written
        finally:
            self._trade_persister.stop(flush=True)
            self._cursor_manager.save_cursors()
    
    def _init_queues_and_persisters(self) -> None:
        """Initialize all queues and persisters for full run."""
        # Event
        self._event_queue, self._event_persister = create_event_persisted_queue(
            output_dir=self._config.output_dirs.event,
            threshold=self._config.queues.event_threshold
        )
        
        # Market
        self._market_queue, self._market_persister = create_market_persisted_queue(
            output_dir=self._config.output_dirs.market,
            threshold=self._config.queues.market_threshold
        )
        
        # Trade
        self._trade_queue, self._trade_persister = create_trade_persisted_queue(
            output_dir=self._config.output_dirs.trade,
            threshold=self._config.queues.trade_threshold
        )
        
        # Orderbook
        self._orderbook_queue, self._orderbook_persister = create_orderbook_persisted_queue(
            output_dir=self._config.output_dirs.orderbook,
            threshold=self._config.queues.orderbook_threshold
        )
        
        # Inter-fetcher queues
        self._event_to_market_queue = Queue()
        self._market_to_trade_queue = Queue()
        self._market_to_orderbook_queue = Queue()
    
    def _start_persisters(self) -> None:
        """Start all persister threads."""
        if self._event_persister:
            self._event_persister.start()
        if self._market_persister:
            self._market_persister.start()
        if self._trade_persister:
            self._trade_persister.start()
        if self._orderbook_persister:
            self._orderbook_persister.start()
    
    def _stop_persisters(self) -> None:
        """Stop all persister threads and flush data."""
        if self._event_persister:
            self._event_persister.stop(flush=True)
        if self._market_persister:
            self._market_persister.stop(flush=True)
        if self._trade_persister:
            self._trade_persister.stop(flush=True)
        if self._orderbook_persister:
            self._orderbook_persister.stop(flush=True)
    
    def _run_event_fetcher(self, limit: Optional[int] = None) -> int:
        """Run event fetcher and return count."""
        fetcher = EventFetcher(
            worker_manager=self._manager,
            config=self._config,
            output_queue=self._event_queue,
            market_event_queue=self._event_to_market_queue,
            cursor_manager=self._cursor_manager,
        )
        
        try:
            events = fetcher.fetch_all_events(limit=limit)
            return len(events)
        finally:
            fetcher.close()
    
    def _run_market_fetcher(self, limit: Optional[int] = None) -> int:
        """Run market fetcher and return count."""
        fetcher = MarketFetcher(
            worker_manager=self._manager,
            config=self._config,
            output_queue=self._market_queue,
            trade_ticker_queue=self._market_to_trade_queue,
            orderbook_ticker_queue=self._market_to_orderbook_queue,
            event_queue=self._event_to_market_queue,
            cursor_manager=self._cursor_manager,
        )
        
        try:
            # If we have an event queue, consume from it
            if self._event_to_market_queue:
                fetcher.run_from_event_queue()
                return self._market_persister.items_written if self._market_persister else 0
            else:
                markets = fetcher.fetch_all_markets(limit=limit)
                return len(markets)
        finally:
            fetcher.close()
    
    def _run_trade_fetcher(self) -> None:
        """Run trade fetcher consuming from ticker queue."""
        fetcher = TradeFetcher(
            worker_manager=self._manager,
            config=self._config,
            output_queue=self._trade_queue,
            ticker_queue=self._market_to_trade_queue,
            cursor_manager=self._cursor_manager,
        )
        
        try:
            fetcher.run_from_ticker_queue()
        finally:
            fetcher.close()
    
    def _run_orderbook_fetcher(self) -> None:
        """Run orderbook fetcher consuming from ticker queue."""
        fetcher = OrderbookFetcher(
            worker_manager=self._manager,
            config=self._config,
            output_queue=self._orderbook_queue,
            ticker_queue=self._market_to_orderbook_queue,
            cursor_manager=self._cursor_manager,
        )
        
        try:
            fetcher.run_from_ticker_queue()
        finally:
            fetcher.close()
