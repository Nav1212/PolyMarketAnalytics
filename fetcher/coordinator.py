"""
FetcherCoordinator - Orchestrates all fetchers with proper load ordering.

Load Order (hardcoded):
    1. MarketFetcher - Runs first, populates downstream queues
    2. TradeFetcher, PriceFetcher, LeaderboardFetcher - Run in parallel after markets

Dependencies:
    MarketFetcher → TradeFetcher (via condition_id)
    MarketFetcher → PriceFetcher (via token_id)
    MarketFetcher → LeaderboardFetcher (via condition_id)

Cursor Persistence:
    On shutdown, cursors are saved to cursor.json for resuming:
    - trades: offset + market + filter_amount
    - prices: start_ts (datetime window) + token_id
    - leaderboard: category + time_period + pending markets
"""

from enum import IntEnum
from queue import Queue
from threading import Thread
from typing import Optional, List, Dict, Any, Union

import time

from config import Config, get_config
from worker_manager import WorkerManager, get_worker_manager, set_worker_manager
from swappable_queue import SwappableQueue
from market_fetcher import MarketFetcher
from trade_fetcher import TradeFetcher
from price_fetcher import PriceFetcher
from leaderboard_fetcher import LeaderboardFetcher
from parquet_persister import (
    ParquetPersister,
    create_market_persisted_queue,
    create_market_token_persisted_queue,
    create_trade_persisted_queue,
    create_price_persisted_queue,
    create_leaderboard_persisted_queue,
    DataType
)
from cursor_manager import (
    CursorManager, 
    get_cursor_manager, 
    set_cursor_manager,
    TradeCursor,
    PriceCursor,
    LeaderboardCursor
)


class LoadOrder(IntEnum):
    """Hardcoded load order for fetchers."""
    MARKET = 1       # Runs first - populates downstream queues
    TRADE = 2        # Runs after market
    PRICE = 2        # Runs after market (parallel with trade)
    LEADERBOARD = 2  # Runs after market (parallel with trade/price)


class FetcherCoordinator:
    """
    Coordinates all fetchers with proper load ordering and queue management.
    
    MarketFetcher runs first and feeds condition_ids to TradeFetcher and LeaderboardFetcher,
    and token_ids to PriceFetcher.
    
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
            trade_rate=self._config.rate_limits.trade,
            market_rate=self._config.rate_limits.market,
            price_rate=self._config.rate_limits.price,
            leaderboard_rate=self._config.rate_limits.leaderboard,
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
        
        # Load any existing cursors
        self._cursor_manager.load_cursors()
        
        # Queues for inter-fetcher communication
        self._trade_market_queue: Optional[Queue] = None
        self._price_token_queue: Optional[Queue] = None
        self._leaderboard_market_queue: Optional[Queue] = None
        
        # Output queues
        self._market_output_queue: Optional[Union[Queue, SwappableQueue]] = None
        self._market_token_output_queue: Optional[Union[Queue, SwappableQueue]] = None
        self._trade_output_queue: Optional[Union[Queue, SwappableQueue]] = None
        self._price_output_queue: Optional[Union[Queue, SwappableQueue]] = None
        self._leaderboard_output_queue: Optional[Union[Queue, SwappableQueue]] = None
        
        # Fetcher instances
        self._market_fetcher: Optional[MarketFetcher] = None
        self._trade_fetcher: Optional[TradeFetcher] = None
        self._price_fetcher: Optional[PriceFetcher] = None
        self._leaderboard_fetcher: Optional[LeaderboardFetcher] = None
        
        # Parquet persisters
        self._market_persister: Optional[ParquetPersister] = None
        self._market_token_persister: Optional[ParquetPersister] = None
        self._trade_persister: Optional[ParquetPersister] = None
        self._price_persister: Optional[ParquetPersister] = None
        self._leaderboard_persister: Optional[ParquetPersister] = None
        
        # Worker threads
        self._worker_threads: List[Thread] = []
    
    def _create_queues(self, use_swappable: bool = True) -> None:
        """Create all inter-fetcher and output queues with parquet persistence."""
        # Inter-fetcher queues (regular Queue for signaling)
        self._trade_market_queue = Queue()
        self._price_token_queue = Queue()
        self._leaderboard_market_queue = Queue()
        
        # Output queues with Parquet persistence
        if use_swappable:
            # Markets: persisted to parquet
            self._market_output_queue, self._market_persister = create_market_persisted_queue(
                threshold=self._config.queues.market_threshold,
                output_dir=self._config.output_dirs.market,
                auto_start=True
            )
            
            # Market Tokens: persisted to parquet (extracted from markets)
            self._market_token_output_queue, self._market_token_persister = create_market_token_persisted_queue(
                threshold=self._config.queues.market_token_threshold,
                output_dir=self._config.output_dirs.market_token,
                auto_start=True
            )
            
            # Trades: persisted to parquet
            self._trade_output_queue, self._trade_persister = create_trade_persisted_queue(
                threshold=self._config.queues.trade_threshold,
                output_dir=self._config.output_dirs.trade,
                auto_start=True
            )
            
            # Prices: persisted to parquet
            self._price_output_queue, self._price_persister = create_price_persisted_queue(
                threshold=self._config.queues.price_threshold,
                output_dir=self._config.output_dirs.price,
                auto_start=True
            )
            
            # Leaderboard: persisted to parquet
            self._leaderboard_output_queue, self._leaderboard_persister = create_leaderboard_persisted_queue(
                threshold=self._config.queues.leaderboard_threshold,
                output_dir=self._config.output_dirs.leaderboard,
                auto_start=True
            )
        else:
            self._market_output_queue = Queue()
            self._market_token_output_queue = Queue()
            self._trade_output_queue = Queue()
            self._price_output_queue = Queue()
            self._leaderboard_output_queue = Queue()
    
    def _create_fetchers(self) -> None:
        """Create all fetcher instances with proper queue wiring."""
        # MarketFetcher feeds downstream queues
        self._market_fetcher = MarketFetcher(
            output_queue=self._market_output_queue,
            market_token_queue=self._market_token_output_queue,
            trade_market_queue=self._trade_market_queue,
            price_token_queue=self._price_token_queue,
            leaderboard_market_queue=self._leaderboard_market_queue
        )
        
        # TradeFetcher consumes from trade_market_queue
        self._trade_fetcher = TradeFetcher(
            market_queue=self._trade_market_queue
        )
        
        # PriceFetcher consumes from price_token_queue
        self._price_fetcher = PriceFetcher(
            market_queue=self._price_token_queue
        )
        
        # LeaderboardFetcher consumes from leaderboard_market_queue
        self._leaderboard_fetcher = LeaderboardFetcher(
        )
    
    def run_all(
        self,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        use_swappable: bool = True,
        market_warmup_delay: float = 0.5
    ) -> Dict[str, Any]:
        """
        Run all fetchers in parallel.
        
        All fetchers start simultaneously with MarketFetcher given a small head start
        to seed the queues with initial entries. Downstream fetchers will block on
        their queues until market data flows in.
        
        Args:
            start_time: Start timestamp for trade fetching
            end_time: End timestamp for trade fetching
            use_swappable: Use SwappableQueue for outputs (for Parquet persistence)
            market_warmup_delay: Seconds to wait after starting MarketFetcher before
                                 starting downstream fetchers (default 0.5s)
        
        Returns:
            Dict containing all output queues and persisters
        """
        self._create_queues(use_swappable=use_swappable)
        self._create_fetchers()
        
        # Ensure fetchers and queues are created (for type checker)
        assert self._market_fetcher is not None
        assert self._trade_fetcher is not None
        assert self._price_fetcher is not None
        assert self._leaderboard_fetcher is not None
        assert self._leaderboard_market_queue is not None
        assert self._leaderboard_output_queue is not None
        assert self._trade_output_queue is not None
        assert self._price_output_queue is not None
        
        # Get worker counts from config
        market_workers = self._config.workers.market
        trade_workers = self._config.workers.trade
        price_workers = self._config.workers.price
        leaderboard_workers = self._config.workers.leaderboard
        
        print(f"Starting parallel fetch pipeline...")
        print(f"  MarketFetcher warming up for {market_warmup_delay}s before downstream fetchers...")
        
        # Start MarketFetcher first to seed the queues
        market_thread = Thread(
            target=self._market_fetcher.run_workers,
            kwargs={"num_workers": market_workers},
            name="MarketFetcher-Coordinator"
        )
        market_thread.start()
        self._worker_threads.append(market_thread)
        
        # Brief delay to let MarketFetcher add initial entries to downstream queues
        time.sleep(market_warmup_delay)
        
        # Start all downstream fetchers in parallel (they consume from queues as items arrive)
        # TradeFetcher workers
        for i in range(trade_workers):
            t = Thread(
                target=self._trade_fetcher._worker,
                args=(i, self._trade_output_queue),
                name=f"TradeFetcher-Worker-{i}"
            )
            t.start()
            self._worker_threads.append(t)
        
        # PriceFetcher workers
        for i in range(price_workers):
            t = Thread(
                target=self._price_fetcher._worker,
                args=(i, self._price_output_queue, start_time, end_time),
                name=f"PriceFetcher-Worker-{i}"
            )
            t.start()
            self._worker_threads.append(t)
        
        # LeaderboardFetcher - has its own run_workers
        leaderboard_threads = self._leaderboard_fetcher.run_workers(
            market_queue=self._leaderboard_market_queue,
            output_queue=self._leaderboard_output_queue,
            num_workers=leaderboard_workers
        )
        self._worker_threads.extend(leaderboard_threads)
        
        print(f"All fetchers started in parallel mode:")
        print(f"  - MarketFetcher: {market_workers} workers → persisting to {self._config.output_dirs.market}")
        print(f"  - TradeFetcher: {trade_workers} workers → persisting to {self._config.output_dirs.trade}")
        print(f"  - PriceFetcher: {price_workers} workers → persisting to {self._config.output_dirs.price}")
        print(f"  - LeaderboardFetcher: {leaderboard_workers} workers")
        
        return {
            "market_queue": self._market_output_queue,
            "trade_queue": self._trade_output_queue,
            "price_queue": self._price_output_queue,
            "leaderboard_queue": self._leaderboard_output_queue,
            "market_persister": self._market_persister,
            "trade_persister": self._trade_persister,
            "price_persister": self._price_persister
        }
    
    def run_trades(
        self,
        market_ids: List[str],
        num_workers: Optional[int] = None,
        use_swappable: bool = True
    ) -> Union[Queue, SwappableQueue]:
        """
        Run only TradeFetcher for specific market IDs.
        
        Args:
            market_ids: List of market condition IDs to fetch trades for
            start_time: Start timestamp filter
            end_time: End timestamp filter
            num_workers: Number of worker threads (default from config)
            use_swappable: Use SwappableQueue for output
        
        Returns:
            Output queue containing trade data
        """
        num_workers = num_workers or self._config.workers.trade
        
        # Check for existing cursor to resume from
        trade_cursor = self._cursor_manager.get_trade_cursor()
        
        # Create input queue
        input_queue = Queue()
        
        if not trade_cursor.is_empty() and trade_cursor.pending_markets:
            # Resume from cursor - use pending markets from cursor
            print(f"Resuming trades from cursor: {len(trade_cursor.pending_markets)} markets pending")
            resume_market_ids = trade_cursor.pending_markets
        else:
            # Fresh start - use provided market IDs
            resume_market_ids = market_ids
        
        # Store pending markets in cursor for tracking
        self._cursor_manager.update_trade_cursor(
            market=trade_cursor.market if not trade_cursor.is_empty() else "",
            offset=trade_cursor.offset if not trade_cursor.is_empty() else 0,
            filter_amount=trade_cursor.filter_amount if not trade_cursor.is_empty() else 0,
            pending_markets=list(resume_market_ids)
        )
        
        # Populate queue with market IDs
        for market_id in resume_market_ids:
            input_queue.put(market_id)
        
        # Add sentinel values for each worker
        for _ in range(num_workers):
            input_queue.put(None)
        
        # Create output queue
        if use_swappable:
            output_queue = SwappableQueue()
        else:
            output_queue = Queue()
        
        # Create fetcher with cursor manager and start workers
        self._trade_fetcher = TradeFetcher(
            market_queue=input_queue,
            cursor_manager=self._cursor_manager
        )
        
        for i in range(num_workers):
            t = Thread(
                target=self._trade_fetcher._worker,
                args=(i, output_queue),
                name=f"TradeFetcher-Worker-{i}"
            )
            t.start()
            self._worker_threads.append(t)
        
        return output_queue
    
    def run_markets(
        self,
        num_workers: Optional[int] = None,
        use_swappable: bool = True
    ) -> Union[Queue, SwappableQueue]:
        """
        Run only MarketFetcher.
        
        Args:
            num_workers: Number of worker threads (default from config)
            use_swappable: Use SwappableQueue for output
        
        Returns:
            Output queue containing market data
        """
        num_workers = num_workers or self._config.workers.market
        
        if use_swappable:
            output_queue = SwappableQueue()
        else:
            output_queue = Queue()
        
        self._market_fetcher = MarketFetcher(output_queue=output_queue)
        
        market_thread = Thread(
            target=self._market_fetcher.run_workers,
            kwargs={"num_workers": num_workers},
            name="MarketFetcher-Coordinator"
        )
        market_thread.start()
        self._worker_threads.append(market_thread)
        
        return output_queue
    
    def run_prices(
        self,
        token_ids: List[str],
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        num_workers: Optional[int] = None,
        use_swappable: bool = True
    ) -> Union[Queue, SwappableQueue]:
        """
        Run only PriceFetcher for specific token IDs.
        
        Args:
            token_ids: List of token IDs to fetch prices for
            start_time: Start timestamp filter
            end_time: End timestamp filter
            num_workers: Number of worker threads (default from config)
            use_swappable: Use SwappableQueue for output
        
        Returns:
            Output queue containing price data
        """
        num_workers = num_workers or self._config.workers.price
        
        # Check for existing cursor to resume from
        price_cursor = self._cursor_manager.get_price_cursor()
        
        # Create input queue
        input_queue = Queue()
        
        if not price_cursor.is_empty() and price_cursor.pending_tokens:
            # Resume from cursor - use pending tokens from cursor
            print(f"Resuming prices from cursor: {len(price_cursor.pending_tokens)} tokens pending")
            resume_tokens = price_cursor.pending_tokens
        else:
            # Fresh start - convert token_ids to tuple format (token_id, None)
            resume_tokens = [(tid, None) if isinstance(tid, str) else tid for tid in token_ids]
        
        # Store pending tokens in cursor for tracking
        self._cursor_manager.update_price_cursor(
            token_id=price_cursor.token_id if not price_cursor.is_empty() else "",
            start_ts=start_time or 0,
            end_ts=end_time or 0,
            pending_tokens=list(resume_tokens)
        )
        
        # Populate queue with token IDs
        for token_item in resume_tokens:
            input_queue.put(token_item)
        
        # Add sentinel values for each worker
        for _ in range(num_workers):
            input_queue.put(None)
        
        # Create output queue
        if use_swappable:
            output_queue = SwappableQueue()
        else:
            output_queue = Queue()
        
        # Create fetcher with cursor manager and start workers
        self._price_fetcher = PriceFetcher(
            market_queue=input_queue,
            cursor_manager=self._cursor_manager
        )
        
        for i in range(num_workers):
            t = Thread(
                target=self._price_fetcher._worker,
                args=(i, output_queue, start_time, end_time),
                name=f"PriceFetcher-Worker-{i}"
            )
            t.start()
            self._worker_threads.append(t)
        
        return output_queue
    
    def run_leaderboard(
        self,
        market_ids: List[str],
        category: str = "OVERALL",
        time_period: str = "DAY",
        num_workers: Optional[int] = None,
        use_swappable: bool = True
    ) -> Union[Queue, SwappableQueue]:
        """
        Run only LeaderboardFetcher for specific market IDs.
        
        Args:
            market_ids: List of market condition IDs to fetch leaderboard for
            category: Leaderboard category filter
            time_period: Leaderboard time period filter
            num_workers: Number of worker threads (default from config)
            use_swappable: Use SwappableQueue for output
        
        Returns:
            Output queue containing leaderboard data
        """
        num_workers = num_workers or self._config.workers.leaderboard
        
        # Check for existing cursor to resume from
        lb_cursor = self._cursor_manager.get_leaderboard_cursor()
        
        # Create input queue
        input_queue = Queue()
        
        if not lb_cursor.is_empty() and lb_cursor.pending_markets:
            # Resume from cursor - use pending markets and settings from cursor
            print(f"Resuming leaderboard from cursor: {len(lb_cursor.pending_markets)} markets pending")
            resume_market_ids = lb_cursor.pending_markets
            category = lb_cursor.category
            time_period = lb_cursor.time_period
        else:
            # Fresh start - use provided market IDs
            resume_market_ids = market_ids
        
        # Store pending markets in cursor for tracking
        self._cursor_manager.update_leaderboard_cursor(
            category=category,
            time_period=time_period,
            pending_markets=list(resume_market_ids)
        )
        
        # Populate queue with market IDs
        for market_id in resume_market_ids:
            input_queue.put(market_id)
        
        # Add sentinel values for each worker
        for _ in range(num_workers):
            input_queue.put(None)
        
        # Create output queue
        if use_swappable:
            output_queue = SwappableQueue()
        else:
            output_queue = Queue()
        
        # Create fetcher with cursor manager and start workers
        self._leaderboard_fetcher = LeaderboardFetcher(
            cursor_manager=self._cursor_manager
        )
        
        leaderboard_threads = self._leaderboard_fetcher.run_workers(
            market_queue=input_queue,
            output_queue=output_queue,
            category=category,
            timePeriod=time_period,
            num_workers=num_workers
        )
        self._worker_threads.extend(leaderboard_threads)
        
        return output_queue
    
    def wait_for_completion(self, timeout: Optional[float] = None) -> bool:
        """
        Wait for all worker threads to complete and flush persisters.
        
        Args:
            timeout: Maximum time to wait (None for infinite)
        
        Returns:
            True if all threads completed, False if timeout occurred
        """
        try:
            for thread in self._worker_threads:
                thread.join(timeout=timeout)
                if thread.is_alive():
                    # Timeout occurred, save cursors for resume
                    self._cursor_manager.save_cursors()
                    self._stop_persisters()
                    return False
            
            # All threads completed successfully
            # Stop persisters to flush remaining data
            self._stop_persisters()
            
            # Clear cursors since we completed successfully
            self._cursor_manager.clear_trade_cursor()
            self._cursor_manager.clear_price_cursor()
            self._cursor_manager.clear_leaderboard_cursor()
            self._cursor_manager.clear_market_cursor()
            print("All fetchers completed successfully, cursors cleared.")
            return True
            
        except KeyboardInterrupt:
            # User interrupted, save cursors for resume
            print("\nInterrupted! Saving cursors and flushing data...")
            self._cursor_manager.save_cursors()
            self._stop_persisters()
            raise
    
    def _stop_persisters(self) -> None:
        """Stop all parquet persisters and flush remaining data."""
        if self._market_persister:
            self._market_persister.stop()
            print(f"  Market persister: {self._market_persister.stats}")
        if self._market_token_persister:
            self._market_token_persister.stop()
            print(f"  Market token persister: {self._market_token_persister.stats}")
        if self._trade_persister:
            self._trade_persister.stop()
            print(f"  Trade persister: {self._trade_persister.stats}")
        if self._price_persister:
            self._price_persister.stop()
            print(f"  Price persister: {self._price_persister.stats}")
        if self._leaderboard_persister:
            self._leaderboard_persister.stop()
            print(f"  Leaderboard persister: {self._leaderboard_persister.stats}")
    
    def signal_shutdown(self) -> None:
        """Signal all fetchers to shut down by sending sentinel values."""
        # Save cursors before shutdown
        self._cursor_manager.save_cursors()
        
        # Send None sentinels to inter-fetcher queues
        if self._trade_market_queue:
            num_trade_workers = self._config.workers.trade
            for _ in range(num_trade_workers):
                self._trade_market_queue.put(None)
        
        if self._price_token_queue:
            num_price_workers = self._config.workers.price
            for _ in range(num_price_workers):
                self._price_token_queue.put(None)
        
        if self._leaderboard_market_queue:
            num_leaderboard_workers = self._config.workers.leaderboard
            for _ in range(num_leaderboard_workers):
                self._leaderboard_market_queue.put(None)
    
    def print_statistics(self) -> None:
        """Print rate limiting statistics from WorkerManager."""
        self._manager.print_statistics()
    
    def clear_statistics(self) -> None:
        """Clear rate limiting statistics."""
        self._manager.reset_statistics()
    
    @property
    def load_order(self) -> Dict[str, int]:
        """Get the hardcoded load order for fetchers."""
        return {
            "market": LoadOrder.MARKET,
            "trade": LoadOrder.TRADE,
            "price": LoadOrder.PRICE,
            "leaderboard": LoadOrder.LEADERBOARD
        }
