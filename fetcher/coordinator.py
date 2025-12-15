"""
FetcherCoordinator - Orchestrates all fetchers with proper load ordering.

Load Order (hardcoded):
    1. MarketFetcher - Runs first, populates downstream queues
    2. TradeFetcher, PriceFetcher, LeaderboardFetcher - Run in parallel after markets

Dependencies:
    MarketFetcher → TradeFetcher (via condition_id)
    MarketFetcher → PriceFetcher (via token_id)
    MarketFetcher → LeaderboardFetcher (via condition_id)
"""

from enum import IntEnum
from queue import Queue
from threading import Thread
from typing import Optional, List, Dict, Any

from config import Config, get_config
from worker_manager import WorkerManager, get_worker_manager, set_worker_manager
from swappable_queue import SwappableQueue
from market_fetcher import MarketFetcher
from trade_fetcher import TradeFetcher
from price_fetcher import PriceFetcher
from leaderboard_fetcher import LeaderboardFetcher


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
    """
    
    def __init__(self, config: Config = None):
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
        
        # Queues for inter-fetcher communication
        self._trade_market_queue: Optional[Queue] = None
        self._price_token_queue: Optional[Queue] = None
        self._leaderboard_market_queue: Optional[Queue] = None
        
        # Output queues
        self._market_output_queue: Optional[Queue] = None
        self._trade_output_queue: Optional[SwappableQueue] = None
        self._price_output_queue: Optional[SwappableQueue] = None
        self._leaderboard_output_queue: Optional[SwappableQueue] = None
        
        # Fetcher instances
        self._market_fetcher: Optional[MarketFetcher] = None
        self._trade_fetcher: Optional[TradeFetcher] = None
        self._price_fetcher: Optional[PriceFetcher] = None
        self._leaderboard_fetcher: Optional[LeaderboardFetcher] = None
        
        # Worker threads
        self._worker_threads: List[Thread] = []
    
    def _create_queues(self, use_swappable: bool = True) -> None:
        """Create all inter-fetcher and output queues."""
        # Inter-fetcher queues (regular Queue for signaling)
        self._trade_market_queue = Queue()
        self._price_token_queue = Queue()
        self._leaderboard_market_queue = Queue()
        
        # Output queues (SwappableQueue for Parquet persistence)
        if use_swappable:
            self._market_output_queue = SwappableQueue()
            self._trade_output_queue = SwappableQueue()
            self._price_output_queue = SwappableQueue()
            self._leaderboard_output_queue = SwappableQueue()
        else:
            self._market_output_queue = Queue()
            self._trade_output_queue = Queue()
            self._price_output_queue = Queue()
            self._leaderboard_output_queue = Queue()
    
    def _create_fetchers(self) -> None:
        """Create all fetcher instances with proper queue wiring."""
        # MarketFetcher feeds downstream queues
        self._market_fetcher = MarketFetcher(
            output_queue=self._market_output_queue,
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
            market_queue=self._leaderboard_market_queue
        )
    
    def run_all(
        self,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        use_swappable: bool = True
    ) -> Dict[str, Any]:
        """
        Run all fetchers in proper load order.
        
        Load Order:
            1. MarketFetcher starts first
            2. TradeFetcher, PriceFetcher, LeaderboardFetcher start after markets begin flowing
        
        Args:
            start_time: Start timestamp for trade fetching
            end_time: End timestamp for trade fetching
            use_swappable: Use SwappableQueue for outputs (for Parquet persistence)
        
        Returns:
            Dict containing all output queues
        """
        self._create_queues(use_swappable=use_swappable)
        self._create_fetchers()
        
        # Get worker counts from config
        market_workers = self._config.workers.market
        trade_workers = self._config.workers.trade
        price_workers = self._config.workers.price
        leaderboard_workers = self._config.workers.leaderboard
        
        # Load Order 1: Start MarketFetcher
        market_thread = Thread(
            target=self._market_fetcher.run_workers,
            kwargs={"num_workers": market_workers},
            name="MarketFetcher-Coordinator"
        )
        market_thread.start()
        self._worker_threads.append(market_thread)
        
        # Load Order 2: Start downstream fetchers (parallel)
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
            output_queue=self._leaderboard_output_queue,
            num_workers=leaderboard_workers
        )
        self._worker_threads.extend(leaderboard_threads)
        
        return {
            "market_queue": self._market_output_queue,
            "trade_queue": self._trade_output_queue,
            "price_queue": self._price_output_queue,
            "leaderboard_queue": self._leaderboard_output_queue
        }
    
    def run_trades(
        self,
        market_ids: List[str],
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        num_workers: Optional[int] = None,
        use_swappable: bool = True
    ) -> SwappableQueue:
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
        
        # Create input queue and populate with market IDs
        input_queue = Queue()
        for market_id in market_ids:
            input_queue.put(market_id)
        
        # Add sentinel values for each worker
        for _ in range(num_workers):
            input_queue.put(None)
        
        # Create output queue
        if use_swappable:
            output_queue = SwappableQueue()
        else:
            output_queue = Queue()
        
        # Create fetcher and start workers
        self._trade_fetcher = TradeFetcher(market_queue=input_queue)
        
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
    ) -> SwappableQueue:
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
        num_workers: Optional[int] = None,
        use_swappable: bool = True
    ) -> SwappableQueue:
        """
        Run only PriceFetcher for specific token IDs.
        
        Args:
            token_ids: List of token IDs to fetch prices for
            num_workers: Number of worker threads (default from config)
            use_swappable: Use SwappableQueue for output
        
        Returns:
            Output queue containing price data
        """
        num_workers = num_workers or self._config.workers.price
        
        # Create input queue and populate with token IDs
        input_queue = Queue()
        for token_id in token_ids:
            input_queue.put(token_id)
        
        # Add sentinel values for each worker
        for _ in range(num_workers):
            input_queue.put(None)
        
        # Create output queue
        if use_swappable:
            output_queue = SwappableQueue()
        else:
            output_queue = Queue()
        
        # Create fetcher and start workers
        self._price_fetcher = PriceFetcher(market_queue=input_queue)
        
        for i in range(num_workers):
            t = Thread(
                target=self._price_fetcher._worker,
                args=(i, output_queue),
                name=f"PriceFetcher-Worker-{i}"
            )
            t.start()
            self._worker_threads.append(t)
        
        return output_queue
    
    def run_leaderboard(
        self,
        market_ids: List[str],
        num_workers: Optional[int] = None,
        use_swappable: bool = True
    ) -> SwappableQueue:
        """
        Run only LeaderboardFetcher for specific market IDs.
        
        Args:
            market_ids: List of market condition IDs to fetch leaderboard for
            num_workers: Number of worker threads (default from config)
            use_swappable: Use SwappableQueue for output
        
        Returns:
            Output queue containing leaderboard data
        """
        num_workers = num_workers or self._config.workers.leaderboard
        
        # Create input queue and populate with market IDs
        input_queue = Queue()
        for market_id in market_ids:
            input_queue.put(market_id)
        
        # Add sentinel values for each worker
        for _ in range(num_workers):
            input_queue.put(None)
        
        # Create output queue
        if use_swappable:
            output_queue = SwappableQueue()
        else:
            output_queue = Queue()
        
        # Create fetcher and start workers
        self._leaderboard_fetcher = LeaderboardFetcher(
            market_queue=input_queue
        )
        
        leaderboard_threads = self._leaderboard_fetcher.run_workers(
            output_queue=output_queue,
            num_workers=num_workers
        )
        self._worker_threads.extend(leaderboard_threads)
        
        return output_queue
    
    def wait_for_completion(self, timeout: Optional[float] = None) -> bool:
        """
        Wait for all worker threads to complete.
        
        Args:
            timeout: Maximum time to wait (None for infinite)
        
        Returns:
            True if all threads completed, False if timeout occurred
        """
        for thread in self._worker_threads:
            thread.join(timeout=timeout)
            if thread.is_alive():
                return False
        return True
    
    def signal_shutdown(self) -> None:
        """Signal all fetchers to shut down by sending sentinel values."""
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
        self._manager.clear_statistics()
    
    @property
    def load_order(self) -> Dict[str, int]:
        """Get the hardcoded load order for fetchers."""
        return {
            "market": LoadOrder.MARKET,
            "trade": LoadOrder.TRADE,
            "price": LoadOrder.PRICE,
            "leaderboard": LoadOrder.LEADERBOARD
        }
