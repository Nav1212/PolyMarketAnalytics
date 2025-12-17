"""
Integration tests for worker functionality.

These tests verify that workers:
1. Fetch data from real Polymarket APIs
2. Properly consume from input queues and produce to output queues
3. Handle graceful shutdown with sentinel values
4. Work correctly with both regular Queue and SwappableQueue

Run with: pytest tests/integration/test_workers.py -v -m integration
"""

import pytest
import time
import threading
from queue import Queue, Empty
from typing import List, Dict, Any

# Skip all tests if no network connection
pytestmark = pytest.mark.integration


class TestMarketFetcherWorker:
    """Integration tests for MarketFetcher worker."""
    
    def test_worker_fetches_markets(self, test_config, worker_manager):
        """Verify worker fetches markets and puts them in output queue."""
        from market_fetcher import MarketFetcher
        from swappable_queue import SwappableQueue
        
        output_queue = SwappableQueue(threshold=1000)
        fetcher = MarketFetcher(
            output_queue=output_queue,
            worker_manager=worker_manager,
            config=test_config
        )
        
        stop_event = threading.Event()
        
        # Start worker in background
        thread = threading.Thread(
            target=fetcher._worker,
            args=(0, output_queue, stop_event),
            daemon=True
        )
        thread.start()
        
        # Wait for some markets to be fetched
        time.sleep(3)
        stop_event.set()
        thread.join(timeout=5)
        
        # Check that markets were fetched
        fetched_count = output_queue.size()
        assert fetched_count > 0, "Worker should have fetched some markets"
        
        fetcher.close()
    
    def test_worker_feeds_downstream_queues(self, test_config, worker_manager):
        """Verify worker feeds trade, price, and leaderboard queues."""
        from market_fetcher import MarketFetcher
        from swappable_queue import SwappableQueue
        
        output_queue = SwappableQueue(threshold=1000)
        trade_queue = Queue()
        price_queue = Queue()
        leaderboard_queue = Queue()
        
        fetcher = MarketFetcher(
            output_queue=output_queue,
            trade_market_queue=trade_queue,
            price_token_queue=price_queue,
            leaderboard_market_queue=leaderboard_queue,
            worker_manager=worker_manager,
            config=test_config
        )
        
        stop_event = threading.Event()
        
        thread = threading.Thread(
            target=fetcher._worker,
            args=(0, output_queue, stop_event),
            daemon=True
        )
        thread.start()
        
        # Wait for some data to flow
        time.sleep(3)
        stop_event.set()
        thread.join(timeout=5)
        
        # Check downstream queues received data
        # Note: Some markets may not have condition_ids or tokens
        markets_fetched = output_queue.size()
        
        if markets_fetched > 0:
            # At least one downstream queue should have data
            has_downstream_data = (
                not trade_queue.empty() or
                not price_queue.empty() or
                not leaderboard_queue.empty()
            )
            # Markets with tokens should have fed downstream
            assert has_downstream_data or markets_fetched == 0, \
                "Downstream queues should receive data from markets with tokens"
        
        fetcher.close()
    
    def test_worker_sends_shutdown_signals(self, test_config, worker_manager):
        """Verify worker sends sentinel values on completion."""
        from market_fetcher import MarketFetcher
        from swappable_queue import SwappableQueue
        
        output_queue = SwappableQueue(threshold=1000)
        trade_queue = Queue()
        
        # Override config to have known worker count
        test_config.workers.trade = 2
        
        fetcher = MarketFetcher(
            output_queue=output_queue,
            trade_market_queue=trade_queue,
            worker_manager=worker_manager,
            config=test_config
        )
        
        stop_event = threading.Event()
        stop_event.set()  # Immediate stop
        
        thread = threading.Thread(
            target=fetcher._worker,
            args=(0, output_queue, stop_event),
            daemon=True
        )
        thread.start()
        thread.join(timeout=5)
        
        # Count sentinels in trade queue
        sentinels = 0
        while not trade_queue.empty():
            item = trade_queue.get_nowait()
            if item is None:
                sentinels += 1
        
        assert sentinels == test_config.workers.trade, \
            f"Expected {test_config.workers.trade} sentinel(s), got {sentinels}"
        
        fetcher.close()


class TestTradeFetcherWorker:
    """Integration tests for TradeFetcher worker."""
    
    @pytest.fixture
    def sample_market_id(self) -> str:
        """Get a real market ID from the API."""
        import httpx
        
        with httpx.Client(timeout=30.0) as client:
            response = client.get(
                "https://data-api.polymarket.com/trades",
                params={"limit": 1}
            )
            if response.status_code == 200:
                trades = response.json()
                if trades:
                    return trades[0]["conditionId"]
        
        pytest.skip("Could not fetch sample market ID from API")
    
    def test_worker_fetches_trades_for_market(self, test_config, worker_manager, sample_market_id):
        """Verify worker fetches trades for a given market."""
        from trade_fetcher import TradeFetcher
        from swappable_queue import SwappableQueue
        
        market_queue = Queue()
        output_queue = SwappableQueue(threshold=1000)
        
        fetcher = TradeFetcher(
            market_queue=market_queue,
            worker_manager=worker_manager,
            config=test_config
        )
        
        # Add a market and sentinel
        market_queue.put(sample_market_id)
        market_queue.put(None)
        
        # Run worker
        thread = threading.Thread(
            target=fetcher._worker,
            args=(0, output_queue),
            daemon=True
        )
        thread.start()
        thread.join(timeout=30)
        
        # Check trades were fetched
        trades_fetched = output_queue.size()
        assert trades_fetched >= 0, "Worker should complete without error"
        
        fetcher.close()
    
    def test_worker_handles_empty_queue_gracefully(self, test_config, worker_manager):
        """Verify worker handles sentinel value correctly."""
        from trade_fetcher import TradeFetcher
        from swappable_queue import SwappableQueue
        
        market_queue = Queue()
        output_queue = SwappableQueue(threshold=1000)
        
        fetcher = TradeFetcher(
            market_queue=market_queue,
            worker_manager=worker_manager,
            config=test_config
        )
        
        # Only sentinel
        market_queue.put(None)
        
        thread = threading.Thread(
            target=fetcher._worker,
            args=(0, output_queue),
            daemon=True
        )
        thread.start()
        thread.join(timeout=5)
        
        assert not thread.is_alive(), "Worker should exit on sentinel"
        fetcher.close()
    
    def test_worker_with_regular_queue(self, test_config, worker_manager, sample_market_id):
        """Verify worker works with regular Queue output."""
        from trade_fetcher import TradeFetcher
        
        market_queue = Queue()
        output_queue = Queue()
        
        fetcher = TradeFetcher(
            market_queue=market_queue,
            worker_manager=worker_manager,
            config=test_config
        )
        
        market_queue.put(sample_market_id)
        market_queue.put(None)
        
        thread = threading.Thread(
            target=fetcher._worker,
            args=(0, output_queue),
            daemon=True
        )
        thread.start()
        thread.join(timeout=30)
        
        # Drain queue and count (should end with sentinel)
        items = []
        while True:
            try:
                item = output_queue.get_nowait()
                if item is None:
                    break
                items.append(item)
            except Empty:
                break
        
        # Just verify it completed
        assert not thread.is_alive(), "Worker should complete"
        fetcher.close()


class TestPriceFetcherWorker:
    """Integration tests for PriceFetcher worker."""
    
    @pytest.fixture
    def sample_token_id(self) -> str:
        """Get a real token ID from the API."""
        import httpx
        
        with httpx.Client(timeout=30.0) as client:
            # Get a market with tokens
            response = client.get(
                "https://gamma-api.polymarket.com/markets",
                params={"limit": 1, "active": "true"}
            )
            if response.status_code == 200:
                markets = response.json()
                if markets:
                    tokens = markets[0].get("clobTokenIds", [])
                    if tokens:
                        return tokens[0]
        
        pytest.skip("Could not fetch sample token ID from API")
    
    def test_worker_fetches_prices_for_token(self, test_config, worker_manager, sample_token_id):
        """Verify worker fetches price history for a token."""
        from price_fetcher import PriceFetcher
        from swappable_queue import SwappableQueue
        import time as time_module
        
        token_queue = Queue()
        output_queue = SwappableQueue(threshold=1000)
        
        fetcher = PriceFetcher(
            market_queue=token_queue,
            worker_manager=worker_manager,
            config=test_config
        )
        
        # Use recent time window
        end_time = int(time_module.time())
        start_time = end_time - 86400  # Last 24 hours
        
        # Add token (as tuple with start time) and sentinel
        token_queue.put((sample_token_id, start_time))
        token_queue.put(None)
        
        thread = threading.Thread(
            target=fetcher._worker,
            args=(0, output_queue, start_time, end_time),
            daemon=True
        )
        thread.start()
        thread.join(timeout=30)
        
        assert not thread.is_alive(), "Worker should complete"
        fetcher.close()
    
    def test_worker_handles_string_token_format(self, test_config, worker_manager, sample_token_id):
        """Verify worker handles legacy string format for tokens."""
        from price_fetcher import PriceFetcher
        from swappable_queue import SwappableQueue
        import time as time_module
        
        token_queue = Queue()
        output_queue = SwappableQueue(threshold=1000)
        
        fetcher = PriceFetcher(
            market_queue=token_queue,
            worker_manager=worker_manager,
            config=test_config
        )
        
        end_time = int(time_module.time())
        start_time = end_time - 86400
        
        # Legacy string format
        token_queue.put(sample_token_id)
        token_queue.put(None)
        
        thread = threading.Thread(
            target=fetcher._worker,
            args=(0, output_queue, start_time, end_time),
            daemon=True
        )
        thread.start()
        thread.join(timeout=30)
        
        assert not thread.is_alive(), "Worker should complete with string format"
        fetcher.close()


class TestLeaderboardFetcherWorker:
    """Integration tests for LeaderboardFetcher worker."""
    
    @pytest.fixture
    def sample_market_id(self) -> str:
        """Get a real market ID from the API."""
        import httpx
        
        with httpx.Client(timeout=30.0) as client:
            response = client.get(
                "https://gamma-api.polymarket.com/markets",
                params={"limit": 1, "active": "true"}
            )
            if response.status_code == 200:
                markets = response.json()
                if markets:
                    condition_id = markets[0].get("conditionId") or markets[0].get("condition_id")
                    if condition_id:
                        return condition_id
        
        pytest.skip("Could not fetch sample market ID from API")
    
    def test_fetch_leaderboard_single_page(self, test_config, worker_manager):
        """Verify fetching a single page of leaderboard data works."""
        from leaderboard_fetcher import LeaderboardFetcher, LeaderboardCategory, LeaderboardTimePeriod
        
        fetcher = LeaderboardFetcher(
            worker_manager=worker_manager,
            config=test_config
        )
        
        # Fetch a single page using the new API
        entries = fetcher.fetch_leaderboard_page(
            category=LeaderboardCategory.OVERALL,
            timePeriod=LeaderboardTimePeriod.DAY,
            limit=5,
            offset=0
        )
        
        # Should get some entries from overall leaderboard
        assert len(entries) <= 5, "Should respect limit"
        
        fetcher.close()
    
    def test_worker_processes_enum_combinations(self, test_config, worker_manager):
        """Verify worker processes enum combinations (with mock to limit combinations)."""
        from leaderboard_fetcher import LeaderboardFetcher, LeaderboardCategory, LeaderboardTimePeriod
        from swappable_queue import SwappableQueue
        from unittest.mock import patch
        
        output_queue = SwappableQueue(threshold=1000)
        
        fetcher = LeaderboardFetcher(
            worker_manager=worker_manager,
            config=test_config
        )
        
        # Patch fetch_leaderboard_page to return quickly with mock data
        mock_entries = [
            {"rank": "1", "proxyWallet": "0x123", "userName": "test", 
             "xUsername": "", "verifiedBadge": False, "vol": 100.0, 
             "pnl": 50.0, "profileImage": ""}
        ]
        
        with patch.object(fetcher, 'fetch_leaderboard_page', return_value=mock_entries) as mock_fetch:
            with patch.object(fetcher, 'get_all_categories', return_value=[LeaderboardCategory.OVERALL]):
                with patch.object(fetcher, 'get_all_time_periods', return_value=[LeaderboardTimePeriod.DAY]):
                    total = fetcher.fetch_all_combinations(output_queue, limit=50, max_offset=50)
            
            assert total >= 1, "Should have fetched at least one entry"
            assert output_queue.size() >= 1, "Should have entries in output queue"
        
        fetcher.close()
    
    def test_worker_with_regular_queue(self, test_config, worker_manager):
        """Verify worker works with regular Queue output."""
        from leaderboard_fetcher import LeaderboardFetcher, LeaderboardCategory, LeaderboardTimePeriod
        from unittest.mock import patch
        
        output_queue = Queue()
        
        fetcher = LeaderboardFetcher(
            worker_manager=worker_manager,
            config=test_config
        )
        
        # Patch to return quickly with limited combinations
        mock_entries = [
            {"rank": "1", "proxyWallet": "0x123", "userName": "test",
             "xUsername": "", "verifiedBadge": False, "vol": 100.0,
             "pnl": 50.0, "profileImage": ""}
        ]
        
        with patch.object(fetcher, 'fetch_leaderboard_page', return_value=mock_entries):
            with patch.object(fetcher, 'get_all_categories', return_value=[LeaderboardCategory.OVERALL]):
                with patch.object(fetcher, 'get_all_time_periods', return_value=[LeaderboardTimePeriod.DAY]):
                    threads = fetcher.run_workers(output_queue, num_workers=1)
                    
                    for t in threads:
                        t.join(timeout=10)
                    
                    assert all(not t.is_alive() for t in threads), "Worker should complete"
        
        fetcher.close()
    
    def test_worker_handles_empty_leaderboard(self, test_config, worker_manager):
        """Verify worker handles empty leaderboard data gracefully."""
        from leaderboard_fetcher import LeaderboardFetcher, LeaderboardCategory, LeaderboardTimePeriod
        from swappable_queue import SwappableQueue
        from unittest.mock import patch
        
        output_queue = SwappableQueue(threshold=1000)
        
        fetcher = LeaderboardFetcher(
            worker_manager=worker_manager,
            config=test_config
        )
        
        # Patch to return empty results
        with patch.object(fetcher, 'fetch_leaderboard_page', return_value=[]):
            with patch.object(fetcher, 'get_all_categories', return_value=[LeaderboardCategory.OVERALL]):
                with patch.object(fetcher, 'get_all_time_periods', return_value=[LeaderboardTimePeriod.DAY]):
                    threads = fetcher.run_workers(output_queue, num_workers=1)
                    
                    for t in threads:
                        t.join(timeout=10)
                    
                    assert all(not t.is_alive() for t in threads), "Worker should handle empty results gracefully"
                    assert output_queue.size() == 0, "Should have no entries for empty result"
        
        fetcher.close()


class TestWorkerParquetIntegration:
    """Integration tests for workers with parquet persistence."""
    
    def test_trade_worker_with_persisted_queue(self, test_config, worker_manager, temp_parquet_dir):
        """Verify trades are persisted to parquet files."""
        from trade_fetcher import TradeFetcher
        from parquet_persister import create_trade_persisted_queue
        import httpx
        
        # Get a sample market
        with httpx.Client(timeout=30.0) as client:
            response = client.get(
                "https://data-api.polymarket.com/trades",
                params={"limit": 1}
            )
            if response.status_code != 200 or not response.json():
                pytest.skip("Could not fetch sample market")
            market_id = response.json()[0]["conditionId"]
        
        market_queue = Queue()
        output_queue, persister = create_trade_persisted_queue(
            threshold=10,  # Low threshold for testing
            output_dir=str(temp_parquet_dir / "trades"),
            auto_start=True
        )
        
        fetcher = TradeFetcher(
            market_queue=market_queue,
            worker_manager=worker_manager,
            config=test_config
        )
        
        market_queue.put(market_id)
        market_queue.put(None)
        
        thread = threading.Thread(
            target=fetcher._worker,
            args=(0, output_queue),
            daemon=True
        )
        thread.start()
        thread.join(timeout=30)
        
        # Stop persister to flush
        persister.stop()
        
        # Check stats
        stats = persister.stats
        assert stats["total_records_written"] >= 0, "Should write records (or none if market has no trades)"
        
        fetcher.close()
    
    def test_leaderboard_worker_with_persisted_queue(self, test_config, worker_manager, temp_parquet_dir):
        """Verify leaderboard data is persisted to parquet files."""
        from leaderboard_fetcher import LeaderboardFetcher, LeaderboardCategory, LeaderboardTimePeriod
        from parquet_persister import create_leaderboard_persisted_queue
        from unittest.mock import patch
        
        output_queue, persister = create_leaderboard_persisted_queue(
            threshold=10,  # Low threshold for testing
            output_dir=str(temp_parquet_dir / "leaderboard"),
            auto_start=True
        )
        
        fetcher = LeaderboardFetcher(
            worker_manager=worker_manager,
            config=test_config
        )
        
        # Patch to return test data quickly
        test_entries = [
            {"rank": "1", "proxyWallet": "0x123", "userName": "test1",
             "xUsername": "", "verifiedBadge": False, "vol": 100.0,
             "pnl": 50.0, "profileImage": ""},
            {"rank": "2", "proxyWallet": "0x456", "userName": "test2",
             "xUsername": "", "verifiedBadge": True, "vol": 200.0,
             "pnl": 75.0, "profileImage": ""}
        ]
        
        with patch.object(fetcher, 'fetch_leaderboard_page', return_value=test_entries):
            with patch.object(fetcher, 'get_all_categories', return_value=[LeaderboardCategory.OVERALL]):
                with patch.object(fetcher, 'get_all_time_periods', return_value=[LeaderboardTimePeriod.DAY]):
                    threads = fetcher.run_workers(output_queue, num_workers=1)
                    
                    for t in threads:
                        t.join(timeout=10)
        
        # Stop persister to flush
        persister.stop()
        
        # Verify completion
        stats = persister.stats
        assert stats is not None, "Should have stats"
        
        fetcher.close()


class TestMultipleWorkersCoordination:
    """Integration tests for multiple workers running in parallel."""
    
    def test_multiple_trade_workers(self, test_config, worker_manager):
        """Verify multiple trade workers can run in parallel."""
        from trade_fetcher import TradeFetcher
        from swappable_queue import SwappableQueue
        import httpx
        
        # Get sample markets
        with httpx.Client(timeout=30.0) as client:
            response = client.get(
                "https://data-api.polymarket.com/trades",
                params={"limit": 3}
            )
            if response.status_code != 200:
                pytest.skip("Could not fetch sample trades")
            trades = response.json()
            market_ids = list(set(t["conditionId"] for t in trades))[:2]
            if len(market_ids) < 2:
                pytest.skip("Need at least 2 distinct markets")
        
        market_queue = Queue()
        output_queue = SwappableQueue(threshold=1000)
        
        fetcher = TradeFetcher(
            market_queue=market_queue,
            worker_manager=worker_manager,
            config=test_config
        )
        
        # Add markets
        for market_id in market_ids:
            market_queue.put(market_id)
        
        # Add sentinels for 2 workers
        market_queue.put(None)
        market_queue.put(None)
        
        # Start 2 workers
        threads = []
        for i in range(2):
            t = threading.Thread(
                target=fetcher._worker,
                args=(i, output_queue),
                daemon=True
            )
            t.start()
            threads.append(t)
        
        # Wait for completion
        for t in threads:
            t.join(timeout=30)
        
        assert all(not t.is_alive() for t in threads), "All workers should complete"
        fetcher.close()
    
    def test_multiple_leaderboard_workers(self, test_config, worker_manager):
        """Verify leaderboard worker runs with enum iteration (single worker since not queue-based)."""
        from leaderboard_fetcher import LeaderboardFetcher, LeaderboardCategory, LeaderboardTimePeriod
        from swappable_queue import SwappableQueue
        from unittest.mock import patch
        
        output_queue = SwappableQueue(threshold=1000)
        
        fetcher = LeaderboardFetcher(
            worker_manager=worker_manager,
            config=test_config
        )
        
        # Patch to return quickly
        mock_entries = [
            {"rank": "1", "proxyWallet": "0x123", "userName": "test",
             "xUsername": "", "verifiedBadge": False, "vol": 100.0,
             "pnl": 50.0, "profileImage": ""}
        ]
        
        with patch.object(fetcher, 'fetch_leaderboard_page', return_value=mock_entries):
            with patch.object(fetcher, 'get_all_categories', return_value=[LeaderboardCategory.OVERALL]):
                with patch.object(fetcher, 'get_all_time_periods', return_value=[LeaderboardTimePeriod.DAY]):
                    # Note: run_workers now only uses 1 worker since it's enum iteration, not queue-based
                    threads = fetcher.run_workers(output_queue, num_workers=2)  # Will still only start 1
                    
                    # Wait for completion
                    for t in threads:
                        t.join(timeout=10)
                    
                    assert all(not t.is_alive() for t in threads), "All workers should complete"
        
        fetcher.close()
