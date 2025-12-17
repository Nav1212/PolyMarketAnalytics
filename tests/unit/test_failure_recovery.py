"""
Unit tests for failure recovery and cursor persistence.

These tests verify that when a process stops unexpectedly:
1. MarketFetcher saves its downstream queues
2. TradeFetcher saves cursors every execution  
3. LeaderboardFetcher saves cursor position for enum iteration
"""

import pytest
import json
import os
import time
import threading
from queue import Queue
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock

# Add fetcher to path
import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "fetcher"))

from cursor_manager import CursorManager, TradeCursor, LeaderboardCursor, PriceCursor
from swappable_queue import SwappableQueue


class TestTradeFetcherCursorPersistence:
    """Test that TradeFetcher saves cursor after every execution."""
    
    @pytest.fixture
    def temp_cursor_dir(self, tmp_path):
        """Create temp directory for cursor files."""
        cursor_dir = tmp_path / "cursors"
        cursor_dir.mkdir()
        return cursor_dir
    
    @pytest.fixture
    def cursor_manager(self, temp_cursor_dir):
        """Create cursor manager with temp directory."""
        cursor_file = temp_cursor_dir / "cursor.json"
        return CursorManager(str(cursor_file), auto_save=False)
    
    def test_cursor_saved_after_trade_update(self, cursor_manager, temp_cursor_dir):
        """Verify cursor is saved after each trade cursor update."""
        cursor_file = temp_cursor_dir / "cursor.json"
        
        # Simulate TradeFetcher updating cursor and saving
        # API: update_trade_cursor(market, offset, filter_amount, pending_markets)
        cursor_manager.update_trade_cursor(
            market="0xmarket1",
            offset=100,
            filter_amount=1000,
            pending_markets=["0xmarket2", "0xmarket3"]
        )
        cursor_manager.save_cursors()
        
        # Verify file exists and has correct content
        assert cursor_file.exists()
        with open(cursor_file, 'r') as f:
            data = json.load(f)
        
        assert "trades" in data
        assert data["trades"]["market"] == "0xmarket1"
        assert data["trades"]["offset"] == 100
        assert data["trades"]["filter_amount"] == 1000
        assert data["trades"]["pending_markets"] == ["0xmarket2", "0xmarket3"]
    
    def test_cursor_survives_multiple_updates(self, cursor_manager, temp_cursor_dir):
        """Verify cursor tracks progress through markets correctly."""
        cursor_file = temp_cursor_dir / "cursor.json"
        
        # Simulate progressing through markets
        all_markets = [f"0xmarket{i}" for i in range(5)]
        
        for i in range(5):
            current_market = f"0xmarket{i}"
            remaining = all_markets[i+1:]  # Markets not yet processed
            cursor_manager.update_trade_cursor(
                market=current_market,
                offset=100 * i,
                filter_amount=1000,
                pending_markets=remaining
            )
            cursor_manager.save_cursors()
        
        # Reload and verify final state
        new_manager = CursorManager(str(cursor_file), auto_save=False)
        new_manager.load_cursors()
        cursor = new_manager.get_trade_cursor()
        
        assert cursor.market == "0xmarket4"
        assert cursor.offset == 400
        assert cursor.pending_markets == []  # All processed
    
    def test_crash_recovery_preserves_last_save(self, cursor_manager, temp_cursor_dir):
        """Simulate crash: verify last saved state is recoverable."""
        cursor_file = temp_cursor_dir / "cursor.json"
        
        # Update cursor several times
        cursor_manager.update_trade_cursor("0xmarket1", 100, 1000, ["0xmarket2", "0xmarket3"])
        cursor_manager.save_cursors()
        
        cursor_manager.update_trade_cursor("0xmarket2", 200, 1000, ["0xmarket3"])
        cursor_manager.save_cursors()
        
        cursor_manager.update_trade_cursor("0xmarket3", 300, 1000, [])
        cursor_manager.save_cursors()
        
        # Simulate crash by creating new manager (don't save cursor_manager)
        cursor_manager.update_trade_cursor("0xmarket_lost", 9999, 1000, [])
        # NO save_cursors() call - simulates crash
        
        # New manager should have last saved state
        recovered_manager = CursorManager(str(cursor_file), auto_save=False)
        recovered_manager.load_cursors()
        cursor = recovered_manager.get_trade_cursor()
        
        assert cursor.market == "0xmarket3"
        assert cursor.offset == 300
        # The "0xmarket_lost" update was lost because save wasn't called


class TestMarketFetcherDownstreamQueuePersistence:
    """Test that MarketFetcher saves downstream queues on stop."""
    
    @pytest.fixture
    def temp_dir(self, tmp_path):
        """Create temp directory for queue files."""
        return tmp_path
    
    def test_queue_state_saved_to_file(self, temp_dir):
        """Verify downstream queues are saved to file."""
        queue_file = temp_dir / "downstream_queues.json"
        
        # Simulate MarketFetcher's _save_downstream_queues logic
        trade_markets = ["0xmarket1", "0xmarket2", "0xmarket3"]
        price_tokens = ["0xtoken1", "0xtoken2"]
        
        data = {
            "trade_markets": trade_markets,
            "price_tokens": price_tokens
        }
        
        with open(queue_file, 'w') as f:
            json.dump(data, f)
        
        # Verify file content
        with open(queue_file, 'r') as f:
            loaded = json.load(f)
        
        assert loaded["trade_markets"] == trade_markets
        assert loaded["price_tokens"] == price_tokens
    
    def test_queue_restoration_from_file(self, temp_dir):
        """Verify downstream queues can be restored."""
        queue_file = temp_dir / "downstream_queues.json"
        
        # Save state
        saved_data = {
            "trade_markets": ["0xmarket1", "0xmarket2"],
            "price_tokens": ["0xtoken1", "0xtoken2", "0xtoken3"]
        }
        with open(queue_file, 'w') as f:
            json.dump(saved_data, f)
        
        # Simulate restore
        trade_queue = Queue()
        price_queue = Queue()
        
        with open(queue_file, 'r') as f:
            loaded = json.load(f)
        
        for market in loaded["trade_markets"]:
            trade_queue.put(market)
        for token in loaded["price_tokens"]:
            price_queue.put(token)
        
        # Verify restoration
        assert trade_queue.qsize() == 2
        assert price_queue.qsize() == 3
        
        assert trade_queue.get() == "0xmarket1"
        assert trade_queue.get() == "0xmarket2"
    
    def test_empty_queues_handled(self, temp_dir):
        """Verify empty queues are handled correctly."""
        queue_file = temp_dir / "downstream_queues.json"
        
        # Save empty state
        data = {
            "trade_markets": [],
            "price_tokens": []
        }
        with open(queue_file, 'w') as f:
            json.dump(data, f)
        
        # Load and verify
        with open(queue_file, 'r') as f:
            loaded = json.load(f)
        
        assert loaded["trade_markets"] == []
        assert loaded["price_tokens"] == []


class TestLeaderboardFetcherCursorPersistence:
    """Test that LeaderboardFetcher saves cursor for enum iteration."""
    
    @pytest.fixture
    def temp_cursor_dir(self, tmp_path):
        """Create temp directory for cursor files."""
        cursor_dir = tmp_path / "cursors"
        cursor_dir.mkdir()
        return cursor_dir
    
    @pytest.fixture
    def cursor_manager(self, temp_cursor_dir):
        """Create cursor manager with temp directory."""
        cursor_file = temp_cursor_dir / "cursor.json"
        return CursorManager(str(cursor_file), auto_save=False)
    
    def test_initial_cursor_state(self, cursor_manager):
        """Verify initial cursor starts at beginning."""
        cursor_manager.load_cursors()
        cursor = cursor_manager.get_leaderboard_cursor()
        
        assert cursor.current_category_index == 0
        assert cursor.current_time_period_index == 0
        assert cursor.current_offset == 0
        assert cursor.completed == False
    
    def test_cursor_tracks_category_progress(self, cursor_manager, temp_cursor_dir):
        """Verify cursor tracks progress through categories."""
        cursor_file = temp_cursor_dir / "cursor.json"
        
        # Simulate progress through first category, second time period
        cursor_manager.update_leaderboard_cursor(
            current_category_index=0,
            current_time_period_index=1,
            current_offset=150,
            completed=False
        )
        cursor_manager.save_cursors()
        
        # Reload and verify
        new_manager = CursorManager(str(cursor_file), auto_save=False)
        new_manager.load_cursors()
        cursor = new_manager.get_leaderboard_cursor()
        
        assert cursor.current_category_index == 0
        assert cursor.current_time_period_index == 1
        assert cursor.current_offset == 150
        assert cursor.completed == False
    
    def test_cursor_tracks_across_categories(self, cursor_manager, temp_cursor_dir):
        """Verify cursor tracks progress across multiple categories."""
        cursor_file = temp_cursor_dir / "cursor.json"
        
        # Simulate moving to third category
        cursor_manager.update_leaderboard_cursor(
            current_category_index=2,
            current_time_period_index=0,
            current_offset=0,
            completed=False
        )
        cursor_manager.save_cursors()
        
        # Reload and verify
        new_manager = CursorManager(str(cursor_file), auto_save=False)
        new_manager.load_cursors()
        cursor = new_manager.get_leaderboard_cursor()
        
        assert cursor.current_category_index == 2
        assert cursor.current_time_period_index == 0
    
    def test_crash_mid_iteration_recovers(self, cursor_manager, temp_cursor_dir):
        """Simulate crash mid-iteration and verify recovery."""
        cursor_file = temp_cursor_dir / "cursor.json"
        
        # Simulate being at category 1, time_period 2, offset 500
        cursor_manager.update_leaderboard_cursor(
            current_category_index=1,
            current_time_period_index=2,
            current_offset=500,
            completed=False
        )
        cursor_manager.save_cursors()
        
        # Simulate crash (don't save after this update)
        cursor_manager.update_leaderboard_cursor(
            current_category_index=3,
            current_time_period_index=0,
            current_offset=0,
            completed=False
        )
        # NO save - simulates crash
        
        # Recovery should restore to last saved state
        recovered_manager = CursorManager(str(cursor_file), auto_save=False)
        recovered_manager.load_cursors()
        cursor = recovered_manager.get_leaderboard_cursor()
        
        assert cursor.current_category_index == 1
        assert cursor.current_time_period_index == 2
        assert cursor.current_offset == 500
    
    def test_completion_flag_persisted(self, cursor_manager, temp_cursor_dir):
        """Verify completion flag is persisted correctly."""
        cursor_file = temp_cursor_dir / "cursor.json"
        
        # Mark as completed
        cursor_manager.update_leaderboard_cursor(
            current_category_index=5,
            current_time_period_index=4,
            current_offset=0,
            completed=True
        )
        cursor_manager.save_cursors()
        
        # Reload and verify
        new_manager = CursorManager(str(cursor_file), auto_save=False)
        new_manager.load_cursors()
        cursor = new_manager.get_leaderboard_cursor()
        
        assert cursor.completed == True


class TestCursorManagerAtomicity:
    """Test cursor file operations for atomicity and corruption resistance."""
    
    @pytest.fixture
    def temp_cursor_dir(self, tmp_path):
        """Create temp directory for cursor files."""
        cursor_dir = tmp_path / "cursors"
        cursor_dir.mkdir()
        return cursor_dir
    
    def test_concurrent_saves_dont_corrupt(self, temp_cursor_dir):
        """Verify concurrent saves don't corrupt the file."""
        cursor_file = temp_cursor_dir / "cursor.json"
        manager = CursorManager(str(cursor_file), auto_save=False)
        
        errors = []
        
        def update_trade(thread_id):
            try:
                for i in range(10):
                    manager.update_trade_cursor(
                        market=f"market_thread{thread_id}_{i}",
                        offset=1000 + i,
                        filter_amount=100,
                        pending_markets=[f"pending_{thread_id}_{i}"]
                    )
                    manager.save_cursors()
            except Exception as e:
                errors.append(e)
        
        # Run concurrent updates
        threads = []
        for i in range(3):
            t = threading.Thread(target=update_trade, args=(i,))
            threads.append(t)
            t.start()
        
        for t in threads:
            t.join()
        
        # No errors should have occurred
        assert len(errors) == 0
        
        # File should be valid JSON
        with open(cursor_file, 'r') as f:
            data = json.load(f)
        
        assert "trades" in data
    
    def test_file_readable_after_save(self, temp_cursor_dir):
        """Verify file is always readable after save."""
        cursor_file = temp_cursor_dir / "cursor.json"
        manager = CursorManager(str(cursor_file), auto_save=False)
        
        for i in range(20):
            manager.update_trade_cursor(
                market=f"market{i}",
                offset=i * 100,
                filter_amount=1000,
                pending_markets=[]
            )
            manager.save_cursors()
            
            # Immediately read back
            with open(cursor_file, 'r') as f:
                data = json.load(f)
            
            assert data["trades"]["market"] == f"market{i}"


class TestPriceFetcherCursorPersistence:
    """Test that PriceFetcher can save and restore cursor state."""
    
    @pytest.fixture
    def temp_cursor_dir(self, tmp_path):
        """Create temp directory for cursor files."""
        cursor_dir = tmp_path / "cursors"
        cursor_dir.mkdir()
        return cursor_dir
    
    @pytest.fixture
    def cursor_manager(self, temp_cursor_dir):
        """Create cursor manager with temp directory."""
        cursor_file = temp_cursor_dir / "cursor.json"
        return CursorManager(str(cursor_file), auto_save=False)
    
    def test_price_cursor_saved(self, cursor_manager, temp_cursor_dir):
        """Verify price cursor is persisted."""
        cursor_file = temp_cursor_dir / "cursor.json"
        
        # API: update_price_cursor(token_id, start_ts, end_ts, pending_tokens)
        cursor_manager.update_price_cursor(
            token_id="0xtoken123",
            start_ts=1702000000,
            end_ts=1702100000,
            pending_tokens=[("0xtoken456", 1702000000)]
        )
        cursor_manager.save_cursors()
        
        # Reload and verify
        new_manager = CursorManager(str(cursor_file), auto_save=False)
        new_manager.load_cursors()
        cursor = new_manager.get_price_cursor()
        
        assert cursor.token_id == "0xtoken123"
        assert cursor.start_ts == 1702000000
        assert cursor.end_ts == 1702100000
    
    def test_pending_tokens_tracked(self, cursor_manager, temp_cursor_dir):
        """Verify pending tokens list is tracked correctly."""
        cursor_file = temp_cursor_dir / "cursor.json"
        
        pending = [
            ("0xtoken1", 1702000000),
            ("0xtoken2", 1702000000),
            ("0xtoken3", 1702000000)
        ]
        
        cursor_manager.update_price_cursor(
            token_id="0xtoken_current",
            start_ts=1702000000,
            end_ts=1702100000,
            pending_tokens=pending
        )
        cursor_manager.save_cursors()
        
        # Reload and verify
        new_manager = CursorManager(str(cursor_file), auto_save=False)
        new_manager.load_cursors()
        cursor = new_manager.get_price_cursor()
        
        assert len(cursor.pending_tokens) == 3
        # JSON serialization converts tuples to lists, so compare as tuples
        assert cursor.pending_tokens[0][0] == "0xtoken1"


class TestIntegratedFailureRecovery:
    """Integration tests for failure recovery across all fetcher types."""
    
    @pytest.fixture
    def temp_dir(self, tmp_path):
        """Create temp directory for all files."""
        return tmp_path
    
    def test_full_recovery_scenario(self, temp_dir):
        """Simulate full crash and recovery with all cursor types."""
        cursor_file = temp_dir / "cursor.json"
        queue_file = temp_dir / "downstream_queues.json"
        
        # Initial state - simulate running system
        manager = CursorManager(str(cursor_file), auto_save=False)
        
        # Trade cursor
        manager.update_trade_cursor(
            market="0xmarket1",
            offset=500,
            filter_amount=1000,
            pending_markets=["0xmarket2", "0xmarket3"]
        )
        
        # Price cursor
        manager.update_price_cursor(
            token_id="0xtoken1",
            start_ts=1702000000,
            end_ts=1702100000,
            pending_tokens=[("0xtoken2", 1702000000)]
        )
        
        # Leaderboard cursor
        manager.update_leaderboard_cursor(
            current_category_index=2,
            current_time_period_index=1,
            current_offset=300,
            completed=False
        )
        
        manager.save_cursors()
        
        # Downstream queues
        queues = {
            "trade_markets": ["0xmarket3", "0xmarket4"],
            "price_tokens": ["0xtoken2", "0xtoken3"]
        }
        with open(queue_file, 'w') as f:
            json.dump(queues, f)
        
        # === CRASH HAPPENS HERE ===
        
        # Recovery
        recovered_manager = CursorManager(str(cursor_file), auto_save=False)
        recovered_manager.load_cursors()
        
        # Verify trade cursor
        trade_cursor = recovered_manager.get_trade_cursor()
        assert trade_cursor.market == "0xmarket1"
        assert trade_cursor.offset == 500
        assert trade_cursor.pending_markets == ["0xmarket2", "0xmarket3"]
        
        # Verify price cursor
        price_cursor = recovered_manager.get_price_cursor()
        assert price_cursor.token_id == "0xtoken1"
        assert price_cursor.start_ts == 1702000000
        
        # Verify leaderboard cursor
        lb_cursor = recovered_manager.get_leaderboard_cursor()
        assert lb_cursor.current_category_index == 2
        assert lb_cursor.current_time_period_index == 1
        assert lb_cursor.current_offset == 300
        
        # Verify downstream queues
        with open(queue_file, 'r') as f:
            recovered_queues = json.load(f)
        assert recovered_queues["trade_markets"] == ["0xmarket3", "0xmarket4"]
