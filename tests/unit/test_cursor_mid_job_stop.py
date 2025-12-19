"""
Unit tests for cursor persistence when workers stop mid-job.

These tests verify that when a worker is interrupted mid-execution:
1. The cursor state reflects the exact progress made before stopping
2. A new worker can resume from the saved cursor position
3. No data is duplicated or lost during resume

Test scenarios:
- Trade worker stops mid-market pagination
- Trade worker stops between markets
- Price worker stops mid-token processing
- Leaderboard worker stops mid-category/time-period iteration
"""

import pytest
import json
import threading
import time
from queue import Queue, Empty
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock, PropertyMock
from typing import List, Dict, Any

# Add project root to path for fetcher package imports
import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from fetcher.cursors import CursorManager, TradeCursor, LeaderboardCursor, PriceCursor, MarketCursor
from fetcher.persistence import SwappableQueue


class TestTradeWorkerMidJobStop:
    """Test cursor persistence when trade worker stops mid-job."""
    
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
    
    def test_stop_mid_market_pagination(self, cursor_manager, temp_cursor_dir):
        """
        Scenario: Worker is paginating through trades for a market
        and stops after processing 3 batches (offset=1500).
        
        Expected: Cursor saves offset=1500 so resume starts from there.
        """
        cursor_file = temp_cursor_dir / "cursor.json"
        market = "0xmarket_abc123"
        pending = ["0xmarket_def456", "0xmarket_ghi789"]
        
        # Simulate worker processing batches
        for batch_num in range(4):  # 0, 1, 2, 3 batches
            offset = batch_num * 500
            cursor_manager.update_trade_cursor(
                market=market,
                offset=offset,
                filter_amount=0,
                pending_markets=pending
            )
            cursor_manager.save_cursors()
        
        # Now at offset=1500, simulate crash (no more saves)
        cursor_manager.update_trade_cursor(
            market=market,
            offset=2000,  # This would be next batch
            filter_amount=0,
            pending_markets=pending
        )
        # NO save_cursors() - simulating crash
        
        # Recover and verify
        recovered = CursorManager(str(cursor_file), auto_save=False)
        recovered.load_cursors()
        cursor = recovered.get_trade_cursor()
        
        assert cursor.market == market
        assert cursor.offset == 1500  # Last saved offset
        assert cursor.pending_markets == pending
    
    def test_stop_between_markets(self, cursor_manager, temp_cursor_dir):
        """
        Scenario: Worker finishes one market and is about to start the next
        when it stops.
        
        Expected: Cursor shows market cleared, pending list updated.
        """
        cursor_file = temp_cursor_dir / "cursor.json"
        
        all_markets = ["0xmarket1", "0xmarket2", "0xmarket3"]
        
        # Process first market completely
        cursor_manager.update_trade_cursor(
            market="0xmarket1",
            offset=500,
            filter_amount=0,
            pending_markets=["0xmarket2", "0xmarket3"]
        )
        cursor_manager.save_cursors()
        
        # First market done - clear it
        cursor_manager.update_trade_cursor(
            market="",
            offset=0,
            filter_amount=0,
            pending_markets=["0xmarket2", "0xmarket3"]
        )
        cursor_manager.save_cursors()
        
        # About to start market2 - crash happens here
        cursor_manager.update_trade_cursor(
            market="0xmarket2",
            offset=0,
            filter_amount=0,
            pending_markets=["0xmarket3"]
        )
        # NO save - crash
        
        # Recover
        recovered = CursorManager(str(cursor_file), auto_save=False)
        recovered.load_cursors()
        cursor = recovered.get_trade_cursor()
        
        # Should still have market2 and market3 pending
        assert cursor.market == ""
        assert "0xmarket2" in cursor.pending_markets
        assert "0xmarket3" in cursor.pending_markets
    
    def test_filter_amount_progression_preserved(self, cursor_manager, temp_cursor_dir):
        """
        Scenario: Worker is using filter_amount to skip small trades
        and increases it progressively.
        
        Expected: Last saved filter_amount is preserved for resume.
        """
        cursor_file = temp_cursor_dir / "cursor.json"
        market = "0xmarket_large"
        
        # Simulate progressive filter increases
        filter_amounts = [0, 100, 250, 500, 1000]
        
        for i, filter_amt in enumerate(filter_amounts):
            cursor_manager.update_trade_cursor(
                market=market,
                offset=(i % 3) * 500,  # Reset offset with filter
                filter_amount=filter_amt,
                pending_markets=[]
            )
            cursor_manager.save_cursors()
        
        # Crash after filter=1000 saved
        cursor_manager.update_trade_cursor(
            market=market,
            offset=500,
            filter_amount=2000,  # Would be next filter level
            pending_markets=[]
        )
        # NO save
        
        # Recover
        recovered = CursorManager(str(cursor_file), auto_save=False)
        recovered.load_cursors()
        cursor = recovered.get_trade_cursor()
        
        assert cursor.filter_amount == 1000  # Last saved filter
        assert cursor.offset == 500  # i=4, offset = (4 % 3) * 500 = 1 * 500 = 500
    
    def test_resume_continues_from_cursor(self, cursor_manager, temp_cursor_dir):
        """
        Test that a simulated resume correctly picks up from cursor state.
        """
        cursor_file = temp_cursor_dir / "cursor.json"
        
        # Set up a mid-job cursor state
        cursor_manager.update_trade_cursor(
            market="0xmarket_resume_test",
            offset=2500,
            filter_amount=500,
            pending_markets=["0xmarket_next"]
        )
        cursor_manager.save_cursors()
        
        # Simulate new worker starting and checking cursor
        new_manager = CursorManager(str(cursor_file), auto_save=False)
        new_manager.load_cursors()
        
        cursor = new_manager.get_trade_cursor()
        
        # Worker should detect it needs to resume
        assert not cursor.is_empty()
        assert cursor.market == "0xmarket_resume_test"
        assert cursor.offset == 2500
        assert cursor.filter_amount == 500
        
        # Simulate worker continuing from cursor
        # It would start fetching from offset=2500 for this market
        new_manager.update_trade_cursor(
            market="0xmarket_resume_test",
            offset=3000,  # Next batch
            filter_amount=500,
            pending_markets=["0xmarket_next"]
        )
        new_manager.save_cursors()
        
        # Verify progression
        final = new_manager.get_trade_cursor()
        assert final.offset == 3000


class TestPriceWorkerMidJobStop:
    """Test cursor persistence when price worker stops mid-job."""
    
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
    
    def test_stop_mid_token_processing(self, cursor_manager, temp_cursor_dir):
        """
        Scenario: Worker is fetching price history for a token
        when it stops.
        
        Expected: Cursor saves current token and pending list.
        """
        cursor_file = temp_cursor_dir / "cursor.json"
        
        current_token = "0xtoken_current"
        pending = [
            ("0xtoken_pending1", 1702000000),
            ("0xtoken_pending2", 1702000000),
            ("0xtoken_pending3", 1702000000)
        ]
        
        # Worker starts processing current token
        cursor_manager.update_price_cursor(
            token_id=current_token,
            start_ts=1702000000,
            end_ts=1702100000,
            pending_tokens=pending
        )
        cursor_manager.save_cursors()
        
        # Crash mid-processing (before completion)
        
        # Recover
        recovered = CursorManager(str(cursor_file), auto_save=False)
        recovered.load_cursors()
        cursor = recovered.get_price_cursor()
        
        assert cursor.token_id == current_token
        assert cursor.start_ts == 1702000000
        assert len(cursor.pending_tokens) == 3
    
    def test_stop_between_tokens(self, cursor_manager, temp_cursor_dir):
        """
        Scenario: Worker completes one token and is about to start the next.
        
        Expected: Current token cleared, pending list reduced.
        """
        cursor_file = temp_cursor_dir / "cursor.json"
        
        # Token completed - clear current, update pending
        cursor_manager.update_price_cursor(
            token_id="",  # Cleared
            start_ts=1702000000,
            end_ts=1702100000,
            pending_tokens=[
                ("0xtoken_next", 1702000000),
                ("0xtoken_after", 1702000000)
            ]
        )
        cursor_manager.save_cursors()
        
        # About to start next token - crash
        cursor_manager.update_price_cursor(
            token_id="0xtoken_next",
            start_ts=1702000000,
            end_ts=1702100000,
            pending_tokens=[("0xtoken_after", 1702000000)]
        )
        # NO save
        
        # Recover
        recovered = CursorManager(str(cursor_file), auto_save=False)
        recovered.load_cursors()
        cursor = recovered.get_price_cursor()
        
        # Should still have both tokens pending (crashed before processing)
        assert cursor.token_id == ""
        assert len(cursor.pending_tokens) == 2
    
    def test_pending_tokens_with_market_timestamps(self, cursor_manager, temp_cursor_dir):
        """
        Verify pending tokens preserve their market start timestamps.
        """
        cursor_file = temp_cursor_dir / "cursor.json"
        
        pending = [
            ("0xtoken1", 1700000000),
            ("0xtoken2", 1701000000),
            ("0xtoken3", 1702000000)
        ]
        
        cursor_manager.update_price_cursor(
            token_id="0xtoken_current",
            start_ts=1699000000,
            end_ts=1702100000,
            pending_tokens=pending
        )
        cursor_manager.save_cursors()
        
        # Recover
        recovered = CursorManager(str(cursor_file), auto_save=False)
        recovered.load_cursors()
        cursor = recovered.get_price_cursor()
        
        # Verify timestamps preserved (JSON converts tuples to lists)
        assert len(cursor.pending_tokens) == 3
        # Check first token has correct timestamp
        token1_entry = cursor.pending_tokens[0]
        assert token1_entry[0] == "0xtoken1"
        assert token1_entry[1] == 1700000000


class TestLeaderboardWorkerMidJobStop:
    """Test cursor persistence when leaderboard worker stops mid-job."""
    
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
    
    def test_stop_mid_category_time_period_iteration(self, cursor_manager, temp_cursor_dir):
        """
        Scenario: Worker is iterating through category/time_period combinations
        and stops mid-way through a particular combination.
        
        Expected: Cursor saves exact position (category, time_period, offset).
        """
        cursor_file = temp_cursor_dir / "cursor.json"
        
        # Simulate being at category 3 (e.g., CRYPTO), time_period 1 (WEEK), offset 200
        cursor_manager.update_leaderboard_cursor(
            current_category_index=3,
            current_time_period_index=1,
            current_offset=200,
            completed=False
        )
        cursor_manager.save_cursors()
        
        # Continue to offset 400
        cursor_manager.update_leaderboard_cursor(
            current_category_index=3,
            current_time_period_index=1,
            current_offset=400,
            completed=False
        )
        cursor_manager.save_cursors()
        
        # Crash before saving offset 600
        cursor_manager.update_leaderboard_cursor(
            current_category_index=3,
            current_time_period_index=1,
            current_offset=600,
            completed=False
        )
        # NO save
        
        # Recover
        recovered = CursorManager(str(cursor_file), auto_save=False)
        recovered.load_cursors()
        cursor = recovered.get_leaderboard_cursor()
        
        assert cursor.current_category_index == 3
        assert cursor.current_time_period_index == 1
        assert cursor.current_offset == 400  # Last saved
        assert cursor.completed == False
    
    def test_stop_transitioning_time_periods(self, cursor_manager, temp_cursor_dir):
        """
        Scenario: Worker finishes one time_period and is transitioning to next.
        
        Expected: Cursor saves at the new time_period with offset 0.
        """
        cursor_file = temp_cursor_dir / "cursor.json"
        
        # Finish time_period 0 for category 2
        cursor_manager.update_leaderboard_cursor(
            current_category_index=2,
            current_time_period_index=0,
            current_offset=500,
            completed=False
        )
        cursor_manager.save_cursors()
        
        # Move to time_period 1
        cursor_manager.update_leaderboard_cursor(
            current_category_index=2,
            current_time_period_index=1,
            current_offset=0,
            completed=False
        )
        cursor_manager.save_cursors()
        
        # Crash before making progress in time_period 1
        cursor_manager.update_leaderboard_cursor(
            current_category_index=2,
            current_time_period_index=1,
            current_offset=100,
            completed=False
        )
        # NO save
        
        # Recover
        recovered = CursorManager(str(cursor_file), auto_save=False)
        recovered.load_cursors()
        cursor = recovered.get_leaderboard_cursor()
        
        assert cursor.current_category_index == 2
        assert cursor.current_time_period_index == 1
        assert cursor.current_offset == 0  # Last saved (just started)
    
    def test_stop_transitioning_categories(self, cursor_manager, temp_cursor_dir):
        """
        Scenario: Worker finishes all time_periods for a category
        and is transitioning to next category.
        
        Expected: Cursor saves at new category, time_period 0.
        """
        cursor_file = temp_cursor_dir / "cursor.json"
        
        # Last time_period of category 1
        cursor_manager.update_leaderboard_cursor(
            current_category_index=1,
            current_time_period_index=3,  # Assuming 4 time periods (0-3)
            current_offset=200,
            completed=False
        )
        cursor_manager.save_cursors()
        
        # Move to category 2
        cursor_manager.update_leaderboard_cursor(
            current_category_index=2,
            current_time_period_index=0,
            current_offset=0,
            completed=False
        )
        cursor_manager.save_cursors()
        
        # Crash
        
        # Recover
        recovered = CursorManager(str(cursor_file), auto_save=False)
        recovered.load_cursors()
        cursor = recovered.get_leaderboard_cursor()
        
        assert cursor.current_category_index == 2
        assert cursor.current_time_period_index == 0
        assert cursor.current_offset == 0
    
    def test_resume_skips_completed_combinations(self, cursor_manager, temp_cursor_dir):
        """
        Verify that resume logic would correctly skip already-processed combinations.
        """
        cursor_file = temp_cursor_dir / "cursor.json"
        
        # Cursor at category 4, time_period 2
        # This means categories 0-3 are done, and for category 4,
        # time_periods 0-1 are done
        cursor_manager.update_leaderboard_cursor(
            current_category_index=4,
            current_time_period_index=2,
            current_offset=150,
            completed=False
        )
        cursor_manager.save_cursors()
        
        # Recover and verify resume point
        recovered = CursorManager(str(cursor_file), auto_save=False)
        recovered.load_cursors()
        cursor = recovered.get_leaderboard_cursor()
        
        # A resume would:
        # - Skip categories 0, 1, 2, 3
        # - For category 4: skip time_periods 0, 1
        # - Start at category 4, time_period 2, offset 150
        
        assert cursor.current_category_index == 4
        assert cursor.current_time_period_index == 2
        assert cursor.current_offset == 150
    
    def test_completion_preserved_after_restart(self, cursor_manager, temp_cursor_dir):
        """
        Verify that completed=True is preserved after restart.
        """
        cursor_file = temp_cursor_dir / "cursor.json"
        
        # Mark as completed
        cursor_manager.update_leaderboard_cursor(
            current_category_index=9,  # Last category
            current_time_period_index=3,  # Last time period
            current_offset=0,
            completed=True
        )
        cursor_manager.save_cursors()
        
        # Recover
        recovered = CursorManager(str(cursor_file), auto_save=False)
        recovered.load_cursors()
        cursor = recovered.get_leaderboard_cursor()
        
        assert cursor.completed == True
        # A new run should detect this and skip leaderboard entirely


class TestMarketWorkerMidJobStop:
    """Test cursor persistence when market worker stops mid-job."""
    
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
    
    def test_stop_mid_pagination(self, cursor_manager, temp_cursor_dir):
        """
        Scenario: Worker is paginating through markets and stops.
        
        Expected: Cursor saves next_cursor for resume.
        """
        cursor_file = temp_cursor_dir / "cursor.json"
        
        # Simulate pagination
        cursors = ["cursor_page1", "cursor_page2", "cursor_page3"]
        
        for c in cursors:
            cursor_manager.update_market_cursor(next_cursor=c, completed=False)
            cursor_manager.save_cursors()
        
        # Crash before saving cursor_page4
        cursor_manager.update_market_cursor(next_cursor="cursor_page4", completed=False)
        # NO save
        
        # Recover
        recovered = CursorManager(str(cursor_file), auto_save=False)
        recovered.load_cursors()
        cursor = recovered.get_market_cursor()
        
        assert cursor.next_cursor == "cursor_page3"
        assert cursor.completed == False
    
    def test_completion_mid_job(self, cursor_manager, temp_cursor_dir):
        """
        Scenario: Worker reaches end of markets but crashes before marking complete.
        
        Expected: Cursor has last cursor but not completed flag.
        """
        cursor_file = temp_cursor_dir / "cursor.json"
        
        # Last page
        cursor_manager.update_market_cursor(next_cursor="final_cursor", completed=False)
        cursor_manager.save_cursors()
        
        # About to mark complete - crash
        cursor_manager.update_market_cursor(next_cursor="", completed=True)
        # NO save
        
        # Recover
        recovered = CursorManager(str(cursor_file), auto_save=False)
        recovered.load_cursors()
        cursor = recovered.get_market_cursor()
        
        # Should resume from final_cursor
        assert cursor.next_cursor == "final_cursor"
        assert cursor.completed == False


class TestMultipleWorkersCoordination:
    """Test cursor behavior when multiple workers are running."""
    
    @pytest.fixture
    def temp_cursor_dir(self, tmp_path):
        """Create temp directory for cursor files."""
        cursor_dir = tmp_path / "cursors"
        cursor_dir.mkdir()
        return cursor_dir
    
    def test_concurrent_cursor_updates_thread_safe(self, temp_cursor_dir):
        """
        Verify that concurrent cursor updates from multiple threads
        don't corrupt the cursor file.
        """
        cursor_file = temp_cursor_dir / "cursor.json"
        manager = CursorManager(str(cursor_file), auto_save=False)
        
        errors = []
        updates_per_thread = 20
        
        def trade_updater(thread_id):
            try:
                for i in range(updates_per_thread):
                    manager.update_trade_cursor(
                        market=f"market_t{thread_id}_{i}",
                        offset=i * 100,
                        filter_amount=i * 10,
                        pending_markets=[f"pending_{thread_id}"]
                    )
                    manager.save_cursors()
                    time.sleep(0.001)  # Small delay to increase interleaving
            except Exception as e:
                errors.append(("trade", thread_id, e))
        
        def price_updater(thread_id):
            try:
                for i in range(updates_per_thread):
                    manager.update_price_cursor(
                        token_id=f"token_p{thread_id}_{i}",
                        start_ts=1700000000 + i,
                        end_ts=1702000000 + i,
                        pending_tokens=[(f"pending_token_{thread_id}", 1700000000)]
                    )
                    manager.save_cursors()
                    time.sleep(0.001)
            except Exception as e:
                errors.append(("price", thread_id, e))
        
        def leaderboard_updater(thread_id):
            try:
                for i in range(updates_per_thread):
                    manager.update_leaderboard_cursor(
                        current_category_index=i % 10,
                        current_time_period_index=i % 4,
                        current_offset=i * 50,
                        completed=False
                    )
                    manager.save_cursors()
                    time.sleep(0.001)
            except Exception as e:
                errors.append(("leaderboard", thread_id, e))
        
        # Start multiple threads for each cursor type
        threads = []
        for i in range(2):
            threads.append(threading.Thread(target=trade_updater, args=(i,)))
            threads.append(threading.Thread(target=price_updater, args=(i,)))
            threads.append(threading.Thread(target=leaderboard_updater, args=(i,)))
        
        for t in threads:
            t.start()
        
        for t in threads:
            t.join()
        
        # No errors should have occurred
        assert len(errors) == 0, f"Errors occurred: {errors}"
        
        # File should be valid JSON
        with open(cursor_file, 'r') as f:
            data = json.load(f)
        
        # All cursor types should be present
        assert "trades" in data
        assert "prices" in data
        assert "leaderboard" in data
        assert "markets" in data
    
    def test_all_cursors_independent(self, temp_cursor_dir):
        """
        Verify that updating one cursor type doesn't affect others.
        """
        cursor_file = temp_cursor_dir / "cursor.json"
        manager = CursorManager(str(cursor_file), auto_save=False)
        
        # Set initial state for all cursors
        manager.update_trade_cursor("market1", 100, 50, ["market2"])
        manager.update_price_cursor("token1", 1700000000, 1702000000, [])
        manager.update_leaderboard_cursor(2, 1, 150, False)
        manager.update_market_cursor("cursor123", False)
        manager.save_cursors()
        
        # Update only trade cursor
        manager.update_trade_cursor("market_new", 500, 100, [])
        manager.save_cursors()
        
        # Reload and verify others unchanged
        recovered = CursorManager(str(cursor_file), auto_save=False)
        recovered.load_cursors()
        
        trade = recovered.get_trade_cursor()
        price = recovered.get_price_cursor()
        lb = recovered.get_leaderboard_cursor()
        market = recovered.get_market_cursor()
        
        # Trade updated
        assert trade.market == "market_new"
        assert trade.offset == 500
        
        # Others unchanged
        assert price.token_id == "token1"
        assert price.start_ts == 1700000000
        
        assert lb.current_category_index == 2
        assert lb.current_time_period_index == 1
        
        assert market.next_cursor == "cursor123"


class TestEdgeCases:
    """Test edge cases for cursor mid-job stops."""
    
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
    
    def test_empty_pending_lists(self, cursor_manager, temp_cursor_dir):
        """Verify empty pending lists are handled correctly."""
        cursor_file = temp_cursor_dir / "cursor.json"
        
        cursor_manager.update_trade_cursor("market", 100, 0, [])
        cursor_manager.update_price_cursor("token", 1700000000, 1702000000, [])
        cursor_manager.save_cursors()
        
        recovered = CursorManager(str(cursor_file), auto_save=False)
        recovered.load_cursors()
        
        trade = recovered.get_trade_cursor()
        price = recovered.get_price_cursor()
        
        assert trade.pending_markets == []
        assert price.pending_tokens == []
    
    def test_very_long_market_ids(self, cursor_manager, temp_cursor_dir):
        """Verify long market IDs are preserved."""
        cursor_file = temp_cursor_dir / "cursor.json"
        
        long_id = "0x" + "a" * 64  # 66 character hex ID
        
        cursor_manager.update_trade_cursor(
            market=long_id,
            offset=0,
            filter_amount=0,
            pending_markets=[long_id, long_id + "_2"]
        )
        cursor_manager.save_cursors()
        
        recovered = CursorManager(str(cursor_file), auto_save=False)
        recovered.load_cursors()
        
        cursor = recovered.get_trade_cursor()
        assert cursor.market == long_id
        assert cursor.pending_markets[0] == long_id
    
    def test_large_offset_values(self, cursor_manager, temp_cursor_dir):
        """Verify large offset values are preserved."""
        cursor_file = temp_cursor_dir / "cursor.json"
        
        large_offset = 10_000_000
        
        cursor_manager.update_trade_cursor(
            market="market",
            offset=large_offset,
            filter_amount=999999,
            pending_markets=[]
        )
        cursor_manager.save_cursors()
        
        recovered = CursorManager(str(cursor_file), auto_save=False)
        recovered.load_cursors()
        
        cursor = recovered.get_trade_cursor()
        assert cursor.offset == large_offset
        assert cursor.filter_amount == 999999
    
    def test_cursor_file_corruption_recovery(self, cursor_manager, temp_cursor_dir):
        """Verify graceful handling of corrupted cursor file."""
        cursor_file = temp_cursor_dir / "cursor.json"
        
        # Create valid cursor
        cursor_manager.update_trade_cursor("market", 100, 0, [])
        cursor_manager.save_cursors()
        
        # Corrupt the file
        with open(cursor_file, 'w') as f:
            f.write("{ invalid json")
        
        # Should recover with empty cursors (not crash)
        recovered = CursorManager(str(cursor_file), auto_save=False)
        recovered.load_cursors()
        
        cursor = recovered.get_trade_cursor()
        assert cursor.is_empty()  # Falls back to empty
    
    def test_rapid_save_load_cycles(self, cursor_manager, temp_cursor_dir):
        """Verify cursor survives rapid save/load cycles."""
        cursor_file = temp_cursor_dir / "cursor.json"
        
        for i in range(50):
            cursor_manager.update_trade_cursor(
                market=f"market_{i}",
                offset=i * 100,
                filter_amount=i * 10,
                pending_markets=[f"pending_{i}"]
            )
            cursor_manager.save_cursors()
            
            # Immediately reload
            temp_manager = CursorManager(str(cursor_file), auto_save=False)
            temp_manager.load_cursors()
            cursor = temp_manager.get_trade_cursor()
            
            assert cursor.market == f"market_{i}"
            assert cursor.offset == i * 100
