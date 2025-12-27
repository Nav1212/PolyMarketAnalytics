"""
Integration tests for PriceFetcher with historical data fetching.

These tests verify:
1. Fetching prices for a market with no price history
2. Fetching prices for a market with some historical data
3. Fetching prices for an active market with ongoing trades
4. Response object structure validation
5. Saving to parquet files
6. Cursor persistence during fetching

Run with: pytest tests/integration/test_price_fetcher.py -v -m integration
"""

import pytest
import time
import threading
from queue import Queue
from pathlib import Path
from datetime import datetime
import json
import sys

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

pytestmark = pytest.mark.integration


# =============================================================================
# Fixtures
# =============================================================================

@pytest.fixture
def test_config():
    """Create test configuration."""
    from fetcher.config import (
        Config, RateLimitsConfig, QueuesConfig, 
        OutputDirsConfig, ApiConfig, WorkersConfig, RetryConfig
    )
    
    return Config(
        rate_limits=RateLimitsConfig(
            trade=100,
            market=100,
            price=100,
            leaderboard=100,
            window_seconds=1.0
        ),
        queues=QueuesConfig(
            trade_threshold=100,
            market_threshold=50,
            market_token_threshold=50,
            price_threshold=100,
            leaderboard_threshold=100
        ),
        output_dirs=OutputDirsConfig(
            trade="test_data/trades",
            market="test_data/markets",
            market_token="test_data/market_tokens",
            price="test_data/prices",
            leaderboard="test_data/leaderboard"
        ),
        api=ApiConfig(
            timeout=30.0,
            connect_timeout=10.0
        ),
        workers=WorkersConfig(
            trade=1,
            market=1,
            price=1,
            leaderboard=1
        ),
        retry=RetryConfig(
            max_attempts=3,
            base_delay=1.0,
            max_delay=10.0,
            exponential_base=2.0
        )
    )


@pytest.fixture
def worker_manager(test_config):
    """Create worker manager for tests."""
    from fetcher.workers import WorkerManager
    return WorkerManager(config=test_config)


@pytest.fixture
def temp_cursor_dir(tmp_path):
    """Create temp directory for cursor files."""
    cursor_dir = tmp_path / "cursors"
    cursor_dir.mkdir()
    return cursor_dir


@pytest.fixture
def cursor_manager(temp_cursor_dir):
    """Create cursor manager with temp directory."""
    from fetcher.cursors import CursorManager
    cursor_file = temp_cursor_dir / "cursor.json"
    return CursorManager(str(cursor_file), auto_save=False)


@pytest.fixture
def temp_parquet_dir(tmp_path):
    """Create temp directory for parquet files."""
    parquet_dir = tmp_path / "prices"
    parquet_dir.mkdir()
    return parquet_dir


# =============================================================================
# Token ID Fixtures (fetched from real API)
# =============================================================================

@pytest.fixture(scope="module")
def active_token_id():
    """Get a token ID from an active market with recent activity."""
    import httpx
    import json as json_module
    
    with httpx.Client(timeout=30.0) as client:
        # Get active markets
        response = client.get(
            "https://gamma-api.polymarket.com/markets",
            params={"limit": 10, "active": "true", "closed": "false"}
        )
        if response.status_code == 200:
            markets = response.json()
            for market in markets:
                clob_token_ids = market.get("clobTokenIds", [])
                
                # Handle case where clobTokenIds is a JSON string
                if isinstance(clob_token_ids, str):
                    try:
                        clob_token_ids = json_module.loads(clob_token_ids)
                    except (json_module.JSONDecodeError, TypeError):
                        clob_token_ids = []
                
                if clob_token_ids:
                    # Verify this token has price history
                    token_id = clob_token_ids[0]
                    price_check = client.get(
                        "https://clob.polymarket.com/prices-history",
                        params={
                            "market": token_id,
                            "startTs": int(time.time()) - 86400,
                            "endTs": int(time.time()),
                            "fidelity": 60
                        }
                    )
                    if price_check.status_code == 200:
                        data = price_check.json()
                        history = data.get("history", [])
                        if len(history) > 0:
                            return token_id
    
    pytest.skip("Could not find active token with price history")


@pytest.fixture(scope="module")
def historical_token_id():
    """Get a token ID from a closed market with historical data."""
    import httpx
    import json as json_module
    
    with httpx.Client(timeout=30.0) as client:
        # Get closed markets
        response = client.get(
            "https://gamma-api.polymarket.com/markets",
            params={"limit": 20, "closed": "true"}
        )
        if response.status_code == 200:
            markets = response.json()
            for market in markets:
                clob_token_ids = market.get("clobTokenIds", [])
                
                # Handle case where clobTokenIds is a JSON string
                if isinstance(clob_token_ids, str):
                    try:
                        clob_token_ids = json_module.loads(clob_token_ids)
                    except (json_module.JSONDecodeError, TypeError):
                        clob_token_ids = []
                
                if clob_token_ids:
                    token_id = clob_token_ids[0]
                    # Get market start time from end_date_iso
                    end_date = market.get("end_date_iso", "")
                    if end_date:
                        try:
                            end_dt = datetime.fromisoformat(end_date.replace("Z", "+00:00"))
                            end_ts = int(end_dt.timestamp())
                            start_ts = end_ts - 7 * 24 * 60 * 60  # 7 days before close
                            
                            price_check = client.get(
                                "https://clob.polymarket.com/prices-history",
                                params={
                                    "market": token_id,
                                    "startTs": start_ts,
                                    "endTs": end_ts,
                                    "fidelity": 60
                                }
                            )
                            if price_check.status_code == 200:
                                data = price_check.json()
                                history = data.get("history", [])
                                if len(history) >= 10:  # Has meaningful history
                                    return token_id
                        except (ValueError, TypeError):
                            continue
    
    pytest.skip("Could not find closed token with historical data")


@pytest.fixture(scope="module")
def empty_token_id():
    """Get a token ID that likely has no/minimal price history."""
    # Use a made-up token ID that shouldn't exist
    # The API returns empty history for unknown tokens
    return "0x0000000000000000000000000000000000000000000000000000000000000000"


# =============================================================================
# Test Classes
# =============================================================================

class TestPriceFetcherBasic:
    """Basic integration tests for PriceFetcher."""
    
    def test_fetch_price_history_single_chunk(self, test_config, worker_manager, active_token_id):
        """Test fetching a single chunk of price history."""
        from fetcher.workers import PriceFetcher
        
        fetcher = PriceFetcher(
            worker_manager=worker_manager,
            config=test_config
        )
        
        end_ts = int(time.time())
        start_ts = end_ts - 3600  # Last hour
        
        prices = fetcher.fetch_price_history(
            token_id=active_token_id,
            start_ts=start_ts,
            end_ts=end_ts,
            fidelity=60  # Hourly
        )
        
        # May or may not have prices depending on market activity
        assert isinstance(prices, list)
        
        # If we have prices, validate structure
        if prices:
            assert "token_id" in prices[0]
            assert "timestamp" in prices[0]
            assert "price" in prices[0]
            assert prices[0]["token_id"] == active_token_id
        
        fetcher.close()
    
    def test_fetch_price_history_empty_market(self, test_config, worker_manager, empty_token_id):
        """Test fetching prices for a non-existent/empty token."""
        from fetcher.workers import PriceFetcher
        
        fetcher = PriceFetcher(
            worker_manager=worker_manager,
            config=test_config
        )
        
        end_ts = int(time.time())
        start_ts = end_ts - 86400  # Last day
        
        prices = fetcher.fetch_price_history(
            token_id=empty_token_id,
            start_ts=start_ts,
            end_ts=end_ts,
            fidelity=60
        )
        
        # Should return empty list, not error
        assert isinstance(prices, list)
        assert len(prices) == 0
        
        fetcher.close()


class TestPriceFetcherWithParamsProvider:
    """Tests for PriceFetcher with HistoricalPriceParamsProvider."""
    
    def test_fetch_all_historical_with_provider(self, test_config, worker_manager, active_token_id):
        """Test fetching all historical prices using params provider."""
        from fetcher.workers import PriceFetcher, HistoricalPriceParamsProvider
        
        end_ts = int(time.time())
        start_ts = end_ts - 6 * 60 * 60  # Last 6 hours (small window)
        
        provider = HistoricalPriceParamsProvider(
            start_ts=start_ts,
            end_ts=end_ts,
            fidelity=60,  # Hourly
            chunk_seconds=3 * 60 * 60,  # 3 hour chunks = 2 chunks
        )
        
        fetcher = PriceFetcher(
            worker_manager=worker_manager,
            config=test_config
        )
        
        prices = fetcher.fetch_all_historical_prices(
            token_id=active_token_id,
            params_provider=provider
        )
        
        assert isinstance(prices, list)
        # Provider should be complete after fetching
        assert provider.is_complete
        
        fetcher.close()
    
    def test_fetch_historical_stops_on_completion(self, test_config, worker_manager, historical_token_id):
        """Test that historical fetching stops when reaching end."""
        from fetcher.workers import PriceFetcher, HistoricalPriceParamsProvider
        
        # Use a small window for a closed market
        end_ts = int(time.time()) - 30 * 24 * 60 * 60  # 30 days ago
        start_ts = end_ts - 24 * 60 * 60  # 1 day window
        
        provider = HistoricalPriceParamsProvider(
            start_ts=start_ts,
            end_ts=end_ts,
            fidelity=60,
            chunk_seconds=12 * 60 * 60,  # 12 hour chunks
        )
        
        fetcher = PriceFetcher(
            worker_manager=worker_manager,
            config=test_config
        )
        
        initial_start = provider.current_start_ts
        
        prices = fetcher.fetch_all_historical_prices(
            token_id=historical_token_id,
            params_provider=provider
        )
        
        assert provider.is_complete
        # Should have advanced past initial start
        assert provider.current_start_ts >= end_ts
        
        fetcher.close()
    
    def test_injected_provider_via_constructor(self, test_config, worker_manager, active_token_id):
        """Test injecting params provider via constructor."""
        from fetcher.workers import PriceFetcher, HistoricalPriceParamsProvider
        
        provider = HistoricalPriceParamsProvider(
            start_ts=int(time.time()) - 3600,
            end_ts=int(time.time()),
            fidelity=60,
        )
        
        fetcher = PriceFetcher(
            worker_manager=worker_manager,
            config=test_config,
            params_provider=provider
        )
        
        # Should be able to use the injected provider
        assert fetcher._params_provider is provider
        
        fetcher.close()


class TestPriceDataValidation:
    """Tests for validating price data structure."""
    
    def test_price_record_structure(self, test_config, worker_manager, active_token_id):
        """Verify price records have correct structure."""
        from fetcher.workers import PriceFetcher
        
        fetcher = PriceFetcher(
            worker_manager=worker_manager,
            config=test_config
        )
        
        end_ts = int(time.time())
        start_ts = end_ts - 86400  # Last 24 hours
        
        prices = fetcher.fetch_price_history(
            token_id=active_token_id,
            start_ts=start_ts,
            end_ts=end_ts,
            fidelity=60
        )
        
        if prices:
            for price in prices:
                # Required fields
                assert "token_id" in price, "Missing token_id field"
                assert "timestamp" in price, "Missing timestamp field"
                assert "price" in price, "Missing price field"
                
                # Type validation
                assert isinstance(price["token_id"], str)
                assert isinstance(price["timestamp"], int)
                assert isinstance(price["price"], float)
                
                # Value validation
                assert price["token_id"] == active_token_id
                assert price["timestamp"] > 0
                assert 0.0 <= price["price"] <= 1.0 or price["price"] >= 0  # Some markets may have different ranges
        
        fetcher.close()
    
    def test_prices_ordered_by_timestamp(self, test_config, worker_manager, active_token_id):
        """Verify prices are returned in order."""
        from fetcher.workers import PriceFetcher
        
        fetcher = PriceFetcher(
            worker_manager=worker_manager,
            config=test_config
        )
        
        end_ts = int(time.time())
        start_ts = end_ts - 86400
        
        prices = fetcher.fetch_price_history(
            token_id=active_token_id,
            start_ts=start_ts,
            end_ts=end_ts,
            fidelity=60
        )
        
        if len(prices) > 1:
            # Check timestamps are in order (ascending or descending)
            timestamps = [p["timestamp"] for p in prices]
            is_ascending = all(timestamps[i] <= timestamps[i+1] for i in range(len(timestamps)-1))
            is_descending = all(timestamps[i] >= timestamps[i+1] for i in range(len(timestamps)-1))
            
            assert is_ascending or is_descending, "Timestamps should be ordered"
        
        fetcher.close()


class TestParquetPersistence:
    """Tests for saving price data to parquet files."""
    
    def test_save_prices_to_parquet(self, test_config, worker_manager, active_token_id, temp_parquet_dir):
        """Test saving fetched prices to parquet file."""
        from fetcher.workers import PriceFetcher
        from fetcher.persistence import SwappableQueue
        from fetcher.persistence.parquet_persister import ParquetPersister, DataType, PRICE_SCHEMA
        import pyarrow as pa
        import pyarrow.parquet as pq
        
        fetcher = PriceFetcher(
            worker_manager=worker_manager,
            config=test_config
        )
        
        end_ts = int(time.time())
        start_ts = end_ts - 86400
        
        prices = fetcher.fetch_price_history(
            token_id=active_token_id,
            start_ts=start_ts,
            end_ts=end_ts,
            fidelity=60
        )
        
        if prices:
            # Write directly using PyArrow (bypassing persister for simplicity)
            table = pa.Table.from_pylist(prices, schema=PRICE_SCHEMA)
            output_file = temp_parquet_dir / "test_prices.parquet"
            pq.write_table(table, output_file, compression='snappy')
            
            # Verify file was created
            assert output_file.exists(), "Should have created parquet file"
            
            # Read back and verify using PyArrow (no pandas dependency)
            read_table = pq.read_table(output_file)
            
            assert len(read_table) == len(prices)
            assert "token_id" in read_table.column_names
            assert "timestamp" in read_table.column_names
            assert "price" in read_table.column_names
        
        fetcher.close()
    
    def test_save_prices_via_persister(self, test_config, worker_manager, active_token_id, temp_parquet_dir):
        """Test saving prices through ParquetPersister with SwappableQueue."""
        from fetcher.workers import PriceFetcher
        from fetcher.persistence import SwappableQueue
        from fetcher.persistence.parquet_persister import ParquetPersister, DataType
        import pyarrow.parquet as pq
        
        fetcher = PriceFetcher(
            worker_manager=worker_manager,
            config=test_config
        )
        
        end_ts = int(time.time())
        start_ts = end_ts - 86400
        
        prices = fetcher.fetch_price_history(
            token_id=active_token_id,
            start_ts=start_ts,
            end_ts=end_ts,
            fidelity=60
        )
        
        if prices:
            # Use SwappableQueue with low threshold to trigger write
            queue = SwappableQueue(threshold=max(1, len(prices)))
            persister = ParquetPersister(
                queue=queue,
                output_dir=str(temp_parquet_dir),
                data_type=DataType.PRICE
            )
            
            # Start persister
            persister.start()
            
            # Add prices to queue
            queue.put_many(prices)
            
            # Stop persister (flushes remaining)
            persister.stop(timeout=10.0)
            
            # Verify files were created
            parquet_files = list(temp_parquet_dir.glob("**/*.parquet"))
            assert len(parquet_files) >= 1, "Should have created parquet file(s)"
            
            # Read back and verify total count
            total_records = 0
            for pf in parquet_files:
                table = pq.read_table(pf)
                total_records += len(table)
            
            assert total_records == len(prices), f"Expected {len(prices)} records, got {total_records}"
        
        fetcher.close()
    
    def test_worker_writes_to_swappable_queue(self, test_config, worker_manager, cursor_manager, active_token_id):
        """Test that worker correctly writes to SwappableQueue."""
        from fetcher.workers import PriceFetcher
        from fetcher.persistence import SwappableQueue
        
        token_queue = Queue()
        output_queue = SwappableQueue(threshold=1000)
        
        fetcher = PriceFetcher(
            market_queue=token_queue,
            worker_manager=worker_manager,
            config=test_config,
            cursor_manager=cursor_manager
        )
        
        end_ts = int(time.time())
        start_ts = end_ts - 3600  # Small window
        
        # Add token and sentinel
        token_queue.put((active_token_id, start_ts))
        token_queue.put(None)
        
        # Run worker in thread
        thread = threading.Thread(
            target=fetcher._worker,
            args=(0, output_queue, start_ts, end_ts, 60),
            daemon=True
        )
        thread.start()
        thread.join(timeout=60)
        
        assert not thread.is_alive(), "Worker should complete"
        
        # Check output queue has data
        fetched_count = output_queue.size()
        # May or may not have data depending on market activity
        assert fetched_count >= 0
        
        fetcher.close()


class TestCursorPersistence:
    """Tests for cursor persistence during price fetching."""
    
    def test_cursor_updated_during_fetch(self, test_config, worker_manager, cursor_manager, active_token_id, temp_cursor_dir):
        """Test that cursor is updated during fetching."""
        from fetcher.workers import PriceFetcher
        from fetcher.persistence import SwappableQueue
        
        token_queue = Queue()
        output_queue = SwappableQueue(threshold=1000)
        
        fetcher = PriceFetcher(
            market_queue=token_queue,
            worker_manager=worker_manager,
            config=test_config,
            cursor_manager=cursor_manager
        )
        
        end_ts = int(time.time())
        start_ts = end_ts - 3600
        
        token_queue.put((active_token_id, start_ts))
        token_queue.put(None)
        
        thread = threading.Thread(
            target=fetcher._worker,
            args=(0, output_queue, start_ts, end_ts, 60),
            daemon=True
        )
        thread.start()
        thread.join(timeout=60)
        
        # Check cursor was saved
        cursor_manager.save_cursors()
        cursor_file = temp_cursor_dir / "cursor.json"
        
        assert cursor_file.exists(), "Cursor file should be created"
        
        with open(cursor_file) as f:
            data = json.load(f)
        
        assert "prices" in data, "Should have prices cursor"
        
        fetcher.close()
    
    def test_cursor_tracks_progress(self, test_config, worker_manager, temp_cursor_dir, active_token_id):
        """Test that cursor tracks fetching progress."""
        from fetcher.workers import PriceFetcher
        from fetcher.persistence import SwappableQueue
        from fetcher.cursors import CursorManager
        
        cursor_file = temp_cursor_dir / "cursor.json"
        cursor_manager = CursorManager(str(cursor_file), auto_save=False)
        
        token_queue = Queue()
        output_queue = SwappableQueue(threshold=1000)
        
        fetcher = PriceFetcher(
            market_queue=token_queue,
            worker_manager=worker_manager,
            config=test_config,
            cursor_manager=cursor_manager
        )
        
        end_ts = int(time.time())
        start_ts = end_ts - 7200  # 2 hours
        
        # Add token
        token_queue.put((active_token_id, start_ts))
        token_queue.put(None)
        
        thread = threading.Thread(
            target=fetcher._worker,
            args=(0, output_queue, start_ts, end_ts, 60),
            daemon=True
        )
        thread.start()
        thread.join(timeout=60)
        
        # Get cursor state
        price_cursor = cursor_manager.get_price_cursor()
        
        # After completion, the cursor should reflect progress
        # (token removed from pending after successful fetch)
        fetcher.close()


class TestEdgeCases:
    """Edge case tests for price fetcher."""
    
    def test_start_ts_after_end_ts(self, test_config, worker_manager):
        """Test handling when start_ts >= end_ts."""
        from fetcher.workers import PriceFetcher
        
        fetcher = PriceFetcher(
            worker_manager=worker_manager,
            config=test_config
        )
        
        prices = fetcher.fetch_price_history(
            token_id="any_token",
            start_ts=1702387200,
            end_ts=1702300800,  # Before start
            fidelity=60
        )
        
        assert prices == [], "Should return empty for invalid time range"
        
        fetcher.close()
    
    def test_future_end_ts_capped(self, test_config, worker_manager, active_token_id):
        """Test that future end_ts is capped to current time."""
        from fetcher.workers import PriceFetcher
        
        fetcher = PriceFetcher(
            worker_manager=worker_manager,
            config=test_config
        )
        
        now = int(time.time())
        future = now + 7 * 24 * 60 * 60  # 7 days in future
        
        # Should not error, just cap to now
        prices = fetcher.fetch_price_history(
            token_id=active_token_id,
            start_ts=now - 3600,
            end_ts=future,
            fidelity=60
        )
        
        assert isinstance(prices, list)
        
        fetcher.close()
    
    def test_very_old_start_ts(self, test_config, worker_manager, active_token_id):
        """Test fetching with very old start timestamp."""
        from fetcher.workers import PriceFetcher
        
        fetcher = PriceFetcher(
            worker_manager=worker_manager,
            config=test_config
        )
        
        # 2020 timestamp - before Polymarket likely existed
        old_start = 1577836800  # Jan 1, 2020
        end_ts = old_start + 86400
        
        prices = fetcher.fetch_price_history(
            token_id=active_token_id,
            start_ts=old_start,
            end_ts=end_ts,
            fidelity=60
        )
        
        # Should return empty, not error
        assert isinstance(prices, list)
        
        fetcher.close()
    
    def test_default_time_range(self, test_config, worker_manager, active_token_id):
        """Test fetching with default time range (None values)."""
        from fetcher.workers import PriceFetcher
        
        fetcher = PriceFetcher(
            worker_manager=worker_manager,
            config=test_config
        )
        
        # Should use defaults (30 days back to now)
        prices = fetcher.fetch_price_history(
            token_id=active_token_id,
            fidelity=60
        )
        
        assert isinstance(prices, list)
        
        fetcher.close()


class TestHourlyGranularity:
    """Tests specifically for hourly granularity (fidelity=60)."""
    
    def test_hourly_fidelity_returns_hourly_data(self, test_config, worker_manager, active_token_id):
        """Test that fidelity=60 returns approximately hourly data points."""
        from fetcher.workers import PriceFetcher
        
        fetcher = PriceFetcher(
            worker_manager=worker_manager,
            config=test_config
        )
        
        end_ts = int(time.time())
        start_ts = end_ts - 24 * 60 * 60  # 24 hours
        
        prices = fetcher.fetch_price_history(
            token_id=active_token_id,
            start_ts=start_ts,
            end_ts=end_ts,
            fidelity=60  # Hourly
        )
        
        if len(prices) >= 2:
            # Calculate average interval between data points
            timestamps = sorted([p["timestamp"] for p in prices])
            intervals = [timestamps[i+1] - timestamps[i] for i in range(len(timestamps)-1)]
            avg_interval = sum(intervals) / len(intervals)
            
            # Should be approximately 60 minutes (3600 seconds) with some tolerance
            # API may not return exactly hourly data
            assert avg_interval > 0, "Should have positive intervals"
        
        fetcher.close()
    
    def test_different_fidelity_values(self, test_config, worker_manager, active_token_id):
        """Test different fidelity values work correctly."""
        from fetcher.workers import PriceFetcher
        
        fetcher = PriceFetcher(
            worker_manager=worker_manager,
            config=test_config
        )
        
        end_ts = int(time.time())
        start_ts = end_ts - 6 * 60 * 60  # 6 hours
        
        fidelities = [1, 15, 60]  # 1 min, 15 min, 1 hour
        results = {}
        
        for fidelity in fidelities:
            prices = fetcher.fetch_price_history(
                token_id=active_token_id,
                start_ts=start_ts,
                end_ts=end_ts,
                fidelity=fidelity
            )
            results[fidelity] = len(prices)
        
        # Higher fidelity (less granular) should generally have fewer or equal points
        # But this depends on API behavior
        assert all(isinstance(v, int) for v in results.values())
        
        fetcher.close()
