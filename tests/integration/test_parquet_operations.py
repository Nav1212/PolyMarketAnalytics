"""
Integration tests for Parquet persistence operations.

Tests actual reading/writing of parquet files.
Run with: pytest tests/integration/test_parquet_operations.py -v
"""

import pytest
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from datetime import datetime
import json
import time

pytestmark = pytest.mark.integration


class TestParquetWrite:
    """Tests for writing data to parquet files."""
    
    def test_write_trades_to_parquet(self, sample_trades, temp_parquet_dir):
        """Test writing trade data to a parquet file."""
        from fetcher.persistence import TRADE_SCHEMA
        
        # Convert trades to PyArrow table
        arrays = {
            'proxyWallet': [t['proxyWallet'] for t in sample_trades],
            'side': [t['side'] for t in sample_trades],
            'price': [float(t['price']) for t in sample_trades],
            'size': [float(t['size']) for t in sample_trades],
            'conditionId': [t['conditionId'] for t in sample_trades],
            'timestamp': [int(t['timestamp']) for t in sample_trades],
            'transactionHash': [t['transactionHash'] for t in sample_trades],
            'outcome': [t['outcome'] for t in sample_trades],
        }
        
        table = pa.table(arrays, schema=TRADE_SCHEMA)
        
        # Write to parquet
        output_path = temp_parquet_dir / "trades.parquet"
        pq.write_table(table, output_path)
        
        # Verify file exists and is readable
        assert output_path.exists()
        
        # Read back and verify
        read_table = pq.read_table(output_path)
        assert len(read_table) == len(sample_trades)
    
    def test_write_with_hive_partitioning(self, sample_trades, temp_parquet_dir):
        """Test writing with Hive-style date partitioning."""
        from fetcher.persistence import TRADE_SCHEMA
        
        # Create partition directory
        today = datetime.now().strftime("%Y-%m-%d")
        partition_dir = temp_parquet_dir / f"dt={today}"
        partition_dir.mkdir(parents=True, exist_ok=True)
        
        # Convert trades to table
        arrays = {
            'proxyWallet': [t['proxyWallet'] for t in sample_trades],
            'side': [t['side'] for t in sample_trades],
            'price': [float(t['price']) for t in sample_trades],
            'size': [float(t['size']) for t in sample_trades],
            'conditionId': [t['conditionId'] for t in sample_trades],
            'timestamp': [int(t['timestamp']) for t in sample_trades],
            'transactionHash': [t['transactionHash'] for t in sample_trades],
            'outcome': [t['outcome'] for t in sample_trades],
        }
        
        table = pa.table(arrays, schema=TRADE_SCHEMA)
        
        # Write to partitioned path
        output_path = partition_dir / f"trades_{int(time.time())}.parquet"
        pq.write_table(table, output_path)
        
        assert output_path.exists()
        assert "dt=" in str(output_path)


class TestParquetRead:
    """Tests for reading data from parquet files."""
    
    def test_load_parquet_data(self, sample_trades, temp_parquet_dir):
        """Test loading data from parquet using DuckDB."""
        from fetcher.persistence import TRADE_SCHEMA
        from fetcher.persistence.parquet_persister import load_parquet_data
        
        # First write some data
        arrays = {
            'proxyWallet': [t['proxyWallet'] for t in sample_trades],
            'side': [t['side'] for t in sample_trades],
            'price': [float(t['price']) for t in sample_trades],
            'size': [float(t['size']) for t in sample_trades],
            'conditionId': [t['conditionId'] for t in sample_trades],
            'timestamp': [int(t['timestamp']) for t in sample_trades],
            'transactionHash': [t['transactionHash'] for t in sample_trades],
            'outcome': [t['outcome'] for t in sample_trades],
        }
        
        table = pa.table(arrays, schema=TRADE_SCHEMA)
        output_path = temp_parquet_dir / "trades.parquet"
        pq.write_table(table, output_path)
        
        # Load using our function
        loaded = load_parquet_data(str(temp_parquet_dir))
        
        assert len(loaded) == len(sample_trades)
        assert all('proxyWallet' in record for record in loaded)
    
    def test_load_parquet_with_query(self, sample_trades, temp_parquet_dir):
        """Test loading parquet data with SQL query."""
        from fetcher.persistence import TRADE_SCHEMA
        from fetcher.persistence.parquet_persister import load_parquet_data
        
        # Write data
        arrays = {
            'proxyWallet': [t['proxyWallet'] for t in sample_trades],
            'side': [t['side'] for t in sample_trades],
            'price': [float(t['price']) for t in sample_trades],
            'size': [float(t['size']) for t in sample_trades],
            'conditionId': [t['conditionId'] for t in sample_trades],
            'timestamp': [int(t['timestamp']) for t in sample_trades],
            'transactionHash': [t['transactionHash'] for t in sample_trades],
            'outcome': [t['outcome'] for t in sample_trades],
        }
        
        table = pa.table(arrays, schema=TRADE_SCHEMA)
        pq.write_table(table, temp_parquet_dir / "trades.parquet")
        
        # Query for BUY trades only
        query = "SELECT * FROM data WHERE side = 'BUY'"
        loaded = load_parquet_data(str(temp_parquet_dir), query=query)
        
        assert all(record['side'] == 'BUY' for record in loaded)
    
    def test_load_nonexistent_path_returns_empty(self):
        """Test that loading from nonexistent path returns empty list."""
        from fetcher.persistence.parquet_persister import load_parquet_data
        
        result = load_parquet_data("/nonexistent/path/to/data")
        assert result == []


class TestCursorOperations:
    """Tests for cursor persistence (tracking last run state)."""
    
    def test_save_and_load_cursor(self, temp_parquet_dir):
        """Test saving and loading cursor data."""
        from fetcher.persistence.parquet_persister import save_cursor, load_cursor
        
        cursor_data = {
            "last_timestamp": 1702300800,
            "last_offset": 100,
            "market_id": "0xabc123"
        }
        
        # Save cursor
        save_cursor(str(temp_parquet_dir), cursor_data)
        
        # Load cursor
        loaded = load_cursor(str(temp_parquet_dir))
        
        assert loaded is not None
        assert loaded["last_timestamp"] == 1702300800
        assert loaded["last_offset"] == 100
        assert loaded["market_id"] == "0xabc123"
        assert "updated_at" in loaded  # Should be added by save_cursor
    
    def test_load_cursor_nonexistent_returns_none(self, temp_parquet_dir):
        """Test that loading nonexistent cursor returns None."""
        from fetcher.persistence.parquet_persister import load_cursor
        
        result = load_cursor(str(temp_parquet_dir / "nonexistent"))
        assert result is None


class TestParquetPersisterIntegration:
    """Integration tests for the ParquetPersister class."""
    
    def test_persister_writes_on_threshold(self, temp_parquet_dir, sample_trades):
        """Test that ParquetPersister writes when threshold is reached."""
        from fetcher.persistence import ParquetPersister, DataType, SwappableQueue
        
        # Small threshold for testing
        queue = SwappableQueue(threshold=10)
        
        persister = ParquetPersister(
            queue=queue,
            output_dir=str(temp_parquet_dir),
            data_type=DataType.TRADE,
            poll_interval=0.1
        )
        
        persister.start()
        
        try:
            # Add enough trades to trigger threshold
            for trade in sample_trades[:15]:
                queue.put(trade)
            
            # Wait for write to complete
            time.sleep(1.0)
            
            # Check that a file was written
            parquet_files = list(temp_parquet_dir.rglob("*.parquet"))
            assert len(parquet_files) >= 1
            
        finally:
            persister.stop(timeout=5.0)
    
    def test_persister_flushes_on_stop(self, temp_parquet_dir, sample_trades):
        """Test that ParquetPersister flushes remaining items on stop."""
        from fetcher.persistence import ParquetPersister, DataType, SwappableQueue
        
        # Threshold higher than items we'll add
        queue = SwappableQueue(threshold=100)
        
        persister = ParquetPersister(
            queue=queue,
            output_dir=str(temp_parquet_dir),
            data_type=DataType.TRADE,
            poll_interval=0.1
        )
        
        persister.start()
        
        # Add some trades (below threshold)
        for trade in sample_trades[:5]:
            queue.put(trade)
        
        # Stop should flush
        persister.stop(timeout=5.0)
        
        # Check that remaining items were flushed
        parquet_files = list(temp_parquet_dir.rglob("*.parquet"))
        assert len(parquet_files) >= 1
