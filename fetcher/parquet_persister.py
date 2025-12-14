"""
Non-blocking Parquet persistence for trade data.

Runs a daemon thread that monitors a SwappableQueue and writes batches
to timestamped parquet files when the threshold is reached.
"""

import threading
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any, Optional

import pyarrow as pa
import pyarrow.parquet as pq

from swappable_queue import SwappableQueue


# =============================================================================
# TRADE SCHEMA DEFINITION
# =============================================================================
# TODO: Fill in the schema fields based on your trade data structure
# Example fields shown below - modify to match actual API response

TRADE_SCHEMA = pa.schema([
    # -------------------------------------------------------------------------
    # Define your schema fields here
    # -------------------------------------------------------------------------
    # ('timestamp', pa.int64()),
    ('proxyWallet', pa.string()),
    ('side' , pa.string()),
    ('price', pa.float64()),
    ('size', pa.float64()),
    ('conditionId', pa.string()),
    ('timestamp', pa.int64()),
    ('transactionHash', pa.string()),
    ('outcome', pa.string())
    # ('maker', pa.string()),
    # ('taker', pa.string()),
    # ('market', pa.string()),
    # ('asset_id', pa.string()),
    # ('trade_id', pa.string()),
    # -------------------------------------------------------------------------
])

MARKET_SCHEMA = pa.schema([
    ('condition_Id', pa.string()),
    ('end_date_iso', pa.string()),   
    ('game_start_time', pa.string()),
    ('description', pa.string()),
    ('maker_base_fee', pa.float64()),
    ('fpmm', pa.string()),
    ('question', pa.string()),
    ('closed', pa.bool_()),
    ('active', pa.bool_())
])

MARKET_TOKEN_SCHEMA = pa.schema([
    ('condition_Id', pa.string()),
    ('price', pa.float64()),
    ('token_id', pa.string()),
    ('winner', pa.bool())
])


class ParquetPersister:
    """
    Daemon thread that monitors a SwappableQueue and persists batches to parquet.
    
    When the queue reaches the threshold (default 10k items), atomically swaps
    out the buffer and writes it to a timestamped parquet file in a background
    thread. The queue remains available for workers during the write.
    
    Usage:
        queue = SwappableQueue(threshold=10000)
        persister = ParquetPersister(queue, output_dir="data/trades")
        persister.start()
        
        # ... workers add to queue ...
        
        persister.stop()  # Flushes remaining items
    """
    
    def __init__(
        self,
        queue: SwappableQueue,
        output_dir: str = "data/trades",
        poll_interval: float = 0.5,
        use_hive_partitioning: bool = True
    ):
        """
        Initialize the parquet persister.
        
        Args:
            queue: SwappableQueue to monitor
            output_dir: Base directory for parquet files
            poll_interval: Seconds between threshold checks (default 0.5s)
            use_hive_partitioning: If True, partition by date (dt=YYYY-MM-DD/)
        """
        self._queue = queue
        self._output_dir = Path(output_dir)
        self._poll_interval = poll_interval
        self._use_hive_partitioning = use_hive_partitioning
        
        # Create output directory
        self._output_dir.mkdir(parents=True, exist_ok=True)
        
        # Monitor thread
        self._monitor_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        
        # Stats
        self._files_written = 0
        self._total_records_written = 0
        self._lock = threading.Lock()
    
    def start(self) -> None:
        """Start the monitor thread."""
        if self._monitor_thread is not None and self._monitor_thread.is_alive():
            return
        
        self._stop_event.clear()
        self._monitor_thread = threading.Thread(
            target=self._monitor_loop,
            name="ParquetPersister-Monitor",
            daemon=True
        )
        self._monitor_thread.start()
        print(f"[ParquetPersister] Started monitoring queue (threshold={self._queue._threshold})")
    
    def stop(self, timeout: float = 30.0) -> None:
        """
        Stop the monitor thread and flush remaining items.
        
        Args:
            timeout: Maximum seconds to wait for thread to finish
        """
        print("[ParquetPersister] Stopping...")
        
        # Signal shutdown
        self._stop_event.set()
        self._queue.shutdown()
        
        # Wait for monitor thread
        if self._monitor_thread is not None:
            self._monitor_thread.join(timeout=timeout)
        
        # Flush any remaining items
        remaining = self._queue.drain()
        if remaining:
            print(f"[ParquetPersister] Flushing {len(remaining)} remaining items...")
            self._write_parquet(remaining)
        
        print(f"[ParquetPersister] Stopped. Total files: {self._files_written}, Total records: {self._total_records_written}")
    
    def _monitor_loop(self) -> None:
        """Main loop that monitors queue and triggers writes."""
        while not self._stop_event.is_set():
            # Wait for threshold or timeout
            triggered = self._queue.wait_for_threshold(timeout=self._poll_interval)
            
            if self._stop_event.is_set():
                break
            
            if triggered and not self._queue.is_shutdown:
                # Atomically swap out the buffer
                items = self._queue.swap_if_ready()
                
                if items:
                    # Spawn a write thread (non-blocking)
                    write_thread = threading.Thread(
                        target=self._write_parquet,
                        args=(items,),
                        name=f"ParquetPersister-Write-{self._files_written + 1}",
                        daemon=True
                    )
                    write_thread.start()
    
    def _write_trade_parquet(self, items: List[Dict[str, Any]]) -> None:
        """
        Write a batch of items to a parquet file.
        
        Args:
            items: List of trade dictionaries to write
        """
        if not items:
            return
        
        try:
            timestamp = datetime.now()
            
            # Build output path
            if self._use_hive_partitioning:
                date_partition = timestamp.strftime("dt=%Y-%m-%d")
                partition_dir = self._output_dir / date_partition
                partition_dir.mkdir(parents=True, exist_ok=True)
            else:
                partition_dir = self._output_dir
            
            filename = f"trades_{timestamp.strftime('%Y%m%d_%H%M%S_%f')}.parquet"
            filepath = partition_dir / filename
            
            # Convert to PyArrow table
            # If schema is defined, use it; otherwise infer from data
            if len(TRADE_SCHEMA) > 0:
                table = pa.Table.from_pylist(items, schema=TRADE_SCHEMA)
            else:
                # Infer schema from data (fallback)
                table = pa.Table.from_pylist(items)
            
            # Write parquet file
            pq.write_table(
                table,
                filepath,
                compression='snappy',
                use_dictionary=True,
                write_statistics=True
            )
            
            # Update stats
            with self._lock:
                self._files_written += 1
                self._total_records_written += len(items)
                file_num = self._files_written
            
            print(f"[ParquetPersister] Written {len(items)} records to {filepath.name} (file #{file_num})")
        
        except Exception as e:
            print(f"[ParquetPersister] Error writing parquet: {e}")
            # TODO: Consider retry logic or dead-letter queue
            raise
    def _write_market_token_parquet(self, items: List[Dict[str, Any]]) -> None:
        """
        Write a batch of items to a parquet file.
        
        Args:
            items: List of market token dictionaries to write
        """
        if not items:
            return
        
        try:
            timestamp = datetime.now()
            
            # Build output path
            if self._use_hive_partitioning:
                date_partition = timestamp.strftime("dt=%Y-%m-%d")
                partition_dir = self._output_dir / date_partition
                partition_dir.mkdir(parents=True, exist_ok=True)
            else:
                partition_dir = self._output_dir
            
            filename = f"market_tokens_{timestamp.strftime('%Y%m%d_%H%M%S_%f')}.parquet"
            filepath = partition_dir / filename
            
            # Convert to PyArrow table
            # If schema is defined, use it; otherwise infer from data
            if len(MARKET_TOKEN_SCHEMA) > 0:
                table = pa.Table.from_pylist(items, schema=MARKET_TOKEN_SCHEMA)
            else:
                # Infer schema from data (fallback)
                table = pa.Table.from_pylist(items)
            
            # Write parquet file
            pq.write_table(
                table,
                filepath,
                compression='snappy',
                use_dictionary=True,
                write_statistics=True
            )
            
            # Update stats
            with self._lock:
                self._files_written += 1
                self._total_records_written += len(items)
                file_num = self._files_written
            
            print(f"[ParquetPersister] Written {len(items)} records to {filepath.name} (file #{file_num})")
        
        except Exception as e:
            print(f"[ParquetPersister] Error writing parquet: {e}")
    def _write_market_parquet(self, items: List[Dict[str, Any]]) -> None:
        """
        Write a batch of items to a parquet file.
        
        Args:
            items: List of market dictionaries to write
        """
        if not items:
            return
        
        try:
            timestamp = datetime.now()
            
            # Build output path
            if self._use_hive_partitioning:
                date_partition = timestamp.strftime("dt=%Y-%m-%d")
                partition_dir = self._output_dir / date_partition
                partition_dir.mkdir(parents=True, exist_ok=True)
            else:
                partition_dir = self._output_dir
            
            filename = f"markets_{timestamp.strftime('%Y%m%d_%H%M%S_%f')}.parquet"
            filepath = partition_dir / filename
            
            # Convert to PyArrow table
            # If schema is defined, use it; otherwise infer from data
            if len(MARKET_SCHEMA) > 0:
                table = pa.Table.from_pylist(items, schema=MARKET_SCHEMA)
            else:
                # Infer schema from data (fallback)
                table = pa.Table.from_pylist(items)
            
            # Write parquet file
            pq.write_table(
                table,
                filepath,
                compression='snappy',
                use_dictionary=True,
                write_statistics=True
            )
            
            # Update stats
            with self._lock:
                self._files_written += 1
                self._total_records_written += len(items)
                file_num = self._files_written
            
            print(f"[ParquetPersister] Written {len(items)} records to {filepath.name} (file #{file_num})")
        
        except Exception as e:
            print(f"[ParquetPersister] Error writing parquet: {e}")
    @property
    def stats(self) -> Dict[str, int]:
        """Return current write statistics."""
        with self._lock:
            return {
                "files_written": self._files_written,
                "total_records_written": self._total_records_written,
                "queue_size": self._queue.size()
            }


# =============================================================================
# Convenience function for integration
# =============================================================================

def create_trade_persisted_queue(
    threshold: int = 10000,
    output_dir: str = "data/trades",
    auto_start: bool = True
) -> tuple[SwappableQueue, ParquetPersister]:
    """
    Create a queue with attached parquet persister.
    
    Args:
        threshold: Number of items that triggers a write
        output_dir: Directory for parquet files
        auto_start: Start the persister immediately
    
    Returns:
        Tuple of (queue, persister)
    
    Example:
        queue, persister = create_persisted_queue(threshold=10000)
        
        # Workers add trades
        for trade in trades:
            queue.put(trade)
        
        # When done
        persister.stop()
    """
    queue = SwappableQueue(threshold=threshold)
    persister = ParquetPersister(queue, output_dir=output_dir)
    
    if auto_start:
        persister.start()
    
    return queue, persister
