"""
Non-blocking Parquet persistence for trade data.

Runs a daemon thread that monitors a SwappableQueue and writes batches
to timestamped parquet files when the threshold is reached.
Supports cursor persistence to track last run state.
"""

import json
import threading
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any, Optional
from queue import Queue, Empty

import pyarrow as pa
import pyarrow.parquet as pq
import duckdb

from fetcher.persistence.swappable_queue import SwappableQueue
from enum import Enum
from fetcher.utils.logging_config import get_logger
from fetcher.utils.exceptions import ParquetWriteError, CursorError

logger = get_logger("parquet_persister")

class DataType(Enum):
    """Supported data types for parquet persistence."""
    TRADE = "trade"
    MARKET = "market"
    MARKET_TOKEN = "market_token"
    PRICE = "price"
    LEADERBOARD = "leaderboard"
    GAMMA_MARKET = "gamma_market"
    GAMMA_EVENT = "gamma_event"
    GAMMA_CATEGORY = "gamma_category"


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
    ('outcome', pa.string()),
    ('name', pa.string()),
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
    ('question', pa.string()),
    ('maker_base_fee', pa.float64()),
    ('fpmm', pa.string()),
    ('question', pa.string()),
    ('closed', pa.bool_()),
    ('active', pa.bool_()),
    ('volume', pa.float64()),
    ('liquidity', pa.float64()),
    ('active', pa.bool8())

])

GAMMA_MARKET_SCHEMA = pa.schema([
    ('id', pa.string()),
    ('conditionId', pa.string()),
    ('question', pa.string()),
    ('slug', pa.string()),
    ('category', pa.string()),
    ('description', pa.string()),
    ('liquidity', pa.string()),
    ('volume', pa.string()),
    ('active', pa.bool_()),
    ('closed', pa.bool_()),
    ('startDate', pa.string()),
    ('endDate', pa.string()),
    ('outcomes', pa.string()),
    ('outcomePrices', pa.string()),
    ('clobTokenIds', pa.string()),
    ('volumeNum', pa.float64()),
    ('liquidityNum', pa.float64()),
    ('marketGroup', pa.int64()),
])

GAMMA_EVENT_SCHEMA = pa.schema([
    ('marketId', pa.string()),  # FK to gamma market
    ('eventId', pa.string()),
    ('ticker', pa.string()),
    ('slug', pa.string()),
    ('title', pa.string()),
    ('description', pa.string()),
    ('category', pa.string()),
    ('subcategory', pa.string()),
    ('liquidity', pa.float64()),
    ('volume', pa.float64()),
    ('active', pa.bool_()),
    ('closed', pa.bool_()),
])

GAMMA_CATEGORY_SCHEMA = pa.schema([
    ('marketId', pa.string()),  # FK to gamma market
    ('categoryId', pa.string()),
    ('label', pa.string()),
    ('parentCategory', pa.string()),
    ('slug', pa.string()),
])

MARKET_TOKEN_SCHEMA = pa.schema([
    ('condition_Id', pa.string()),
    ('price', pa.float64()),
    ('token_id', pa.string()),
    ('winner', pa.bool_()),
    ('outcome', pa.string())
])

PRICE_SCHEMA = pa.schema([
    ('timestamp', pa.int64()),
    ('token_id', pa.string()),
    ('price', pa.float64())
])

LEADERBOARD_SCHEMA = pa.schema([
    ('rank', pa.string()),
    ('proxyWallet', pa.string()),
    ('userName', pa.string()),
    ('xUsername', pa.string()),
    ('verifiedBadge', pa.bool_()),
    ('vol', pa.float64()),
    ('pnl', pa.float64()),
    ('profileImage', pa.string())
])


# =============================================================================
# CURSOR MANAGEMENT
# =============================================================================

def load_cursor(output_dir: str, cursor_filename: str = "cursor.json") -> Optional[Dict[str, Any]]:
    """
    Load cursor from a JSON file in the output directory.
    
    Args:
        output_dir: Directory containing the cursor file
        cursor_filename: Name of the cursor file
    
    Returns:
        Dictionary with cursor data, or None if not found
    """
    cursor_path = Path(output_dir) / cursor_filename
    if cursor_path.exists():
        try:
            with open(cursor_path, 'r') as f:
                cursor = json.load(f)
            logger.info(f"Loaded cursor from {cursor_path}")
            return cursor
        except Exception as e:
            logger.error(f"Error loading cursor from {cursor_path}: {e}")
            return None
    return None


def save_cursor(
    output_dir: str,
    cursor_data: Dict[str, Any],
    cursor_filename: str = "cursor.json"
) -> None:
    """
    Save cursor to a JSON file in the output directory.
    
    Args:
        output_dir: Directory to save the cursor file
        cursor_data: Dictionary with cursor data (e.g., last_timestamp, last_offset)
        cursor_filename: Name of the cursor file
    """
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    cursor_path = output_path / cursor_filename
    
    # Add metadata
    cursor_data["updated_at"] = datetime.now().isoformat()
    
    try:
        with open(cursor_path, 'w') as f:
            json.dump(cursor_data, f, indent=2)
        logger.info(f"Saved cursor to {cursor_path}")
    except Exception as e:
        logger.error(f"Error saving cursor to {cursor_path}: {e}")
        raise CursorError(f"Failed to save cursor: {e}", cursor_path=str(cursor_path))


# =============================================================================
# PARQUET READING
# =============================================================================

def load_market_parquet(
    parquet_path: str,
    columns: Optional[List[str]] = None
) -> List[str]:
    """
    Load market IDs (condition_Id) from market parquet files.
    
    Args:
        parquet_path: Path to market parquet directory or file
        columns: Optional list of columns to load (default: condition_Id only)
    
    Returns:
        List of market condition IDs
    """
    path = Path(parquet_path)
    
    if not path.exists():
        logger.warning(f"Path not found: {parquet_path}")
        return []
    
    try:
        # Use DuckDB for efficient parquet reading with glob support
        conn = duckdb.connect(":memory:")
        
        if path.is_dir():
            # Support Hive partitioning
            glob_pattern = str(path / "**" / "*.parquet")
            query = f"""
                SELECT DISTINCT condition_Id 
                FROM read_parquet('{glob_pattern}', hive_partitioning=true)
                WHERE condition_Id IS NOT NULL
            """
        else:
            query = f"""
                SELECT DISTINCT condition_Id 
                FROM read_parquet('{path}')
                WHERE condition_Id IS NOT NULL
            """
        
        result = conn.execute(query).fetchall()
        conn.close()
        
        market_ids = [row[0] for row in result]
        logger.info(f"Loaded {len(market_ids)} market IDs from {parquet_path}")
        return market_ids
        
    except Exception as e:
        logger.error(f"Error loading markets from {parquet_path}: {e}")
        return []


def load_parquet_data(
    parquet_path: str,
    query: Optional[str] = None,
    columns: Optional[List[str]] = None
) -> List[Dict[str, Any]]:
    """
    Load data from parquet files with optional query.
    
    Args:
        parquet_path: Path to parquet directory or file
        query: Optional SQL query (use 'data' as table name)
        columns: Optional list of columns to load
    
    Returns:
        List of dictionaries with the data
    """
    path = Path(parquet_path)
    
    if not path.exists():
        logger.warning(f"Path not found: {parquet_path}")
        return []
    
    try:
        conn = duckdb.connect(":memory:")
        
        if path.is_dir():
            glob_pattern = str(path / "**" / "*.parquet")
            conn.execute(f"""
                CREATE VIEW data AS 
                SELECT * FROM read_parquet('{glob_pattern}', hive_partitioning=true)
            """)
        else:
            conn.execute(f"""
                CREATE VIEW data AS 
                SELECT * FROM read_parquet('{path}')
            """)
        
        if query:
            result = conn.execute(query).fetchdf()
        elif columns:
            cols = ", ".join(columns)
            result = conn.execute(f"SELECT {cols} FROM data").fetchdf()
        else:
            result = conn.execute("SELECT * FROM data").fetchdf()
        
        conn.close()
        records: List[Dict[str, Any]] = [{str(k): v for k, v in record.items()} for record in result.to_dict('records')]
        return records
        
    except Exception as e:
        logger.error(f"Error loading data from {parquet_path}: {e}")
        return []


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
        use_hive_partitioning: bool = True,
        data_type: DataType = DataType.TRADE
    ):
        """
        Initialize the parquet persister.
        
        Args:
            queue: SwappableQueue to monitor
            output_dir: Base directory for parquet files
            poll_interval: Seconds between threshold checks (default 0.5s)
            use_hive_partitioning: If True, partition by date (dt=YYYY-MM-DD/)
            data_type: Type of data being persisted (TRADE, MARKET, MARKET_TOKEN)
        """
        self._queue = queue
        self._output_dir = Path(output_dir)
        self._poll_interval = poll_interval
        self._use_hive_partitioning = use_hive_partitioning
        self._data_type = data_type
        
        # Create output directory
        self._output_dir.mkdir(parents=True, exist_ok=True)
        
        # Monitor thread (watches data queue for threshold)
        self._monitor_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        
        # Dedicated writer thread with its own write queue
        self._write_queue: Queue = Queue()
        self._writer_thread: Optional[threading.Thread] = None
        
        # Stats
        self._files_written = 0
        self._total_records_written = 0
        self._lock = threading.Lock()
    
    def start(self) -> None:
        """Start the monitor thread and dedicated writer thread."""
        if self._monitor_thread is not None and self._monitor_thread.is_alive():
            return
        
        self._stop_event.clear()
        
        # Start dedicated writer thread
        self._writer_thread = threading.Thread(
            target=self._writer_loop,
            name=f"ParquetPersister-Writer-{self._data_type.value}",
            daemon=True
        )
        self._writer_thread.start()
        
        # Start monitor thread
        self._monitor_thread = threading.Thread(
            target=self._monitor_loop,
            name=f"ParquetPersister-Monitor-{self._data_type.value}",
            daemon=True
        )
        self._monitor_thread.start()
        logger.info(
            f"Started monitoring queue for {self._data_type.value}",
            extra={"data_type": self._data_type.value, "threshold": self._queue._threshold}
        )
    
    def stop(self, timeout: float = 30.0) -> None:
        """
        Stop the monitor and writer threads, flush remaining items.
        
        Args:
            timeout: Maximum seconds to wait for threads to finish
        """
        logger.info(f"Stopping ParquetPersister for {self._data_type.value}...")
        
        # Signal shutdown
        self._stop_event.set()
        self._queue.shutdown()
        
        # Wait for monitor thread
        if self._monitor_thread is not None:
            self._monitor_thread.join(timeout=timeout)
        
        # Flush any remaining items from data queue to write queue
        remaining = self._queue.drain()
        if remaining:
            logger.info(
                f"Flushing {len(remaining)} remaining items for {self._data_type.value}",
                extra={"remaining_count": len(remaining)}
            )
            self._write_queue.put(remaining)
        
        # Send sentinel to stop writer thread
        self._write_queue.put(None)
        
        # Wait for writer thread to finish processing
        if self._writer_thread is not None:
            self._writer_thread.join(timeout=timeout)
        
        logger.info(
            f"Stopped ParquetPersister for {self._data_type.value}",
            extra={
                "files_written": self._files_written,
                "total_records": self._total_records_written
            }
        )
    
    def _monitor_loop(self) -> None:
        """Main loop that monitors data queue and sends batches to writer."""
        while not self._stop_event.is_set():
            # Wait for threshold or timeout
            triggered = self._queue.wait_for_threshold(timeout=self._poll_interval)
            
            if self._stop_event.is_set():
                break
            
            if triggered and not self._queue.is_shutdown:
                # Atomically swap out the buffer
                items = self._queue.swap_if_ready()
                
                if items:
                    # Send to dedicated writer thread via write queue
                    self._write_queue.put(items)
    
    def _writer_loop(self) -> None:
        """Dedicated writer thread that processes write requests from write queue."""
        while True:
            try:
                # Block waiting for items to write
                items = self._write_queue.get(timeout=1.0)
                
                if items is None:
                    # Sentinel received, exit loop
                    break
                
                # Perform the write
                self._write_parquet(items)
                self._write_queue.task_done()
                
            except Empty:
                # Timeout, check if we should stop
                if self._stop_event.is_set() and self._write_queue.empty():
                    break
                continue
            except Exception as e:
                logger.exception(
                    f"Writer error for {self._data_type.value}: {e}",
                    extra={"data_type": self._data_type.value}
                )
                continue

    def _write_parquet(self, items: List[Dict[str, Any]]) -> None:
        """
        Dispatch to the appropriate write method based on data_type.
        
        Args:
            items: List of dictionaries to write
        """
        if self._data_type == DataType.TRADE:
            self._write_trade_parquet(items)
        elif self._data_type == DataType.MARKET:
            self._write_market_parquet(items)
        elif self._data_type == DataType.MARKET_TOKEN:
            self._write_market_token_parquet(items)
        elif self._data_type == DataType.PRICE:
            self._write_price_parquet(items)
        elif self._data_type == DataType.LEADERBOARD:
            self._write_leaderboard_parquet(items)
        elif self._data_type == DataType.GAMMA_MARKET:
            self._write_gamma_market_parquet(items)
        elif self._data_type == DataType.GAMMA_EVENT:
            self._write_gamma_event_parquet(items)
        elif self._data_type == DataType.GAMMA_CATEGORY:
            self._write_gamma_category_parquet(items)
        else:
            raise ValueError(f"Unknown data type: {self._data_type}")

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
            
            logger.info(
                f"Written {len(items)} trade records to {filepath.name}",
                extra={"file_num": file_num, "record_count": len(items), "filepath": str(filepath)}
            )
        
        except Exception as e:
            logger.exception(
                f"Error writing trade parquet: {e}",
                extra={"filepath": str(filepath) if 'filepath' in locals() else None}
            )
            # Consider retry logic or dead-letter queue
            raise ParquetWriteError(f"Failed to write trades: {e}", file_path=str(filepath) if 'filepath' in locals() else None)
    
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
            
            logger.info(
                f"Written {len(items)} market_token records to {filepath.name}",
                extra={"file_num": file_num, "record_count": len(items), "filepath": str(filepath)}
            )
        
        except Exception as e:
            logger.exception(
                f"Error writing market_token parquet: {e}",
                extra={"filepath": str(filepath) if 'filepath' in locals() else None}
            )
    
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
            
            logger.info(
                f"Written {len(items)} market records to {filepath.name}",
                extra={"file_num": file_num, "record_count": len(items), "filepath": str(filepath)}
            )
        
        except Exception as e:
            logger.exception(
                f"Error writing market parquet: {e}",
                extra={"filepath": str(filepath) if 'filepath' in locals() else None}
            )

    def _write_price_parquet(self, items: List[Dict[str, Any]]) -> None:
        """
        Write a batch of price history items to a parquet file.
        
        Args:
            items: List of price dictionaries to write
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
            
            filename = f"prices_{timestamp.strftime('%Y%m%d_%H%M%S_%f')}.parquet"
            filepath = partition_dir / filename
            
            # Convert to PyArrow table
            if len(PRICE_SCHEMA) > 0:
                table = pa.Table.from_pylist(items, schema=PRICE_SCHEMA)
            else:
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
            
            logger.info(
                f"Written {len(items)} price records to {filepath.name}",
                extra={"file_num": file_num, "record_count": len(items), "filepath": str(filepath)}
            )
        
        except Exception as e:
            logger.exception(
                f"Error writing price parquet: {e}",
                extra={"filepath": str(filepath) if 'filepath' in locals() else None}
            )

    def _write_leaderboard_parquet(self, items: List[Dict[str, Any]]) -> None:
        """
        Write a batch of leaderboard items to a parquet file.
        
        Args:
            items: List of leaderboard dictionaries to write
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
            
            filename = f"leaderboard_{timestamp.strftime('%Y%m%d_%H%M%S_%f')}.parquet"
            filepath = partition_dir / filename
            
            # Convert to PyArrow table
            if len(LEADERBOARD_SCHEMA) > 0:
                table = pa.Table.from_pylist(items, schema=LEADERBOARD_SCHEMA)
            else:
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
            
            logger.info(
                f"Written {len(items)} leaderboard records to {filepath.name}",
                extra={"file_num": file_num, "record_count": len(items), "filepath": str(filepath)}
            )
        
        except Exception as e:
            logger.exception(
                f"Error writing leaderboard parquet: {e}",
                extra={"filepath": str(filepath) if 'filepath' in locals() else None}
            )

    def _write_gamma_market_parquet(self, items: List[Dict[str, Any]]) -> None:
        """
        Write a batch of Gamma market items to a parquet file.
        
        Args:
            items: List of Gamma market dictionaries to write
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
            
            filename = f"gamma_markets_{timestamp.strftime('%Y%m%d_%H%M%S_%f')}.parquet"
            filepath = partition_dir / filename
            
            # Convert to PyArrow table
            if len(GAMMA_MARKET_SCHEMA) > 0:
                table = pa.Table.from_pylist(items, schema=GAMMA_MARKET_SCHEMA)
            else:
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
            
            logger.info(
                f"Written {len(items)} gamma_market records to {filepath.name}",
                extra={"file_num": file_num, "record_count": len(items), "filepath": str(filepath)}
            )
        
        except Exception as e:
            logger.exception(
                f"Error writing gamma_market parquet: {e}",
                extra={"filepath": str(filepath) if 'filepath' in locals() else None}
            )

    def _write_gamma_event_parquet(self, items: List[Dict[str, Any]]) -> None:
        """
        Write a batch of Gamma event items to a parquet file.
        
        Args:
            items: List of Gamma event dictionaries to write
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
            
            filename = f"gamma_events_{timestamp.strftime('%Y%m%d_%H%M%S_%f')}.parquet"
            filepath = partition_dir / filename
            
            # Convert to PyArrow table
            if len(GAMMA_EVENT_SCHEMA) > 0:
                table = pa.Table.from_pylist(items, schema=GAMMA_EVENT_SCHEMA)
            else:
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
            
            logger.info(
                f"Written {len(items)} gamma_event records to {filepath.name}",
                extra={"file_num": file_num, "record_count": len(items), "filepath": str(filepath)}
            )
        
        except Exception as e:
            logger.exception(
                f"Error writing gamma_event parquet: {e}",
                extra={"filepath": str(filepath) if 'filepath' in locals() else None}
            )

    def _write_gamma_category_parquet(self, items: List[Dict[str, Any]]) -> None:
        """
        Write a batch of Gamma category items to a parquet file.
        
        Args:
            items: List of Gamma category dictionaries to write
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
            
            filename = f"gamma_categories_{timestamp.strftime('%Y%m%d_%H%M%S_%f')}.parquet"
            filepath = partition_dir / filename
            
            # Convert to PyArrow table
            if len(GAMMA_CATEGORY_SCHEMA) > 0:
                table = pa.Table.from_pylist(items, schema=GAMMA_CATEGORY_SCHEMA)
            else:
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
            
            logger.info(
                f"Written {len(items)} gamma_category records to {filepath.name}",
                extra={"file_num": file_num, "record_count": len(items), "filepath": str(filepath)}
            )
        
        except Exception as e:
            logger.exception(
                f"Error writing gamma_category parquet: {e}",
                extra={"filepath": str(filepath) if 'filepath' in locals() else None}
            )

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
# Convenience functions for integration
# =============================================================================

def create_persisted_queue(
    threshold: int = 10000,
    output_dir: str = "data",
    data_type: DataType = DataType.TRADE,
    auto_start: bool = True
) -> tuple[SwappableQueue, ParquetPersister]:
    """
    Create a queue with attached parquet persister for any data type.
    
    Args:
        threshold: Number of items that triggers a write
        output_dir: Directory for parquet files
        data_type: Type of data (DataType.TRADE, DataType.MARKET, DataType.MARKET_TOKEN)
        auto_start: Start the persister immediately
    
    Returns:
        Tuple of (queue, persister)
    
    Example:
        # For trades
        queue, persister = create_persisted_queue(
            threshold=10000,
            output_dir="data/trades",
            data_type=DataType.TRADE
        )
        
        # For markets
        queue, persister = create_persisted_queue(
            threshold=1000,
            output_dir="data/markets",
            data_type=DataType.MARKET
        )
        
        # Workers add items
        queue.put(item)
        
        # When done
        persister.stop()
    """
    queue = SwappableQueue(threshold=threshold)
    persister = ParquetPersister(queue, output_dir=output_dir, data_type=data_type)
    
    if auto_start:
        persister.start()
    
    return queue, persister


def create_trade_persisted_queue(
    threshold: int = 10000,
    output_dir: str = "data/trades",
    auto_start: bool = True
) -> tuple[SwappableQueue, ParquetPersister]:
    """
    Create a queue with attached parquet persister for trade data.
    Convenience wrapper around create_persisted_queue.
    
    Args:
        threshold: Number of items that triggers a write
        output_dir: Directory for parquet files
        auto_start: Start the persister immediately
    
    Returns:
        Tuple of (queue, persister)
    """
    return create_persisted_queue(
        threshold=threshold,
        output_dir=output_dir,
        data_type=DataType.TRADE,
        auto_start=auto_start
    )


def create_market_persisted_queue(
    threshold: int = 1000,
    output_dir: str = "data/markets",
    auto_start: bool = True
) -> tuple[SwappableQueue, ParquetPersister]:
    """
    Create a queue with attached parquet persister for market data.
    Convenience wrapper around create_persisted_queue.
    
    Args:
        threshold: Number of items that triggers a write
        output_dir: Directory for parquet files
        auto_start: Start the persister immediately
    
    Returns:
        Tuple of (queue, persister)
    """
    return create_persisted_queue(
        threshold=threshold,
        output_dir=output_dir,
        data_type=DataType.MARKET,
        auto_start=auto_start
    )


def create_market_token_persisted_queue(
    threshold: int = 5000,
    output_dir: str = "data/market_tokens",
    auto_start: bool = True
) -> tuple[SwappableQueue, ParquetPersister]:
    """
    Create a queue with attached parquet persister for market token data.
    Convenience wrapper around create_persisted_queue.
    
    Args:
        threshold: Number of items that triggers a write
        output_dir: Directory for parquet files
        auto_start: Start the persister immediately
    
    Returns:
        Tuple of (queue, persister)
    """
    return create_persisted_queue(
        threshold=threshold,
        output_dir=output_dir,
        data_type=DataType.MARKET_TOKEN,
        auto_start=auto_start
    )


def create_price_persisted_queue(
    threshold: int = 10000,
    output_dir: str = "data/prices",
    auto_start: bool = True
) -> tuple[SwappableQueue, ParquetPersister]:
    """
    Create a queue with attached parquet persister for price data.
    Convenience wrapper around create_persisted_queue.
    
    Args:
        threshold: Number of items that triggers a write
        output_dir: Directory for parquet files
        auto_start: Start the persister immediately
    
    Returns:
        Tuple of (queue, persister)
    """
    return create_persisted_queue(
        threshold=threshold,
        output_dir=output_dir,
        data_type=DataType.PRICE,
        auto_start=auto_start
    )


def create_leaderboard_persisted_queue(
    threshold: int = 5000,
    output_dir: str = "data/leaderboard",
    auto_start: bool = True
) -> tuple[SwappableQueue, ParquetPersister]:
    """
    Create a queue with attached parquet persister for leaderboard data.
    Convenience wrapper around create_persisted_queue.
    
    Args:
        threshold: Number of items that triggers a write
        output_dir: Directory for parquet files
        auto_start: Start the persister immediately
    
    Returns:
        Tuple of (queue, persister)
    """
    return create_persisted_queue(
        threshold=threshold,
        output_dir=output_dir,
        data_type=DataType.LEADERBOARD,
        auto_start=auto_start
    )


def create_gamma_market_persisted_queue(
    threshold: int = 1000,
    output_dir: str = "data/gamma_markets",
    auto_start: bool = True
) -> tuple[SwappableQueue, ParquetPersister]:
    """
    Create a queue with attached parquet persister for Gamma market data.
    Convenience wrapper around create_persisted_queue.
    
    Args:
        threshold: Number of items that triggers a write
        output_dir: Directory for parquet files
        auto_start: Start the persister immediately
    
    Returns:
        Tuple of (queue, persister)
    """
    return create_persisted_queue(
        threshold=threshold,
        output_dir=output_dir,
        data_type=DataType.GAMMA_MARKET,
        auto_start=auto_start
    )


def create_gamma_event_persisted_queue(
    threshold: int = 1000,
    output_dir: str = "data/gamma_events",
    auto_start: bool = True
) -> tuple[SwappableQueue, ParquetPersister]:
    """
    Create a queue with attached parquet persister for Gamma event data.
    Convenience wrapper around create_persisted_queue.
    
    Args:
        threshold: Number of items that triggers a write
        output_dir: Directory for parquet files
        auto_start: Start the persister immediately
    
    Returns:
        Tuple of (queue, persister)
    """
    return create_persisted_queue(
        threshold=threshold,
        output_dir=output_dir,
        data_type=DataType.GAMMA_EVENT,
        auto_start=auto_start
    )


def create_gamma_category_persisted_queue(
    threshold: int = 1000,
    output_dir: str = "data/gamma_categories",
    auto_start: bool = True
) -> tuple[SwappableQueue, ParquetPersister]:
    """
    Create a queue with attached parquet persister for Gamma category data.
    Convenience wrapper around create_persisted_queue.
    
    Args:
        threshold: Number of items that triggers a write
        output_dir: Directory for parquet files
        auto_start: Start the persister immediately
    
    Returns:
        Tuple of (queue, persister)
    """
    return create_persisted_queue(
        threshold=threshold,
        output_dir=output_dir,
        data_type=DataType.GAMMA_CATEGORY,
        auto_start=auto_start
    )
