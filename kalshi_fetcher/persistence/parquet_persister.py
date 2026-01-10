"""
Non-blocking Parquet persistence for Kalshi data.

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

from kalshi_fetcher.persistence.swappable_queue import SwappableQueue
from enum import Enum
from kalshi_fetcher.utils.logging_config import get_logger
from kalshi_fetcher.utils.exceptions import ParquetWriteError, CursorError

logger = get_logger("parquet_persister")


class DataType(Enum):
    """Supported data types for parquet persistence."""
    EVENT = "event"
    MARKET = "market"
    TRADE = "trade"
    ORDERBOOK = "orderbook"


# =============================================================================
# KALSHI SCHEMA DEFINITIONS
# =============================================================================

# Event Schema - Top level grouping in Kalshi
# Events contain multiple markets (contracts)
EVENT_SCHEMA = pa.schema([
    ('event_ticker', pa.string()),          # Unique event identifier (e.g., "KXBTC-25JAN10")
    ('series_ticker', pa.string()),         # Series this event belongs to
    ('title', pa.string()),                 # Human readable title
    ('sub_title', pa.string()),             # Subtitle/description
    ('category', pa.string()),              # Category (e.g., "Crypto", "Politics")
    ('mutually_exclusive', pa.bool_()),     # Whether markets are mutually exclusive
    ('strike_date', pa.string()),           # Settlement date
    ('strike_period', pa.string()),         # Settlement period type
    ('status', pa.string()),                # Event status (active, settled, etc.)
])

# Market Schema - Individual contracts within events
# Each market represents a specific outcome/strike price
MARKET_SCHEMA = pa.schema([
    ('ticker', pa.string()),                # Unique market ticker (e.g., "KXBTC-25JAN10-T95000")
    ('event_ticker', pa.string()),          # Parent event ticker (FK)
    ('title', pa.string()),                 # Market title/question
    ('subtitle', pa.string()),              # Additional description
    ('status', pa.string()),                # Market status (open, closed, settled)
    ('yes_bid', pa.float64()),              # Best yes bid price (0-100 cents)
    ('yes_ask', pa.float64()),              # Best yes ask price (0-100 cents)
    ('no_bid', pa.float64()),               # Best no bid price
    ('no_ask', pa.float64()),               # Best no ask price
    ('last_price', pa.float64()),           # Last traded price
    ('volume', pa.int64()),                 # Total volume traded (contracts)
    ('volume_24h', pa.int64()),             # 24h volume
    ('open_interest', pa.int64()),          # Open interest (contracts)
    ('open_time', pa.string()),             # When market opened
    ('close_time', pa.string()),            # When market closes
    ('expiration_time', pa.string()),       # Expiration timestamp
    ('result', pa.string()),                # Settlement result (yes/no/null)
    ('strike_type', pa.string()),           # Type of strike (greater, less, between)
    ('floor_strike', pa.float64()),         # Floor strike value
    ('cap_strike', pa.float64()),           # Cap strike value
])

# Trade Schema - Individual trades on markets
TRADE_SCHEMA = pa.schema([
    ('trade_id', pa.string()),              # Unique trade identifier
    ('ticker', pa.string()),                # Market ticker (FK)
    ('side', pa.string()),                  # 'yes' or 'no'
    ('count', pa.int64()),                  # Number of contracts
    ('price', pa.float64()),                # Price in cents (0-100)
    ('taker_side', pa.string()),            # Which side was taker
    ('created_time', pa.string()),          # Trade timestamp ISO
    ('created_time_ts', pa.int64()),        # Trade timestamp Unix ms
])

# Orderbook Snapshot Schema
ORDERBOOK_SCHEMA = pa.schema([
    ('ticker', pa.string()),                # Market ticker
    ('snapshot_time', pa.int64()),          # Snapshot timestamp
    ('side', pa.string()),                  # 'yes' or 'no'
    ('price', pa.float64()),                # Price level
    ('quantity', pa.int64()),               # Quantity at price
])


# =============================================================================
# SCHEMA MAPPING
# =============================================================================

SCHEMA_MAP = {
    DataType.EVENT: EVENT_SCHEMA,
    DataType.MARKET: MARKET_SCHEMA,
    DataType.TRADE: TRADE_SCHEMA,
    DataType.ORDERBOOK: ORDERBOOK_SCHEMA,
}


# =============================================================================
# PARQUET PERSISTER
# =============================================================================

class ParquetPersister:
    """
    Daemon thread that monitors a SwappableQueue and writes batches to parquet.
    """
    
    def __init__(
        self,
        queue: SwappableQueue,
        output_dir: str,
        data_type: DataType,
        check_interval: float = 1.0,
    ):
        """
        Initialize the parquet persister.
        
        Args:
            queue: SwappableQueue to monitor for items
            output_dir: Base directory for output files
            data_type: Type of data being persisted
            check_interval: How often to check queue (seconds)
        """
        self._queue = queue
        self._output_dir = Path(output_dir)
        self._data_type = data_type
        self._schema = SCHEMA_MAP[data_type]
        self._check_interval = check_interval
        
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._items_written = 0
        self._files_written = 0
    
    def start(self) -> None:
        """Start the persister daemon thread."""
        if self._thread is not None and self._thread.is_alive():
            return
        
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run,
            daemon=True,
            name=f"ParquetPersister-{self._data_type.value}"
        )
        self._thread.start()
        logger.info(f"Started {self._data_type.value} persister")
    
    def stop(self, flush: bool = True) -> None:
        """
        Stop the persister thread.
        
        Args:
            flush: If True, write any remaining items before stopping
        """
        self._stop_event.set()
        
        if flush:
            # Write any remaining items
            remaining = self._queue.swap()
            if remaining:
                self._write_batch(remaining)
        
        if self._thread is not None:
            self._thread.join(timeout=5.0)
        
        logger.info(
            f"Stopped {self._data_type.value} persister. "
            f"Total: {self._items_written} items in {self._files_written} files"
        )
    
    def _run(self) -> None:
        """Main loop for the persister thread."""
        while not self._stop_event.is_set():
            # Wait for threshold or timeout
            if self._queue.wait_for_threshold(timeout=self._check_interval):
                if self._queue.is_shutdown():
                    break
                
                # Swap and write
                items = self._queue.swap()
                if items:
                    self._write_batch(items)
    
    def _write_batch(self, items: List[Dict[str, Any]]) -> None:
        """
        Write a batch of items to a parquet file.
        
        Args:
            items: List of dictionaries to write
        """
        if not items:
            return
        
        # Create date partition directory
        today = datetime.now().strftime("%Y-%m-%d")
        partition_dir = self._output_dir / f"dt={today}"
        partition_dir.mkdir(parents=True, exist_ok=True)
        
        # Generate filename with timestamp
        timestamp = datetime.now().strftime("%H%M%S_%f")
        filename = f"{self._data_type.value}_{timestamp}.parquet"
        filepath = partition_dir / filename
        
        try:
            # Convert to PyArrow table
            table = self._items_to_table(items)
            
            # Write parquet
            pq.write_table(table, filepath, compression='snappy')
            
            self._items_written += len(items)
            self._files_written += 1
            
            logger.info(
                f"Wrote {len(items)} {self._data_type.value} items to {filepath}"
            )
            
        except Exception as e:
            logger.error(f"Failed to write parquet: {e}")
            raise ParquetWriteError(str(filepath), str(e))
    
    def _items_to_table(self, items: List[Dict[str, Any]]) -> pa.Table:
        """
        Convert list of dictionaries to PyArrow table using schema.
        
        Args:
            items: List of dictionaries
        
        Returns:
            PyArrow Table
        """
        # Build columns from schema
        columns = {}
        for field in self._schema:
            field_name = field.name
            values = [item.get(field_name) for item in items]
            columns[field_name] = values
        
        return pa.table(columns, schema=self._schema)
    
    @property
    def items_written(self) -> int:
        """Total number of items written."""
        return self._items_written
    
    @property
    def files_written(self) -> int:
        """Total number of files written."""
        return self._files_written


# =============================================================================
# FACTORY FUNCTIONS
# =============================================================================

def create_event_persisted_queue(
    output_dir: str = "data/kalshi/events",
    threshold: int = 1000
) -> tuple[SwappableQueue, ParquetPersister]:
    """Create event queue with attached persister."""
    queue = SwappableQueue(threshold=threshold)
    persister = ParquetPersister(queue, output_dir, DataType.EVENT)
    return queue, persister


def create_market_persisted_queue(
    output_dir: str = "data/kalshi/markets",
    threshold: int = 5000
) -> tuple[SwappableQueue, ParquetPersister]:
    """Create market queue with attached persister."""
    queue = SwappableQueue(threshold=threshold)
    persister = ParquetPersister(queue, output_dir, DataType.MARKET)
    return queue, persister


def create_trade_persisted_queue(
    output_dir: str = "data/kalshi/trades",
    threshold: int = 10000
) -> tuple[SwappableQueue, ParquetPersister]:
    """Create trade queue with attached persister."""
    queue = SwappableQueue(threshold=threshold)
    persister = ParquetPersister(queue, output_dir, DataType.TRADE)
    return queue, persister


def create_orderbook_persisted_queue(
    output_dir: str = "data/kalshi/orderbook",
    threshold: int = 5000
) -> tuple[SwappableQueue, ParquetPersister]:
    """Create orderbook queue with attached persister."""
    queue = SwappableQueue(threshold=threshold)
    persister = ParquetPersister(queue, output_dir, DataType.ORDERBOOK)
    return queue, persister


# =============================================================================
# PARQUET READING UTILITIES
# =============================================================================

def load_parquet_data(
    parquet_path: str,
    query: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Load data from parquet files with optional query.
    
    Args:
        parquet_path: Path to parquet directory or file
        query: Optional SQL query (use 'data' as table name)
    
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
            result = conn.execute(query).fetchall()
            columns = [desc[0] for desc in conn.description]
        else:
            result = conn.execute("SELECT * FROM data").fetchall()
            columns = [desc[0] for desc in conn.description]
        
        conn.close()
        
        return [dict(zip(columns, row)) for row in result]
        
    except Exception as e:
        logger.error(f"Error loading data from {parquet_path}: {e}")
        return []


def load_market_tickers(parquet_path: str) -> List[str]:
    """
    Load market tickers from market parquet files.
    
    Args:
        parquet_path: Path to market parquet directory
    
    Returns:
        List of market tickers
    """
    path = Path(parquet_path)
    
    if not path.exists():
        logger.warning(f"Path not found: {parquet_path}")
        return []
    
    try:
        conn = duckdb.connect(":memory:")
        
        if path.is_dir():
            glob_pattern = str(path / "**" / "*.parquet")
            query = f"""
                SELECT DISTINCT ticker 
                FROM read_parquet('{glob_pattern}', hive_partitioning=true)
                WHERE ticker IS NOT NULL
            """
        else:
            query = f"""
                SELECT DISTINCT ticker 
                FROM read_parquet('{path}')
                WHERE ticker IS NOT NULL
            """
        
        result = conn.execute(query).fetchall()
        conn.close()
        
        tickers = [row[0] for row in result]
        logger.info(f"Loaded {len(tickers)} market tickers from {parquet_path}")
        return tickers
        
    except Exception as e:
        logger.error(f"Error loading tickers from {parquet_path}: {e}")
        return []
