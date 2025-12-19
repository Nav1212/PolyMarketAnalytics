"""
Persistence module - Handles data persistence to parquet files.

This module isolates all persistence-related functionality including
thread-safe queues and parquet file writing.

Exports:
    - SwappableQueue: Thread-safe queue with atomic swap capability
    - ParquetPersister: Non-blocking parquet persistence daemon
    - DataType: Supported data types enum
    - create_*_persisted_queue: Factory functions for each data type
    - Schema definitions: TRADE_SCHEMA, MARKET_SCHEMA, etc.
"""

from fetcher.persistence.swappable_queue import SwappableQueue
from fetcher.persistence.parquet_persister import (
    ParquetPersister,
    DataType,
    create_persisted_queue,
    create_trade_persisted_queue,
    create_market_persisted_queue,
    create_market_token_persisted_queue,
    create_price_persisted_queue,
    create_leaderboard_persisted_queue,
    TRADE_SCHEMA,
    MARKET_SCHEMA,
    MARKET_TOKEN_SCHEMA,
    PRICE_SCHEMA,
    LEADERBOARD_SCHEMA,
)

__all__ = [
    "SwappableQueue",
    "ParquetPersister",
    "DataType",
    "create_persisted_queue",
    "create_trade_persisted_queue",
    "create_market_persisted_queue",
    "create_market_token_persisted_queue",
    "create_price_persisted_queue",
    "create_leaderboard_persisted_queue",
    "TRADE_SCHEMA",
    "MARKET_SCHEMA",
    "MARKET_TOKEN_SCHEMA",
    "PRICE_SCHEMA",
    "LEADERBOARD_SCHEMA",
]
