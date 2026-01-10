"""
Persistence module for Kalshi fetcher.
"""

from kalshi_fetcher.persistence.swappable_queue import SwappableQueue
from kalshi_fetcher.persistence.parquet_persister import (
    ParquetPersister,
    DataType,
    EVENT_SCHEMA,
    MARKET_SCHEMA,
    TRADE_SCHEMA,
    ORDERBOOK_SCHEMA,
    create_event_persisted_queue,
    create_market_persisted_queue,
    create_trade_persisted_queue,
    create_orderbook_persisted_queue,
    load_parquet_data,
    load_market_tickers,
)

__all__ = [
    "SwappableQueue",
    "ParquetPersister",
    "DataType",
    "EVENT_SCHEMA",
    "MARKET_SCHEMA",
    "TRADE_SCHEMA",
    "ORDERBOOK_SCHEMA",
    "create_event_persisted_queue",
    "create_market_persisted_queue",
    "create_trade_persisted_queue",
    "create_orderbook_persisted_queue",
    "load_parquet_data",
    "load_market_tickers",
]
