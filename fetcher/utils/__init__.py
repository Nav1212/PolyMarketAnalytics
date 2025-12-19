"""
Utils package for Polymarket fetcher.

Contains shared utilities:
- logging_config: Centralized logging configuration
- exceptions: Custom exception classes
- retry: Retry decorator with exponential backoff
"""

from fetcher.utils.logging_config import get_logger, setup_logging, LogContext
from fetcher.utils.exceptions import (
    PolymarketError,
    PolymarketAPIError,
    RateLimitExceededError,
    NetworkTimeoutError,
    MarketNotFoundError,
    DataValidationError,
    ParquetWriteError,
    DatabaseError,
)
from fetcher.utils.retry import retry, retry_async, RetryContext

__all__ = [
    # Logging
    "get_logger",
    "setup_logging", 
    "LogContext",
    # Exceptions
    "PolymarketError",
    "PolymarketAPIError",
    "RateLimitExceededError",
    "NetworkTimeoutError",
    "MarketNotFoundError",
    "DataValidationError",
    "ParquetWriteError",
    "DatabaseError",
    # Retry
    "retry",
    "retry_async",
    "RetryContext",
]
