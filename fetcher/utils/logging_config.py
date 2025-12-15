"""
Centralized logging configuration for Polymarket fetcher.

Provides structured logging with console and file handlers,
log rotation, and consistent formatting across all modules.
"""

import logging
import logging.handlers
import sys
from pathlib import Path
from typing import Optional


# Log format with timestamp, level, module, and message
LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)-20s | %(message)s"
LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

# Default log directory (relative to fetcher module)
DEFAULT_LOG_DIR = Path(__file__).parent.parent.parent / "logs"


def setup_logging(
    level: int = logging.INFO,
    log_dir: Optional[Path] = None,
    console: bool = True,
    file: bool = True,
    max_bytes: int = 10 * 1024 * 1024,  # 10 MB
    backup_count: int = 7,
) -> logging.Logger:
    """
    Configure logging for the entire application.
    
    Args:
        level: Logging level (default INFO)
        log_dir: Directory for log files (default: ../logs)
        console: Enable console output
        file: Enable file output with rotation
        max_bytes: Max size per log file before rotation
        backup_count: Number of backup files to keep
    
    Returns:
        Root logger for the polymarket namespace
    """
    log_dir = log_dir or DEFAULT_LOG_DIR
    log_dir.mkdir(parents=True, exist_ok=True)
    
    # Create root logger for polymarket namespace
    root_logger = logging.getLogger("polymarket")
    root_logger.setLevel(level)
    
    # Clear any existing handlers
    root_logger.handlers.clear()
    
    # Create formatter
    formatter = logging.Formatter(LOG_FORMAT, datefmt=LOG_DATE_FORMAT)
    
    # Console handler
    if console:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(level)
        console_handler.setFormatter(formatter)
        root_logger.addHandler(console_handler)
    
    # File handler with rotation
    if file:
        log_file = log_dir / "polymarket.log"
        file_handler = logging.handlers.RotatingFileHandler(
            log_file,
            maxBytes=max_bytes,
            backupCount=backup_count,
            encoding="utf-8"
        )
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)
        
        # Error-only file handler
        error_file = log_dir / "polymarket_errors.log"
        error_handler = logging.handlers.RotatingFileHandler(
            error_file,
            maxBytes=max_bytes,
            backupCount=backup_count,
            encoding="utf-8"
        )
        error_handler.setLevel(logging.ERROR)
        error_handler.setFormatter(formatter)
        root_logger.addHandler(error_handler)
    
    return root_logger


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger for a specific module.
    
    Args:
        name: Module name (e.g., "trade_fetcher", "coordinator")
    
    Returns:
        Logger instance under the polymarket namespace
    
    Example:
        >>> logger = get_logger("trade_fetcher")
        >>> logger.info("Fetching trades", extra={"market_id": "0x123"})
    """
    return logging.getLogger(f"polymarket.{name}")


class LogContext:
    """
    Context manager for adding contextual information to log messages.
    
    Example:
        >>> with LogContext(logger, market_id="0x123", worker_id=1):
        ...     logger.info("Processing market")
    """
    
    def __init__(self, logger: logging.Logger, **context):
        self.logger = logger
        self.context = context
        self._old_factory = None
    
    def __enter__(self):
        self._old_factory = logging.getLogRecordFactory()
        context = self.context
        
        def record_factory(*args, **kwargs):
            record = self._old_factory(*args, **kwargs)
            for key, value in context.items():
                setattr(record, key, value)
            return record
        
        logging.setLogRecordFactory(record_factory)
        return self
    
    def __exit__(self, *args):
        logging.setLogRecordFactory(self._old_factory)


# Initialize logging on module import (can be reconfigured later)
_initialized = False


def ensure_logging_initialized():
    """Ensure logging is initialized (call once at startup)."""
    global _initialized
    if not _initialized:
        setup_logging()
        _initialized = True
