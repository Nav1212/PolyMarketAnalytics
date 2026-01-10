"""
Logging configuration for Kalshi fetcher.
Provides consistent logging across all modules.
"""

import logging
import sys
from pathlib import Path
from typing import Optional


def get_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """
    Get a configured logger instance.
    
    Args:
        name: Logger name (usually module name)
        level: Logging level (default INFO)
    
    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(f"kalshi.{name}")
    
    if not logger.handlers:
        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(level)
        
        # Formatter
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        console_handler.setFormatter(formatter)
        
        logger.addHandler(console_handler)
        logger.setLevel(level)
        logger.propagate = False
    
    return logger


def setup_file_logging(log_dir: Optional[Path] = None, level: int = logging.DEBUG) -> None:
    """
    Add file handler to root kalshi logger.
    
    Args:
        log_dir: Directory for log files. If None, uses ./logs
        level: File logging level (default DEBUG)
    """
    if log_dir is None:
        log_dir = Path(__file__).parent.parent.parent / "logs"
    
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / "kalshi.log"
    
    root_logger = logging.getLogger("kalshi")
    
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(level)
    
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    file_handler.setFormatter(formatter)
    
    root_logger.addHandler(file_handler)
