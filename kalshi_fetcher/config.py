"""
Configuration loader for Kalshi fetcher.
Loads settings from config.json with fallback defaults.
"""

import json
from pathlib import Path
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class RateLimitsConfig:
    """Rate limiting configuration."""
    event: int = 100
    market: int = 100
    trade: int = 70
    orderbook: int = 100
    window_seconds: float = 10.0


@dataclass
class QueuesConfig:
    """Queue threshold configuration."""
    event_threshold: int = 1000
    market_threshold: int = 5000
    trade_threshold: int = 10000
    orderbook_threshold: int = 5000


@dataclass
class OutputDirsConfig:
    """Output directory configuration."""
    event: str = "data/kalshi/events"
    market: str = "data/kalshi/markets"
    trade: str = "data/kalshi/trades"
    orderbook: str = "data/kalshi/orderbook"


@dataclass
class ApiConfig:
    """API configuration."""
    base_url: str = "https://api.elections.kalshi.com/trade-api/v2"
    timeout: float = 30.0
    connect_timeout: float = 10.0


@dataclass
class WorkersConfig:
    """Worker configuration per function type."""
    event: int = 1
    market: int = 2
    trade: int = 2
    orderbook: int = 1


@dataclass
class CursorsConfig:
    """Cursor persistence configuration."""
    enabled: bool = True
    filename: str = "kalshi_cursor.json"


@dataclass
class RetryConfig:
    """Retry configuration for failed worker executions."""
    max_attempts: int = 3
    base_delay: float = 1.0
    max_delay: float = 30.0
    exponential_base: float = 2.0


@dataclass
class Config:
    """Root configuration object."""
    rate_limits: RateLimitsConfig = field(default_factory=RateLimitsConfig)
    queues: QueuesConfig = field(default_factory=QueuesConfig)
    output_dirs: OutputDirsConfig = field(default_factory=OutputDirsConfig)
    api: ApiConfig = field(default_factory=ApiConfig)
    workers: WorkersConfig = field(default_factory=WorkersConfig)
    cursors: CursorsConfig = field(default_factory=CursorsConfig)
    retry: RetryConfig = field(default_factory=RetryConfig)


def load_config(config_path: Optional[Path] = None) -> Config:
    """
    Load configuration from JSON file.
    
    Args:
        config_path: Path to config.json. If None, uses default location.
    
    Returns:
        Config dataclass populated from JSON.
    """
    if config_path is None:
        config_path = Path(__file__).parent / "config.json"
    
    config = Config()
    
    if config_path.exists():
        with open(config_path) as f:
            data = json.load(f)
        
        # Rate limits
        if "rate_limits" in data:
            rl = data["rate_limits"]
            config.rate_limits = RateLimitsConfig(
                event=rl.get("event", 100),
                market=rl.get("market", 100),
                trade=rl.get("trade", 70),
                orderbook=rl.get("orderbook", 100),
                window_seconds=rl.get("window_seconds", 10.0),
            )
        
        # Queues
        if "queues" in data:
            q = data["queues"]
            config.queues = QueuesConfig(
                event_threshold=q.get("event_threshold", 1000),
                market_threshold=q.get("market_threshold", 5000),
                trade_threshold=q.get("trade_threshold", 10000),
                orderbook_threshold=q.get("orderbook_threshold", 5000),
            )
        
        # Output dirs
        if "output_dirs" in data:
            od = data["output_dirs"]
            config.output_dirs = OutputDirsConfig(
                event=od.get("event", "data/kalshi/events"),
                market=od.get("market", "data/kalshi/markets"),
                trade=od.get("trade", "data/kalshi/trades"),
                orderbook=od.get("orderbook", "data/kalshi/orderbook"),
            )
        
        # API
        if "api" in data:
            api = data["api"]
            config.api = ApiConfig(
                base_url=api.get("base_url", "https://api.elections.kalshi.com/trade-api/v2"),
                timeout=api.get("timeout", 30.0),
                connect_timeout=api.get("connect_timeout", 10.0),
            )
        
        # Workers
        if "workers" in data:
            w = data["workers"]
            config.workers = WorkersConfig(
                event=w.get("event", 1),
                market=w.get("market", 2),
                trade=w.get("trade", 2),
                orderbook=w.get("orderbook", 1),
            )
        
        # Cursors
        if "cursors" in data:
            c = data["cursors"]
            config.cursors = CursorsConfig(
                enabled=c.get("enabled", True),
                filename=c.get("filename", "kalshi_cursor.json"),
            )
        
        # Retry
        if "retry" in data:
            r = data["retry"]
            config.retry = RetryConfig(
                max_attempts=r.get("max_attempts", 3),
                base_delay=r.get("base_delay", 1.0),
                max_delay=r.get("max_delay", 30.0),
                exponential_base=r.get("exponential_base", 2.0),
            )
    
    return config


# Global config singleton
_config: Optional[Config] = None


def get_config() -> Config:
    """Get global config singleton, loading if needed."""
    global _config
    if _config is None:
        _config = load_config()
    return _config


def set_config(config: Config) -> None:
    """Set global config singleton (for testing)."""
    global _config
    _config = config
