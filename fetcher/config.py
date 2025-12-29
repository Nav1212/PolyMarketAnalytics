"""
Configuration loader for Polymarket fetcher.
Loads settings from config.json with fallback defaults.
"""

import json
from pathlib import Path
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class RateLimitsConfig:
    """Rate limiting configuration."""
    trade: int = 70
    market: int = 100
    price: int = 100
    leaderboard: int = 70
    gamma_market: int = 100
    window_seconds: float = 10.0


@dataclass
class QueuesConfig:
    """Queue threshold configuration."""
    trade_threshold: int = 10000
    market_threshold: int = 1000
    market_token_threshold: int = 5000
    price_threshold: int = 10000
    leaderboard_threshold: int = 5000
    gamma_market_threshold: int = 1000
    gamma_event_threshold: int = 1000
    gamma_category_threshold: int = 1000


@dataclass
class BatchSizesConfig:
    """Batch size configuration (aliases queue thresholds for persistence)."""
    trade: int = 10000
    market: int = 1000
    market_token: int = 5000
    price: int = 10000


@dataclass
class OutputDirsConfig:
    """Output directory configuration."""
    trade: str = "data/trades"
    market: str = "data/markets"
    market_token: str = "data/market_tokens"
    price: str = "data/prices"
    leaderboard: str = "data/leaderboard"
    gamma_market: str = "data/gamma_markets"
    gamma_event: str = "data/gamma_events"
    gamma_category: str = "data/gamma_categories"


@dataclass
class ApiConfig:
    """API configuration."""
    data_api_base: str = "https://data-api.polymarket.com"
    clob_api_base: str = "https://clob.polymarket.com"
    price_api_base: str = "https://clob.polymarket.com"
    gamma_api_base: str = "https://gamma-api.polymarket.com"
    timeout: float = 30.0
    connect_timeout: float = 10.0


@dataclass
class WorkersConfig:
    """Worker configuration per function type."""
    trade: int = 2
    market: int = 3
    price: int = 2
    leaderboard: int = 1
    gamma_market: int = 1


@dataclass
class CursorsConfig:
    """Cursor persistence configuration."""
    enabled: bool = True
    filename: str = "cursor.json"


@dataclass
class RetryConfig:
    """Retry configuration for failed worker executions."""
    max_attempts: int = 3
    base_delay: float = 1.0
    max_delay: float = 30.0
    exponential_base: float = 2.0


@dataclass
class PriceResolutionConfig:
    """
    Configuration for volume-based adaptive price resolution.
    
    Markets are categorized by volume into three tiers:
    - Low volume (< low_volume_threshold): Hourly resolution
    - High volume (> high_volume_threshold): Minute resolution  
    - Mid volume (between thresholds): Starts hourly, switches to minute
      if price delta exceeds delta_percent_trigger across responses
    
    Attributes:
        low_volume_threshold: Volume below this uses hourly (default $10K)
        high_volume_threshold: Volume above this uses minute (default $100K)
        delta_percent_trigger: Price change % that triggers minute resolution for mid-tier
        hourly_fidelity: Fidelity value for hourly resolution (60 minutes)
        minute_fidelity: Fidelity value for minute resolution (1 minute)
        hourly_chunk_seconds: Chunk size for hourly fetches (1 day)
        minute_chunk_seconds: Chunk size for minute fetches (1 hour)
    """
    low_volume_threshold: float = 10000.0
    high_volume_threshold: float = 100000.0
    delta_percent_trigger: float = 5.0
    hourly_fidelity: int = 60
    minute_fidelity: int = 1
    hourly_chunk_seconds: int = 86400  # 1 day
    minute_chunk_seconds: int = 3600   # 1 hour


@dataclass
class Config:
    """Main configuration class."""
    rate_limits: RateLimitsConfig = field(default_factory=RateLimitsConfig)
    queues: QueuesConfig = field(default_factory=QueuesConfig)
    output_dirs: OutputDirsConfig = field(default_factory=OutputDirsConfig)
    api: ApiConfig = field(default_factory=ApiConfig)
    workers: WorkersConfig = field(default_factory=WorkersConfig)
    cursors: CursorsConfig = field(default_factory=CursorsConfig)
    retry: RetryConfig = field(default_factory=RetryConfig)
    price_resolution: PriceResolutionConfig = field(default_factory=PriceResolutionConfig)

    @property
    def batch_sizes(self) -> BatchSizesConfig:
        """Return batch sizes derived from queue thresholds."""
        return BatchSizesConfig(
            trade=self.queues.trade_threshold,
            market=self.queues.market_threshold,
            market_token=self.queues.market_token_threshold,
            price=self.queues.price_threshold
        )

    @classmethod
    def from_dict(cls, data: dict) -> "Config":
        """Create Config from dictionary."""
        return cls(
            rate_limits=RateLimitsConfig(**data.get("rate_limits", {})),
            queues=QueuesConfig(**data.get("queues", {})),
            output_dirs=OutputDirsConfig(**data.get("output_dirs", {})),
            api=ApiConfig(**data.get("api", {})),
            workers=WorkersConfig(**data.get("workers", {})),
            cursors=CursorsConfig(**data.get("cursors", {})),
            retry=RetryConfig(**data.get("retry", {})),
            price_resolution=PriceResolutionConfig(**data.get("price_resolution", {})),
        )
    
    def to_dict(self) -> dict:
        """Convert Config to dictionary."""
        return {
            "rate_limits": {
                "trade": self.rate_limits.trade,
                "market": self.rate_limits.market,
                "price": self.rate_limits.price,
                "leaderboard": self.rate_limits.leaderboard,
                "window_seconds": self.rate_limits.window_seconds
            },
            "queues": {
                "trade_threshold": self.queues.trade_threshold,
                "market_threshold": self.queues.market_threshold,
                "market_token_threshold": self.queues.market_token_threshold,
                "price_threshold": self.queues.price_threshold
            },
            "output_dirs": {
                "trade": self.output_dirs.trade,
                "market": self.output_dirs.market,
                "market_token": self.output_dirs.market_token,
                "price": self.output_dirs.price
            },
            "api": {
                "data_api_base": self.api.data_api_base,
                "timeout": self.api.timeout,
                "connect_timeout": self.api.connect_timeout
            },
            "workers": {
                "trade": self.workers.trade,
                "market": self.workers.market,
                "price": self.workers.price,
                "leaderboard": self.workers.leaderboard
            },
            "cursors": {
                "enabled": self.cursors.enabled,
                "filename": self.cursors.filename
            },
            "retry": {
                "max_attempts": self.retry.max_attempts,
                "base_delay": self.retry.base_delay,
                "max_delay": self.retry.max_delay,
                "exponential_base": self.retry.exponential_base
            },
            "price_resolution": {
                "low_volume_threshold": self.price_resolution.low_volume_threshold,
                "high_volume_threshold": self.price_resolution.high_volume_threshold,
                "delta_percent_trigger": self.price_resolution.delta_percent_trigger,
                "hourly_fidelity": self.price_resolution.hourly_fidelity,
                "minute_fidelity": self.price_resolution.minute_fidelity,
                "hourly_chunk_seconds": self.price_resolution.hourly_chunk_seconds,
                "minute_chunk_seconds": self.price_resolution.minute_chunk_seconds,
            },
        }


# Global config instance
_config: Optional[Config] = None


def load_config(config_path: Optional[str] = None) -> Config:
    """
    Load configuration from JSON file.
    
    Args:
        config_path: Path to config.json. If None, looks in same directory as this file.
    
    Returns:
        Config instance with loaded or default values.
    """
    global _config
    
    if config_path is None:
        resolved_path = Path(__file__).parent / "config.json"
    else:
        resolved_path = Path(config_path)
    
    if resolved_path.exists():
        try:
            with open(resolved_path, 'r') as f:
                data = json.load(f)
            _config = Config.from_dict(data)
            print(f"[Config] Loaded configuration from {resolved_path}")
        except Exception as e:
            print(f"[Config] Error loading {resolved_path}: {e}. Using defaults.")
            _config = Config()
    else:
        print(f"[Config] {resolved_path} not found. Using defaults.")
        _config = Config()
    
    return _config


def get_config() -> Config:
    """
    Get the current configuration. Loads from file if not already loaded.
    
    Returns:
        Config instance.
    """
    global _config
    if _config is None:
        _config = load_config()
    return _config


def set_config(config: Config) -> None:
    """
    Set a custom configuration.
    
    Args:
        config: Config instance to use.
    """
    global _config
    _config = config


def save_config(config: Config, config_path: Optional[str] = None) -> None:
    """
    Save configuration to JSON file.
    
    Args:
        config: Config instance to save.
        config_path: Path to save to. If None, saves to default location.
    """
    if config_path is None:
        resolved_path = Path(__file__).parent / "config.json"
    else:
        resolved_path = Path(config_path)
    
    with open(resolved_path, 'w') as f:
        json.dump(config.to_dict(), f, indent=4)
    
    print(f"[Config] Saved configuration to {resolved_path}")
