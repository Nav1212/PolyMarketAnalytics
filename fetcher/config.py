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
    window_seconds: float = 10.0


@dataclass
class QueuesConfig:
    """Queue threshold configuration."""
    trade_threshold: int = 10000
    market_threshold: int = 1000
    market_token_threshold: int = 5000
    price_threshold: int = 10000


@dataclass
class OutputDirsConfig:
    """Output directory configuration."""
    trade: str = "data/trades"
    market: str = "data/markets"
    market_token: str = "data/market_tokens"
    price: str = "data/prices"


@dataclass
class ApiConfig:
    """API configuration."""
    data_api_base: str = "https://data-api.polymarket.com"
    timeout: float = 30.0
    connect_timeout: float = 10.0


@dataclass
class WorkersConfig:
    """Worker configuration per function type."""
    trade: int = 2
    market: int = 3
    price: int = 2
    leaderboard: int = 1


@dataclass
class CursorsConfig:
    """Cursor persistence configuration."""
    enabled: bool = True
    filename: str = "cursor.json"


@dataclass
class Config:
    """Main configuration class."""
    rate_limits: RateLimitsConfig = field(default_factory=RateLimitsConfig)
    queues: QueuesConfig = field(default_factory=QueuesConfig)
    output_dirs: OutputDirsConfig = field(default_factory=OutputDirsConfig)
    api: ApiConfig = field(default_factory=ApiConfig)
    workers: WorkersConfig = field(default_factory=WorkersConfig)
    cursors: CursorsConfig = field(default_factory=CursorsConfig)
    
    @classmethod
    def from_dict(cls, data: dict) -> "Config":
        """Create Config from dictionary."""
        return cls(
            rate_limits=RateLimitsConfig(**data.get("rate_limits", {})),
            queues=QueuesConfig(**data.get("queues", {})),
            output_dirs=OutputDirsConfig(**data.get("output_dirs", {})),
            api=ApiConfig(**data.get("api", {})),
            workers=WorkersConfig(**data.get("workers", {})),
            cursors=CursorsConfig(**data.get("cursors", {}))
        )
    
    def to_dict(self) -> dict:
        """Convert Config to dictionary."""
        return {
            "rate_limits": {
                "trade": self.rate_limits.trade,
                "market": self.rate_limits.market,
                "price": self.rate_limits.price,
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
            }
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
        config_path = Path(__file__).parent / "config.json"
    else:
        config_path = Path(config_path)
    
    if config_path.exists():
        try:
            with open(config_path, 'r') as f:
                data = json.load(f)
            _config = Config.from_dict(data)
            print(f"[Config] Loaded configuration from {config_path}")
        except Exception as e:
            print(f"[Config] Error loading {config_path}: {e}. Using defaults.")
            _config = Config()
    else:
        print(f"[Config] {config_path} not found. Using defaults.")
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
        config_path = Path(__file__).parent / "config.json"
    else:
        config_path = Path(config_path)
    
    with open(config_path, 'w') as f:
        json.dump(config.to_dict(), f, indent=4)
    
    print(f"[Config] Saved configuration to {config_path}")
