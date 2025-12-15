"""
Unit tests for Config module.

Tests configuration loading, dataclass behavior, and defaults.
"""

import pytest
import json
import tempfile
from pathlib import Path
import sys

# Add fetcher module to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "fetcher"))

from config import (
    Config,
    RateLimitsConfig,
    QueuesConfig,
    OutputDirsConfig,
    ApiConfig,
    WorkersConfig,
    CursorsConfig,
    load_config,
    get_config,
)


class TestRateLimitsConfig:
    """Test RateLimitsConfig dataclass."""
    
    def test_default_values(self):
        """Test default rate limit values."""
        config = RateLimitsConfig()
        assert config.trade == 70
        assert config.market == 100
        assert config.price == 100
        assert config.leaderboard == 70
        assert config.window_seconds == 10.0
    
    def test_custom_values(self):
        """Test custom rate limit values."""
        config = RateLimitsConfig(
            trade=50,
            market=80,
            price=60,
            leaderboard=40,
            window_seconds=5.0
        )
        assert config.trade == 50
        assert config.market == 80
        assert config.price == 60
        assert config.leaderboard == 40
        assert config.window_seconds == 5.0


class TestQueuesConfig:
    """Test QueuesConfig dataclass."""
    
    def test_default_values(self):
        """Test default queue threshold values."""
        config = QueuesConfig()
        assert config.trade_threshold == 10000
        assert config.market_threshold == 1000
        assert config.market_token_threshold == 5000
        assert config.price_threshold == 10000
    
    def test_custom_values(self):
        """Test custom queue threshold values."""
        config = QueuesConfig(
            trade_threshold=5000,
            market_threshold=500,
            market_token_threshold=2500,
            price_threshold=5000
        )
        assert config.trade_threshold == 5000
        assert config.market_threshold == 500


class TestOutputDirsConfig:
    """Test OutputDirsConfig dataclass."""
    
    def test_default_values(self):
        """Test default output directory values."""
        config = OutputDirsConfig()
        assert config.trade == "data/trades"
        assert config.market == "data/markets"
        assert config.market_token == "data/market_tokens"
        assert config.price == "data/prices"
    
    def test_custom_values(self):
        """Test custom output directory values."""
        config = OutputDirsConfig(
            trade="/custom/trades",
            market="/custom/markets"
        )
        assert config.trade == "/custom/trades"
        assert config.market == "/custom/markets"


class TestApiConfig:
    """Test ApiConfig dataclass."""
    
    def test_default_values(self):
        """Test default API configuration values."""
        config = ApiConfig()
        assert config.data_api_base == "https://data-api.polymarket.com"
        assert config.clob_api_base == "https://clob.polymarket.com"
        assert config.timeout == 30.0
        assert config.connect_timeout == 10.0
    
    def test_custom_values(self):
        """Test custom API configuration values."""
        config = ApiConfig(timeout=60.0, connect_timeout=20.0)
        assert config.timeout == 60.0
        assert config.connect_timeout == 20.0


class TestWorkersConfig:
    """Test WorkersConfig dataclass."""
    
    def test_default_values(self):
        """Test default worker configuration values."""
        config = WorkersConfig()
        assert config.trade == 2
        assert config.market == 3
        assert config.price == 2
        assert config.leaderboard == 1
    
    def test_custom_values(self):
        """Test custom worker configuration values."""
        config = WorkersConfig(trade=5, market=5, price=3, leaderboard=2)
        assert config.trade == 5
        assert config.market == 5


class TestConfig:
    """Test main Config class."""
    
    def test_default_config(self):
        """Test creating config with all defaults."""
        config = Config()
        assert isinstance(config.rate_limits, RateLimitsConfig)
        assert isinstance(config.queues, QueuesConfig)
        assert isinstance(config.output_dirs, OutputDirsConfig)
        assert isinstance(config.api, ApiConfig)
        assert isinstance(config.workers, WorkersConfig)
        assert isinstance(config.cursors, CursorsConfig)
    
    def test_from_dict_empty(self):
        """Test from_dict with empty dictionary uses defaults."""
        config = Config.from_dict({})
        assert config.rate_limits.trade == 70
        assert config.queues.trade_threshold == 10000
    
    def test_from_dict_partial(self):
        """Test from_dict with partial configuration."""
        data = {
            "rate_limits": {
                "trade": 50
            }
        }
        config = Config.from_dict(data)
        assert config.rate_limits.trade == 50
        # Other values should be defaults
        assert config.rate_limits.market == 100
    
    def test_from_dict_full(self):
        """Test from_dict with full configuration."""
        data = {
            "rate_limits": {
                "trade": 50,
                "market": 80,
                "price": 60,
                "leaderboard": 40,
                "window_seconds": 5.0
            },
            "queues": {
                "trade_threshold": 5000,
                "market_threshold": 500,
                "market_token_threshold": 2500,
                "price_threshold": 5000
            },
            "workers": {
                "trade": 4,
                "market": 4,
                "price": 3,
                "leaderboard": 2
            }
        }
        config = Config.from_dict(data)
        assert config.rate_limits.trade == 50
        assert config.queues.trade_threshold == 5000
        assert config.workers.trade == 4
    
    def test_to_dict(self):
        """Test converting config to dictionary."""
        config = Config()
        data = config.to_dict()
        
        assert "rate_limits" in data
        assert "queues" in data
        assert "output_dirs" in data
        assert "api" in data
        assert "workers" in data
        
        assert data["rate_limits"]["trade"] == 70
        assert data["queues"]["trade_threshold"] == 10000
    
    def test_round_trip(self):
        """Test config survives round-trip through dict."""
        original = Config(
            rate_limits=RateLimitsConfig(trade=42),
            workers=WorkersConfig(trade=7)
        )
        
        data = original.to_dict()
        restored = Config.from_dict(data)
        
        assert restored.rate_limits.trade == 42
        assert restored.workers.trade == 7


class TestLoadConfig:
    """Test loading configuration from file."""
    
    def test_load_from_json_file(self, tmp_path):
        """Test loading configuration from JSON file."""
        config_data = {
            "rate_limits": {
                "trade": 42
            },
            "workers": {
                "trade": 8
            }
        }
        
        config_file = tmp_path / "config.json"
        config_file.write_text(json.dumps(config_data))
        
        config = load_config(str(config_file))
        
        assert config.rate_limits.trade == 42
        assert config.workers.trade == 8
    
    def test_load_missing_file_returns_defaults(self, tmp_path):
        """Test that missing config file returns default config."""
        config = load_config(str(tmp_path / "nonexistent.json"))
        assert config.rate_limits.trade == 70  # Default
    
    def test_load_invalid_json_returns_defaults(self, tmp_path):
        """Test that invalid JSON returns default config."""
        config_file = tmp_path / "invalid.json"
        config_file.write_text("{ invalid json }")
        
        config = load_config(str(config_file))
        assert config.rate_limits.trade == 70  # Default


class TestGetConfig:
    """Test get_config singleton behavior."""
    
    def test_get_config_returns_config(self):
        """Test that get_config returns a Config instance."""
        config = get_config()
        assert isinstance(config, Config)
    
    def test_get_config_returns_same_instance(self):
        """Test that get_config returns the same instance."""
        config1 = get_config()
        config2 = get_config()
        assert config1 is config2
