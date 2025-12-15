"""
Shared pytest fixtures for Polymarket fetcher tests.

Provides mock data, test configurations, and reusable fixtures
for unit and integration tests.
"""

import pytest
import sys
from pathlib import Path
from typing import Dict, Any, List
from unittest.mock import MagicMock, patch
from queue import Queue
import tempfile
import json

# Add fetcher module to path
sys.path.insert(0, str(Path(__file__).parent.parent / "fetcher"))

from config import Config, RateLimitsConfig, QueuesConfig, OutputDirsConfig, ApiConfig, WorkersConfig
from swappable_queue import SwappableQueue
from worker_manager import WorkerManager


# =============================================================================
# Configuration Fixtures
# =============================================================================

@pytest.fixture
def test_config() -> Config:
    """Create a test configuration with fast timeouts."""
    return Config(
        rate_limits=RateLimitsConfig(
            trade=100,
            market=100,
            price=100,
            leaderboard=100,
            window_seconds=1.0  # Fast for testing
        ),
        queues=QueuesConfig(
            trade_threshold=100,  # Small for testing
            market_threshold=50,
            market_token_threshold=50,
            price_threshold=100
        ),
        output_dirs=OutputDirsConfig(
            trade="test_data/trades",
            market="test_data/markets",
            market_token="test_data/market_tokens",
            price="test_data/prices"
        ),
        api=ApiConfig(
            timeout=5.0,
            connect_timeout=2.0
        ),
        workers=WorkersConfig(
            trade=1,
            market=1,
            price=1,
            leaderboard=1
        )
    )


@pytest.fixture
def temp_output_dir(tmp_path) -> Path:
    """Create a temporary output directory for tests."""
    output_dir = tmp_path / "test_output"
    output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir


# =============================================================================
# Queue Fixtures
# =============================================================================

@pytest.fixture
def swappable_queue() -> SwappableQueue:
    """Create a SwappableQueue with small threshold for testing."""
    return SwappableQueue(threshold=10)


@pytest.fixture
def regular_queue() -> Queue:
    """Create a regular Queue for testing."""
    return Queue()


# =============================================================================
# Worker Manager Fixtures
# =============================================================================

@pytest.fixture
def worker_manager(test_config) -> WorkerManager:
    """Create a WorkerManager for testing."""
    return WorkerManager(config=test_config)


# =============================================================================
# Mock Data Fixtures
# =============================================================================

@pytest.fixture
def sample_trade() -> Dict[str, Any]:
    """Create a sample trade dictionary."""
    return {
        "proxyWallet": "0x1234567890abcdef1234567890abcdef12345678",
        "side": "BUY",
        "price": 0.65,
        "size": 100.0,
        "conditionId": "0xabcdef1234567890abcdef1234567890abcdef12",
        "timestamp": 1702300800,
        "transactionHash": "0x9876543210fedcba9876543210fedcba98765432",
        "outcome": "Yes"
    }


@pytest.fixture
def sample_trades(sample_trade) -> List[Dict[str, Any]]:
    """Create a list of sample trades."""
    trades = []
    for i in range(25):
        trade = sample_trade.copy()
        trade["timestamp"] = sample_trade["timestamp"] + i * 60
        trade["size"] = 100.0 + i * 10
        trade["price"] = 0.65 + (i % 10) * 0.01
        trades.append(trade)
    return trades


@pytest.fixture
def sample_market() -> Dict[str, Any]:
    """Create a sample market dictionary."""
    return {
        "condition_id": "0xabcdef1234567890abcdef1234567890abcdef12",
        "question": "Will Bitcoin reach $100,000 by end of 2025?",
        "description": "This market resolves to Yes if BTC price exceeds $100,000 USD.",
        "end_date_iso": "2025-12-31T23:59:59Z",
        "game_start_time": "2025-01-01T00:00:00Z",
        "maker_base_fee": 0.0,
        "fpmm": "0x1111111111111111111111111111111111111111",
        "closed": False,
        "active": True,
        "tokens": [
            {"token_id": "token_yes_123", "outcome": "Yes", "price": 0.65},
            {"token_id": "token_no_456", "outcome": "No", "price": 0.35}
        ],
        "clobTokenIds": ["token_yes_123", "token_no_456"]
    }


@pytest.fixture
def sample_markets(sample_market) -> List[Dict[str, Any]]:
    """Create a list of sample markets."""
    markets = []
    for i in range(10):
        market = sample_market.copy()
        market["condition_id"] = f"0x{'0' * 30}{i:010d}"
        market["question"] = f"Test Market Question #{i}?"
        markets.append(market)
    return markets


@pytest.fixture
def sample_api_response(sample_trades) -> Dict[str, Any]:
    """Create a sample API response with trades."""
    return sample_trades


# =============================================================================
# Mock HTTP Client Fixtures
# =============================================================================

@pytest.fixture
def mock_httpx_client():
    """Create a mock httpx client."""
    with patch("httpx.Client") as mock_client:
        instance = MagicMock()
        mock_client.return_value = instance
        yield instance


@pytest.fixture
def mock_successful_response(sample_trades):
    """Create a mock successful HTTP response."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = sample_trades
    mock_response.raise_for_status.return_value = None
    return mock_response


@pytest.fixture
def mock_rate_limit_response():
    """Create a mock rate limit HTTP response."""
    mock_response = MagicMock()
    mock_response.status_code = 429
    mock_response.headers = {"Retry-After": "5"}
    mock_response.text = "Rate limit exceeded"
    return mock_response


# =============================================================================
# File System Fixtures
# =============================================================================

@pytest.fixture
def temp_parquet_dir(tmp_path) -> Path:
    """Create a temporary directory for parquet files."""
    parquet_dir = tmp_path / "parquet"
    parquet_dir.mkdir(parents=True, exist_ok=True)
    return parquet_dir


@pytest.fixture
def temp_cursor_file(tmp_path) -> Path:
    """Create a temporary cursor file."""
    cursor_file = tmp_path / "cursor.json"
    cursor_data = {
        "last_timestamp": 1702300800,
        "last_offset": 0,
        "updated_at": "2024-12-11T12:00:00"
    }
    cursor_file.write_text(json.dumps(cursor_data))
    return cursor_file


# =============================================================================
# Cleanup Fixtures
# =============================================================================

@pytest.fixture(autouse=True)
def cleanup_global_state():
    """Clean up global state after each test."""
    yield
    # Reset global worker manager
    from worker_manager import set_worker_manager
    set_worker_manager(None)
