"""
Unit tests for TradeFetcher.

Tests API interactions, error handling, and worker behavior.
"""

import pytest
from unittest.mock import MagicMock, patch, PropertyMock
import httpx
import sys
from pathlib import Path

# Add fetcher module to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "fetcher"))

from trade_fetcher import TradeFetcher
from utils.exceptions import RateLimitExceededError, NetworkTimeoutError


class TestTradeFetcherInit:
    """Test TradeFetcher initialization."""
    
    def test_init_with_defaults(self, test_config, worker_manager):
        """Test initialization with default parameters."""
        with patch('trade_fetcher.get_config', return_value=test_config):
            with patch('trade_fetcher.get_worker_manager', return_value=worker_manager):
                fetcher = TradeFetcher()
                assert fetcher._config == test_config
                fetcher.close()
    
    def test_init_with_custom_config(self, test_config, worker_manager):
        """Test initialization with custom config."""
        fetcher = TradeFetcher(
            config=test_config,
            worker_manager=worker_manager
        )
        assert fetcher._config == test_config
        assert fetcher._manager == worker_manager
        fetcher.close()
    
    def test_init_with_custom_timeout(self, test_config, worker_manager):
        """Test initialization with custom timeout."""
        fetcher = TradeFetcher(
            timeout=60.0,
            config=test_config,
            worker_manager=worker_manager
        )
        # Timeout should be applied to client
        fetcher.close()


class TestTradeFetcherContextManager:
    """Test context manager behavior."""
    
    def test_context_manager_closes_client(self, test_config, worker_manager):
        """Test that context manager properly closes the HTTP client."""
        with TradeFetcher(config=test_config, worker_manager=worker_manager) as fetcher:
            assert fetcher.client is not None
        # Client should be closed after exiting context


class TestTradeFetcherFetchTrades:
    """Test fetch_trades method."""
    
    def test_fetch_trades_success(self, test_config, worker_manager, sample_trades):
        """Test successful trade fetch."""
        with patch('httpx.Client') as mock_client_class:
            mock_client = MagicMock()
            mock_client_class.return_value = mock_client
            
            mock_response = MagicMock()
            mock_response.json.return_value = sample_trades
            mock_response.raise_for_status.return_value = None
            mock_client.get.return_value = mock_response
            
            fetcher = TradeFetcher(config=test_config, worker_manager=worker_manager)
            fetcher.client = mock_client
            
            trades = fetcher.fetch_trades(
                market="0x1234567890abcdef",
                limit=500
            )
            
            assert trades == sample_trades
            mock_client.get.assert_called_once()
            fetcher.close()
    
    def test_fetch_trades_empty_response(self, test_config, worker_manager):
        """Test handling of empty response."""
        with patch('httpx.Client') as mock_client_class:
            mock_client = MagicMock()
            mock_client_class.return_value = mock_client
            
            mock_response = MagicMock()
            mock_response.json.return_value = []
            mock_response.raise_for_status.return_value = None
            mock_client.get.return_value = mock_response
            
            fetcher = TradeFetcher(config=test_config, worker_manager=worker_manager)
            fetcher.client = mock_client
            
            trades = fetcher.fetch_trades(market="0x1234567890abcdef")
            
            assert trades == []
            fetcher.close()
    
    def test_fetch_trades_http_error(self, test_config, worker_manager):
        """Test handling of HTTP errors."""
        with patch('httpx.Client') as mock_client_class:
            mock_client = MagicMock()
            mock_client_class.return_value = mock_client
            
            mock_response = MagicMock()
            mock_response.status_code = 500
            mock_response.text = "Internal Server Error"
            mock_client.get.return_value = mock_response
            mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
                "500 Internal Server Error",
                request=MagicMock(),
                response=mock_response
            )
            
            fetcher = TradeFetcher(config=test_config, worker_manager=worker_manager)
            fetcher.client = mock_client
            
            trades = fetcher.fetch_trades(market="0x1234567890abcdef")
            
            # Should return empty list on error
            assert trades == []
            fetcher.close()
    
    def test_fetch_trades_rate_limit_error(self, test_config, worker_manager):
        """Test handling of rate limit (429) errors."""
        with patch('httpx.Client') as mock_client_class:
            mock_client = MagicMock()
            mock_client_class.return_value = mock_client
            
            mock_response = MagicMock()
            mock_response.status_code = 429
            mock_response.text = "Rate limit exceeded"
            mock_client.get.return_value = mock_response
            mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
                "429 Too Many Requests",
                request=MagicMock(),
                response=mock_response
            )
            
            fetcher = TradeFetcher(config=test_config, worker_manager=worker_manager)
            fetcher.client = mock_client
            
            with pytest.raises(RateLimitExceededError):
                fetcher.fetch_trades(market="0x1234567890abcdef")
            
            fetcher.close()
    
    def test_fetch_trades_timeout_error(self, test_config, worker_manager):
        """Test handling of timeout errors."""
        with patch('httpx.Client') as mock_client_class:
            mock_client = MagicMock()
            mock_client_class.return_value = mock_client
            
            mock_client.get.side_effect = httpx.TimeoutException("Request timed out")
            
            fetcher = TradeFetcher(config=test_config, worker_manager=worker_manager)
            fetcher.client = mock_client
            
            with pytest.raises(NetworkTimeoutError):
                fetcher.fetch_trades(market="0x1234567890abcdef")
            
            fetcher.close()
    
    def test_fetch_trades_request_error(self, test_config, worker_manager):
        """Test handling of request errors."""
        with patch('httpx.Client') as mock_client_class:
            mock_client = MagicMock()
            mock_client_class.return_value = mock_client
            
            mock_client.get.side_effect = httpx.RequestError("Connection failed")
            
            fetcher = TradeFetcher(config=test_config, worker_manager=worker_manager)
            fetcher.client = mock_client
            
            trades = fetcher.fetch_trades(market="0x1234567890abcdef")
            
            # Should return empty list on request error
            assert trades == []
            fetcher.close()


class TestTradeFetcherParameters:
    """Test fetch_trades parameter handling."""
    
    def test_fetch_trades_with_all_parameters(self, test_config, worker_manager):
        """Test that all parameters are passed correctly."""
        with patch('httpx.Client') as mock_client_class:
            mock_client = MagicMock()
            mock_client_class.return_value = mock_client
            
            mock_response = MagicMock()
            mock_response.json.return_value = []
            mock_response.raise_for_status.return_value = None
            mock_client.get.return_value = mock_response
            
            fetcher = TradeFetcher(config=test_config, worker_manager=worker_manager)
            fetcher.client = mock_client
            
            fetcher.fetch_trades(
                market="0x123",
                filtertype="CASH",
                filteramount=100,
                offset=500,
                limit=250
            )
            
            # Verify the call was made with correct parameters
            call_args = mock_client.get.call_args
            params = call_args.kwargs.get('params', call_args[1].get('params', {}))
            
            assert params['market'] == "0x123"
            assert params['filterType'] == "CASH"
            assert params['filterAmount'] == 100
            assert params['offset'] == 500
            assert params['limit'] == 250
            
            fetcher.close()


class TestTradeFetcherRateLimiting:
    """Test rate limiting integration."""
    
    def test_acquire_called_before_request(self, test_config, worker_manager):
        """Test that rate limit is acquired before making request."""
        with patch('httpx.Client') as mock_client_class:
            mock_client = MagicMock()
            mock_client_class.return_value = mock_client
            
            mock_response = MagicMock()
            mock_response.json.return_value = []
            mock_response.raise_for_status.return_value = None
            mock_client.get.return_value = mock_response
            
            # Use a mock worker manager to track calls
            mock_manager = MagicMock()
            
            fetcher = TradeFetcher(config=test_config, worker_manager=mock_manager)
            fetcher.client = mock_client
            
            fetcher.fetch_trades(market="0x123")
            
            # Verify acquire_trade was called
            mock_manager.acquire_trade.assert_called_once()
            
            fetcher.close()
