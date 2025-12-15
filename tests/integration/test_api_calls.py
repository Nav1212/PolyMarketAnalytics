"""
Integration tests for API calls to Polymarket.

These tests make REAL API calls to verify connectivity and response format.
Run with: pytest tests/integration/test_api_calls.py -v
"""

import pytest
import httpx
from datetime import datetime, timedelta

# Skip all tests if no network connection
pytestmark = pytest.mark.integration


class TestTradeAPI:
    """Tests for the trades API endpoint."""
    
    BASE_URL = "https://data-api.polymarket.com"
    
    def test_fetch_trades_returns_list(self):
        """Verify trades endpoint returns a list."""
        with httpx.Client(timeout=30.0) as client:
            response = client.get(
                f"{self.BASE_URL}/trades",
                params={"limit": 10}
            )
            
            assert response.status_code == 200
            data = response.json()
            assert isinstance(data, list)
    
    def test_fetch_trades_has_required_fields(self):
        """Verify trades have all required fields for our schema."""
        required_fields = [
            "proxyWallet", "side", "price", "size", 
            "conditionId", "timestamp", "transactionHash", "outcome"
        ]
        
        with httpx.Client(timeout=30.0) as client:
            response = client.get(
                f"{self.BASE_URL}/trades",
                params={"limit": 5}
            )
            
            assert response.status_code == 200
            trades = response.json()
            
            if len(trades) > 0:
                trade = trades[0]
                for field in required_fields:
                    assert field in trade, f"Missing field: {field}"
    
    def test_fetch_trades_with_market_filter(self):
        """Verify trades can be filtered by market."""
        # First get any trade to find a valid market ID
        with httpx.Client(timeout=30.0) as client:
            response = client.get(
                f"{self.BASE_URL}/trades",
                params={"limit": 1}
            )
            
            assert response.status_code == 200
            trades = response.json()
            
            if len(trades) > 0:
                market_id = trades[0]["conditionId"]
                
                # Now fetch trades for that specific market
                response = client.get(
                    f"{self.BASE_URL}/trades",
                    params={"market": market_id, "limit": 10}
                )
                
                assert response.status_code == 200
                filtered_trades = response.json()
                
                # All trades should be for this market
                for trade in filtered_trades:
                    assert trade["conditionId"] == market_id
    
    def test_fetch_trades_with_offset(self):
        """Verify pagination with offset works."""
        with httpx.Client(timeout=30.0) as client:
            # Get first page
            response1 = client.get(
                f"{self.BASE_URL}/trades",
                params={"limit": 5, "offset": 0}
            )
            
            # Get second page
            response2 = client.get(
                f"{self.BASE_URL}/trades",
                params={"limit": 5, "offset": 5}
            )
            
            assert response1.status_code == 200
            assert response2.status_code == 200
            
            trades1 = response1.json()
            trades2 = response2.json()
            
            # Pages should be different (assuming there are enough trades)
            if len(trades1) > 0 and len(trades2) > 0:
                assert trades1[0] != trades2[0]


class TestMarketAPI:
    """Tests for the markets API endpoint."""
    
    BASE_URL = "https://gamma-api.polymarket.com"
    
    def test_fetch_markets_returns_list(self):
        """Verify markets endpoint returns data."""
        with httpx.Client(timeout=30.0) as client:
            response = client.get(
                f"{self.BASE_URL}/markets",
                params={"limit": 10, "active": True}
            )
            
            assert response.status_code == 200
            data = response.json()
            assert isinstance(data, list)
    
    def test_fetch_markets_has_condition_id(self):
        """Verify markets have condition_id field."""
        with httpx.Client(timeout=30.0) as client:
            response = client.get(
                f"{self.BASE_URL}/markets",
                params={"limit": 5, "active": True}
            )
            
            assert response.status_code == 200
            markets = response.json()
            
            if len(markets) > 0:
                market = markets[0]
                # Check for condition_id (may be named differently)
                has_id = "condition_id" in market or "conditionId" in market
                assert has_id, f"Market missing condition_id. Keys: {market.keys()}"


class TestPriceAPI:
    """Tests for the price history API endpoint."""
    
    BASE_URL = "https://clob.polymarket.com"
    
    def test_fetch_prices_endpoint_exists(self):
        """Verify price endpoint is accessible."""
        with httpx.Client(timeout=30.0) as client:
            # This may return 400 without proper params, but should not 404
            response = client.get(
                f"{self.BASE_URL}/prices-history",
                params={"interval": "1d", "fidelity": 60}
            )
            
            # Should not be 404 - endpoint exists
            assert response.status_code != 404


class TestTradeFetcherIntegration:
    """Integration tests using the actual TradeFetcher class."""
    
    def test_trade_fetcher_fetch_trades(self, test_config, worker_manager):
        """Test TradeFetcher can fetch real trades."""
        from trade_fetcher import TradeFetcher
        
        with TradeFetcher(
            config=test_config,
            worker_manager=worker_manager
        ) as fetcher:
            trades = fetcher.fetch_trades(
                market="",  # Empty to get any trades
                limit=5
            )
            
            # Should return a list (may be empty if no trades)
            assert isinstance(trades, list)
    
    def test_trade_fetcher_handles_invalid_market(self, test_config, worker_manager):
        """Test TradeFetcher handles invalid market gracefully."""
        from trade_fetcher import TradeFetcher
        
        with TradeFetcher(
            config=test_config,
            worker_manager=worker_manager
        ) as fetcher:
            trades = fetcher.fetch_trades(
                market="0xinvalidmarketid",
                limit=5
            )
            
            # Should return empty list, not crash
            assert isinstance(trades, list)
