"""
Simple Trade Fetcher for Polymarket
Fetches trades for a given market and time range
"""

import httpx
from typing import Optional, List, Dict, Any
from datetime import datetime


class TradeFetcher:
    """
    Simple class to fetch trades from Polymarket Data API
    """
    
    DATA_API_BASE = "https://data-api.polymarket.com"
    
    def __init__(self, timeout: float = 30.0):
        """
        Initialize the trade fetcher.
        
        Args:
            timeout: Request timeout in seconds
        """
        self.client = httpx.Client(
            timeout=httpx.Timeout(timeout, connect=10.0),
            headers={
                "User-Agent": "PolymarketTradeFetcher/1.0",
                "Accept": "application/json"
            }
        )
    
    def close(self):
        """Close HTTP client"""
        self.client.close()
    
    def __enter__(self):
        return self
    
    def __exit__(self, *args):
        self.close()
    
    def fetch_trades(
        self,
        market: str,
        start_time: int,
        end_time: int,
        limit: int = 500
    ) -> List[Dict[str, Any]]:
        """
        Fetch trades for a specific market within a time range.
        
        Args:
            market: Market condition_id (e.g., "0x123abc...")
            start_time: Start timestamp in Unix seconds
            end_time: End timestamp in Unix seconds
            limit: Maximum number of trades per request (max 500)
        
        Returns:
            List of trade dictionaries
        
        Example:
            >>> fetcher = TradeFetcher()
            >>> trades = fetcher.fetch_trades(
            ...     market="0x123abc...",
            ...     start_time=1702300800,
            ...     end_time=1702387200,
            ...     limit=500
            ... )
            >>> print(f"Fetched {len(trades)} trades")
        """
        params = {
            "market": market,
            "startTs": start_time,
            "endTs": end_time,
            "limit": limit
        }
        
        try:
            response = self.client.get(
                f"{self.DATA_API_BASE}/trades",
                params=params
            )
            response.raise_for_status()
            return response.json()
        
        except httpx.HTTPStatusError as e:
            print(f"HTTP error fetching trades: {e.response.status_code} - {e.response.text}")
            return []
        
        except httpx.RequestError as e:
            print(f"Request error fetching trades: {e}")
            return []
        
        except Exception as e:
            print(f"Unexpected error fetching trades: {e}")
            return []
    
    def fetch_all_trades(
        self,
        market: str,
        start_time: int,
        end_time: int
    ) -> List[Dict[str, Any]]:
        """
        Fetch ALL trades for a market within a time range.
        Makes multiple requests if necessary to get all trades.
        
        Args:
            market: Market condition_id
            start_time: Start timestamp in Unix seconds
            end_time: End timestamp in Unix seconds
        
        Returns:
            Complete list of all trades in the time range
        """
        all_trades = []
        current_start = start_time
        
        while current_start < end_time:
            trades = self.fetch_trades(
                market=market,
                start_time=current_start,
                end_time=end_time,
                limit=500
            )
            
            if not trades:
                break
            
            all_trades.extend(trades)
            
            # Get the timestamp of the last trade to continue from there
            last_trade_ts = trades[-1].get('timestamp', 0)
            
            # If we got less than the limit, we've fetched everything
            if len(trades) < 500:
                break
            
            # Move to the next batch (start after the last trade)
            current_start = last_trade_ts + 1
        
        return all_trades


# Example usage
if __name__ == "__main__":
    # Example: Fetch trades for a market
    with TradeFetcher() as fetcher:
        # Replace with actual market ID and timestamps
        market_id = "0x1234567890abcdef1234567890abcdef12345678"
        start_ts = int(datetime(2024, 12, 1).timestamp())
        end_ts = int(datetime(2024, 12, 2).timestamp())
        
        print(f"Fetching trades for market {market_id[:10]}...")
        print(f"Time range: {start_ts} to {end_ts}")
        
        trades = fetcher.fetch_all_trades(
            market=market_id,
            start_time=start_ts,
            end_time=end_ts
        )
        
        print(f"\nFetched {len(trades)} total trades")
        
        if trades:
            print(f"\nFirst trade:")
            print(trades[0])
