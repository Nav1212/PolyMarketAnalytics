"""
Main entry point for the Polymarket Trade Fetcher
"""

from datetime import datetime, timedelta
from trade_fetcher import TradeFetcher


def main():
    """
    Main function to fetch trades from Polymarket
    """
    # Initialize the fetcher
    with TradeFetcher() as fetcher:
        # Example market ID - replace with actual market condition_id
        market_id = "0x1234567890abcdef1234567890abcdef12345678"
        
        # Set time range - last 24 hours as example
        end_time = datetime.now()
        start_time = end_time - timedelta(hours=24)
        
        # Convert to Unix timestamps
        start_ts = int(start_time.timestamp())
        end_ts = int(end_time.timestamp())
        
        print("=" * 60)
        print("Polymarket Trade Fetcher")
        print("=" * 60)
        print(f"Market ID: {market_id}")
        print(f"Start Time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"End Time: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 60)
        
        # Fetch all trades in the time range
        print("\nFetching trades...")
        trades = fetcher.fetch_all_trades(
            market=market_id,
            start_time=start_ts,
            end_time=end_ts
        )
        
        print(f"\n✓ Fetched {len(trades)} trades")
        
        # Display sample trades if available
        if trades:
            print("\n" + "=" * 60)
            print("Sample Trades (first 3):")
            print("=" * 60)
            for i, trade in enumerate(trades[:3], 1):
                print(f"\nTrade {i}:")
                for key, value in trade.items():
                    print(f"  {key}: {value}")
        else:
            print("\nNo trades found for the specified time range.")


if __name__ == "__main__":
    main()
