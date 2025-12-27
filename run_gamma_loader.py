"""Run the Gamma Market Fetcher standalone."""

from fetcher.workers import GammaMarketFetcher
from fetcher.cursors import get_cursor_manager

def main():
    # Reset cursor to allow a fresh run
    cursor_manager = get_cursor_manager()
    cursor_manager.load_cursors()
    cursor_manager.clear_gamma_market_cursor()
    cursor_manager.save_cursors()
    print("Cursor reset. Starting Gamma market fetch...")

    # Run the fetcher with parquet persistence
    fetcher = GammaMarketFetcher()
    try:
        result = fetcher.fetch_all_markets_to_parquet()
        print(f"\nDone!")
        print(f"  Markets: {result['markets']}")
        print(f"  Events: {result['events']}")
        print(f"  Categories: {result['categories']}")
    finally:
        fetcher.close()

if __name__ == "__main__":
    main()
