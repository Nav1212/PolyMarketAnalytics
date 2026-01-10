"""
Main entry point for the Kalshi Data Fetcher

Supports cursor persistence for resumable fetching:
- On interrupt/shutdown, cursors are saved to kalshi_cursor.json
- On startup, if kalshi_cursor.json exists, fetching resumes from last position
- Use --fresh flag to ignore existing cursors

Usage:
    # Full pipeline (events → markets → trades + orderbooks)
    python -m kalshi_fetcher.main --mode=all
    
    # Events only
    python -m kalshi_fetcher.main --mode=events --limit=100
    
    # Markets only
    python -m kalshi_fetcher.main --mode=markets
    
    # Fresh start (ignore cursors)
    python -m kalshi_fetcher.main --mode=all --fresh
"""

import argparse
import sys
from typing import Optional

from kalshi_fetcher.config import get_config
from kalshi_fetcher.coordination import FetcherCoordinator
from kalshi_fetcher.cursors import get_cursor_manager
from kalshi_fetcher.utils.logging_config import get_logger, setup_file_logging

logger = get_logger("main")


def run_pipeline(
    mode: str = "all",
    fresh: bool = False,
    limit: Optional[int] = None,
) -> dict:
    """
    Run the Kalshi data pipeline.
    
    Args:
        mode: Pipeline mode (all, events, markets)
        fresh: If True, ignore existing cursors
        limit: Maximum items to fetch
    
    Returns:
        Dict with run statistics
    """
    config = get_config()
    coordinator = FetcherCoordinator(config=config)
    
    if mode == "all":
        return coordinator.run_all(fresh=fresh, event_limit=limit)
    elif mode == "events":
        count = coordinator.run_events(limit=limit, fresh=fresh)
        return {"events": count}
    elif mode == "markets":
        count = coordinator.run_markets(limit=limit, fresh=fresh)
        return {"markets": count}
    else:
        logger.error(f"Unknown mode: {mode}")
        return {}


def main():
    """Main entry point with CLI argument parsing."""
    parser = argparse.ArgumentParser(
        description="Kalshi Data Pipeline - Fetch prediction market data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Full pipeline
    python -m kalshi_fetcher.main --mode=all
    
    # Events only with limit
    python -m kalshi_fetcher.main --mode=events --limit=100
    
    # Fresh start (ignore saved cursors)
    python -m kalshi_fetcher.main --mode=all --fresh
    
    # Show current cursor status
    python -m kalshi_fetcher.main --status
        """
    )
    
    parser.add_argument(
        "--mode",
        choices=["all", "events", "markets"],
        default="all",
        help="Pipeline mode (default: all)"
    )
    
    parser.add_argument(
        "--fresh",
        action="store_true",
        help="Start fresh, ignoring saved cursors"
    )
    
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum items to fetch"
    )
    
    parser.add_argument(
        "--status",
        action="store_true",
        help="Show current cursor status and exit"
    )
    
    parser.add_argument(
        "--log-file",
        action="store_true",
        help="Enable file logging to logs/kalshi.log"
    )
    
    args = parser.parse_args()
    
    # Setup file logging if requested
    if args.log_file:
        setup_file_logging()
    
    # Show status and exit
    if args.status:
        cursor_manager = get_cursor_manager()
        cursor_manager.load_cursors()
        cursors = cursor_manager.cursors
        
        print("\n=== Kalshi Fetcher Cursor Status ===")
        print(f"Last Updated: {cursors.last_updated or 'Never'}")
        print()
        
        print("Events:")
        print(f"  Cursor: {cursors.events.cursor or '(none)'}")
        print(f"  Completed: {cursors.events.completed}")
        print()
        
        print("Markets:")
        print(f"  Cursor: {cursors.markets.cursor or '(none)'}")
        print(f"  Event: {cursors.markets.event_ticker or '(none)'}")
        print(f"  Pending Events: {len(cursors.markets.pending_events)}")
        print(f"  Completed: {cursors.markets.completed}")
        print()
        
        print("Trades:")
        print(f"  Cursor: {cursors.trades.cursor or '(none)'}")
        print(f"  Ticker: {cursors.trades.ticker or '(none)'}")
        print(f"  Pending: {len(cursors.trades.pending_tickers)}")
        print(f"  Completed: {cursors.trades.completed}")
        print()
        
        print("Orderbook:")
        print(f"  Ticker: {cursors.orderbook.ticker or '(none)'}")
        print(f"  Pending: {len(cursors.orderbook.pending_tickers)}")
        print(f"  Completed: {cursors.orderbook.completed}")
        print()
        
        return
    
    # Run the pipeline
    try:
        logger.info(f"Starting Kalshi pipeline: mode={args.mode}, fresh={args.fresh}")
        
        stats = run_pipeline(
            mode=args.mode,
            fresh=args.fresh,
            limit=args.limit,
        )
        
        print("\n=== Pipeline Complete ===")
        for key, value in stats.items():
            if key == "rate_limit_stats":
                print(f"\nRate Limit Stats:")
                for worker, worker_stats in value.items():
                    if worker_stats.get("count", 0) > 0:
                        print(f"  {worker}: {worker_stats}")
            else:
                print(f"  {key}: {value}")
        
    except KeyboardInterrupt:
        print("\n\nInterrupted by user. Cursors saved for resume.")
        sys.exit(130)
    except Exception as e:
        logger.exception(f"Pipeline failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
