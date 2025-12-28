"""
Bronze to Silver Layer Load Script

Main entry point for loading bronze parquet data into silver DuckDB tables.

Usage:
    # Default paths
    python -m Ingestion.load_silver

    # Custom paths
    python -m Ingestion.load_silver --bronze data --silver path/to/silver.duckdb

    # Specific tables only
    python -m Ingestion.load_silver --tables MarketDim MarketTokenDim

    # Verbose logging
    python -m Ingestion.load_silver --verbose
"""

import argparse
import sys
from pathlib import Path

from fetcher.utils.logging_config import setup_logging, get_logger
from Ingestion.silver_loader import SilverLoadCoordinator, WorkerConfig
from Ingestion.silver_create import DEFAULT_SILVER_DB_PATH


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Load bronze parquet data into silver DuckDB tables",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Run full load with defaults
    python -m Ingestion.load_silver

    # Specify custom paths
    python -m Ingestion.load_silver --bronze ./data --silver ./silver.duckdb

    # Adjust worker scaling
    python -m Ingestion.load_silver --min-workers 2 --max-workers 16

    # Enable verbose logging
    python -m Ingestion.load_silver --verbose
        """
    )

    parser.add_argument(
        "--bronze",
        type=str,
        default="data",
        help="Path to bronze data directory (default: data)"
    )

    parser.add_argument(
        "--silver",
        type=str,
        default=None,
        help=f"Path to silver DuckDB file (default: {DEFAULT_SILVER_DB_PATH})"
    )

    parser.add_argument(
        "--min-workers",
        type=int,
        default=1,
        help="Minimum number of workers (default: 1)"
    )

    parser.add_argument(
        "--max-workers",
        type=int,
        default=8,
        help="Maximum number of workers (default: 8)"
    )

    parser.add_argument(
        "--files-per-worker",
        type=int,
        default=10,
        help="Number of parquet files per worker (default: 10)"
    )

    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose logging"
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be loaded without making changes"
    )

    return parser.parse_args()


def main():
    """Main entry point."""
    args = parse_args()

    # Setup logging
    import logging
    level = logging.DEBUG if args.verbose else logging.INFO
    setup_logging(level=level)

    logger = get_logger("load_silver")

    logger.info("="*60)
    logger.info("Bronze → Silver Layer Load")
    logger.info("="*60)
    logger.info(f"Bronze path: {args.bronze}")
    logger.info(f"Silver path: {args.silver or DEFAULT_SILVER_DB_PATH}")
    logger.info(f"Workers: {args.min_workers}-{args.max_workers}")

    if args.dry_run:
        logger.info("DRY RUN - No changes will be made")
        # Just count files and exit
        bronze_path = Path(args.bronze)
        for dir_name in ["markets", "gamma_markets", "market_tokens", "trades", "leaderboard"]:
            dir_path = bronze_path / dir_name
            if dir_path.exists():
                count = len(list(dir_path.glob("**/*.parquet")))
                logger.info(f"  {dir_name}: {count} parquet files")
        return 0

    # Create worker config
    worker_config = WorkerConfig(
        min_workers=args.min_workers,
        max_workers=args.max_workers,
        files_per_worker=args.files_per_worker
    )

    # Create and run coordinator
    coordinator = SilverLoadCoordinator(
        bronze_base_path=Path(args.bronze),
        silver_db_path=Path(args.silver) if args.silver else None,
        worker_config=worker_config
    )

    results = coordinator.run()

    # Return exit code
    failures = [r for r in results if not r.success]
    if failures:
        logger.error(f"{len(failures)} transformer(s) failed")
        return 1

    logger.info("All transformers completed successfully")
    return 0


if __name__ == "__main__":
    sys.exit(main())
