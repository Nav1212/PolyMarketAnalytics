"""
Silver Layer Loader Coordinator

Orchestrates batch loading from bronze parquet files to silver DuckDB tables.
Spawns workers based on the number of parquet files in bronze.

Architecture:
- MarketDim is preloaded first (blocking)
- Other dimension tables run in parallel after MarketDim completes
- Worker count scales with parquet file count

Usage:
    from Ingestion.silver_loader import SilverLoadCoordinator

    coordinator = SilverLoadCoordinator()
    coordinator.run()
"""

import threading
from pathlib import Path
from typing import Dict, Any, List, Optional, Type
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import duckdb

from Ingestion.silver_create import create_silver_schema, DEFAULT_SILVER_DB_PATH
from Ingestion.transformers.base import BaseTransformer
from Ingestion.transformers.market_dim import MarketDimTransformer
from Ingestion.transformers.market_token_dim import MarketTokenDimTransformer
from Ingestion.transformers.trader_dim import TraderDimTransformer
from fetcher.utils.logging_config import get_logger


logger = get_logger("SilverLoadCoordinator")


@dataclass
class WorkerConfig:
    """Configuration for worker scaling."""
    min_workers: int = 1
    max_workers: int = 8
    files_per_worker: int = 10  # Spawn 1 worker per N parquet files


@dataclass
class LoadResult:
    """Result of a transformer load operation."""
    transformer_name: str
    records_processed: int
    records_inserted: int
    records_updated: int
    records_skipped: int
    duration_seconds: float
    success: bool
    error: Optional[str] = None


class SilverLoadCoordinator:
    """
    Coordinates batch loading from bronze to silver layer.

    Load order:
    1. MarketDim (blocking - must complete first)
    2. MarketTokenDim, TraderDim (parallel after MarketDim)

    Worker scaling:
    - Counts parquet files in bronze directories
    - Scales worker count based on file count
    """

    def __init__(
        self,
        bronze_base_path: Optional[Path] = None,
        silver_db_path: Optional[Path] = None,
        worker_config: Optional[WorkerConfig] = None,
        nlp_enricher: Optional[Any] = None
    ):
        """
        Initialize the coordinator.

        Args:
            bronze_base_path: Path to bronze data directory
            silver_db_path: Path to silver DuckDB database
            worker_config: Worker scaling configuration
            nlp_enricher: Optional NLP enricher for MarketDim (placeholder)
        """
        self.bronze_base_path = bronze_base_path or Path("data")
        self.silver_db_path = silver_db_path or DEFAULT_SILVER_DB_PATH
        self.worker_config = worker_config or WorkerConfig()
        self.nlp_enricher = nlp_enricher

        self._conn: Optional[duckdb.DuckDBPyConnection] = None
        self._results: List[LoadResult] = []
        self._lock = threading.Lock()

    def run(self) -> List[LoadResult]:
        """
        Execute the full bronze-to-silver load.

        Returns:
            List of LoadResult for each transformer
        """
        logger.info("="*60)
        logger.info("Starting Bronze → Silver Load")
        logger.info("="*60)

        start_time = datetime.now()

        # Initialize silver database
        self._init_silver_db()

        # Calculate worker count based on parquet files
        worker_count = self._calculate_worker_count()
        logger.info(f"Worker count: {worker_count}")

        # Phase 1: Preload MarketDim (blocking)
        logger.info("-"*40)
        logger.info("Phase 1: Preloading MarketDim")
        logger.info("-"*40)
        market_result = self._run_market_dim_transformer()
        self._results.append(market_result)

        if not market_result.success:
            logger.error("MarketDim failed - aborting load")
            return self._results

        # Phase 2: Load other dimension tables (parallel)
        logger.info("-"*40)
        logger.info("Phase 2: Loading dimension tables (parallel)")
        logger.info("-"*40)
        parallel_results = self._run_parallel_transformers(worker_count)
        self._results.extend(parallel_results)

        # Close connection
        if self._conn:
            self._conn.close()

        # Summary
        duration = (datetime.now() - start_time).total_seconds()
        self._log_summary(duration)

        return self._results

    def _init_silver_db(self) -> None:
        """Initialize the silver database and connection."""
        logger.info(f"Initializing silver database at: {self.silver_db_path}")
        self._conn = create_silver_schema(db_path=self.silver_db_path)

    def _calculate_worker_count(self) -> int:
        """
        Calculate worker count based on parquet file count.

        Returns:
            Number of workers to use
        """
        total_files = 0

        # Count files in each bronze directory
        directories = ["markets", "gamma_markets", "market_tokens", "trades", "leaderboard"]

        for dir_name in directories:
            dir_path = self.bronze_base_path / dir_name
            if dir_path.exists():
                file_count = len(list(dir_path.glob("**/*.parquet")))
                total_files += file_count
                logger.info(f"  {dir_name}: {file_count} parquet files")

        # Calculate workers
        workers = max(
            self.worker_config.min_workers,
            min(
                self.worker_config.max_workers,
                total_files // self.worker_config.files_per_worker
            )
        )

        logger.info(f"Total parquet files: {total_files}")
        return workers

    def _run_market_dim_transformer(self) -> LoadResult:
        """
        Run the MarketDim transformer (blocking).

        Returns:
            LoadResult for MarketDim transformation
        """
        start_time = datetime.now()
        transformer_name = "MarketDimTransformer"

        try:
            transformer = MarketDimTransformer(
                conn=self._conn,
                bronze_base_path=self.bronze_base_path,
            )

            records = transformer.transform()
            stats = transformer.get_stats()
            duration = (datetime.now() - start_time).total_seconds()

            logger.info(f"MarketDim completed in {duration:.2f}s")

            return LoadResult(
                transformer_name=transformer_name,
                records_processed=records,
                records_inserted=stats['records_inserted'],
                records_updated=stats['records_updated'],
                records_skipped=stats['records_skipped'],
                duration_seconds=duration,
                success=True
            )

        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            logger.exception(f"MarketDim failed: {e}")

            return LoadResult(
                transformer_name=transformer_name,
                records_processed=0,
                records_inserted=0,
                records_updated=0,
                records_skipped=0,
                duration_seconds=duration,
                success=False,
                error=str(e)
            )

    def _run_parallel_transformers(self, worker_count: int) -> List[LoadResult]:
        """
        Run dimension transformers in parallel.

        Args:
            worker_count: Number of workers to use

        Returns:
            List of LoadResult for each transformer
        """
        results = []

        # Define transformers to run in parallel
        transformer_classes: List[Type[BaseTransformer]] = [
            MarketTokenDimTransformer,
            TraderDimTransformer,
        ]

        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            futures = {}

            for transformer_class in transformer_classes:
                # Each transformer gets its own connection for thread safety
                conn = duckdb.connect(str(self.silver_db_path))
                transformer = transformer_class(
                    conn=conn,
                    bronze_base_path=self.bronze_base_path
                )

                future = executor.submit(self._run_transformer, transformer, conn)
                futures[future] = transformer_class.__name__

            for future in as_completed(futures):
                transformer_name = futures[future]
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    logger.exception(f"{transformer_name} failed: {e}")
                    results.append(LoadResult(
                        transformer_name=transformer_name,
                        records_processed=0,
                        records_inserted=0,
                        records_updated=0,
                        records_skipped=0,
                        duration_seconds=0,
                        success=False,
                        error=str(e)
                    ))

        return results

    def _run_transformer(
        self,
        transformer: BaseTransformer,
        conn: duckdb.DuckDBPyConnection
    ) -> LoadResult:
        """
        Run a single transformer.

        Args:
            transformer: The transformer instance to run
            conn: DuckDB connection (will be closed after)

        Returns:
            LoadResult for the transformation
        """
        start_time = datetime.now()
        transformer_name = transformer.__class__.__name__

        try:
            records = transformer.transform()
            stats = transformer.get_stats()
            duration = (datetime.now() - start_time).total_seconds()

            logger.info(f"{transformer_name} completed in {duration:.2f}s")

            return LoadResult(
                transformer_name=transformer_name,
                records_processed=records,
                records_inserted=stats['records_inserted'],
                records_updated=stats['records_updated'],
                records_skipped=stats['records_skipped'],
                duration_seconds=duration,
                success=True
            )

        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            logger.exception(f"{transformer_name} failed: {e}")

            return LoadResult(
                transformer_name=transformer_name,
                records_processed=0,
                records_inserted=0,
                records_updated=0,
                records_skipped=0,
                duration_seconds=duration,
                success=False,
                error=str(e)
            )

        finally:
            conn.close()

    def _log_summary(self, total_duration: float) -> None:
        """Log a summary of all transformations."""
        logger.info("="*60)
        logger.info("Load Summary")
        logger.info("="*60)

        total_inserted = 0
        total_updated = 0
        total_skipped = 0
        failures = []

        for result in self._results:
            status = "✓" if result.success else "✗"
            logger.info(
                f"  {status} {result.transformer_name}: "
                f"{result.records_inserted} inserted, "
                f"{result.records_updated} updated, "
                f"{result.records_skipped} skipped "
                f"({result.duration_seconds:.2f}s)"
            )

            if result.success:
                total_inserted += result.records_inserted
                total_updated += result.records_updated
                total_skipped += result.records_skipped
            else:
                failures.append(result.transformer_name)

        logger.info("-"*40)
        logger.info(
            f"Total: {total_inserted} inserted, "
            f"{total_updated} updated, "
            f"{total_skipped} skipped"
        )
        logger.info(f"Duration: {total_duration:.2f}s")

        if failures:
            logger.warning(f"Failures: {', '.join(failures)}")
        else:
            logger.info("All transformers completed successfully")


def run_silver_load(
    bronze_path: Optional[str] = None,
    silver_path: Optional[str] = None
) -> List[LoadResult]:
    """
    Convenience function to run the silver load.

    Args:
        bronze_path: Optional path to bronze data directory
        silver_path: Optional path to silver database

    Returns:
        List of LoadResult for each transformer
    """
    coordinator = SilverLoadCoordinator(
        bronze_base_path=Path(bronze_path) if bronze_path else None,
        silver_db_path=Path(silver_path) if silver_path else None
    )
    return coordinator.run()


if __name__ == "__main__":
    import sys

    bronze = sys.argv[1] if len(sys.argv) > 1 else None
    silver = sys.argv[2] if len(sys.argv) > 2 else None

    results = run_silver_load(bronze, silver)

    # Exit with error code if any failures
    if any(not r.success for r in results):
        sys.exit(1)
