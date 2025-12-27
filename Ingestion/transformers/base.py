"""
Base Transformer Class

Abstract base class for all bronze-to-silver transformers.
Defines the interface and common functionality for data transformation.
"""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime
import duckdb

from fetcher.utils.logging_config import get_logger


class BaseTransformer(ABC):
    """
    Abstract base class for bronze-to-silver data transformers.

    Each transformer is responsible for:
    1. Reading parquet files from bronze layer
    2. Transforming/normalizing the data
    3. Loading into the appropriate silver layer table

    Subclasses must implement:
    - transform(): Core transformation logic
    - get_bronze_path(): Path to bronze parquet files
    - get_table_name(): Target silver table name
    """

    def __init__(
        self,
        conn: duckdb.DuckDBPyConnection,
        bronze_base_path: Path,
        logger_name: Optional[str] = None
    ):
        """
        Initialize the transformer.

        Args:
            conn: DuckDB connection to silver database
            bronze_base_path: Base path to bronze data directory
            logger_name: Optional logger name (defaults to class name)
        """
        self.conn = conn
        self.bronze_base_path = Path(bronze_base_path)
        self.logger = get_logger(logger_name or self.__class__.__name__)

        self._records_processed = 0
        self._records_inserted = 0
        self._records_updated = 0
        self._records_skipped = 0

    @abstractmethod
    def get_bronze_path(self) -> Path:
        """Return the path to bronze parquet files for this transformer."""
        pass

    @abstractmethod
    def get_table_name(self) -> str:
        """Return the target silver table name."""
        pass

    @abstractmethod
    def transform(self) -> int:
        """
        Execute the transformation.

        Returns:
            Number of records processed
        """
        pass

    def get_parquet_files(self) -> List[Path]:
        """
        Get all parquet files from the bronze path.

        Returns:
            List of parquet file paths, sorted by modification time
        """
        bronze_path = self.get_bronze_path()

        if not bronze_path.exists():
            self.logger.warning(f"Bronze path does not exist: {bronze_path}")
            return []

        # Find all parquet files (including in Hive partitions)
        files = list(bronze_path.glob("**/*.parquet"))

        # Sort by modification time (oldest first for consistent processing)
        files.sort(key=lambda f: f.stat().st_mtime)

        self.logger.info(f"Found {len(files)} parquet files in {bronze_path}")
        return files

    def count_parquet_files(self) -> int:
        """Return the number of parquet files in bronze path."""
        return len(self.get_parquet_files())

    def read_bronze_data(self, query: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Read data from bronze parquet files.

        Args:
            query: Optional SQL query (use 'bronze' as table alias)
                   If not provided, reads all data.

        Returns:
            List of dictionaries with the data
        """
        bronze_path = self.get_bronze_path()

        if not bronze_path.exists():
            self.logger.warning(f"Bronze path does not exist: {bronze_path}")
            return []

        try:
            # Create a temporary in-memory connection for reading
            read_conn = duckdb.connect(":memory:")

            glob_pattern = str(bronze_path / "**" / "*.parquet")
            read_conn.execute(f"""
                CREATE VIEW bronze AS
                SELECT * FROM read_parquet('{glob_pattern}', hive_partitioning=true)
            """)

            if query:
                result = read_conn.execute(query).fetchdf()
            else:
                result = read_conn.execute("SELECT * FROM bronze").fetchdf()

            read_conn.close()

            records = result.to_dict('records')
            self.logger.info(f"Read {len(records)} records from bronze")
            return records

        except Exception as e:
            self.logger.error(f"Error reading bronze data: {e}")
            return []

    def get_stats(self) -> Dict[str, int]:
        """Return transformation statistics."""
        return {
            "records_processed": self._records_processed,
            "records_inserted": self._records_inserted,
            "records_updated": self._records_updated,
            "records_skipped": self._records_skipped,
        }

    def reset_stats(self) -> None:
        """Reset transformation statistics."""
        self._records_processed = 0
        self._records_inserted = 0
        self._records_updated = 0
        self._records_skipped = 0
