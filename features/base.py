"""
Base Feature Extractor

Provides common functionality for all feature extractors:
- Database connection management
- Caching
- Feature composition
"""

from abc import ABC, abstractmethod
from typing import Optional, List, Dict, Any
from pathlib import Path
import duckdb
import pandas as pd

# Default database path (same as silver layer)
DEFAULT_SILVER_DB_PATH = Path(r"C:\Users\User\Desktop\VibeCoding\PolyMarketData\silver.duckdb")


class BaseFeatureExtractor(ABC):
    """
    Abstract base class for feature extractors.

    All feature extractors should inherit from this class and implement
    the extract() method.
    """

    def __init__(
        self,
        conn: Optional[duckdb.DuckDBPyConnection] = None,
        db_path: Optional[Path] = None,
    ):
        """
        Initialize the feature extractor.

        Args:
            conn: Existing DuckDB connection (optional)
            db_path: Path to DuckDB file (uses default if not provided)
        """
        self._owns_connection = conn is None
        if conn is None:
            db_path = db_path or DEFAULT_SILVER_DB_PATH
            self.conn = duckdb.connect(str(db_path), read_only=True)
        else:
            self.conn = conn

        self._cache: Dict[str, pd.DataFrame] = {}

    def close(self) -> None:
        """Close the database connection if we own it."""
        if self._owns_connection:
            self.conn.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    @abstractmethod
    def extract(self, **kwargs) -> pd.DataFrame:
        """
        Extract features based on provided filters.

        Returns:
            DataFrame with extracted features
        """
        pass

    @abstractmethod
    def get_feature_names(self) -> List[str]:
        """
        Return list of feature column names this extractor produces.

        Returns:
            List of feature names
        """
        pass

    def clear_cache(self) -> None:
        """Clear the feature cache."""
        self._cache.clear()

    def _cache_key(self, **kwargs) -> str:
        """Generate a cache key from keyword arguments."""
        items = sorted(kwargs.items())
        return str(items)

    def _get_cached(self, **kwargs) -> Optional[pd.DataFrame]:
        """Get cached result if available."""
        key = self._cache_key(**kwargs)
        return self._cache.get(key)

    def _set_cached(self, df: pd.DataFrame, **kwargs) -> None:
        """Cache a result."""
        key = self._cache_key(**kwargs)
        self._cache[key] = df
