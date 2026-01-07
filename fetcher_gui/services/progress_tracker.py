"""
Progress Tracker Service

Tracks loading progress across all data types by reading:
- Parquet file metadata (counts, dates)
- Cursor file for pending progress
"""

import json
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional
import os

# Try to import pyarrow for parquet reading
try:
    import pyarrow.parquet as pq
    HAS_PYARROW = True
except ImportError:
    HAS_PYARROW = False


class ProgressTracker:
    """
    Tracks data loading progress by analyzing parquet files and cursors.
    """

    def __init__(self, data_dir: Optional[str] = None, cursor_file: Optional[str] = None):
        """
        Initialize the progress tracker.

        Args:
            data_dir: Path to data directory (default: project_root/data)
            cursor_file: Path to cursor file (default: project_root/fetcher/cursor.json)
        """
        project_root = Path(__file__).parent.parent.parent

        self._data_dir = Path(data_dir) if data_dir else project_root / "data"
        self._cursor_file = Path(cursor_file) if cursor_file else project_root / "fetcher" / "cursor.json"

        # Cache for file stats
        self._cache: Dict[str, Any] = {}
        self._cache_time: Optional[datetime] = None
        self._cache_ttl_seconds = 30  # Cache for 30 seconds

    def refresh(self) -> None:
        """Force refresh the cache."""
        self._cache = {}
        self._cache_time = None

    def _is_cache_valid(self) -> bool:
        """Check if cache is still valid."""
        if self._cache_time is None:
            return False
        elapsed = (datetime.now() - self._cache_time).total_seconds()
        return elapsed < self._cache_ttl_seconds

    def _count_parquet_files(self, data_type: str) -> Dict[str, Any]:
        """
        Count records and get latest date for a data type.

        Args:
            data_type: One of 'markets', 'trades', 'prices', 'leaderboard', etc.

        Returns:
            Dict with count, file_count, last_date
        """
        type_dir = self._data_dir / data_type

        if not type_dir.exists():
            return {"count": 0, "file_count": 0, "last_date": None}

        total_count = 0
        file_count = 0
        latest_date = None

        # Find all parquet files in date partitions
        for partition_dir in type_dir.iterdir():
            if partition_dir.is_dir() and partition_dir.name.startswith("dt="):
                # Extract date from partition name (e.g., dt=2024-12-26)
                date_str = partition_dir.name.replace("dt=", "")

                if latest_date is None or date_str > latest_date:
                    latest_date = date_str

                # Count parquet files
                for parquet_file in partition_dir.glob("*.parquet"):
                    file_count += 1

                    if HAS_PYARROW:
                        try:
                            metadata = pq.read_metadata(str(parquet_file))
                            total_count += metadata.num_rows
                        except Exception:
                            # If we can't read metadata, estimate based on file size
                            # Rough estimate: 1KB per 10 rows
                            size_kb = parquet_file.stat().st_size / 1024
                            total_count += int(size_kb * 10)
                    else:
                        # Without pyarrow, estimate based on file size
                        size_kb = parquet_file.stat().st_size / 1024
                        total_count += int(size_kb * 10)

        return {
            "count": total_count,
            "file_count": file_count,
            "last_date": latest_date
        }

    def get_load_status(self) -> Dict[str, Dict[str, Any]]:
        """
        Get loading status for all data types.

        Returns:
            Dict mapping data type to status info
        """
        if self._is_cache_valid() and "load_status" in self._cache:
            return self._cache["load_status"]

        data_types = [
            "markets",
            "trades",
            "prices",
            "leaderboard",
            "market_tokens",
            "gamma_markets",
            "gamma_events",
            "gamma_categories"
        ]

        status = {}
        for data_type in data_types:
            status[data_type] = self._count_parquet_files(data_type)

        self._cache["load_status"] = status
        self._cache_time = datetime.now()

        return status

    def get_cursor_status(self) -> Dict[str, Any]:
        """
        Get the current cursor status.

        Returns:
            Dict with cursor information for each data type
        """
        if not self._cursor_file.exists():
            return {
                "has_progress": False,
                "last_updated": None,
                "markets": {"is_empty": True},
                "trades": {"is_empty": True},
                "prices": {"is_empty": True},
                "leaderboard": {"is_empty": True},
                "gamma_markets": {"is_empty": True}
            }

        try:
            with open(self._cursor_file, "r") as f:
                data = json.load(f)
        except (json.JSONDecodeError, IOError):
            return {
                "has_progress": False,
                "last_updated": None,
                "error": "Could not read cursor file"
            }

        # Parse market cursor
        markets_data = data.get("markets", {})
        markets_cursor = {
            "is_empty": not markets_data.get("next_cursor") and not markets_data.get("completed"),
            "next_cursor": markets_data.get("next_cursor", "")[:20] + "..." if markets_data.get("next_cursor") else "",
            "completed": markets_data.get("completed", False)
        }

        # Parse trade cursor
        trades_data = data.get("trades", {})
        pending_markets = trades_data.get("pending_markets", [])
        trades_cursor = {
            "is_empty": not trades_data.get("market") and not pending_markets,
            "current_market": trades_data.get("market", "")[:16] + "..." if trades_data.get("market") else "",
            "offset": trades_data.get("offset", 0),
            "filter_amount": trades_data.get("filter_amount", 0),
            "pending_markets": len(pending_markets)
        }

        # Parse price cursor
        prices_data = data.get("prices", {})
        pending_tokens = prices_data.get("pending_tokens", [])
        prices_cursor = {
            "is_empty": not prices_data.get("token_id") and not pending_tokens and not prices_data.get("completed"),
            "current_token": prices_data.get("token_id", "")[:16] + "..." if prices_data.get("token_id") else "",
            "pending_tokens": len(pending_tokens),
            "completed": prices_data.get("completed", False)
        }

        # Parse leaderboard cursor
        lb_data = data.get("leaderboard", {})
        leaderboard_cursor = {
            "is_empty": (
                lb_data.get("current_category_index", 0) == 0 and
                lb_data.get("current_time_period_index", 0) == 0 and
                lb_data.get("current_offset", 0) == 0 and
                not lb_data.get("completed", False)
            ),
            "category_index": lb_data.get("current_category_index", 0),
            "time_period_index": lb_data.get("current_time_period_index", 0),
            "offset": lb_data.get("current_offset", 0),
            "completed": lb_data.get("completed", False)
        }

        # Parse gamma market cursor
        gamma_data = data.get("gamma_markets", {})
        gamma_cursor = {
            "is_empty": not gamma_data.get("completed", False),
            "completed": gamma_data.get("completed", False)
        }

        # Check if there's any progress
        has_progress = (
            not markets_cursor["is_empty"] or
            not trades_cursor["is_empty"] or
            not prices_cursor["is_empty"] or
            not leaderboard_cursor["is_empty"] or
            not gamma_cursor["is_empty"]
        )

        return {
            "has_progress": has_progress,
            "last_updated": data.get("last_updated"),
            "markets": markets_cursor,
            "trades": trades_cursor,
            "prices": prices_cursor,
            "leaderboard": leaderboard_cursor,
            "gamma_markets": gamma_cursor
        }

    def get_last_load_date(self) -> Optional[str]:
        """
        Get the most recent load date across all data types.

        Returns:
            ISO date string or None
        """
        status = self.get_load_status()

        latest = None
        for data_type, info in status.items():
            date = info.get("last_date")
            if date and (latest is None or date > latest):
                latest = date

        return latest

    def get_summary(self) -> Dict[str, Any]:
        """
        Get a summary of all progress information.

        Returns:
            Dict with load_status, cursor_status, and last_load_date
        """
        return {
            "load_status": self.get_load_status(),
            "cursor_status": self.get_cursor_status(),
            "last_load_date": self.get_last_load_date()
        }
