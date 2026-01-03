"""
Settings service for persistent configuration storage.
"""

import json
from datetime import datetime
from typing import Optional, Any
import duckdb


class SettingsService:
    """
    Service for managing persistent application settings.

    Usage:
        service = SettingsService(conn)
        service.set("selected_models", ["llama3.2", "mistral"])
        models = service.get("selected_models", default=[])
    """

    # Setting keys
    SELECTED_MODELS = "selected_models"
    REQUIRE_UNANIMOUS = "require_unanimous"
    MIN_VOTES_FOR_MAJORITY = "min_votes_for_majority"

    def __init__(self, conn: duckdb.DuckDBPyConnection):
        self.conn = conn

    def get(self, key: str, default: Any = None) -> Any:
        """
        Get a setting value by key.

        Args:
            key: The setting key
            default: Default value if key not found

        Returns:
            The setting value (JSON-decoded) or default
        """
        row = self.conn.execute(
            "SELECT value FROM Settings WHERE key = ?",
            [key]
        ).fetchone()

        if not row:
            return default

        try:
            return json.loads(row[0])
        except (json.JSONDecodeError, TypeError):
            return row[0]

    def set(self, key: str, value: Any) -> None:
        """
        Set a setting value.

        Args:
            key: The setting key
            value: The value to store (will be JSON-encoded)
        """
        json_value = json.dumps(value)
        now = datetime.now()

        # Try update first, then insert if not exists
        result = self.conn.execute(
            """
            UPDATE Settings
            SET value = ?, updated_at = ?
            WHERE key = ?
            """,
            [json_value, now, key]
        )

        if result.rowcount == 0:
            self.conn.execute(
                """
                INSERT INTO Settings (key, value, updated_at)
                VALUES (?, ?, ?)
                """,
                [key, json_value, now]
            )

    def delete(self, key: str) -> bool:
        """
        Delete a setting.

        Args:
            key: The setting key to delete

        Returns:
            True if setting was deleted, False if not found
        """
        result = self.conn.execute(
            "DELETE FROM Settings WHERE key = ?",
            [key]
        )
        return result.rowcount > 0

    def get_all(self) -> dict[str, Any]:
        """
        Get all settings as a dictionary.

        Returns:
            Dictionary of all settings
        """
        rows = self.conn.execute(
            "SELECT key, value FROM Settings"
        ).fetchall()

        settings = {}
        for key, value in rows:
            try:
                settings[key] = json.loads(value)
            except (json.JSONDecodeError, TypeError):
                settings[key] = value

        return settings

    # Convenience methods for common settings

    def get_selected_models(self) -> list[str]:
        """Get the list of selected models for classification."""
        return self.get(self.SELECTED_MODELS, default=[])

    def set_selected_models(self, models: list[str]) -> None:
        """Set the list of selected models for classification."""
        self.set(self.SELECTED_MODELS, models)

    def get_require_unanimous(self) -> bool:
        """Get whether unanimous consensus is required."""
        return self.get(self.REQUIRE_UNANIMOUS, default=False)

    def set_require_unanimous(self, value: bool) -> None:
        """Set whether unanimous consensus is required."""
        self.set(self.REQUIRE_UNANIMOUS, value)

    def get_min_votes_for_majority(self) -> int:
        """Get minimum votes needed for majority consensus."""
        return self.get(self.MIN_VOTES_FOR_MAJORITY, default=2)

    def set_min_votes_for_majority(self, value: int) -> None:
        """Set minimum votes needed for majority consensus."""
        self.set(self.MIN_VOTES_FOR_MAJORITY, value)
