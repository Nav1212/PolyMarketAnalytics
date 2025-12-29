"""Database utilities for tag manager."""

from tag_manager.db.connection import get_connection, init_schema, DEFAULT_DB_PATH

__all__ = ["get_connection", "init_schema", "DEFAULT_DB_PATH"]
