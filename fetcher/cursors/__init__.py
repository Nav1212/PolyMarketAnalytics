"""
Cursors module - Manages cursor state, persistence, and resume logic.

This module isolates all cursor-related functionality from ingestion,
networking, and orchestration logic.

Exports:
    - CursorManager: Main class for cursor state management
    - get_cursor_manager: Get global singleton instance
    - set_cursor_manager: Set global singleton instance
    - TradeCursor, PriceCursor, LeaderboardCursor, MarketCursor: Dataclasses
    - Cursors: Container for all cursor types
"""

from fetcher.cursors.manager import (
    CursorManager,
    get_cursor_manager,
    set_cursor_manager,
    TradeCursor,
    PriceCursor,
    LeaderboardCursor,
    MarketCursor,
    Cursors,
)

__all__ = [
    "CursorManager",
    "get_cursor_manager",
    "set_cursor_manager",
    "TradeCursor",
    "PriceCursor",
    "LeaderboardCursor",
    "MarketCursor",
    "Cursors",
]
