"""
Cursor Manager for Polymarket Fetcher
Handles persistence of fetch progress to enable resume from interruption.

Cursor Types:
    - trades: offset + market + filter_amount
    - prices: start_ts (datetime window) + token_id (market)
    - leaderboard: category + time_period + current markets list
"""

import json
import atexit
import signal
import threading
from pathlib import Path
from dataclasses import dataclass, field, asdict
from typing import Optional, List, Dict, Any
from datetime import datetime


@dataclass
class TradeCursor:
    """Cursor for trade fetching progress."""
    market: str = ""
    offset: int = 0
    filter_amount: int = 0
    pending_markets: List[str] = field(default_factory=list)
    
    def is_empty(self) -> bool:
        return not self.market and not self.pending_markets


@dataclass
class PriceCursor:
    """Cursor for price fetching progress."""
    token_id: str = ""
    start_ts: int = 0
    end_ts: int = 0
    pending_tokens: List[tuple] = field(default_factory=list)  # List of (token_id, market_start_ts)
    completed: bool = False
    
    def is_empty(self) -> bool:
        return not self.token_id and not self.pending_tokens and not self.completed


@dataclass
class LeaderboardCursor:
    """Cursor for leaderboard fetching progress through all category/time_period combinations."""
    current_category_index: int = 0  # Index into category enum list
    current_time_period_index: int = 0  # Index into time_period enum list
    current_offset: int = 0  # Offset within current combination
    completed: bool = False
    
    def is_empty(self) -> bool:
        return self.current_category_index == 0 and self.current_time_period_index == 0 and self.current_offset == 0 and not self.completed


@dataclass
class MarketCursor:
    """Cursor for market fetching progress."""
    next_cursor: str = ""
    completed: bool = False
    
    def is_empty(self) -> bool:
        return not self.next_cursor and not self.completed


@dataclass
class Cursors:
    """Container for all cursor types."""
    trades: TradeCursor = field(default_factory=TradeCursor)
    prices: PriceCursor = field(default_factory=PriceCursor)
    leaderboard: LeaderboardCursor = field(default_factory=LeaderboardCursor)
    markets: MarketCursor = field(default_factory=MarketCursor)
    last_updated: str = ""
    
    def has_any_progress(self) -> bool:
        """Check if any cursor has progress to resume from."""
        return (
            not self.trades.is_empty() or
            not self.prices.is_empty() or
            not self.leaderboard.is_empty() or
            not self.markets.is_empty()
        )


class CursorManager:
    """
    Manages cursor persistence for resumable fetching.
    
    Saves cursor state to a JSON file on shutdown or periodic intervals.
    Loads cursor state on startup to resume from last position.
    """
    
    def __init__(
        self,
        cursor_file: Optional[str] = None,
        auto_save: bool = True,
        enabled: bool = True
    ):
        """
        Initialize the cursor manager.
        
        Args:
            cursor_file: Path to cursor file (default: fetcher/cursor.json)
            auto_save: Whether to register shutdown handlers for auto-save
            enabled: Whether cursor persistence is enabled
        """
        self._enabled = enabled
        if cursor_file is None:
            self._cursor_file = Path(__file__).parent / "cursor.json"
        else:
            self._cursor_file = Path(cursor_file)
        
        self._cursors = Cursors()
        self._lock = threading.Lock()
        self._dirty = False
        
        if auto_save and enabled:
            self._register_shutdown_handlers()
    
    def _register_shutdown_handlers(self) -> None:
        """Register handlers to save cursors on program exit."""
        # Register atexit handler for normal exits
        atexit.register(self.save_cursors)
        
        # Register signal handlers for interrupts
        # Note: Signal handlers can only be registered in main thread
        try:
            signal.signal(signal.SIGINT, self._signal_handler)
            signal.signal(signal.SIGTERM, self._signal_handler)
        except ValueError:
            # Not in main thread, skip signal registration
            pass
    
    def _signal_handler(self, signum: int, frame) -> None:
        """Handle shutdown signals by saving cursors."""
        print(f"\nReceived signal {signum}, saving cursors...")
        self.save_cursors()
        # Re-raise to allow normal shutdown
        raise KeyboardInterrupt
    
    def load_cursors(self) -> Cursors:
        """
        Load cursors from file. Creates the file with default values if it doesn't exist.
        
        Returns:
            Cursors object with loaded or default values
        """
        if not self._enabled:
            return self._cursors
            
        with self._lock:
            if self._cursor_file.exists():
                try:
                    with open(self._cursor_file, 'r') as f:
                        data = json.load(f)
                    
                    # Parse trade cursor
                    trade_data = data.get('trades', {})
                    self._cursors.trades = TradeCursor(
                        market=trade_data.get('market', ''),
                        offset=trade_data.get('offset', 0),
                        filter_amount=trade_data.get('filter_amount', 0),
                        pending_markets=trade_data.get('pending_markets', [])
                    )
                    
                    # Parse price cursor
                    price_data = data.get('prices', {})
                    pending_tokens_raw = price_data.get('pending_tokens', [])
                    # Convert lists back to tuples
                    pending_tokens = [tuple(t) if isinstance(t, list) else t for t in pending_tokens_raw]
                    self._cursors.prices = PriceCursor(
                        token_id=price_data.get('token_id', ''),
                        start_ts=price_data.get('start_ts', 0),
                        end_ts=price_data.get('end_ts', 0),
                        pending_tokens=pending_tokens,
                        completed=price_data.get('completed', False)
                    )
                    
                    # Parse leaderboard cursor
                    lb_data = data.get('leaderboard', {})
                    self._cursors.leaderboard = LeaderboardCursor(
                        current_category_index=lb_data.get('current_category_index', 0),
                        current_time_period_index=lb_data.get('current_time_period_index', 0),
                        current_offset=lb_data.get('current_offset', 0),
                        completed=lb_data.get('completed', False)
                    )
                    
                    # Parse market cursor
                    market_data = data.get('markets', {})
                    self._cursors.markets = MarketCursor(
                        next_cursor=market_data.get('next_cursor', ''),
                        completed=market_data.get('completed', False)
                    )
                    
                    self._cursors.last_updated = data.get('last_updated', '')
                    
                    print(f"Loaded cursors from {self._cursor_file}")
                    if self._cursors.has_any_progress():
                        print("  → Resuming from previous progress")
                    
                except (json.JSONDecodeError, KeyError) as e:
                    print(f"Warning: Could not parse cursor file: {e}")
                    self._cursors = Cursors()
            else:
                # File doesn't exist - create it with default values
                print(f"Cursor file not found, creating: {self._cursor_file}")
                self._cursors = Cursors()
                self._dirty = True
                self._create_default_cursor_file()
            
            return self._cursors
    
    def _create_default_cursor_file(self) -> None:
        """Create cursor file with default empty values."""
        data = {
            'trades': {
                'market': '',
                'offset': 0,
                'filter_amount': 0,
                'pending_markets': []
            },
            'prices': {
                'token_id': '',
                'start_ts': 0,
                'end_ts': 0,
                'pending_tokens': [],
                'completed': False
            },
            'leaderboard': {
                'current_category_index': 0,
                'current_time_period_index': 0,
                'current_offset': 0,
                'completed': False
            },
            'markets': {
                'next_cursor': '',
                'completed': False
            },
            'last_updated': datetime.now().isoformat()
        }
        
        try:
            # Ensure parent directory exists
            self._cursor_file.parent.mkdir(parents=True, exist_ok=True)
            
            with open(self._cursor_file, 'w') as f:
                json.dump(data, f, indent=2)
            
            print(f"Created default cursor file: {self._cursor_file}")
            self._dirty = False
            
        except Exception as e:
            print(f"Error creating cursor file: {e}")
    
    def save_cursors(self) -> None:
        """Save current cursors to file."""
        if not self._enabled:
            return
            
        with self._lock:
            if not self._dirty and not self._cursors.has_any_progress():
                return
            
            self._cursors.last_updated = datetime.now().isoformat()
            
            data = {
                'trades': {
                    'market': self._cursors.trades.market,
                    'offset': self._cursors.trades.offset,
                    'filter_amount': self._cursors.trades.filter_amount,
                    'pending_markets': self._cursors.trades.pending_markets
                },
                'prices': {
                    'token_id': self._cursors.prices.token_id,
                    'start_ts': self._cursors.prices.start_ts,
                    'end_ts': self._cursors.prices.end_ts,
                    'pending_tokens': self._cursors.prices.pending_tokens,
                    'completed': self._cursors.prices.completed
                },
                'leaderboard': {
                    'current_category_index': self._cursors.leaderboard.current_category_index,
                    'current_time_period_index': self._cursors.leaderboard.current_time_period_index,
                    'current_offset': self._cursors.leaderboard.current_offset,
                    'completed': self._cursors.leaderboard.completed
                },
                'markets': {
                    'next_cursor': self._cursors.markets.next_cursor,
                    'completed': self._cursors.markets.completed
                },
                'last_updated': self._cursors.last_updated
            }
            
            try:
                # Ensure parent directory exists
                self._cursor_file.parent.mkdir(parents=True, exist_ok=True)
                
                with open(self._cursor_file, 'w') as f:
                    json.dump(data, f, indent=2)
                
                self._dirty = False
                print(f"Saved cursors to {self._cursor_file}")
                
            except Exception as e:
                print(f"Error saving cursors: {e}")
    
    def clear_cursors(self) -> None:
        """Clear all cursors and delete the cursor file."""
        with self._lock:
            self._cursors = Cursors()
            self._dirty = False
            
            if self._cursor_file.exists():
                try:
                    self._cursor_file.unlink()
                    print(f"Deleted cursor file: {self._cursor_file}")
                except Exception as e:
                    print(f"Error deleting cursor file: {e}")
    
    # Trade cursor methods
    def update_trade_cursor(
        self,
        market: str,
        offset: int,
        filter_amount: int,
        pending_markets: Optional[List[str]] = None
    ) -> None:
        """Update the trade cursor."""
        with self._lock:
            self._cursors.trades.market = market
            self._cursors.trades.offset = offset
            self._cursors.trades.filter_amount = filter_amount
            if pending_markets is not None:
                self._cursors.trades.pending_markets = pending_markets
            self._dirty = True
    
    def get_trade_cursor(self) -> TradeCursor:
        """Get the current trade cursor."""
        with self._lock:
            return TradeCursor(
                market=self._cursors.trades.market,
                offset=self._cursors.trades.offset,
                filter_amount=self._cursors.trades.filter_amount,
                pending_markets=list(self._cursors.trades.pending_markets)
            )
    
    def clear_trade_cursor(self) -> None:
        """Clear the trade cursor (called when trades complete successfully)."""
        with self._lock:
            self._cursors.trades = TradeCursor()
            self._dirty = True
    
    # Price cursor methods
    def update_price_cursor(
        self,
        token_id: str,
        start_ts: int,
        end_ts: int,
        pending_tokens: Optional[List[tuple]] = None,
        completed: bool = False
    ) -> None:
        """Update the price cursor."""
        with self._lock:
            self._cursors.prices.token_id = token_id
            self._cursors.prices.start_ts = start_ts
            self._cursors.prices.end_ts = end_ts
            if pending_tokens is not None:
                self._cursors.prices.pending_tokens = pending_tokens
            self._cursors.prices.completed = completed
            self._dirty = True
    
    def get_price_cursor(self) -> PriceCursor:
        """Get the current price cursor."""
        with self._lock:
            return PriceCursor(
                token_id=self._cursors.prices.token_id,
                start_ts=self._cursors.prices.start_ts,
                end_ts=self._cursors.prices.end_ts,
                pending_tokens=list(self._cursors.prices.pending_tokens),
                completed=self._cursors.prices.completed
            )
    
    def clear_price_cursor(self) -> None:
        """Clear the price cursor."""
        with self._lock:
            self._cursors.prices = PriceCursor()
            self._dirty = True
    
    # Leaderboard cursor methods
    def update_leaderboard_cursor(
        self,
        current_category_index: int,
        current_time_period_index: int,
        current_offset: int = 0,
        completed: bool = False
    ) -> None:
        """Update the leaderboard cursor."""
        with self._lock:
            self._cursors.leaderboard.current_category_index = current_category_index
            self._cursors.leaderboard.current_time_period_index = current_time_period_index
            self._cursors.leaderboard.current_offset = current_offset
            self._cursors.leaderboard.completed = completed
            self._dirty = True
    
    def get_leaderboard_cursor(self) -> LeaderboardCursor:
        """Get the current leaderboard cursor."""
        with self._lock:
            return LeaderboardCursor(
                current_category_index=self._cursors.leaderboard.current_category_index,
                current_time_period_index=self._cursors.leaderboard.current_time_period_index,
                current_offset=self._cursors.leaderboard.current_offset,
                completed=self._cursors.leaderboard.completed
            )
    
    def clear_leaderboard_cursor(self) -> None:
        """Clear the leaderboard cursor."""
        with self._lock:
            self._cursors.leaderboard = LeaderboardCursor()
            self._dirty = True
    
    # Market cursor methods
    def update_market_cursor(self, next_cursor: str, completed: bool = False) -> None:
        """Update the market cursor."""
        with self._lock:
            self._cursors.markets.next_cursor = next_cursor
            self._cursors.markets.completed = completed
            self._dirty = True
    
    def get_market_cursor(self) -> MarketCursor:
        """Get the current market cursor."""
        with self._lock:
            return MarketCursor(
                next_cursor=self._cursors.markets.next_cursor,
                completed=self._cursors.markets.completed
            )
    
    def clear_market_cursor(self) -> None:
        """Clear the market cursor."""
        with self._lock:
            self._cursors.markets = MarketCursor()
            self._dirty = True
    
    @property
    def cursors(self) -> Cursors:
        """Get the current cursors (read-only)."""
        with self._lock:
            return self._cursors
    
    @property
    def has_progress(self) -> bool:
        """Check if there's any progress to resume."""
        with self._lock:
            return self._cursors.has_any_progress()


# Global cursor manager instance
_cursor_manager: Optional[CursorManager] = None


def get_cursor_manager() -> CursorManager:
    """Get the global cursor manager instance."""
    global _cursor_manager
    if _cursor_manager is None:
        _cursor_manager = CursorManager()
    return _cursor_manager


def set_cursor_manager(manager: CursorManager) -> None:
    """Set the global cursor manager instance."""
    global _cursor_manager
    _cursor_manager = manager
