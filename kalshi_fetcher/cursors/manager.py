"""
Cursor Manager for Kalshi Fetcher
Handles persistence of fetch progress to enable resume from interruption.

Cursor Types:
    - events: cursor token for event pagination
    - markets: cursor token + event_ticker for market pagination
    - trades: cursor token + market_ticker for trade pagination
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
class EventCursor:
    """Cursor for event fetching progress."""
    cursor: str = ""
    completed: bool = False
    
    def is_empty(self) -> bool:
        return not self.cursor and not self.completed


@dataclass
class MarketCursor:
    """Cursor for market fetching progress."""
    cursor: str = ""
    event_ticker: str = ""
    pending_events: List[str] = field(default_factory=list)
    completed: bool = False
    
    def is_empty(self) -> bool:
        return not self.cursor and not self.event_ticker and not self.pending_events and not self.completed


@dataclass
class TradeCursor:
    """Cursor for trade fetching progress."""
    cursor: str = ""
    ticker: str = ""
    pending_tickers: List[str] = field(default_factory=list)
    completed: bool = False
    
    def is_empty(self) -> bool:
        return not self.cursor and not self.ticker and not self.pending_tickers and not self.completed


@dataclass
class OrderbookCursor:
    """Cursor for orderbook snapshot progress."""
    ticker: str = ""
    pending_tickers: List[str] = field(default_factory=list)
    completed: bool = False
    
    def is_empty(self) -> bool:
        return not self.ticker and not self.pending_tickers and not self.completed


@dataclass
class Cursors:
    """Container for all cursor types."""
    events: EventCursor = field(default_factory=EventCursor)
    markets: MarketCursor = field(default_factory=MarketCursor)
    trades: TradeCursor = field(default_factory=TradeCursor)
    orderbook: OrderbookCursor = field(default_factory=OrderbookCursor)
    last_updated: str = ""
    
    def has_any_progress(self) -> bool:
        """Check if any cursor has progress to resume from."""
        return (
            not self.events.is_empty() or
            not self.markets.is_empty() or
            not self.trades.is_empty() or
            not self.orderbook.is_empty()
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
            cursor_file: Path to cursor file (default: kalshi_fetcher/kalshi_cursor.json)
            auto_save: Whether to register shutdown handlers for auto-save
            enabled: Whether cursor persistence is enabled
        """
        self._enabled = enabled
        if cursor_file is None:
            self._cursor_file = Path(__file__).parent.parent / "kalshi_cursor.json"
        else:
            self._cursor_file = Path(cursor_file)
        
        self._cursors = Cursors()
        self._lock = threading.Lock()
        self._dirty = False
        
        if auto_save and enabled:
            self._register_shutdown_handlers()
    
    def _register_shutdown_handlers(self) -> None:
        """Register handlers to save cursors on program exit."""
        atexit.register(self.save_cursors)
        
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
        raise KeyboardInterrupt
    
    def load_cursors(self) -> Cursors:
        """
        Load cursors from file.
        
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
                    
                    # Parse event cursor
                    event_data = data.get('events', {})
                    self._cursors.events = EventCursor(
                        cursor=event_data.get('cursor', ''),
                        completed=event_data.get('completed', False)
                    )
                    
                    # Parse market cursor
                    market_data = data.get('markets', {})
                    self._cursors.markets = MarketCursor(
                        cursor=market_data.get('cursor', ''),
                        event_ticker=market_data.get('event_ticker', ''),
                        pending_events=market_data.get('pending_events', []),
                        completed=market_data.get('completed', False)
                    )
                    
                    # Parse trade cursor
                    trade_data = data.get('trades', {})
                    self._cursors.trades = TradeCursor(
                        cursor=trade_data.get('cursor', ''),
                        ticker=trade_data.get('ticker', ''),
                        pending_tickers=trade_data.get('pending_tickers', []),
                        completed=trade_data.get('completed', False)
                    )
                    
                    # Parse orderbook cursor
                    orderbook_data = data.get('orderbook', {})
                    self._cursors.orderbook = OrderbookCursor(
                        ticker=orderbook_data.get('ticker', ''),
                        pending_tickers=orderbook_data.get('pending_tickers', []),
                        completed=orderbook_data.get('completed', False)
                    )
                    
                    self._cursors.last_updated = data.get('last_updated', '')
                    
                except Exception as e:
                    print(f"Warning: Failed to load cursors: {e}")
                    self._cursors = Cursors()
            
            return self._cursors
    
    def save_cursors(self) -> None:
        """Save current cursor state to file."""
        if not self._enabled:
            return
            
        with self._lock:
            self._cursors.last_updated = datetime.now().isoformat()
            
            data = {
                'events': asdict(self._cursors.events),
                'markets': asdict(self._cursors.markets),
                'trades': asdict(self._cursors.trades),
                'orderbook': asdict(self._cursors.orderbook),
                'last_updated': self._cursors.last_updated
            }
            
            try:
                self._cursor_file.parent.mkdir(parents=True, exist_ok=True)
                with open(self._cursor_file, 'w') as f:
                    json.dump(data, f, indent=2)
                self._dirty = False
            except Exception as e:
                print(f"Warning: Failed to save cursors: {e}")
    
    def clear_cursors(self) -> None:
        """Clear all cursor progress (fresh start)."""
        with self._lock:
            self._cursors = Cursors()
            self._dirty = True
            
            if self._cursor_file.exists():
                self._cursor_file.unlink()
    
    # =========================================================================
    # Event Cursor Methods
    # =========================================================================
    
    def get_event_cursor(self) -> EventCursor:
        """Get current event cursor."""
        with self._lock:
            return self._cursors.events
    
    def update_event_cursor(self, cursor: str = "", completed: bool = False) -> None:
        """Update event cursor progress."""
        with self._lock:
            self._cursors.events.cursor = cursor
            self._cursors.events.completed = completed
            self._dirty = True
    
    # =========================================================================
    # Market Cursor Methods
    # =========================================================================
    
    def get_market_cursor(self) -> MarketCursor:
        """Get current market cursor."""
        with self._lock:
            return self._cursors.markets
    
    def update_market_cursor(
        self,
        cursor: str = "",
        event_ticker: str = "",
        pending_events: Optional[List[str]] = None,
        completed: bool = False
    ) -> None:
        """Update market cursor progress."""
        with self._lock:
            if cursor:
                self._cursors.markets.cursor = cursor
            if event_ticker:
                self._cursors.markets.event_ticker = event_ticker
            if pending_events is not None:
                self._cursors.markets.pending_events = pending_events
            self._cursors.markets.completed = completed
            self._dirty = True
    
    # =========================================================================
    # Trade Cursor Methods
    # =========================================================================
    
    def get_trade_cursor(self) -> TradeCursor:
        """Get current trade cursor."""
        with self._lock:
            return self._cursors.trades
    
    def update_trade_cursor(
        self,
        cursor: str = "",
        ticker: str = "",
        pending_tickers: Optional[List[str]] = None,
        completed: bool = False
    ) -> None:
        """Update trade cursor progress."""
        with self._lock:
            if cursor:
                self._cursors.trades.cursor = cursor
            if ticker:
                self._cursors.trades.ticker = ticker
            if pending_tickers is not None:
                self._cursors.trades.pending_tickers = pending_tickers
            self._cursors.trades.completed = completed
            self._dirty = True
    
    # =========================================================================
    # Orderbook Cursor Methods
    # =========================================================================
    
    def get_orderbook_cursor(self) -> OrderbookCursor:
        """Get current orderbook cursor."""
        with self._lock:
            return self._cursors.orderbook
    
    def update_orderbook_cursor(
        self,
        ticker: str = "",
        pending_tickers: Optional[List[str]] = None,
        completed: bool = False
    ) -> None:
        """Update orderbook cursor progress."""
        with self._lock:
            if ticker:
                self._cursors.orderbook.ticker = ticker
            if pending_tickers is not None:
                self._cursors.orderbook.pending_tickers = pending_tickers
            self._cursors.orderbook.completed = completed
            self._dirty = True
    
    # =========================================================================
    # Properties
    # =========================================================================
    
    @property
    def cursors(self) -> Cursors:
        """Get all cursors (read-only snapshot)."""
        with self._lock:
            return Cursors(
                events=self._cursors.events,
                markets=self._cursors.markets,
                trades=self._cursors.trades,
                orderbook=self._cursors.orderbook,
                last_updated=self._cursors.last_updated
            )
    
    @property
    def has_progress(self) -> bool:
        """Check if there is any progress to resume from."""
        with self._lock:
            return self._cursors.has_any_progress()
    
    @property
    def is_dirty(self) -> bool:
        """Check if there are unsaved changes."""
        with self._lock:
            return self._dirty


# =============================================================================
# Global Singleton
# =============================================================================

_cursor_manager: Optional[CursorManager] = None


def get_cursor_manager() -> CursorManager:
    """Get global cursor manager singleton."""
    global _cursor_manager
    if _cursor_manager is None:
        _cursor_manager = CursorManager()
    return _cursor_manager


def set_cursor_manager(manager: CursorManager) -> None:
    """Set global cursor manager singleton (for testing)."""
    global _cursor_manager
    _cursor_manager = manager
