"""
Price Parameters Provider for Polymarket Price Fetcher.
Provides dependency injection for price fetching parameters with support
for historical data fetching.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional, Dict, Any, List
import time


@dataclass
class PriceParams:
    """Parameters for a single price history fetch."""
    start_ts: int
    end_ts: int
    fidelity: int  # Resolution in minutes (60 = hourly)
    is_complete: bool = False  # True when no more data to fetch


class PriceParamsProvider(ABC):
    """
    Abstract base class for price parameters providers.
    Implement this interface to customize how price fetching parameters
    are determined and updated between requests.
    """
    
    @abstractmethod
    def get_params(self) -> PriceParams:
        """
        Get the current parameters for the next price fetch.
        
        Returns:
            PriceParams with start_ts, end_ts, fidelity, and completion status
        """
        pass
    
    @abstractmethod
    def update_params(self, response_data: List[Dict[str, Any]]) -> None:
        """
        Update parameters based on the response from the last fetch.
        This is called after each successful API response to advance
        the time window or mark completion.
        
        Args:
            response_data: The normalized price history data from the last fetch
        """
        pass
    
    @abstractmethod
    def reset(self, start_ts: Optional[int] = None) -> None:
        """
        Reset the provider to start fetching from the beginning.
        
        Args:
            start_ts: Optional new start timestamp
        """
        pass
    
    @property
    @abstractmethod
    def is_complete(self) -> bool:
        """Check if all historical data has been fetched."""
        pass
    
    @property
    @abstractmethod
    def fidelity(self) -> int:
        """Get the fidelity/resolution in minutes."""
        pass
    
    @property
    @abstractmethod
    def chunk_seconds(self) -> int:
        """Get the chunk size in seconds."""
        pass


class HistoricalPriceParamsProvider(PriceParamsProvider):
    """
    Provider for fetching all historical price data.
    
    Starts from a given timestamp and fetches data in chunks until:
    1. The API returns empty history, AND
    2. The end_ts exceeds the current time
    
    Uses hourly granularity by default (fidelity=60).
    """
    
    # Default chunk size: 1 day in seconds
    DEFAULT_CHUNK_SECONDS = 24 * 60 * 60
    # Default granularity: hourly (60 minutes)
    DEFAULT_FIDELITY = 60
    
    def __init__(
        self,
        start_ts: Optional[int] = None,
        end_ts: Optional[int] = None,
        fidelity: int = DEFAULT_FIDELITY,
        chunk_seconds: int = DEFAULT_CHUNK_SECONDS,
    ):
        """
        Initialize the historical price params provider.
        
        Args:
            start_ts: Starting timestamp (defaults to 30 days ago)
            end_ts: Optional end timestamp (defaults to current time)
            fidelity: Resolution in minutes (default 60 = hourly)
            chunk_seconds: Size of each fetch chunk in seconds (default 1 day)
        """
        now = int(time.time())
        
        self._initial_start_ts = start_ts if start_ts is not None else (now - 30 * 24 * 60 * 60)
        self._final_end_ts = end_ts  # None means "up to current time"
        self._fidelity = fidelity
        self._chunk_seconds = chunk_seconds
        
        # Current state
        self._current_start_ts = self._initial_start_ts
        self._is_complete = False
        self._last_response_empty = False
    
    def get_params(self) -> PriceParams:
        """
        Get parameters for the next chunk fetch.
        
        Returns:
            PriceParams for the current time window
        """
        now = int(time.time())
        
        # Determine the end of this chunk
        final_end = self._final_end_ts if self._final_end_ts is not None else now
        chunk_end = min(self._current_start_ts + self._chunk_seconds, final_end)
        
        # Cap to current time if we've gone past it
        if chunk_end > now:
            chunk_end = now
        
        return PriceParams(
            start_ts=self._current_start_ts,
            end_ts=chunk_end,
            fidelity=self._fidelity,
            is_complete=self._is_complete
        )
    
    def update_params(self, response_data: List[Dict[str, Any]]) -> None:
        """
        Update state based on API response.
        
        Advances to the next time chunk. Marks complete when:
        - Response is empty AND end_ts exceeds current time
        
        Args:
            response_data: Normalized price history data
        """
        now = int(time.time())
        params = self.get_params()
        
        # Check if response was empty
        self._last_response_empty = len(response_data) == 0
        
        # Advance to next chunk
        self._current_start_ts = params.end_ts
        
        # Determine final boundary
        final_end = self._final_end_ts if self._final_end_ts is not None else now
        
        # Check completion conditions:
        # 1. Current start has reached or exceeded final end
        # 2. OR response was empty and we're past current time
        if self._current_start_ts >= final_end:
            self._is_complete = True
        elif self._last_response_empty and self._current_start_ts >= now:
            self._is_complete = True
    
    def reset(self, start_ts: Optional[int] = None) -> None:
        """
        Reset to fetch from the beginning.
        
        Args:
            start_ts: Optional new start timestamp
        """
        if start_ts is not None:
            self._initial_start_ts = start_ts
        
        self._current_start_ts = self._initial_start_ts
        self._is_complete = False
        self._last_response_empty = False
    
    @property
    def is_complete(self) -> bool:
        """Check if all historical data has been fetched."""
        return self._is_complete
    
    @property
    def current_start_ts(self) -> int:
        """Get the current start timestamp."""
        return self._current_start_ts
    
    @property
    def fidelity(self) -> int:
        """Get the fidelity/resolution in minutes."""
        return self._fidelity
    
    @fidelity.setter
    def fidelity(self, value: int) -> None:
        """Set the fidelity/resolution in minutes."""
        self._fidelity = value
    
    @property
    def chunk_seconds(self) -> int:
        """Get the chunk size in seconds."""
        return self._chunk_seconds
    
    @chunk_seconds.setter
    def chunk_seconds(self, value: int) -> None:
        """Set the chunk size in seconds."""
        self._chunk_seconds = value


# Default provider factory
_default_provider: Optional[PriceParamsProvider] = None


def get_price_params_provider() -> PriceParamsProvider:
    """Get the default price params provider (creates one if needed)."""
    global _default_provider
    if _default_provider is None:
        _default_provider = HistoricalPriceParamsProvider()
    return _default_provider


def set_price_params_provider(provider: Optional[PriceParamsProvider]) -> None:
    """Set the default price params provider."""
    global _default_provider
    _default_provider = provider
