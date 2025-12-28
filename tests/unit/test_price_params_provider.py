"""
Unit tests for PriceParamsProvider and HistoricalPriceParamsProvider.

These tests verify:
1. Parameter calculation and time window chunking
2. Completion detection (empty response AND end_ts exceeds current time)
3. Provider reset functionality
4. Fidelity and chunk_seconds property access
"""

import pytest
import time
from datetime import datetime
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from fetcher.workers.params_provider import (
    PriceParams,
    PriceParamsProvider,
    HistoricalPriceParamsProvider,
    MarketPriceParamsProvider,
    get_price_params_provider,
    set_price_params_provider,
)


class TestPriceParams:
    """Tests for the PriceParams dataclass."""
    
    def test_price_params_creation(self):
        """Test creating a PriceParams object."""
        params = PriceParams(
            start_ts=1702300800,
            end_ts=1702387200,
            fidelity=60,
            is_complete=False
        )
        
        assert params.start_ts == 1702300800
        assert params.end_ts == 1702387200
        assert params.fidelity == 60
        assert params.is_complete is False
    
    def test_price_params_defaults(self):
        """Test PriceParams default values."""
        params = PriceParams(
            start_ts=1702300800,
            end_ts=1702387200,
            fidelity=60
        )
        
        assert params.is_complete is False


class TestHistoricalPriceParamsProvider:
    """Tests for HistoricalPriceParamsProvider."""
    
    def test_default_initialization(self):
        """Test default initialization values."""
        provider = HistoricalPriceParamsProvider()
        
        # Default fidelity should be 60 (hourly)
        assert provider.fidelity == 60
        # Default chunk size should be 1 day
        assert provider.chunk_seconds == 24 * 60 * 60
        # Should not be complete initially
        assert provider.is_complete is False
    
    def test_custom_initialization(self):
        """Test custom initialization values."""
        start = 1702300800
        end = 1702387200
        
        provider = HistoricalPriceParamsProvider(
            start_ts=start,
            end_ts=end,
            fidelity=30,  # 30 minute resolution
            chunk_seconds=12 * 60 * 60,  # 12 hour chunks
        )
        
        assert provider.fidelity == 30
        assert provider.chunk_seconds == 12 * 60 * 60
        assert provider.current_start_ts == start
    
    def test_get_params_returns_correct_chunk(self):
        """Test that get_params returns the correct time window."""
        start = 1702300800
        end = start + 48 * 60 * 60  # 48 hours later
        
        provider = HistoricalPriceParamsProvider(
            start_ts=start,
            end_ts=end,
            fidelity=60,
            chunk_seconds=24 * 60 * 60,  # 1 day chunks
        )
        
        params = provider.get_params()
        
        assert params.start_ts == start
        assert params.end_ts == start + 24 * 60 * 60  # One chunk
        assert params.fidelity == 60
        assert params.is_complete is False
    
    def test_update_params_advances_time_window(self):
        """Test that update_params advances to next chunk."""
        start = 1702300800
        end = start + 48 * 60 * 60  # 48 hours
        chunk = 24 * 60 * 60  # 1 day chunks
        
        provider = HistoricalPriceParamsProvider(
            start_ts=start,
            end_ts=end,
            fidelity=60,
            chunk_seconds=chunk,
        )
        
        # Get first params
        params1 = provider.get_params()
        assert params1.start_ts == start
        
        # Simulate API response with data
        mock_response = [{"t": start + 3600, "p": 0.5}]
        provider.update_params(mock_response)
        
        # Get second params - should be advanced
        params2 = provider.get_params()
        assert params2.start_ts == start + chunk
    
    def test_completion_on_reaching_end(self):
        """Test completion when start_ts reaches end_ts."""
        start = 1702300800
        end = start + 24 * 60 * 60  # Only 1 day
        chunk = 24 * 60 * 60  # 1 day chunks
        
        provider = HistoricalPriceParamsProvider(
            start_ts=start,
            end_ts=end,
            fidelity=60,
            chunk_seconds=chunk,
        )
        
        assert provider.is_complete is False
        
        # Simulate API response
        provider.update_params([{"t": start + 3600, "p": 0.5}])
        
        # Should be complete after one chunk since we reach end
        assert provider.is_complete is True
    
    def test_completion_on_empty_response_past_current_time(self):
        """Test completion when response is empty AND we're past current time."""
        now = int(time.time())
        start = now - 24 * 60 * 60  # Yesterday
        
        provider = HistoricalPriceParamsProvider(
            start_ts=start,
            end_ts=None,  # Up to current time
            fidelity=60,
            chunk_seconds=24 * 60 * 60,
        )
        
        # First update with data - should not be complete
        provider.update_params([{"t": start + 3600, "p": 0.5}])
        
        # Now the provider is at current time, empty response should mark complete
        # (current_start_ts should now be >= now)
        if provider.current_start_ts >= now:
            provider.update_params([])  # Empty response
            assert provider.is_complete is True
    
    def test_not_complete_on_empty_response_before_end(self):
        """Test that empty response before reaching end doesn't mark complete."""
        start = 1702300800
        end = start + 72 * 60 * 60  # 3 days
        chunk = 24 * 60 * 60  # 1 day
        
        provider = HistoricalPriceParamsProvider(
            start_ts=start,
            end_ts=end,
            fidelity=60,
            chunk_seconds=chunk,
        )
        
        # Empty response but not at end yet
        provider.update_params([])
        
        # Should NOT be complete - still have more time to fetch
        assert provider.is_complete is False
        # Should have advanced to next chunk
        assert provider.current_start_ts == start + chunk
    
    def test_reset_returns_to_initial_state(self):
        """Test that reset returns provider to initial state."""
        start = 1702300800
        end = start + 72 * 60 * 60
        
        provider = HistoricalPriceParamsProvider(
            start_ts=start,
            end_ts=end,
            fidelity=60,
        )
        
        # Advance several times
        provider.update_params([{"t": 1, "p": 0.5}])
        provider.update_params([{"t": 2, "p": 0.5}])
        
        assert provider.current_start_ts != start
        
        # Reset
        provider.reset()
        
        assert provider.current_start_ts == start
        assert provider.is_complete is False
    
    def test_reset_with_new_start_ts(self):
        """Test resetting with a new start timestamp."""
        start = 1702300800
        new_start = 1702400000
        
        provider = HistoricalPriceParamsProvider(
            start_ts=start,
            fidelity=60,
        )
        
        # Reset with new start
        provider.reset(start_ts=new_start)
        
        assert provider.current_start_ts == new_start
    
    def test_fidelity_setter(self):
        """Test fidelity property setter."""
        provider = HistoricalPriceParamsProvider(fidelity=60)
        
        assert provider.fidelity == 60
        
        provider.fidelity = 30
        
        assert provider.fidelity == 30
    
    def test_chunk_seconds_setter(self):
        """Test chunk_seconds property setter."""
        provider = HistoricalPriceParamsProvider(chunk_seconds=86400)
        
        assert provider.chunk_seconds == 86400
        
        provider.chunk_seconds = 43200  # 12 hours
        
        assert provider.chunk_seconds == 43200
    
    def test_multiple_chunks_iteration(self):
        """Test iterating through multiple chunks."""
        start = 1702300800
        chunk = 6 * 60 * 60  # 6 hour chunks
        end = start + 24 * 60 * 60  # 24 hours = 4 chunks
        
        provider = HistoricalPriceParamsProvider(
            start_ts=start,
            end_ts=end,
            fidelity=60,
            chunk_seconds=chunk,
        )
        
        chunks_fetched = 0
        while not provider.is_complete:
            params = provider.get_params()
            assert params.fidelity == 60
            chunks_fetched += 1
            provider.update_params([{"t": 1, "p": 0.5}])
        
        assert chunks_fetched == 4  # Should have 4 chunks
    
    def test_end_ts_capped_at_current_time(self):
        """Test that end_ts in params is capped at current time."""
        now = int(time.time())
        future = now + 7 * 24 * 60 * 60  # 7 days in future
        
        provider = HistoricalPriceParamsProvider(
            start_ts=now - 3600,  # 1 hour ago
            end_ts=future,
            fidelity=60,
            chunk_seconds=24 * 60 * 60,
        )
        
        params = provider.get_params()
        
        # end_ts should be capped to current time, not future
        assert params.end_ts <= now + 1  # +1 for timing tolerance


class TestPriceParamsProviderSingleton:
    """Tests for global singleton accessors."""
    
    def test_get_creates_default_provider(self):
        """Test that get_price_params_provider creates a default provider."""
        set_price_params_provider(None)  # Reset
        
        provider = get_price_params_provider()
        
        assert provider is not None
        assert isinstance(provider, HistoricalPriceParamsProvider)
    
    def test_set_and_get_provider(self):
        """Test setting and getting a custom provider."""
        custom_provider = HistoricalPriceParamsProvider(
            start_ts=1702300800,
            fidelity=30,
        )
        
        set_price_params_provider(custom_provider)
        
        retrieved = get_price_params_provider()
        
        assert retrieved is custom_provider
        assert retrieved.fidelity == 30
    
    def test_set_none_resets_to_default(self):
        """Test that setting None allows new default on next get."""
        custom_provider = HistoricalPriceParamsProvider(fidelity=30)
        set_price_params_provider(custom_provider)
        
        set_price_params_provider(None)
        
        new_provider = get_price_params_provider()
        
        assert new_provider is not custom_provider
        assert new_provider.fidelity == 60  # Default


class TestAbstractProviderInterface:
    """Tests to verify abstract interface implementation."""
    
    def test_historical_provider_implements_abstract_methods(self):
        """Verify HistoricalPriceParamsProvider implements all abstract methods."""
        provider = HistoricalPriceParamsProvider()
        
        # All these should work without error
        params = provider.get_params()
        assert isinstance(params, PriceParams)
        
        provider.update_params([])
        
        provider.reset()
        
        _ = provider.is_complete
        _ = provider.fidelity
        _ = provider.chunk_seconds
    
    def test_cannot_instantiate_abstract_class(self):
        """Verify PriceParamsProvider cannot be instantiated directly."""
        with pytest.raises(TypeError):
            PriceParamsProvider()


class TestMarketPriceParamsProvider:
    """Tests for MarketPriceParamsProvider with volume-based adaptive resolution."""
    
    def test_low_volume_uses_hourly_resolution(self):
        """Test that low volume markets use hourly resolution."""
        provider = MarketPriceParamsProvider(
            volume_num=5000,  # Below 10K threshold
            low_volume_threshold=10000,
            high_volume_threshold=100000,
        )
        
        assert provider.tier == MarketPriceParamsProvider.TIER_LOW
        assert provider.fidelity == 60  # Hourly
        assert provider.using_minute_resolution is False
    
    def test_high_volume_uses_minute_resolution(self):
        """Test that high volume markets use minute resolution."""
        provider = MarketPriceParamsProvider(
            volume_num=150000,  # Above 100K threshold
            low_volume_threshold=10000,
            high_volume_threshold=100000,
        )
        
        assert provider.tier == MarketPriceParamsProvider.TIER_HIGH
        assert provider.fidelity == 1  # Minute
        assert provider.using_minute_resolution is True
    
    def test_mid_volume_starts_hourly(self):
        """Test that mid volume markets start with hourly resolution."""
        provider = MarketPriceParamsProvider(
            volume_num=50000,  # Between 10K and 100K
            low_volume_threshold=10000,
            high_volume_threshold=100000,
        )
        
        assert provider.tier == MarketPriceParamsProvider.TIER_MID
        assert provider.fidelity == 60  # Starts hourly
        assert provider.using_minute_resolution is False
    
    def test_mid_volume_switches_to_minute_on_delta_trigger(self):
        """Test that mid volume switches to minute when delta exceeds threshold."""
        provider = MarketPriceParamsProvider(
            volume_num=50000,
            low_volume_threshold=10000,
            high_volume_threshold=100000,
            delta_percent_trigger=5.0,
        )
        
        assert provider.fidelity == 60  # Starts hourly
        
        # First response with low price
        provider.update_params([{"p": 0.50}])
        assert provider.fidelity == 60  # Still hourly
        assert provider.delta_triggered is False
        
        # Second response with price that creates >5% delta
        # 0.50 to 0.54 = 8% change
        provider.update_params([{"p": 0.54}])
        
        assert provider.delta_triggered is True
        assert provider.fidelity == 1  # Now minute
        assert provider.using_minute_resolution is True
    
    def test_delta_tracked_across_multiple_responses(self):
        """Test that delta tracks min/max across all responses."""
        provider = MarketPriceParamsProvider(
            volume_num=50000,
            delta_percent_trigger=10.0,  # 10% trigger
        )
        
        # Response 1: price at 0.50
        provider.update_params([{"p": 0.50}])
        assert provider.delta_percent == 0.0  # Only one price seen
        
        # Response 2: price at 0.52 (4% up from 0.50)
        provider.update_params([{"p": 0.52}])
        assert pytest.approx(provider.delta_percent, rel=0.01) == 4.0
        
        # Response 3: price at 0.48 (now tracking 0.48 to 0.52 = 8.33%)
        provider.update_params([{"p": 0.48}])
        assert pytest.approx(provider.delta_percent, rel=0.01) == 8.33
        
        # Response 4: price at 0.45 (now 0.45 to 0.52 = 15.5% > 10% trigger)
        provider.update_params([{"p": 0.45}])
        assert provider.delta_triggered is True
        assert provider.fidelity == 1  # Switched to minute
    
    def test_delta_not_tracked_for_low_volume(self):
        """Test that low volume markets don't track delta."""
        provider = MarketPriceParamsProvider(
            volume_num=5000,  # Low volume
            delta_percent_trigger=5.0,
        )
        
        # Feed responses with large delta
        provider.update_params([{"p": 0.50}])
        provider.update_params([{"p": 0.70}])  # 40% change
        
        # Should still be hourly - delta not tracked for low tier
        assert provider.fidelity == 60
        assert provider.using_minute_resolution is False
    
    def test_delta_not_tracked_for_high_volume(self):
        """Test that high volume markets are already at minute resolution."""
        provider = MarketPriceParamsProvider(
            volume_num=200000,  # High volume
        )
        
        # Already at minute resolution from start
        assert provider.fidelity == 1
        assert provider.using_minute_resolution is True
        
        # Stays at minute after updates
        provider.update_params([{"p": 0.50}])
        provider.update_params([{"p": 0.70}])
        
        assert provider.fidelity == 1
    
    def test_reset_clears_delta_tracking(self):
        """Test that reset clears delta tracking state."""
        provider = MarketPriceParamsProvider(
            volume_num=50000,
            delta_percent_trigger=5.0,
        )
        
        # Trigger delta switch
        provider.update_params([{"p": 0.50}])
        provider.update_params([{"p": 0.60}])  # 20% change
        
        assert provider.delta_triggered is True
        assert provider.fidelity == 1
        
        # Reset
        provider.reset()
        
        assert provider.delta_triggered is False
        assert provider.fidelity == 60  # Back to hourly
        assert provider.delta_percent == 0.0
    
    def test_custom_thresholds(self):
        """Test custom volume thresholds."""
        provider = MarketPriceParamsProvider(
            volume_num=500,
            low_volume_threshold=1000,
            high_volume_threshold=5000,
        )
        
        assert provider.tier == MarketPriceParamsProvider.TIER_LOW
        
        provider2 = MarketPriceParamsProvider(
            volume_num=3000,
            low_volume_threshold=1000,
            high_volume_threshold=5000,
        )
        
        assert provider2.tier == MarketPriceParamsProvider.TIER_MID
    
    def test_custom_fidelity_values(self):
        """Test custom fidelity and chunk values."""
        provider = MarketPriceParamsProvider(
            volume_num=5000,  # Low volume
            hourly_fidelity=30,  # 30 min instead of 60
            minute_fidelity=5,  # 5 min instead of 1
            hourly_chunk_seconds=43200,  # 12 hours
            minute_chunk_seconds=1800,  # 30 minutes
        )
        
        assert provider.fidelity == 30
        assert provider.chunk_seconds == 43200
    
    def test_chunk_seconds_changes_on_resolution_switch(self):
        """Test that chunk_seconds changes when resolution switches."""
        provider = MarketPriceParamsProvider(
            volume_num=50000,
            delta_percent_trigger=5.0,
            hourly_chunk_seconds=86400,
            minute_chunk_seconds=3600,
        )
        
        assert provider.chunk_seconds == 86400  # Hourly chunks
        
        # Trigger switch
        provider.update_params([{"p": 0.50}])
        provider.update_params([{"p": 0.60}])
        
        assert provider.chunk_seconds == 3600  # Minute chunks
    
    def test_get_params_returns_correct_values(self):
        """Test that get_params returns correct PriceParams."""
        start = 1702300800
        end = start + 48 * 60 * 60
        
        provider = MarketPriceParamsProvider(
            volume_num=50000,
            start_ts=start,
            end_ts=end,
        )
        
        params = provider.get_params()
        
        assert params.start_ts == start
        assert params.fidelity == 60
        assert params.is_complete is False
    
    def test_completion_on_reaching_end(self):
        """Test completion when start_ts reaches end_ts."""
        start = 1702300800
        end = start + 86400  # 1 day
        
        provider = MarketPriceParamsProvider(
            volume_num=50000,
            start_ts=start,
            end_ts=end,
            hourly_chunk_seconds=86400,
        )
        
        assert provider.is_complete is False
        
        provider.update_params([{"p": 0.5}])
        
        assert provider.is_complete is True
    
    def test_price_field_variations(self):
        """Test that both 'p' and 'price' fields are tracked."""
        provider = MarketPriceParamsProvider(
            volume_num=50000,
            delta_percent_trigger=5.0,
        )
        
        # Use 'p' field (price history format)
        provider.update_params([{"p": 0.50}])

        # Use 'price' field (alternative format)
        provider.update_params([{"price": 0.60}])

        assert provider.delta_triggered is True  # 20% change

    def test_volume_property(self):
        """Test volume_num property."""
        provider = MarketPriceParamsProvider(volume_num=75000)

        assert provider.volume_num == 75000
