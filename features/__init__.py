"""
Feature Engineering Module for PolyMarket Analytics

This module provides modular feature extractors for ML model development.
Features are organized by entity type and can be composed together.

Usage:
    from features import MarketFeatures, TraderFeatures, PriceFeatures

    # Extract features for a specific market
    market_features = MarketFeatures(conn)
    df = market_features.extract(market_id=123)

    # Extract features using tag-based filtering
    df = market_features.extract_by_tag(tag_name="crypto")

Feature Categories:
    - MarketFeatures: Market-level aggregations (volume, liquidity, duration)
    - TraderFeatures: Trader behavior patterns (win rate, PnL, activity)
    - PriceFeatures: Time-series price features (volatility, momentum)
    - TagFeatures: Categorical features from LLM-assigned tags
"""

from features.base import BaseFeatureExtractor
from features.market_features import MarketFeatures
from features.trader_features import TraderFeatures
from features.price_features import PriceFeatures
from features.tag_features import TagFeatures

__all__ = [
    "BaseFeatureExtractor",
    "MarketFeatures",
    "TraderFeatures",
    "PriceFeatures",
    "TagFeatures",
]
