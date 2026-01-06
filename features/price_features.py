"""
Price Features Extractor

Extracts time-series price features for ML models:
- Volatility metrics (std dev, range, ATR)
- Momentum indicators (returns, moving averages)
- Price levels (min, max, final)
"""

from typing import Optional, List, Dict, Any
import pandas as pd
import numpy as np
from pathlib import Path

from features.base import BaseFeatureExtractor


class PriceFeatures(BaseFeatureExtractor):
    """
    Extract price-based features from the silver layer.

    Features include:
    - price_volatility: Standard deviation of prices
    - price_range: Max - Min price
    - final_price: Last recorded price
    - price_return: (final - initial) / initial
    - price_samples: Number of price observations
    - time_weighted_avg: Time-weighted average price
    """

    def get_feature_names(self) -> List[str]:
        return [
            "token_id",
            "market_id",
            "outcome",
            "price_min",
            "price_max",
            "price_range",
            "price_volatility",
            "first_price",
            "final_price",
            "price_return",
            "price_samples",
            "winner",
        ]

    def extract(
        self,
        token_id: Optional[int] = None,
        market_id: Optional[int] = None,
        min_samples: int = 0,
        limit: Optional[int] = None,
    ) -> pd.DataFrame:
        """
        Extract price features for tokens.

        Args:
            token_id: Filter to specific token ID
            market_id: Filter to specific market ID
            min_samples: Minimum number of price samples required
            limit: Maximum number of tokens to return

        Returns:
            DataFrame with price features
        """
        cached = self._get_cached(
            token_id=token_id,
            market_id=market_id,
            min_samples=min_samples,
            limit=limit,
        )
        if cached is not None:
            return cached

        query = """
        WITH price_stats AS (
            SELECT
                p.token_id,
                COUNT(*) as price_samples,
                MIN(p.price) as price_min,
                MAX(p.price) as price_max,
                MAX(p.price) - MIN(p.price) as price_range,
                STDDEV(p.price) as price_volatility,
                FIRST_VALUE(p.price) OVER (
                    PARTITION BY p.token_id
                    ORDER BY p.timestamp ASC
                ) as first_price,
                LAST_VALUE(p.price) OVER (
                    PARTITION BY p.token_id
                    ORDER BY p.timestamp ASC
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ) as final_price
            FROM PriceHistoryFact p
            GROUP BY p.token_id
        )
        SELECT
            t.token_id,
            t.market_id,
            t.outcome,
            COALESCE(ps.price_min, t.price) as price_min,
            COALESCE(ps.price_max, t.price) as price_max,
            COALESCE(ps.price_range, 0) as price_range,
            COALESCE(ps.price_volatility, 0) as price_volatility,
            COALESCE(ps.first_price, t.price) as first_price,
            COALESCE(ps.final_price, t.price) as final_price,
            CASE
                WHEN COALESCE(ps.first_price, t.price) > 0
                THEN (COALESCE(ps.final_price, t.price) - COALESCE(ps.first_price, t.price))
                     / COALESCE(ps.first_price, t.price)
                ELSE 0
            END as price_return,
            COALESCE(ps.price_samples, 0) as price_samples,
            t.winner
        FROM MarketTokenDim t
        LEFT JOIN price_stats ps ON t.token_id = ps.token_id
        WHERE 1=1
        """

        params = []

        if token_id is not None:
            query += " AND t.token_id = ?"
            params.append(token_id)

        if market_id is not None:
            query += " AND t.market_id = ?"
            params.append(market_id)

        if min_samples > 0:
            query += f" AND COALESCE(ps.price_samples, 0) >= {min_samples}"

        query += " ORDER BY t.token_id"

        if limit is not None:
            query += f" LIMIT {limit}"

        try:
            df = self.conn.execute(query, params).fetchdf()
        except Exception:
            df = pd.DataFrame(columns=self.get_feature_names())

        self._set_cached(
            df,
            token_id=token_id,
            market_id=market_id,
            min_samples=min_samples,
            limit=limit,
        )

        return df

    def extract_timeseries(
        self,
        token_id: int,
        resample: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Extract raw price timeseries for a token.

        Args:
            token_id: Token to get timeseries for
            resample: Optional pandas resample frequency (e.g., '1H', '1D')

        Returns:
            DataFrame with timestamp and price columns
        """
        query = """
        SELECT
            timestamp,
            price
        FROM PriceHistoryFact
        WHERE token_id = ?
        ORDER BY timestamp ASC
        """

        try:
            df = self.conn.execute(query, [token_id]).fetchdf()

            if resample and not df.empty:
                df['timestamp'] = pd.to_datetime(df['timestamp'])
                df = df.set_index('timestamp')
                df = df.resample(resample).last().dropna()
                df = df.reset_index()

            return df
        except Exception:
            return pd.DataFrame(columns=["timestamp", "price"])

    def extract_for_market(self, market_id: int) -> pd.DataFrame:
        """
        Extract price features for all tokens in a market.

        Args:
            market_id: Market to get token prices for

        Returns:
            DataFrame with price features for each token in the market
        """
        return self.extract(market_id=market_id)

    def detect_inflection_points(
        self,
        token_id: int,
        threshold: float = 0.05,
        min_duration_hours: int = 1,
    ) -> pd.DataFrame:
        """
        Detect price inflection points (trend reversals) for a token.

        An inflection point is where price direction changes significantly.
        Useful for identifying key moments in market sentiment.

        Args:
            token_id: Token to analyze
            threshold: Minimum price change ratio to count as significant (default 5%)
            min_duration_hours: Minimum hours between inflection points

        Returns:
            DataFrame with inflection points: timestamp, price, direction, magnitude
        """
        # Get timeseries
        ts = self.extract_timeseries(token_id, resample='1H')

        if ts.empty or len(ts) < 3:
            return pd.DataFrame(columns=[
                "timestamp", "price", "direction", "magnitude", "price_before", "price_after"
            ])

        ts['timestamp'] = pd.to_datetime(ts['timestamp'])
        prices = ts['price'].values
        timestamps = ts['timestamp'].values

        inflection_points = []

        # Calculate rolling direction
        for i in range(1, len(prices) - 1):
            prev_change = prices[i] - prices[i - 1]
            next_change = prices[i + 1] - prices[i]

            # Detect direction reversal
            if prev_change * next_change < 0:  # Sign change = reversal
                # Check if change is significant
                magnitude = abs(prices[i + 1] - prices[i - 1]) / max(prices[i - 1], 0.001)

                if magnitude >= threshold:
                    direction = "peak" if prev_change > 0 else "trough"

                    # Check minimum duration from last inflection
                    if inflection_points:
                        last_ts = inflection_points[-1]["timestamp"]
                        hours_diff = (timestamps[i] - last_ts) / np.timedelta64(1, 'h')
                        if hours_diff < min_duration_hours:
                            continue

                    inflection_points.append({
                        "timestamp": timestamps[i],
                        "price": prices[i],
                        "direction": direction,
                        "magnitude": magnitude,
                        "price_before": prices[i - 1],
                        "price_after": prices[i + 1],
                    })

        return pd.DataFrame(inflection_points)

    def extract_inflection_features(
        self,
        token_id: Optional[int] = None,
        market_id: Optional[int] = None,
        threshold: float = 0.05,
    ) -> pd.DataFrame:
        """
        Extract inflection point summary features for tokens.

        Args:
            token_id: Filter to specific token
            market_id: Filter to specific market
            threshold: Price change threshold for inflection detection

        Returns:
            DataFrame with token_id and inflection summary stats
        """
        # Get tokens to analyze
        if token_id is not None:
            tokens = [token_id]
        elif market_id is not None:
            query = "SELECT token_id FROM MarketTokenDim WHERE market_id = ?"
            result = self.conn.execute(query, [market_id]).fetchdf()
            tokens = result['token_id'].tolist() if not result.empty else []
        else:
            return pd.DataFrame(columns=[
                "token_id", "inflection_count", "peaks", "troughs",
                "avg_magnitude", "max_magnitude"
            ])

        features = []
        for tid in tokens:
            inflections = self.detect_inflection_points(tid, threshold=threshold)

            if inflections.empty:
                features.append({
                    "token_id": tid,
                    "inflection_count": 0,
                    "peaks": 0,
                    "troughs": 0,
                    "avg_magnitude": 0.0,
                    "max_magnitude": 0.0,
                })
            else:
                features.append({
                    "token_id": tid,
                    "inflection_count": len(inflections),
                    "peaks": (inflections["direction"] == "peak").sum(),
                    "troughs": (inflections["direction"] == "trough").sum(),
                    "avg_magnitude": inflections["magnitude"].mean(),
                    "max_magnitude": inflections["magnitude"].max(),
                })

        return pd.DataFrame(features)

    def get_major_moves(
        self,
        token_id: int,
        top_n: int = 5,
    ) -> pd.DataFrame:
        """
        Get the largest price moves for a token.

        Args:
            token_id: Token to analyze
            top_n: Number of top moves to return

        Returns:
            DataFrame with timestamp, price_change, pct_change for largest moves
        """
        ts = self.extract_timeseries(token_id, resample='1H')

        if ts.empty or len(ts) < 2:
            return pd.DataFrame(columns=[
                "timestamp", "price", "price_prev", "price_change", "pct_change"
            ])

        ts['timestamp'] = pd.to_datetime(ts['timestamp'])
        ts['price_prev'] = ts['price'].shift(1)
        ts['price_change'] = ts['price'] - ts['price_prev']
        ts['pct_change'] = ts['price_change'] / ts['price_prev'].replace(0, np.nan)
        ts['abs_pct_change'] = ts['pct_change'].abs()

        # Get top moves by absolute percentage change
        top_moves = ts.nlargest(top_n, 'abs_pct_change')[
            ['timestamp', 'price', 'price_prev', 'price_change', 'pct_change']
        ].reset_index(drop=True)

        return top_moves
