"""
Market Features Extractor

Extracts market-level features for ML models:
- Basic market attributes (volume, liquidity, duration)
- Outcome features (winning token, final prices)
- Activity metrics (trade count, unique traders)
"""

from typing import Optional, List
import pandas as pd
import duckdb
from pathlib import Path

from features.base import BaseFeatureExtractor


class MarketFeatures(BaseFeatureExtractor):
    """
    Extract market-level features from the silver layer.

    Features include:
    - volume: Total trading volume
    - liquidity: Market liquidity
    - duration_days: Days between start and end
    - is_resolved: Whether market has a winning outcome
    - winning_outcome: The outcome that won (Yes/No/other)
    - num_tokens: Number of outcome tokens
    - trade_count: Total number of trades
    - unique_traders: Count of unique traders
    """

    def get_feature_names(self) -> List[str]:
        return [
            "market_id",
            "condition_id",
            "question",
            "category",
            "volume",
            "liquidity",
            "duration_days",
            "is_resolved",
            "winning_outcome",
            "num_tokens",
            "trade_count",
            "unique_traders",
            "closed",
            "active",
        ]

    def extract(
        self,
        market_id: Optional[int] = None,
        condition_id: Optional[str] = None,
        closed_only: bool = False,
        resolved_only: bool = False,
        category: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> pd.DataFrame:
        """
        Extract market features.

        Args:
            market_id: Filter to specific market ID
            condition_id: Filter to specific condition ID
            closed_only: Only include closed markets
            resolved_only: Only include markets with a winning outcome
            category: Filter by category
            limit: Maximum number of markets to return

        Returns:
            DataFrame with market features
        """
        # Check cache
        cached = self._get_cached(
            market_id=market_id,
            condition_id=condition_id,
            closed_only=closed_only,
            resolved_only=resolved_only,
            category=category,
            limit=limit,
        )
        if cached is not None:
            return cached

        # Build query
        query = """
        WITH market_tokens AS (
            SELECT
                m.market_id,
                COUNT(t.token_id) as num_tokens,
                MAX(CASE WHEN t.winner = true THEN t.outcome END) as winning_outcome,
                MAX(CASE WHEN t.winner = true THEN 1 ELSE 0 END) as is_resolved
            FROM MarketDim m
            LEFT JOIN MarketTokenDim t ON m.market_id = t.market_id
            GROUP BY m.market_id
        ),
        trade_stats AS (
            SELECT
                t.market_id,
                COUNT(*) as trade_count,
                COUNT(DISTINCT COALESCE(tf.maker_id, 0) + COALESCE(tf.taker_id, 0)) as unique_traders
            FROM MarketTokenDim t
            LEFT JOIN TradeFact tf ON t.token_id = tf.token_id
            GROUP BY t.market_id
        )
        SELECT
            m.market_id,
            m.condition_id,
            m.question,
            m.category,
            m.volume,
            m.liquidity,
            CASE
                WHEN m.start_dt IS NOT NULL AND m.end_dt IS NOT NULL
                THEN DATEDIFF('day', m.start_dt, m.end_dt)
                ELSE NULL
            END as duration_days,
            COALESCE(mt.is_resolved, 0) as is_resolved,
            mt.winning_outcome,
            COALESCE(mt.num_tokens, 0) as num_tokens,
            COALESCE(ts.trade_count, 0) as trade_count,
            COALESCE(ts.unique_traders, 0) as unique_traders,
            m.closed,
            m.active
        FROM MarketDim m
        LEFT JOIN market_tokens mt ON m.market_id = mt.market_id
        LEFT JOIN trade_stats ts ON m.market_id = ts.market_id
        WHERE 1=1
        """

        params = []

        if market_id is not None:
            query += " AND m.market_id = ?"
            params.append(market_id)

        if condition_id is not None:
            query += " AND m.condition_id = ?"
            params.append(condition_id)

        if closed_only:
            query += " AND m.closed = true"

        if resolved_only:
            query += " AND mt.is_resolved = 1"

        if category is not None:
            query += " AND m.category = ?"
            params.append(category)

        query += " ORDER BY m.market_id"

        if limit is not None:
            query += f" LIMIT {limit}"

        try:
            df = self.conn.execute(query, params).fetchdf()
        except Exception as e:
            # Return empty DataFrame with correct columns if query fails
            df = pd.DataFrame(columns=self.get_feature_names())

        self._set_cached(
            df,
            market_id=market_id,
            condition_id=condition_id,
            closed_only=closed_only,
            resolved_only=resolved_only,
            category=category,
            limit=limit,
        )

        return df

    def extract_by_tag(
        self,
        tag_name: str,
        closed_only: bool = False,
        resolved_only: bool = False,
    ) -> pd.DataFrame:
        """
        Extract market features for markets with a specific tag.

        Args:
            tag_name: Name of the tag to filter by
            closed_only: Only include closed markets
            resolved_only: Only include markets with a winning outcome

        Returns:
            DataFrame with market features for tagged markets
        """
        # Get market IDs with this tag
        tag_query = """
        SELECT DISTINCT mtd.market_id
        FROM MarketTagDim mtd
        JOIN Tags t ON mtd.tag_id = t.tag_id
        WHERE t.name = ?
        """

        try:
            market_ids = self.conn.execute(tag_query, [tag_name]).fetchdf()
            if market_ids.empty:
                return pd.DataFrame(columns=self.get_feature_names())

            # Get features for these markets
            all_features = self.extract(closed_only=closed_only, resolved_only=resolved_only)
            return all_features[all_features["market_id"].isin(market_ids["market_id"])]

        except Exception:
            return pd.DataFrame(columns=self.get_feature_names())

    def get_top_traders(
        self,
        market_id: int,
        metric: str = "volume",
        top_n: int = 10,
    ) -> pd.DataFrame:
        """
        Get top traders for a specific market.

        Args:
            market_id: Market to analyze
            metric: Ranking metric - "volume", "trade_count", or "pnl"
            top_n: Number of top traders to return

        Returns:
            DataFrame with trader_id, wallet_address, and performance metrics
        """
        if metric == "pnl":
            # PnL requires resolved market with winner info
            query = """
            WITH market_trades AS (
                SELECT
                    COALESCE(tf.maker_id, tf.taker_id) as trader_id,
                    tf.side,
                    tf.price,
                    tf.size,
                    t.winner as token_won
                FROM TradeFact tf
                JOIN MarketTokenDim t ON tf.token_id = t.token_id
                WHERE t.market_id = ?
                  AND (tf.maker_id IS NOT NULL OR tf.taker_id IS NOT NULL)
            )
            SELECT
                td.trader_id,
                td.wallet_address,
                COUNT(*) as trade_count,
                SUM(mt.size * mt.price) as volume,
                SUM(CASE
                    WHEN mt.side = 'BUY' AND mt.token_won = true THEN mt.size * (1 - mt.price)
                    WHEN mt.side = 'BUY' AND mt.token_won = false THEN -mt.size * mt.price
                    WHEN mt.side = 'SELL' AND mt.token_won = true THEN -mt.size * (1 - mt.price)
                    WHEN mt.side = 'SELL' AND mt.token_won = false THEN mt.size * mt.price
                    ELSE 0
                END) as pnl
            FROM market_trades mt
            JOIN TraderDim td ON mt.trader_id = td.trader_id
            GROUP BY td.trader_id, td.wallet_address
            ORDER BY pnl DESC
            LIMIT ?
            """
        else:
            order_col = "volume" if metric == "volume" else "trade_count"
            query = f"""
            WITH market_trades AS (
                SELECT
                    COALESCE(tf.maker_id, tf.taker_id) as trader_id,
                    tf.size,
                    tf.price
                FROM TradeFact tf
                JOIN MarketTokenDim t ON tf.token_id = t.token_id
                WHERE t.market_id = ?
                  AND (tf.maker_id IS NOT NULL OR tf.taker_id IS NOT NULL)
            )
            SELECT
                td.trader_id,
                td.wallet_address,
                COUNT(*) as trade_count,
                SUM(mt.size * mt.price) as volume
            FROM market_trades mt
            JOIN TraderDim td ON mt.trader_id = td.trader_id
            GROUP BY td.trader_id, td.wallet_address
            ORDER BY {order_col} DESC
            LIMIT ?
            """

        try:
            return self.conn.execute(query, [market_id, top_n]).fetchdf()
        except Exception:
            return pd.DataFrame(columns=["trader_id", "wallet_address", "trade_count", "volume"])

    def extract_with_top_traders(
        self,
        top_n: int = 5,
        limit: Optional[int] = None,
    ) -> pd.DataFrame:
        """
        Extract market features with top trader IDs as a list column.

        Args:
            top_n: Number of top traders to include per market
            limit: Maximum number of markets

        Returns:
            DataFrame with market features plus top_trader_ids column
        """
        base_features = self.extract(limit=limit)

        if base_features.empty:
            base_features["top_trader_ids"] = []
            return base_features

        # Get top traders for each market
        top_traders_list = []
        for market_id in base_features["market_id"]:
            traders = self.get_top_traders(market_id, metric="volume", top_n=top_n)
            trader_ids = traders["trader_id"].tolist() if not traders.empty else []
            top_traders_list.append(trader_ids)

        base_features["top_trader_ids"] = top_traders_list
        return base_features
