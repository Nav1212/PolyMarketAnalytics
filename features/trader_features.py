"""
Trader Features Extractor

Extracts trader-level features for ML models:
- Trading activity (trade count, volume, frequency)
- Performance metrics (win rate, PnL, ROI)
- Behavioral patterns (avg trade size, preferred markets)
"""

from typing import Optional, List, Dict, Any
import pandas as pd
from pathlib import Path

from features.base import BaseFeatureExtractor


class TraderFeatures(BaseFeatureExtractor):
    """
    Extract trader-level features from the silver layer.

    Features include:
    - trade_count: Total number of trades
    - total_volume: Sum of trade sizes
    - avg_trade_size: Average trade size
    - unique_markets: Number of unique markets traded
    - win_rate: Percentage of winning trades (where outcome resolved)
    - total_pnl: Estimated profit/loss
    - first_trade_ts: Timestamp of first trade
    - last_trade_ts: Timestamp of last trade
    - trading_days: Number of distinct days with trades
    """

    def get_feature_names(self) -> List[str]:
        return [
            "trader_id",
            "wallet_address",
            "trade_count",
            "total_volume",
            "avg_trade_size",
            "unique_markets",
            "buy_count",
            "sell_count",
            "buy_ratio",
            "first_trade_ts",
            "last_trade_ts",
            "trading_days",
        ]

    def extract(
        self,
        trader_id: Optional[int] = None,
        wallet_address: Optional[str] = None,
        min_trades: int = 0,
        limit: Optional[int] = None,
    ) -> pd.DataFrame:
        """
        Extract trader features.

        Args:
            trader_id: Filter to specific trader ID
            wallet_address: Filter to specific wallet address
            min_trades: Minimum number of trades to include
            limit: Maximum number of traders to return

        Returns:
            DataFrame with trader features
        """
        cached = self._get_cached(
            trader_id=trader_id,
            wallet_address=wallet_address,
            min_trades=min_trades,
            limit=limit,
        )
        if cached is not None:
            return cached

        query = """
        WITH trader_trades AS (
            SELECT
                COALESCE(tf.maker_id, tf.taker_id) as trader_id,
                tf.trade_id,
                tf.price,
                tf.size,
                tf.side,
                tf.timestamp,
                t.market_id
            FROM TradeFact tf
            JOIN MarketTokenDim t ON tf.token_id = t.token_id
            WHERE tf.maker_id IS NOT NULL OR tf.taker_id IS NOT NULL
        )
        SELECT
            td.trader_id,
            td.wallet_address,
            COUNT(tt.trade_id) as trade_count,
            SUM(tt.size * tt.price) as total_volume,
            AVG(tt.size * tt.price) as avg_trade_size,
            COUNT(DISTINCT tt.market_id) as unique_markets,
            SUM(CASE WHEN tt.side = 'BUY' THEN 1 ELSE 0 END) as buy_count,
            SUM(CASE WHEN tt.side = 'SELL' THEN 1 ELSE 0 END) as sell_count,
            CASE
                WHEN COUNT(tt.trade_id) > 0
                THEN SUM(CASE WHEN tt.side = 'BUY' THEN 1 ELSE 0 END)::FLOAT / COUNT(tt.trade_id)
                ELSE 0
            END as buy_ratio,
            MIN(tt.timestamp) as first_trade_ts,
            MAX(tt.timestamp) as last_trade_ts,
            COUNT(DISTINCT DATE_TRUNC('day', tt.timestamp)) as trading_days
        FROM TraderDim td
        LEFT JOIN trader_trades tt ON td.trader_id = tt.trader_id
        WHERE 1=1
        """

        params = []

        if trader_id is not None:
            query += " AND td.trader_id = ?"
            params.append(trader_id)

        if wallet_address is not None:
            query += " AND td.wallet_address = ?"
            params.append(wallet_address)

        query += " GROUP BY td.trader_id, td.wallet_address"

        if min_trades > 0:
            query += f" HAVING COUNT(tt.trade_id) >= {min_trades}"

        query += " ORDER BY trade_count DESC"

        if limit is not None:
            query += f" LIMIT {limit}"

        try:
            df = self.conn.execute(query, params).fetchdf()
        except Exception:
            df = pd.DataFrame(columns=self.get_feature_names())

        self._set_cached(
            df,
            trader_id=trader_id,
            wallet_address=wallet_address,
            min_trades=min_trades,
            limit=limit,
        )

        return df

    def extract_with_performance(
        self,
        min_resolved_trades: int = 10,
        limit: Optional[int] = None,
    ) -> pd.DataFrame:
        """
        Extract trader features with win rate on resolved markets.

        This is more expensive as it joins with market outcomes.

        Args:
            min_resolved_trades: Minimum resolved trades to calculate win rate
            limit: Maximum number of traders to return

        Returns:
            DataFrame with trader features including win rate
        """
        query = """
        WITH resolved_trades AS (
            SELECT
                COALESCE(tf.maker_id, tf.taker_id) as trader_id,
                tf.trade_id,
                tf.side,
                tf.price,
                tf.size,
                t.winner as token_won,
                t.market_id
            FROM TradeFact tf
            JOIN MarketTokenDim t ON tf.token_id = t.token_id
            WHERE t.winner IS NOT NULL
        ),
        trader_perf AS (
            SELECT
                trader_id,
                COUNT(*) as resolved_trade_count,
                SUM(CASE
                    WHEN side = 'BUY' AND token_won = true THEN size * (1 - price)
                    WHEN side = 'BUY' AND token_won = false THEN -size * price
                    WHEN side = 'SELL' AND token_won = true THEN -size * (1 - price)
                    WHEN side = 'SELL' AND token_won = false THEN size * price
                    ELSE 0
                END) as estimated_pnl,
                AVG(CASE
                    WHEN (side = 'BUY' AND token_won = true) OR (side = 'SELL' AND token_won = false)
                    THEN 1.0 ELSE 0.0
                END) as win_rate
            FROM resolved_trades
            GROUP BY trader_id
            HAVING COUNT(*) >= ?
        )
        SELECT
            td.trader_id,
            td.wallet_address,
            tp.resolved_trade_count,
            tp.estimated_pnl,
            tp.win_rate
        FROM TraderDim td
        JOIN trader_perf tp ON td.trader_id = tp.trader_id
        ORDER BY tp.estimated_pnl DESC
        """

        params = [min_resolved_trades]

        if limit is not None:
            query += f" LIMIT {limit}"

        try:
            return self.conn.execute(query, params).fetchdf()
        except Exception:
            return pd.DataFrame(columns=[
                "trader_id", "wallet_address", "resolved_trade_count",
                "estimated_pnl", "win_rate"
            ])

    def get_best_trades(
        self,
        trader_id: int,
        top_n: int = 5,
    ) -> pd.DataFrame:
        """
        Get a trader's best individual trades by return.

        Args:
            trader_id: Trader to analyze
            top_n: Number of top trades to return

        Returns:
            DataFrame with trade details and estimated return
        """
        query = """
        SELECT
            tf.trade_id,
            tf.timestamp,
            tf.side,
            tf.price,
            tf.size,
            t.outcome,
            t.winner as token_won,
            m.question,
            m.category,
            CASE
                WHEN tf.side = 'BUY' AND t.winner = true THEN tf.size * (1 - tf.price)
                WHEN tf.side = 'BUY' AND t.winner = false THEN -tf.size * tf.price
                WHEN tf.side = 'SELL' AND t.winner = true THEN -tf.size * (1 - tf.price)
                WHEN tf.side = 'SELL' AND t.winner = false THEN tf.size * tf.price
                ELSE 0
            END as trade_pnl
        FROM TradeFact tf
        JOIN MarketTokenDim t ON tf.token_id = t.token_id
        JOIN MarketDim m ON t.market_id = m.market_id
        WHERE (tf.maker_id = ? OR tf.taker_id = ?)
          AND t.winner IS NOT NULL
        ORDER BY trade_pnl DESC
        LIMIT ?
        """

        try:
            return self.conn.execute(query, [trader_id, trader_id, top_n]).fetchdf()
        except Exception:
            return pd.DataFrame(columns=[
                "trade_id", "timestamp", "side", "price", "size",
                "outcome", "token_won", "question", "category", "trade_pnl"
            ])

    def get_largest_return(
        self,
        trader_id: int,
    ) -> Dict[str, Any]:
        """
        Get a trader's single largest return trade.

        Args:
            trader_id: Trader to analyze

        Returns:
            Dict with best trade details, or empty dict if none found
        """
        best_trades = self.get_best_trades(trader_id, top_n=1)

        if best_trades.empty:
            return {}

        row = best_trades.iloc[0]
        return {
            "trade_id": row["trade_id"],
            "pnl": row["trade_pnl"],
            "market_question": row["question"],
            "category": row["category"],
            "side": row["side"],
            "price": row["price"],
            "size": row["size"],
        }

    def get_top_categories(
        self,
        trader_id: int,
        top_n: int = 5,
    ) -> pd.DataFrame:
        """
        Get a trader's most traded categories.

        Args:
            trader_id: Trader to analyze
            top_n: Number of top categories to return

        Returns:
            DataFrame with category, trade_count, volume, pnl
        """
        query = """
        WITH trader_category_stats AS (
            SELECT
                m.category,
                COUNT(*) as trade_count,
                SUM(tf.size * tf.price) as volume,
                SUM(CASE
                    WHEN tf.side = 'BUY' AND t.winner = true THEN tf.size * (1 - tf.price)
                    WHEN tf.side = 'BUY' AND t.winner = false THEN -tf.size * tf.price
                    WHEN tf.side = 'SELL' AND t.winner = true THEN -tf.size * (1 - tf.price)
                    WHEN tf.side = 'SELL' AND t.winner = false THEN tf.size * tf.price
                    ELSE 0
                END) as category_pnl,
                AVG(CASE
                    WHEN t.winner IS NOT NULL THEN
                        CASE WHEN (tf.side = 'BUY' AND t.winner = true) OR (tf.side = 'SELL' AND t.winner = false)
                        THEN 1.0 ELSE 0.0 END
                    ELSE NULL
                END) as win_rate
            FROM TradeFact tf
            JOIN MarketTokenDim t ON tf.token_id = t.token_id
            JOIN MarketDim m ON t.market_id = m.market_id
            WHERE (tf.maker_id = ? OR tf.taker_id = ?)
              AND m.category IS NOT NULL
            GROUP BY m.category
        )
        SELECT
            category,
            trade_count,
            volume,
            category_pnl,
            win_rate
        FROM trader_category_stats
        ORDER BY volume DESC
        LIMIT ?
        """

        try:
            return self.conn.execute(query, [trader_id, trader_id, top_n]).fetchdf()
        except Exception:
            return pd.DataFrame(columns=[
                "category", "trade_count", "volume", "category_pnl", "win_rate"
            ])

    def get_top_tags(
        self,
        trader_id: int,
        top_n: int = 5,
    ) -> pd.DataFrame:
        """
        Get a trader's performance by LLM-assigned market tags.

        Args:
            trader_id: Trader to analyze
            top_n: Number of top tags to return

        Returns:
            DataFrame with tag_name, trade_count, volume, pnl, win_rate
        """
        query = """
        WITH trader_tag_stats AS (
            SELECT
                tg.name as tag_name,
                COUNT(*) as trade_count,
                SUM(tf.size * tf.price) as volume,
                SUM(CASE
                    WHEN tf.side = 'BUY' AND t.winner = true THEN tf.size * (1 - tf.price)
                    WHEN tf.side = 'BUY' AND t.winner = false THEN -tf.size * tf.price
                    WHEN tf.side = 'SELL' AND t.winner = true THEN -tf.size * (1 - tf.price)
                    WHEN tf.side = 'SELL' AND t.winner = false THEN tf.size * tf.price
                    ELSE 0
                END) as tag_pnl,
                AVG(CASE
                    WHEN t.winner IS NOT NULL THEN
                        CASE WHEN (tf.side = 'BUY' AND t.winner = true) OR (tf.side = 'SELL' AND t.winner = false)
                        THEN 1.0 ELSE 0.0 END
                    ELSE NULL
                END) as win_rate
            FROM TradeFact tf
            JOIN MarketTokenDim t ON tf.token_id = t.token_id
            JOIN MarketTagDim mtd ON t.market_id = mtd.market_id
            JOIN Tags tg ON mtd.tag_id = tg.tag_id
            WHERE (tf.maker_id = ? OR tf.taker_id = ?)
              AND tg.is_active = true
            GROUP BY tg.name
        )
        SELECT
            tag_name,
            trade_count,
            volume,
            tag_pnl,
            win_rate
        FROM trader_tag_stats
        ORDER BY volume DESC
        LIMIT ?
        """

        try:
            return self.conn.execute(query, [trader_id, trader_id, top_n]).fetchdf()
        except Exception:
            return pd.DataFrame(columns=[
                "tag_name", "trade_count", "volume", "tag_pnl", "win_rate"
            ])

    def extract_full_profile(
        self,
        trader_id: int,
    ) -> Dict[str, Any]:
        """
        Extract a complete trader profile with all metrics.

        Args:
            trader_id: Trader to analyze

        Returns:
            Dict with all trader features, performance, categories, and best trades
        """
        # Basic features
        basic = self.extract(trader_id=trader_id)
        if basic.empty:
            return {"trader_id": trader_id, "found": False}

        profile = basic.iloc[0].to_dict()
        profile["found"] = True

        # Performance metrics
        perf = self.extract_with_performance(min_resolved_trades=1)
        trader_perf = perf[perf["trader_id"] == trader_id]
        if not trader_perf.empty:
            profile["win_rate"] = trader_perf.iloc[0]["win_rate"]
            profile["estimated_pnl"] = trader_perf.iloc[0]["estimated_pnl"]
            profile["resolved_trade_count"] = trader_perf.iloc[0]["resolved_trade_count"]

        # Largest return
        profile["largest_return"] = self.get_largest_return(trader_id)

        # Top categories
        top_cats = self.get_top_categories(trader_id, top_n=3)
        profile["top_categories"] = top_cats.to_dict("records") if not top_cats.empty else []

        # Top tags
        top_tags = self.get_top_tags(trader_id, top_n=3)
        profile["top_tags"] = top_tags.to_dict("records") if not top_tags.empty else []

        return profile
