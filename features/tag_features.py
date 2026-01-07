"""
Tag Features Extractor

Extracts categorical features from LLM-assigned tags.
Integrates with the Tag Manager system for market classification.
"""

from typing import Optional, List, Dict
import pandas as pd
from pathlib import Path

from features.base import BaseFeatureExtractor


class TagFeatures(BaseFeatureExtractor):
    """
    Extract tag-based categorical features from the silver layer.

    This extractor leverages the LLM council tagging system to provide
    categorical features for ML models without needing embeddings.

    Features include:
    - One-hot encoded tags per market
    - Tag confidence (based on consensus)
    - Tag coverage (how many tags a market has)
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._tag_names: Optional[List[str]] = None

    def get_feature_names(self) -> List[str]:
        """
        Return list of feature names.

        Note: This is dynamic based on existing tags in the database.
        """
        if self._tag_names is None:
            self._load_tag_names()

        base_features = ["market_id", "tag_count"]
        tag_features = [f"tag_{name}" for name in (self._tag_names or [])]
        return base_features + tag_features

    def _load_tag_names(self) -> None:
        """Load active tag names from database."""
        try:
            query = "SELECT name FROM Tags WHERE is_active = true ORDER BY name"
            result = self.conn.execute(query).fetchdf()
            self._tag_names = result["name"].tolist()
        except Exception:
            self._tag_names = []

    def get_available_tags(self) -> List[str]:
        """
        Get list of available tag names.

        Returns:
            List of active tag names
        """
        if self._tag_names is None:
            self._load_tag_names()
        return self._tag_names or []

    def extract(
        self,
        market_id: Optional[int] = None,
        tag_names: Optional[List[str]] = None,
        limit: Optional[int] = None,
    ) -> pd.DataFrame:
        """
        Extract tag features as one-hot encoded columns.

        Args:
            market_id: Filter to specific market ID
            tag_names: Specific tags to include (uses all active if None)
            limit: Maximum number of markets to return

        Returns:
            DataFrame with market_id and one-hot encoded tag columns
        """
        if self._tag_names is None:
            self._load_tag_names()

        tags_to_use = tag_names or self._tag_names or []

        if not tags_to_use:
            return pd.DataFrame(columns=["market_id", "tag_count"])

        # Build dynamic CASE expressions for each tag
        tag_cases = []
        for tag_name in tags_to_use:
            safe_name = tag_name.replace("'", "''")
            tag_cases.append(f"""
                MAX(CASE WHEN t.name = '{safe_name}' THEN 1 ELSE 0 END) as "tag_{tag_name}"
            """)

        tag_select = ",\n".join(tag_cases)

        query = f"""
        SELECT
            m.market_id,
            COUNT(DISTINCT mtd.tag_id) as tag_count,
            {tag_select}
        FROM MarketDim m
        LEFT JOIN MarketTagDim mtd ON m.market_id = mtd.market_id
        LEFT JOIN Tags t ON mtd.tag_id = t.tag_id AND t.is_active = true
        WHERE 1=1
        """

        params = []

        if market_id is not None:
            query += " AND m.market_id = ?"
            params.append(market_id)

        query += " GROUP BY m.market_id"
        query += " ORDER BY m.market_id"

        if limit is not None:
            query += f" LIMIT {limit}"

        try:
            return self.conn.execute(query, params).fetchdf()
        except Exception:
            return pd.DataFrame(columns=self.get_feature_names())

    def extract_with_confidence(
        self,
        tag_name: str,
        limit: Optional[int] = None,
    ) -> pd.DataFrame:
        """
        Extract tag assignments with confidence scores from judge history.

        Args:
            tag_name: Tag to get confidence for
            limit: Maximum number of markets to return

        Returns:
            DataFrame with market_id, has_tag, consensus, and judge details
        """
        query = """
        SELECT
            m.market_id,
            m.question,
            CASE WHEN mtd.market_tag_id IS NOT NULL THEN 1 ELSE 0 END as has_tag,
            jh.consensus,
            jh.judge_votes,
            jh.human_decision,
            jh.decided_by
        FROM MarketDim m
        LEFT JOIN Tags t ON t.name = ?
        LEFT JOIN MarketTagDim mtd ON m.market_id = mtd.market_id AND mtd.tag_id = t.tag_id
        LEFT JOIN JudgeHistory jh ON m.market_id = jh.market_id AND jh.tag_id = t.tag_id
        WHERE t.tag_id IS NOT NULL
        ORDER BY m.market_id
        """

        params = [tag_name]

        if limit is not None:
            query += f" LIMIT {limit}"

        try:
            return self.conn.execute(query, params).fetchdf()
        except Exception:
            return pd.DataFrame(columns=[
                "market_id", "question", "has_tag", "consensus",
                "judge_votes", "human_decision", "decided_by"
            ])

    def get_tag_statistics(self) -> pd.DataFrame:
        """
        Get statistics about tag coverage across markets.

        Returns:
            DataFrame with tag_name, market_count, and coverage_pct
        """
        query = """
        WITH total_markets AS (
            SELECT COUNT(*) as total FROM MarketDim
        )
        SELECT
            t.name as tag_name,
            COUNT(DISTINCT mtd.market_id) as market_count,
            ROUND(COUNT(DISTINCT mtd.market_id)::FLOAT / tm.total * 100, 2) as coverage_pct
        FROM Tags t
        CROSS JOIN total_markets tm
        LEFT JOIN MarketTagDim mtd ON t.tag_id = mtd.tag_id
        WHERE t.is_active = true
        GROUP BY t.tag_id, t.name, tm.total
        ORDER BY market_count DESC
        """

        try:
            return self.conn.execute(query).fetchdf()
        except Exception:
            return pd.DataFrame(columns=["tag_name", "market_count", "coverage_pct"])
