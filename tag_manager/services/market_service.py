"""
Market service for querying markets from the database.
"""

from typing import Optional
from dataclasses import dataclass
from datetime import datetime
import duckdb


@dataclass
class Market:
    """Market data object."""
    market_id: int
    condition_id: str
    question: str
    description: Optional[str]
    category: Optional[str]
    volume: Optional[float]
    active: bool
    closed: bool


class MarketService:
    """
    Service for querying markets.

    Usage:
        service = MarketService(conn)
        markets = service.search_markets("bitcoin")
        market = service.get_market(123)
    """

    def __init__(self, conn: duckdb.DuckDBPyConnection):
        self.conn = conn

    def get_market(self, market_id: int) -> Optional[Market]:
        """Get a single market by ID."""
        row = self.conn.execute(
            """
            SELECT market_id, condition_id, question, description,
                   category, volume, active, closed
            FROM MarketDim
            WHERE market_id = ?
            """,
            [market_id]
        ).fetchone()

        if not row:
            return None

        return Market(
            market_id=row[0],
            condition_id=row[1],
            question=row[2],
            description=row[3],
            category=row[4],
            volume=row[5],
            active=row[6],
            closed=row[7],
        )

    def search_markets(
        self,
        query: str,
        limit: int = 50,
        offset: int = 0,
        active_only: bool = False,
    ) -> list[Market]:
        """Search markets by question text."""
        sql = """
            SELECT market_id, condition_id, question, description,
                   category, volume, active, closed
            FROM MarketDim
            WHERE question ILIKE ?
        """
        params = [f"%{query}%"]

        if active_only:
            sql += " AND active = TRUE AND closed = FALSE"

        sql += " ORDER BY volume DESC NULLS LAST LIMIT ? OFFSET ?"
        params.extend([limit, offset])

        rows = self.conn.execute(sql, params).fetchall()
        return [self._row_to_market(r) for r in rows]

    def get_markets_for_tagging(
        self,
        tag_id: int,
        limit: int = 10,
        after_market_id: Optional[int] = None,
    ) -> list[Market]:
        """
        Get markets that haven't been tagged or judged for a specific tag.

        Returns markets in order by market_id for consistent cursor-based pagination.
        """
        sql = """
            SELECT m.market_id, m.condition_id, m.question, m.description,
                   m.category, m.volume, m.active, m.closed
            FROM MarketDim m
            WHERE m.market_id NOT IN (
                SELECT market_id FROM MarketTagDim WHERE tag_id = ?
            )
            AND m.market_id NOT IN (
                SELECT market_id FROM JudgeHistory WHERE tag_id = ?
            )
            AND m.market_id NOT IN (
                SELECT market_id FROM TagExamples WHERE tag_id = ?
            )
        """
        params = [tag_id, tag_id, tag_id]

        if after_market_id is not None:
            sql += " AND m.market_id > ?"
            params.append(after_market_id)

        sql += " ORDER BY m.market_id LIMIT ?"
        params.append(limit)

        rows = self.conn.execute(sql, params).fetchall()
        return [self._row_to_market(r) for r in rows]

    def get_markets_needing_review(
        self,
        tag_id: int,
        limit: int = 10,
    ) -> list[Market]:
        """
        Get markets where judges didn't reach consensus and need human review.
        """
        sql = """
            SELECT m.market_id, m.condition_id, m.question, m.description,
                   m.category, m.volume, m.active, m.closed
            FROM MarketDim m
            JOIN JudgeHistory j ON m.market_id = j.market_id
            WHERE j.tag_id = ?
              AND j.consensus IS NULL
              AND j.human_decision IS NULL
            ORDER BY j.created_at DESC
            LIMIT ?
        """
        rows = self.conn.execute(sql, [tag_id, limit]).fetchall()
        return [self._row_to_market(r) for r in rows]

    def count_markets(self) -> int:
        """Get total count of markets."""
        return self.conn.execute("SELECT COUNT(*) FROM MarketDim").fetchone()[0]

    def count_tagged_markets(self, tag_id: int) -> int:
        """Get count of markets tagged with a specific tag."""
        return self.conn.execute(
            "SELECT COUNT(*) FROM MarketTagDim WHERE tag_id = ?",
            [tag_id]
        ).fetchone()[0]

    def _row_to_market(self, row) -> Market:
        """Convert a database row to a Market object."""
        return Market(
            market_id=row[0],
            condition_id=row[1],
            question=row[2],
            description=row[3],
            category=row[4],
            volume=row[5],
            active=row[6],
            closed=row[7],
        )
