"""
Judge service for managing LLM-based market classification.
"""

import json
from datetime import datetime
from typing import Optional
from dataclasses import dataclass
import duckdb

from tag_manager.llm.judge_pool import JudgePool, PoolResult
from tag_manager.services.tag_service import TagService
from tag_manager.services.market_service import MarketService


@dataclass
class JudgeHistoryEntry:
    """Judge history entry data object."""
    history_id: int
    tag_id: int
    market_id: int
    judge_votes: dict
    consensus: Optional[bool]
    human_decision: Optional[bool]
    decided_by: str
    created_at: datetime
    updated_at: datetime
    market_question: Optional[str] = None
    market_description: Optional[str] = None
    tag_name: Optional[str] = None


class JudgeService:
    """
    Service for managing LLM judge operations.

    Usage:
        service = JudgeService(conn)
        result = service.classify_market(tag_id=1, market_id=42)
        service.record_human_decision(history_id=5, decision=True)
    """

    def __init__(
        self,
        conn: duckdb.DuckDBPyConnection,
        judge_pool: Optional[JudgePool] = None,
    ):
        self.conn = conn
        self.judge_pool = judge_pool or JudgePool()
        self.tag_service = TagService(conn)
        self.market_service = MarketService(conn)

    def classify_market(self, tag_id: int, market_id: int) -> JudgeHistoryEntry:
        """
        Classify a market for a tag using the LLM judge pool.

        Args:
            tag_id: The tag to classify for
            market_id: The market to classify

        Returns:
            JudgeHistoryEntry with the results
        """
        # Get tag and market info
        tag = self.tag_service.get_tag(tag_id)
        market = self.market_service.get_market(market_id)

        if not tag or not market:
            raise ValueError(f"Tag {tag_id} or market {market_id} not found")

        # Get examples for the tag
        positive_examples = self.tag_service.get_examples(tag_id, positive_only=True)
        negative_examples = self.tag_service.get_examples(tag_id, positive_only=False)

        # Run classification
        result = self.judge_pool.classify(
            tag_name=tag.name,
            tag_description=tag.description or "",
            market_question=market.question,
            market_description=market.description or "",
            positive_examples=[{"question": e.market_question} for e in positive_examples],
            negative_examples=[{"question": e.market_question} for e in negative_examples],
        )

        # Record the result
        entry = self._record_result(tag_id, market_id, result)

        # If there's consensus, also add to MarketTagDim
        if result.consensus is not None:
            self._apply_tag_decision(tag_id, market_id, result.consensus)

        # Update cursor
        self.tag_service.update_cursor(tag_id, market_id)

        return entry

    def _record_result(
        self,
        tag_id: int,
        market_id: int,
        result: PoolResult,
    ) -> JudgeHistoryEntry:
        """Record a classification result in the database."""
        # Get next ID
        max_id = self.conn.execute(
            "SELECT COALESCE(MAX(history_id), 0) FROM JudgeHistory"
        ).fetchone()[0]
        new_id = max_id + 1

        now = datetime.now()

        self.conn.execute(
            """
            INSERT INTO JudgeHistory (
                history_id, tag_id, market_id, judge_votes, consensus,
                decided_by, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                new_id,
                tag_id,
                market_id,
                result.votes_json,
                result.consensus,
                result.decided_by,
                now,
                now,
            ]
        )

        return self.get_history_entry(new_id)

    def record_human_decision(
        self,
        history_id: int,
        decision: bool,
    ) -> JudgeHistoryEntry:
        """
        Record a human decision for a market that lacked consensus.

        Args:
            history_id: The history entry to update
            decision: True if market belongs to tag, False otherwise

        Returns:
            Updated JudgeHistoryEntry
        """
        now = datetime.now()

        self.conn.execute(
            """
            UPDATE JudgeHistory
            SET human_decision = ?, decided_by = 'human', updated_at = ?
            WHERE history_id = ?
            """,
            [decision, now, history_id]
        )

        # Get the entry to apply the tag
        entry = self.get_history_entry(history_id)
        if entry:
            self._apply_tag_decision(entry.tag_id, entry.market_id, decision)

        return entry

    def update_decision(
        self,
        history_id: int,
        decision: bool,
    ) -> JudgeHistoryEntry:
        """
        Update/override a previous decision (for fine-tuning).

        Args:
            history_id: The history entry to update
            decision: The corrected decision

        Returns:
            Updated JudgeHistoryEntry
        """
        now = datetime.now()

        # Get current entry
        entry = self.get_history_entry(history_id)
        if not entry:
            raise ValueError(f"History entry {history_id} not found")

        # Update the decision
        self.conn.execute(
            """
            UPDATE JudgeHistory
            SET human_decision = ?, decided_by = 'human_correction', updated_at = ?
            WHERE history_id = ?
            """,
            [decision, now, history_id]
        )

        # Update the MarketTagDim accordingly
        self._apply_tag_decision(entry.tag_id, entry.market_id, decision)

        return self.get_history_entry(history_id)

    def _apply_tag_decision(
        self,
        tag_id: int,
        market_id: int,
        belongs_to_tag: bool,
    ) -> None:
        """Apply a tag decision to MarketTagDim."""
        if belongs_to_tag:
            # Add to tagged markets
            max_id = self.conn.execute(
                "SELECT COALESCE(MAX(market_tag_id), 0) FROM MarketTagDim"
            ).fetchone()[0]

            self.conn.execute(
                """
                INSERT INTO MarketTagDim (market_tag_id, market_id, tag_id, created_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT (market_id, tag_id) DO NOTHING
                """,
                [max_id + 1, market_id, tag_id, datetime.now()]
            )
        else:
            # Remove from tagged markets if present
            self.conn.execute(
                "DELETE FROM MarketTagDim WHERE market_id = ? AND tag_id = ?",
                [market_id, tag_id]
            )

    def get_history_entry(self, history_id: int) -> Optional[JudgeHistoryEntry]:
        """Get a single history entry by ID."""
        row = self.conn.execute(
            """
            SELECT h.history_id, h.tag_id, h.market_id, h.judge_votes, h.consensus,
                   h.human_decision, h.decided_by, h.created_at, h.updated_at,
                   m.question, m.description, t.name
            FROM JudgeHistory h
            JOIN MarketDim m ON h.market_id = m.market_id
            JOIN Tags t ON h.tag_id = t.tag_id
            WHERE h.history_id = ?
            """,
            [history_id]
        ).fetchone()

        if not row:
            return None

        return self._row_to_entry(row)

    def get_recent_history(
        self,
        tag_id: Optional[int] = None,
        limit: int = 10,
    ) -> list[JudgeHistoryEntry]:
        """Get recent judge history entries."""
        sql = """
            SELECT h.history_id, h.tag_id, h.market_id, h.judge_votes, h.consensus,
                   h.human_decision, h.decided_by, h.created_at, h.updated_at,
                   m.question, m.description, t.name
            FROM JudgeHistory h
            JOIN MarketDim m ON h.market_id = m.market_id
            JOIN Tags t ON h.tag_id = t.tag_id
        """
        params = []

        if tag_id is not None:
            sql += " WHERE h.tag_id = ?"
            params.append(tag_id)

        sql += " ORDER BY h.updated_at DESC LIMIT ?"
        params.append(limit)

        rows = self.conn.execute(sql, params).fetchall()
        return [self._row_to_entry(r) for r in rows]

    def get_pending_reviews(
        self,
        tag_id: Optional[int] = None,
        limit: int = 10,
    ) -> list[JudgeHistoryEntry]:
        """Get entries that need human review (no consensus, no human decision)."""
        sql = """
            SELECT h.history_id, h.tag_id, h.market_id, h.judge_votes, h.consensus,
                   h.human_decision, h.decided_by, h.created_at, h.updated_at,
                   m.question, m.description, t.name
            FROM JudgeHistory h
            JOIN MarketDim m ON h.market_id = m.market_id
            JOIN Tags t ON h.tag_id = t.tag_id
            WHERE h.consensus IS NULL AND h.human_decision IS NULL
        """
        params = []

        if tag_id is not None:
            sql += " AND h.tag_id = ?"
            params.append(tag_id)

        sql += " ORDER BY h.created_at DESC LIMIT ?"
        params.append(limit)

        rows = self.conn.execute(sql, params).fetchall()
        return [self._row_to_entry(r) for r in rows]

    def _row_to_entry(self, row) -> JudgeHistoryEntry:
        """Convert a database row to a JudgeHistoryEntry."""
        votes = json.loads(row[3]) if isinstance(row[3], str) else row[3]

        return JudgeHistoryEntry(
            history_id=row[0],
            tag_id=row[1],
            market_id=row[2],
            judge_votes=votes,
            consensus=row[4],
            human_decision=row[5],
            decided_by=row[6],
            created_at=row[7],
            updated_at=row[8],
            market_question=row[9],
            market_description=row[10],
            tag_name=row[11],
        )

    def close(self):
        """Close the judge pool."""
        self.judge_pool.close()
