"""
Judge service for managing LLM-based market classification.
"""

import json
import threading
import time
from datetime import datetime
from typing import Optional, Callable
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
        majority_yes_only: bool = True,
    ) -> list[JudgeHistoryEntry]:
        """
        Get entries that need human review (no consensus, no human decision).

        Args:
            tag_id: Filter by tag ID
            limit: Maximum entries to return
            majority_yes_only: If True, only return markets where majority voted YES
        """
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
        entries = [self._row_to_entry(r) for r in rows]

        # Filter to only include entries where majority voted YES
        if majority_yes_only:
            filtered = []
            for entry in entries:
                yes_count = sum(1 for v in entry.judge_votes.values() if v is True)
                no_count = sum(1 for v in entry.judge_votes.values() if v is False)
                if yes_count > no_count:
                    filtered.append(entry)
            return filtered

        return entries

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

    def classify_all_new_markets_for_tag(
        self,
        tag_id: int,
        batch_size: int = 50,
        on_progress: Optional[Callable[[int, int], None]] = None,
    ) -> int:
        """
        Classify all new/unprocessed markets for a tag.

        Args:
            tag_id: The tag to classify for
            batch_size: Number of markets to process per batch
            on_progress: Optional callback(processed, total) for progress updates

        Returns:
            Total number of markets classified
        """
        tag = self.tag_service.get_tag(tag_id)
        if not tag:
            return 0

        if tag.example_count < 2:
            return 0

        total_classified = 0

        while True:
            markets = self.market_service.get_markets_for_tagging(
                tag_id=tag_id,
                limit=batch_size,
                after_market_id=tag.last_checked_market_id
            )

            if not markets:
                self.tag_service.mark_all_checked(tag_id, True)
                break

            for market in markets:
                try:
                    self.classify_market(tag_id, market.market_id)
                    total_classified += 1

                    if on_progress:
                        on_progress(total_classified, -1)

                except Exception:
                    pass

            tag = self.tag_service.get_tag(tag_id)

        return total_classified


class BackgroundClassifier:
    """
    Background classifier that automatically processes new markets for all active tags.

    Usage:
        classifier = BackgroundClassifier(db_path)
        classifier.start()
        # ... app runs ...
        classifier.stop()
    """

    def __init__(
        self,
        db_path: str,
        poll_interval: int = 60,
        batch_size: int = 10,
    ):
        """
        Initialize the background classifier.

        Args:
            db_path: Path to the DuckDB database
            poll_interval: Seconds between classification runs
            batch_size: Number of markets to classify per tag per run
        """
        self.db_path = db_path
        self.poll_interval = poll_interval
        self.batch_size = batch_size
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._is_running = False
        self._last_run: Optional[datetime] = None
        self._markets_classified = 0

    @property
    def is_running(self) -> bool:
        return self._is_running

    @property
    def last_run(self) -> Optional[datetime]:
        return self._last_run

    @property
    def markets_classified(self) -> int:
        return self._markets_classified

    def start(self):
        """Start the background classification thread."""
        if self._thread and self._thread.is_alive():
            return

        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()
        self._is_running = True

    def stop(self):
        """Stop the background classification thread."""
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=5)
        self._is_running = False

    def _run_loop(self):
        """Main classification loop."""
        while not self._stop_event.is_set():
            try:
                self._classify_round()
                self._last_run = datetime.now()
            except Exception:
                pass

            self._stop_event.wait(self.poll_interval)

        self._is_running = False

    def _classify_round(self):
        """Run one round of classification for all active tags."""
        conn = duckdb.connect(self.db_path)
        try:
            judge_service = JudgeService(conn)
            tag_service = TagService(conn)

            tags = tag_service.list_tags(active_only=True)

            for tag in tags:
                if self._stop_event.is_set():
                    break

                if tag.example_count < 2:
                    continue

                if tag.all_checked:
                    continue

                markets = judge_service.market_service.get_markets_for_tagging(
                    tag_id=tag.tag_id,
                    limit=self.batch_size,
                    after_market_id=tag.last_checked_market_id
                )

                for market in markets:
                    if self._stop_event.is_set():
                        break

                    try:
                        judge_service.classify_market(tag.tag_id, market.market_id)
                        self._markets_classified += 1
                    except Exception:
                        pass

            judge_service.close()
        finally:
            conn.close()
