"""
Judge service for managing LLM-based market classification.
"""

import json
import logging
import threading
import time
from datetime import datetime
from queue import Queue, Empty
from typing import Optional, Callable
from dataclasses import dataclass
import duckdb

from tag_manager.llm.judge_pool import JudgePool, PoolResult
from tag_manager.services.tag_service import TagService
from tag_manager.services.market_service import MarketService

logger = logging.getLogger(__name__)


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
        search_query: Optional[str] = None,
        decision_filter: Optional[str] = None,
        source_filter: Optional[str] = None,
    ) -> list[JudgeHistoryEntry]:
        """
        Get recent judge history entries with optional filtering.

        Args:
            tag_id: Filter by tag ID
            limit: Maximum entries to return
            search_query: Search market questions (case-insensitive)
            decision_filter: "Positive", "Negative", or "Pending"
            source_filter: "LLM Consensus" or "Human"
        """
        sql = """
            SELECT h.history_id, h.tag_id, h.market_id, h.judge_votes, h.consensus,
                   h.human_decision, h.decided_by, h.created_at, h.updated_at,
                   m.question, m.description, t.name
            FROM JudgeHistory h
            JOIN MarketDim m ON h.market_id = m.market_id
            JOIN Tags t ON h.tag_id = t.tag_id
            WHERE 1=1
        """
        params = []

        if tag_id is not None:
            sql += " AND h.tag_id = ?"
            params.append(tag_id)

        if search_query:
            sql += " AND m.question ILIKE ?"
            params.append(f"%{search_query}%")

        if decision_filter:
            if decision_filter == "Positive":
                # Positive = human_decision is True OR (consensus is True AND human_decision is NULL)
                sql += " AND (h.human_decision = TRUE OR (h.consensus = TRUE AND h.human_decision IS NULL))"
            elif decision_filter == "Negative":
                # Negative = human_decision is False OR (consensus is False AND human_decision is NULL)
                sql += " AND (h.human_decision = FALSE OR (h.consensus = FALSE AND h.human_decision IS NULL))"
            elif decision_filter == "Pending":
                # Pending = consensus is NULL AND human_decision is NULL
                sql += " AND h.consensus IS NULL AND h.human_decision IS NULL"

        if source_filter:
            if source_filter == "LLM Consensus":
                sql += " AND h.human_decision IS NULL AND h.consensus IS NOT NULL"
            elif source_filter == "Human":
                sql += " AND h.human_decision IS NOT NULL"

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
    
    Uses a queue-based worker pool for parallel classification.

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
        num_workers: Optional[int] = None,
    ):
        """
        Initialize the background classifier.

        Args:
            db_path: Path to the DuckDB database
            poll_interval: Seconds between classification runs
            batch_size: Number of markets to queue per tag per run
            num_workers: Number of parallel worker threads (default from settings)
        """
        self.db_path = db_path
        self.poll_interval = poll_interval
        self.batch_size = batch_size
        self._num_workers = num_workers  # Will be loaded from settings if None
        self._stop_event = threading.Event()
        self._coordinator_thread: Optional[threading.Thread] = None
        self._worker_threads: list[threading.Thread] = []
        self._work_queue: Queue[tuple[int, int]] = Queue()  # (tag_id, market_id)
        self._is_running = False
        self._last_run: Optional[datetime] = None
        self._markets_classified = 0
        self._lock = threading.Lock()

    def _get_num_workers(self) -> int:
        """Get number of workers from settings or use provided value."""
        if self._num_workers is not None:
            return self._num_workers
        try:
            from tag_manager.services.settings_service import SettingsService
            conn = duckdb.connect(self.db_path)
            settings = SettingsService(conn)
            num = settings.get_parallel_workers()
            conn.close()
            return num
        except Exception:
            return 2  # Default

    @property
    def is_running(self) -> bool:
        return self._is_running

    @property
    def last_run(self) -> Optional[datetime]:
        return self._last_run

    @property
    def markets_classified(self) -> int:
        return self._markets_classified

    @property
    def queue_size(self) -> int:
        """Get current number of items in the work queue."""
        return self._work_queue.qsize()

    def start(self):
        """Start the background classification with worker pool."""
        if self._coordinator_thread and self._coordinator_thread.is_alive():
            return

        self._stop_event.clear()
        
        # Get number of workers
        num_workers = self._get_num_workers()
        logger.info(f"Starting BackgroundClassifier with {num_workers} workers")

        # Start worker threads
        self._worker_threads = []
        for i in range(num_workers):
            t = threading.Thread(
                target=self._worker_loop,
                args=(i,),
                daemon=True,
                name=f"ClassifierWorker-{i}"
            )
            t.start()
            self._worker_threads.append(t)

        # Start coordinator thread
        self._coordinator_thread = threading.Thread(
            target=self._coordinator_loop,
            daemon=True,
            name="ClassifierCoordinator"
        )
        self._coordinator_thread.start()
        self._is_running = True

    def stop(self):
        """Stop the background classification (waits for workers to finish current task)."""
        logger.info("Stopping BackgroundClassifier...")
        self._stop_event.set()
        
        # Wait for workers to finish their current task
        for t in self._worker_threads:
            t.join(timeout=30)  # Give workers time to finish current classification
        
        if self._coordinator_thread:
            self._coordinator_thread.join(timeout=5)
        
        self._worker_threads = []
        self._is_running = False
        logger.info("BackgroundClassifier stopped")

    def _coordinator_loop(self):
        """Main coordinator loop - enqueues work for workers."""
        while not self._stop_event.is_set():
            try:
                self._enqueue_work()
                self._last_run = datetime.now()
            except Exception as e:
                logger.error(f"Error in coordinator loop: {e}")

            self._stop_event.wait(self.poll_interval)

    def _enqueue_work(self):
        """Enumerate pending markets and add to work queue."""
        conn = duckdb.connect(self.db_path)
        try:
            tag_service = TagService(conn)
            market_service = MarketService(conn)

            tags = tag_service.list_tags(active_only=True)

            for tag in tags:
                if self._stop_event.is_set():
                    break

                if tag.example_count < 2:
                    continue

                if tag.all_checked:
                    continue

                markets = market_service.get_markets_for_tagging(
                    tag_id=tag.tag_id,
                    limit=self.batch_size,
                    after_market_id=tag.last_checked_market_id
                )

                for market in markets:
                    if self._stop_event.is_set():
                        break
                    self._work_queue.put((tag.tag_id, market.market_id))

        except Exception as e:
            logger.error(f"Error enqueuing work: {e}")
        finally:
            conn.close()

    def _worker_loop(self, worker_id: int):
        """Worker loop - processes items from the work queue."""
        # Each worker gets its own connection for thread safety
        conn = duckdb.connect(self.db_path)
        judge_service = JudgeService(conn)
        
        logger.info(f"Worker {worker_id} started")
        
        try:
            while not self._stop_event.is_set():
                try:
                    # Get work item with timeout to check stop event periodically
                    tag_id, market_id = self._work_queue.get(timeout=1.0)
                except Empty:
                    continue

                try:
                    judge_service.classify_market(tag_id, market_id)
                    with self._lock:
                        self._markets_classified += 1
                    logger.debug(f"Worker {worker_id} classified market {market_id} for tag {tag_id}")
                except Exception as e:
                    logger.error(f"Worker {worker_id} error classifying market {market_id}: {e}")
                    # Continue processing next item
                finally:
                    self._work_queue.task_done()

        finally:
            judge_service.close()
            conn.close()
            logger.info(f"Worker {worker_id} stopped")
