"""
Tag service for CRUD operations on tags and examples.
"""

from datetime import datetime
from typing import Optional
from dataclasses import dataclass
import duckdb


@dataclass
class Tag:
    """Tag data object."""
    tag_id: int
    name: str
    description: Optional[str]
    is_active: bool
    all_checked: bool
    last_checked_market_id: Optional[int]
    created_at: datetime
    updated_at: datetime
    example_count: int = 0
    positive_count: int = 0
    negative_count: int = 0


@dataclass
class TagExample:
    """Tag example data object."""
    example_id: int
    tag_id: int
    market_id: int
    is_positive: bool
    created_at: datetime
    market_question: Optional[str] = None
    market_description: Optional[str] = None


class TagService:
    """
    Service for managing tags and their examples.

    Usage:
        service = TagService(conn)
        tags = service.list_tags()
        service.create_tag("Crypto", "Markets about cryptocurrency")
    """

    def __init__(self, conn: duckdb.DuckDBPyConnection):
        self.conn = conn

    def list_tags(self, active_only: bool = False) -> list[Tag]:
        """Get all tags with example counts."""
        query = """
            SELECT
                t.tag_id,
                t.name,
                t.description,
                t.is_active,
                t.all_checked,
                t.last_checked_market_id,
                t.created_at,
                t.updated_at,
                COUNT(e.example_id) as example_count,
                SUM(CASE WHEN e.is_positive THEN 1 ELSE 0 END) as positive_count,
                SUM(CASE WHEN NOT e.is_positive THEN 1 ELSE 0 END) as negative_count
            FROM Tags t
            LEFT JOIN TagExamples e ON t.tag_id = e.tag_id
        """
        if active_only:
            query += " WHERE t.is_active = TRUE"
        query += " GROUP BY t.tag_id, t.name, t.description, t.is_active, t.all_checked, t.last_checked_market_id, t.created_at, t.updated_at ORDER BY t.name"

        rows = self.conn.execute(query).fetchall()
        return [
            Tag(
                tag_id=r[0],
                name=r[1],
                description=r[2],
                is_active=r[3],
                all_checked=r[4],
                last_checked_market_id=r[5],
                created_at=r[6],
                updated_at=r[7],
                example_count=r[8] or 0,
                positive_count=r[9] or 0,
                negative_count=r[10] or 0,
            )
            for r in rows
        ]

    def get_tag(self, tag_id: int) -> Optional[Tag]:
        """Get a single tag by ID."""
        row = self.conn.execute(
            """
            SELECT
                t.tag_id, t.name, t.description, t.is_active, t.all_checked,
                t.last_checked_market_id, t.created_at, t.updated_at,
                COUNT(e.example_id), SUM(CASE WHEN e.is_positive THEN 1 ELSE 0 END),
                SUM(CASE WHEN NOT e.is_positive THEN 1 ELSE 0 END)
            FROM Tags t
            LEFT JOIN TagExamples e ON t.tag_id = e.tag_id
            WHERE t.tag_id = ?
            GROUP BY t.tag_id, t.name, t.description, t.is_active, t.all_checked, t.last_checked_market_id, t.created_at, t.updated_at
            """,
            [tag_id]
        ).fetchone()

        if not row:
            return None

        return Tag(
            tag_id=row[0],
            name=row[1],
            description=row[2],
            is_active=row[3],
            all_checked=row[4],
            last_checked_market_id=row[5],
            created_at=row[6],
            updated_at=row[7],
            example_count=row[8] or 0,
            positive_count=row[9] or 0,
            negative_count=row[10] or 0,
        )

    def get_tag_by_name(self, name: str) -> Optional[Tag]:
        """Get a tag by name."""
        row = self.conn.execute(
            "SELECT tag_id FROM Tags WHERE name = ?",
            [name]
        ).fetchone()

        if not row:
            return None

        return self.get_tag(row[0])

    def create_tag(self, name: str, description: Optional[str] = None) -> Tag:
        """Create a new tag."""
        # Get next ID
        max_id = self.conn.execute(
            "SELECT COALESCE(MAX(tag_id), 0) FROM Tags"
        ).fetchone()[0]
        new_id = max_id + 1

        now = datetime.now()
        self.conn.execute(
            """
            INSERT INTO Tags (tag_id, name, description, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            [new_id, name, description, now, now]
        )

        return self.get_tag(new_id)

    def update_tag(
        self,
        tag_id: int,
        name: Optional[str] = None,
        description: Optional[str] = None,
        is_active: Optional[bool] = None,
    ) -> Optional[Tag]:
        """Update a tag's properties."""
        updates = []
        values = []

        if name is not None:
            updates.append("name = ?")
            values.append(name)
        if description is not None:
            updates.append("description = ?")
            values.append(description)
        if is_active is not None:
            updates.append("is_active = ?")
            values.append(is_active)

        if not updates:
            return self.get_tag(tag_id)

        updates.append("updated_at = ?")
        values.append(datetime.now())
        values.append(tag_id)

        self.conn.execute(
            f"UPDATE Tags SET {', '.join(updates)} WHERE tag_id = ?",
            values
        )

        return self.get_tag(tag_id)

    def delete_tag(self, tag_id: int) -> bool:
        """Delete a tag and all its examples."""
        # Delete examples first
        self.conn.execute("DELETE FROM TagExamples WHERE tag_id = ?", [tag_id])
        self.conn.execute("DELETE FROM MarketTagDim WHERE tag_id = ?", [tag_id])
        result = self.conn.execute("DELETE FROM Tags WHERE tag_id = ?", [tag_id])
        return result.rowcount > 0

    # Example management

    def get_examples(self, tag_id: int, positive_only: Optional[bool] = None) -> list[TagExample]:
        """Get examples for a tag with market details."""
        query = """
            SELECT
                e.example_id, e.tag_id, e.market_id, e.is_positive, e.created_at,
                m.question, m.description
            FROM TagExamples e
            JOIN MarketDim m ON e.market_id = m.market_id
            WHERE e.tag_id = ?
        """
        params = [tag_id]

        if positive_only is not None:
            query += " AND e.is_positive = ?"
            params.append(positive_only)

        query += " ORDER BY e.created_at DESC"

        rows = self.conn.execute(query, params).fetchall()
        return [
            TagExample(
                example_id=r[0],
                tag_id=r[1],
                market_id=r[2],
                is_positive=r[3],
                created_at=r[4],
                market_question=r[5],
                market_description=r[6],
            )
            for r in rows
        ]

    def add_example(self, tag_id: int, market_id: int, is_positive: bool) -> TagExample:
        """Add a market as an example for a tag."""
        # Get next ID
        max_id = self.conn.execute(
            "SELECT COALESCE(MAX(example_id), 0) FROM TagExamples"
        ).fetchone()[0]
        new_id = max_id + 1

        now = datetime.now()

        # Use INSERT OR REPLACE to handle duplicates
        self.conn.execute(
            """
            INSERT INTO TagExamples (example_id, tag_id, market_id, is_positive, created_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT (tag_id, market_id) DO UPDATE SET
                is_positive = EXCLUDED.is_positive,
                created_at = EXCLUDED.created_at
            """,
            [new_id, tag_id, market_id, is_positive, now]
        )

        row = self.conn.execute(
            """
            SELECT e.example_id, e.tag_id, e.market_id, e.is_positive, e.created_at,
                   m.question, m.description
            FROM TagExamples e
            JOIN MarketDim m ON e.market_id = m.market_id
            WHERE e.tag_id = ? AND e.market_id = ?
            """,
            [tag_id, market_id]
        ).fetchone()

        return TagExample(
            example_id=row[0],
            tag_id=row[1],
            market_id=row[2],
            is_positive=row[3],
            created_at=row[4],
            market_question=row[5],
            market_description=row[6],
        )

    def remove_example(self, tag_id: int, market_id: int) -> bool:
        """Remove an example from a tag."""
        result = self.conn.execute(
            "DELETE FROM TagExamples WHERE tag_id = ? AND market_id = ?",
            [tag_id, market_id]
        )
        return result.rowcount > 0

    def update_cursor(self, tag_id: int, market_id: int) -> None:
        """Update the last checked market cursor for a tag."""
        self.conn.execute(
            """
            UPDATE Tags
            SET last_checked_market_id = ?, updated_at = ?
            WHERE tag_id = ?
            """,
            [market_id, datetime.now(), tag_id]
        )

    def mark_all_checked(self, tag_id: int, all_checked: bool = True) -> None:
        """Mark a tag as having all markets checked."""
        self.conn.execute(
            """
            UPDATE Tags
            SET all_checked = ?, updated_at = ?
            WHERE tag_id = ?
            """,
            [all_checked, datetime.now(), tag_id]
        )
