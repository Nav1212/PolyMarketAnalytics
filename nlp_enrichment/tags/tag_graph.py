"""
Tag Graph Management

Provides hierarchical tag management with:
- DAG-based tag hierarchy (parent/child relationships)
- Tag CRUD operations with database persistence
- Embedding-based similarity search
- Tag merging and relationship management

The tag graph is stored in DuckDB with three main tables:
- TagNode: Individual tags with metadata
- TagEdge: Parent-child relationships between tags
- TagEmbedding: Vector embeddings for similarity search

Usage:
    from nlp_enrichment.tags import TagGraph
    from nlp_enrichment.core import EmbeddingService

    graph = TagGraph(conn, embedding_service)

    # Create tags
    politics_id = graph.create_tag("politics", display_name="Politics")
    elections_id = graph.create_tag("elections", display_name="Elections")

    # Create hierarchy
    graph.add_edge(politics_id, elections_id)  # politics -> elections

    # Find similar tags
    similar = graph.find_similar_tags("voting", top_k=5)

    # Get tag hierarchy
    ancestors = graph.get_ancestors(elections_id)
    descendants = graph.get_descendants(politics_id)
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple

import duckdb

from fetcher.utils.logging_config import get_logger
from nlp_enrichment.core.exceptions import (
    TagCycleError,
    TagMergeError,
    TagNotFoundError,
)

logger = get_logger("tag_graph")


# =============================================================================
# Data Classes
# =============================================================================

@dataclass
class Tag:
    """Represents a tag node in the graph."""

    tag_id: int
    name: str
    display_name: Optional[str] = None
    description: Optional[str] = None
    tag_type: str = "topic"
    is_system: bool = False
    usage_count: int = 0
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    @property
    def label(self) -> str:
        """Get display label (display_name if set, else name)."""
        return self.display_name or self.name


@dataclass
class TagEdge:
    """Represents an edge (relationship) between two tags."""

    edge_id: int
    parent_tag_id: int
    child_tag_id: int
    relationship: str = "parent_of"
    weight: float = 1.0
    created_at: Optional[datetime] = None


@dataclass
class TagSearchResult:
    """Result from a tag search or similarity query."""

    tag: Tag
    score: float = 1.0
    match_type: str = "exact"  # "exact", "prefix", "contains", "similar"


# =============================================================================
# Tag Graph Implementation
# =============================================================================

class TagGraph:
    """
    Manages a hierarchical tag graph with embedding-based similarity.

    The graph is a DAG (Directed Acyclic Graph) where:
    - Nodes are tags (topics, categories, entities, etc.)
    - Edges represent parent-child relationships
    - Embeddings enable semantic similarity search

    Thread Safety:
        This class is NOT thread-safe. Use separate instances per thread
        or implement external synchronization.

    Attributes:
        conn: DuckDB connection
        embedding_service: Optional service for embedding operations
    """

    def __init__(
        self,
        conn: duckdb.DuckDBPyConnection,
        embedding_service: Optional[Any] = None,
    ):
        """
        Initialize the TagGraph.

        Args:
            conn: DuckDB connection with TagNode, TagEdge tables
            embedding_service: Optional EmbeddingService for similarity search
        """
        self.conn = conn
        self.embedding_service = embedding_service

        # In-memory cache for frequently accessed tags
        self._tag_cache: Dict[int, Tag] = {}
        self._name_to_id: Dict[str, int] = {}
        self._cache_loaded = False

        logger.info("TagGraph initialized")

    # -------------------------------------------------------------------------
    # Tag CRUD Operations
    # -------------------------------------------------------------------------

    def create_tag(
        self,
        name: str,
        display_name: Optional[str] = None,
        description: Optional[str] = None,
        tag_type: str = "topic",
        is_system: bool = False,
    ) -> int:
        """
        Create a new tag.

        Args:
            name: Unique tag identifier (lowercase, no spaces)
            display_name: Human-readable display name
            description: Tag description
            tag_type: Type of tag (topic, category, entity, etc.)
            is_system: Whether this is a system-defined tag

        Returns:
            New tag ID

        Raises:
            ValueError: If tag with same name already exists
        """
        # Normalize name
        name = name.lower().strip().replace(" ", "_")

        # Check if exists
        existing = self.get_tag_by_name(name)
        if existing:
            raise ValueError(f"Tag '{name}' already exists with ID {existing.tag_id}")

        # Get next ID
        result = self.conn.execute(
            "SELECT COALESCE(MAX(tag_id), 0) + 1 FROM TagNode"
        ).fetchone()
        tag_id = result[0] if result else 1

        # Insert tag
        now = datetime.now()
        self.conn.execute("""
            INSERT INTO TagNode (
                tag_id, name, display_name, description,
                tag_type, is_system, usage_count, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, 0, ?, ?)
        """, [tag_id, name, display_name, description, tag_type, is_system, now, now])

        # Update cache
        tag = Tag(
            tag_id=tag_id,
            name=name,
            display_name=display_name,
            description=description,
            tag_type=tag_type,
            is_system=is_system,
            usage_count=0,
            created_at=now,
            updated_at=now,
        )
        self._tag_cache[tag_id] = tag
        self._name_to_id[name] = tag_id

        # Generate embedding if service available
        if self.embedding_service:
            self._update_tag_embedding(tag)

        logger.debug(f"Created tag: {name} (ID: {tag_id})")
        return tag_id

    def get_tag(self, tag_id: int) -> Optional[Tag]:
        """
        Get a tag by ID.

        Args:
            tag_id: Tag ID to look up

        Returns:
            Tag object or None if not found
        """
        # Check cache
        if tag_id in self._tag_cache:
            return self._tag_cache[tag_id]

        # Query database
        row = self.conn.execute("""
            SELECT tag_id, name, display_name, description,
                   tag_type, is_system, usage_count, created_at, updated_at
            FROM TagNode WHERE tag_id = ?
        """, [tag_id]).fetchone()

        if not row:
            return None

        tag = Tag(
            tag_id=row[0],
            name=row[1],
            display_name=row[2],
            description=row[3],
            tag_type=row[4],
            is_system=row[5],
            usage_count=row[6],
            created_at=row[7],
            updated_at=row[8],
        )

        # Update cache
        self._tag_cache[tag_id] = tag
        self._name_to_id[tag.name] = tag_id

        return tag

    def get_tag_by_name(self, name: str) -> Optional[Tag]:
        """
        Get a tag by name.

        Args:
            name: Tag name (case-insensitive)

        Returns:
            Tag object or None if not found
        """
        name = name.lower().strip().replace(" ", "_")

        # Check cache
        if name in self._name_to_id:
            return self.get_tag(self._name_to_id[name])

        # Query database
        row = self.conn.execute("""
            SELECT tag_id FROM TagNode WHERE name = ?
        """, [name]).fetchone()

        if not row:
            return None

        return self.get_tag(row[0])

    def update_tag(
        self,
        tag_id: int,
        display_name: Optional[str] = None,
        description: Optional[str] = None,
        tag_type: Optional[str] = None,
    ) -> bool:
        """
        Update tag properties.

        Args:
            tag_id: Tag ID to update
            display_name: New display name (None to keep existing)
            description: New description (None to keep existing)
            tag_type: New tag type (None to keep existing)

        Returns:
            True if updated, False if tag not found
        """
        tag = self.get_tag(tag_id)
        if not tag:
            return False

        # Build update query
        updates = []
        params = []

        if display_name is not None:
            updates.append("display_name = ?")
            params.append(display_name)
            tag.display_name = display_name

        if description is not None:
            updates.append("description = ?")
            params.append(description)
            tag.description = description

        if tag_type is not None:
            updates.append("tag_type = ?")
            params.append(tag_type)
            tag.tag_type = tag_type

        if not updates:
            return True

        updates.append("updated_at = ?")
        params.append(datetime.now())
        params.append(tag_id)

        self.conn.execute(f"""
            UPDATE TagNode SET {", ".join(updates)} WHERE tag_id = ?
        """, params)

        # Update embedding if display_name changed
        if display_name is not None and self.embedding_service:
            self._update_tag_embedding(tag)

        return True

    def delete_tag(self, tag_id: int, force: bool = False) -> bool:
        """
        Delete a tag.

        Args:
            tag_id: Tag ID to delete
            force: If True, delete even if tag has assignments

        Returns:
            True if deleted, False if tag not found

        Raises:
            ValueError: If tag has assignments and force=False
        """
        tag = self.get_tag(tag_id)
        if not tag:
            return False

        # Check for assignments
        if not force:
            count = self.conn.execute("""
                SELECT COUNT(*) FROM MarketTagAssignment WHERE tag_id = ?
            """, [tag_id]).fetchone()
            if count and count[0] > 0:
                raise ValueError(
                    f"Tag {tag_id} has {count[0]} assignments. Use force=True to delete."
                )

        # Delete edges
        self.conn.execute("""
            DELETE FROM TagEdge WHERE parent_tag_id = ? OR child_tag_id = ?
        """, [tag_id, tag_id])

        # Delete embedding
        self.conn.execute("""
            DELETE FROM TagEmbedding WHERE tag_id = ?
        """, [tag_id])

        # Delete assignments if force
        if force:
            self.conn.execute("""
                DELETE FROM MarketTagAssignment WHERE tag_id = ?
            """, [tag_id])

        # Delete tag
        self.conn.execute("""
            DELETE FROM TagNode WHERE tag_id = ?
        """, [tag_id])

        # Update cache
        if tag_id in self._tag_cache:
            del self._tag_cache[tag_id]
        if tag.name in self._name_to_id:
            del self._name_to_id[tag.name]

        logger.debug(f"Deleted tag: {tag.name} (ID: {tag_id})")
        return True

    def get_all_tags(self, tag_type: Optional[str] = None) -> List[Tag]:
        """
        Get all tags, optionally filtered by type.

        Args:
            tag_type: Filter by tag type (None for all)

        Returns:
            List of Tag objects
        """
        if tag_type:
            rows = self.conn.execute("""
                SELECT tag_id, name, display_name, description,
                       tag_type, is_system, usage_count, created_at, updated_at
                FROM TagNode WHERE tag_type = ?
                ORDER BY usage_count DESC, name
            """, [tag_type]).fetchall()
        else:
            rows = self.conn.execute("""
                SELECT tag_id, name, display_name, description,
                       tag_type, is_system, usage_count, created_at, updated_at
                FROM TagNode
                ORDER BY usage_count DESC, name
            """).fetchall()

        tags = []
        for row in rows:
            tag = Tag(
                tag_id=row[0],
                name=row[1],
                display_name=row[2],
                description=row[3],
                tag_type=row[4],
                is_system=row[5],
                usage_count=row[6],
                created_at=row[7],
                updated_at=row[8],
            )
            tags.append(tag)
            self._tag_cache[tag.tag_id] = tag
            self._name_to_id[tag.name] = tag.tag_id

        return tags

    # -------------------------------------------------------------------------
    # Edge/Hierarchy Operations
    # -------------------------------------------------------------------------

    def add_edge(
        self,
        parent_id: int,
        child_id: int,
        relationship: str = "parent_of",
        weight: float = 1.0,
    ) -> int:
        """
        Add an edge (relationship) between two tags.

        Args:
            parent_id: Parent tag ID
            child_id: Child tag ID
            relationship: Type of relationship
            weight: Edge weight (for weighted traversal)

        Returns:
            New edge ID

        Raises:
            TagNotFoundError: If parent or child doesn't exist
            TagCycleError: If this would create a cycle
        """
        # Verify tags exist
        parent = self.get_tag(parent_id)
        if not parent:
            raise TagNotFoundError(tag_id=parent_id)

        child = self.get_tag(child_id)
        if not child:
            raise TagNotFoundError(tag_id=child_id)

        # Check for self-loop
        if parent_id == child_id:
            raise TagCycleError(parent.name, child.name, [parent.name])

        # Check for existing edge
        existing = self.conn.execute("""
            SELECT edge_id FROM TagEdge
            WHERE parent_tag_id = ? AND child_tag_id = ? AND relationship = ?
        """, [parent_id, child_id, relationship]).fetchone()

        if existing:
            return existing[0]

        # Check for cycle (child -> ... -> parent path)
        if self._would_create_cycle(parent_id, child_id):
            path = self._find_path(child_id, parent_id)
            path_names = [self.get_tag(tid).name for tid in path] if path else []
            raise TagCycleError(parent.name, child.name, path_names)

        # Get next edge ID
        result = self.conn.execute(
            "SELECT COALESCE(MAX(edge_id), 0) + 1 FROM TagEdge"
        ).fetchone()
        edge_id = result[0] if result else 1

        # Insert edge
        self.conn.execute("""
            INSERT INTO TagEdge (
                edge_id, parent_tag_id, child_tag_id, relationship, weight, created_at
            ) VALUES (?, ?, ?, ?, ?, ?)
        """, [edge_id, parent_id, child_id, relationship, weight, datetime.now()])

        logger.debug(f"Added edge: {parent.name} -> {child.name}")
        return edge_id

    def remove_edge(self, parent_id: int, child_id: int) -> bool:
        """
        Remove an edge between two tags.

        Args:
            parent_id: Parent tag ID
            child_id: Child tag ID

        Returns:
            True if removed, False if edge didn't exist
        """
        result = self.conn.execute("""
            DELETE FROM TagEdge
            WHERE parent_tag_id = ? AND child_tag_id = ?
        """, [parent_id, child_id])

        return result.rowcount > 0 if hasattr(result, 'rowcount') else True

    def get_parents(self, tag_id: int) -> List[Tag]:
        """Get direct parents of a tag."""
        rows = self.conn.execute("""
            SELECT parent_tag_id FROM TagEdge WHERE child_tag_id = ?
        """, [tag_id]).fetchall()

        return [self.get_tag(row[0]) for row in rows if self.get_tag(row[0])]

    def get_children(self, tag_id: int) -> List[Tag]:
        """Get direct children of a tag."""
        rows = self.conn.execute("""
            SELECT child_tag_id FROM TagEdge WHERE parent_tag_id = ?
        """, [tag_id]).fetchall()

        return [self.get_tag(row[0]) for row in rows if self.get_tag(row[0])]

    def get_ancestors(self, tag_id: int, max_depth: int = 10) -> List[Tag]:
        """
        Get all ancestors (parents, grandparents, etc.) of a tag.

        Args:
            tag_id: Tag to get ancestors for
            max_depth: Maximum depth to traverse

        Returns:
            List of ancestor tags (closest first)
        """
        ancestors = []
        visited: Set[int] = set()
        queue = [(tag_id, 0)]

        while queue:
            current_id, depth = queue.pop(0)

            if depth > max_depth:
                continue

            for parent in self.get_parents(current_id):
                if parent.tag_id not in visited:
                    visited.add(parent.tag_id)
                    ancestors.append(parent)
                    queue.append((parent.tag_id, depth + 1))

        return ancestors

    def get_descendants(self, tag_id: int, max_depth: int = 10) -> List[Tag]:
        """
        Get all descendants (children, grandchildren, etc.) of a tag.

        Args:
            tag_id: Tag to get descendants for
            max_depth: Maximum depth to traverse

        Returns:
            List of descendant tags (closest first)
        """
        descendants = []
        visited: Set[int] = set()
        queue = [(tag_id, 0)]

        while queue:
            current_id, depth = queue.pop(0)

            if depth > max_depth:
                continue

            for child in self.get_children(current_id):
                if child.tag_id not in visited:
                    visited.add(child.tag_id)
                    descendants.append(child)
                    queue.append((child.tag_id, depth + 1))

        return descendants

    def get_root_tags(self) -> List[Tag]:
        """Get all tags that have no parents (root nodes)."""
        rows = self.conn.execute("""
            SELECT t.tag_id
            FROM TagNode t
            WHERE NOT EXISTS (
                SELECT 1 FROM TagEdge e WHERE e.child_tag_id = t.tag_id
            )
            ORDER BY t.usage_count DESC
        """).fetchall()

        return [self.get_tag(row[0]) for row in rows if self.get_tag(row[0])]

    def _would_create_cycle(self, parent_id: int, child_id: int) -> bool:
        """Check if adding parent -> child edge would create a cycle."""
        # If child is an ancestor of parent, adding this edge creates a cycle
        visited: Set[int] = set()
        queue = [child_id]

        while queue:
            current = queue.pop(0)
            if current == parent_id:
                return True

            if current in visited:
                continue
            visited.add(current)

            # Check parents of current
            rows = self.conn.execute("""
                SELECT parent_tag_id FROM TagEdge WHERE child_tag_id = ?
            """, [current]).fetchall()

            for row in rows:
                if row[0] not in visited:
                    queue.append(row[0])

        return False

    def _find_path(self, from_id: int, to_id: int) -> Optional[List[int]]:
        """Find path from one tag to another (BFS)."""
        if from_id == to_id:
            return [from_id]

        visited: Set[int] = set()
        queue: List[Tuple[int, List[int]]] = [(from_id, [from_id])]

        while queue:
            current, path = queue.pop(0)

            if current in visited:
                continue
            visited.add(current)

            # Check parents
            rows = self.conn.execute("""
                SELECT parent_tag_id FROM TagEdge WHERE child_tag_id = ?
            """, [current]).fetchall()

            for row in rows:
                parent_id = row[0]
                if parent_id == to_id:
                    return path + [to_id]
                if parent_id not in visited:
                    queue.append((parent_id, path + [parent_id]))

        return None

    # -------------------------------------------------------------------------
    # Search and Similarity
    # -------------------------------------------------------------------------

    def search_tags(
        self,
        query: str,
        limit: int = 10,
        include_similar: bool = True,
    ) -> List[TagSearchResult]:
        """
        Search for tags by name, with optional similarity search.

        Args:
            query: Search query
            limit: Maximum results
            include_similar: Whether to include embedding-based similar tags

        Returns:
            List of TagSearchResult sorted by relevance
        """
        query = query.lower().strip()
        results: List[TagSearchResult] = []
        seen_ids: Set[int] = set()

        # Exact match
        exact = self.get_tag_by_name(query)
        if exact:
            results.append(TagSearchResult(tag=exact, score=1.0, match_type="exact"))
            seen_ids.add(exact.tag_id)

        # Prefix match
        prefix_rows = self.conn.execute("""
            SELECT tag_id FROM TagNode
            WHERE name LIKE ? OR display_name LIKE ?
            ORDER BY usage_count DESC
            LIMIT ?
        """, [f"{query}%", f"{query}%", limit]).fetchall()

        for row in prefix_rows:
            if row[0] not in seen_ids:
                tag = self.get_tag(row[0])
                if tag:
                    results.append(TagSearchResult(tag=tag, score=0.9, match_type="prefix"))
                    seen_ids.add(row[0])

        # Contains match
        contains_rows = self.conn.execute("""
            SELECT tag_id FROM TagNode
            WHERE name LIKE ? OR display_name LIKE ?
            ORDER BY usage_count DESC
            LIMIT ?
        """, [f"%{query}%", f"%{query}%", limit]).fetchall()

        for row in contains_rows:
            if row[0] not in seen_ids:
                tag = self.get_tag(row[0])
                if tag:
                    results.append(TagSearchResult(tag=tag, score=0.7, match_type="contains"))
                    seen_ids.add(row[0])

        # Similarity search
        if include_similar and self.embedding_service and len(results) < limit:
            similar = self.find_similar_tags(query, top_k=limit - len(results))
            for sim_result in similar:
                if sim_result.tag.tag_id not in seen_ids:
                    results.append(sim_result)
                    seen_ids.add(sim_result.tag.tag_id)

        return results[:limit]

    def find_similar_tags(
        self,
        text: str,
        top_k: int = 10,
        min_similarity: float = 0.3,
    ) -> List[TagSearchResult]:
        """
        Find tags similar to the given text using embeddings.

        Args:
            text: Text to find similar tags for
            top_k: Maximum number of results
            min_similarity: Minimum similarity threshold

        Returns:
            List of TagSearchResult sorted by similarity
        """
        if not self.embedding_service:
            logger.debug("No embedding service - skipping similarity search")
            return []

        try:
            # Generate embedding for query text
            result = self.embedding_service.embed(text)
            query_embedding = result.embedding

            # Get all tag embeddings
            rows = self.conn.execute("""
                SELECT te.tag_id, te.embedding
                FROM TagEmbedding te
            """).fetchall()

            results = []
            for tag_id, embedding in rows:
                # Convert embedding if needed
                if hasattr(embedding, 'tolist'):
                    embedding = embedding.tolist()

                similarity = self._cosine_similarity(query_embedding, embedding)

                if similarity >= min_similarity:
                    tag = self.get_tag(tag_id)
                    if tag:
                        results.append(TagSearchResult(
                            tag=tag,
                            score=similarity,
                            match_type="similar",
                        ))

            # Sort by similarity
            results.sort(key=lambda x: x.score, reverse=True)
            return results[:top_k]

        except Exception as e:
            logger.error(f"Similarity search failed: {e}")
            return []

    def _update_tag_embedding(self, tag: Tag) -> None:
        """Generate and store embedding for a tag."""
        if not self.embedding_service:
            return

        try:
            # Use display_name + description for richer embedding
            text = tag.label
            if tag.description:
                text = f"{text}: {tag.description}"

            result = self.embedding_service.embed(text)

            # Upsert embedding
            self.conn.execute("""
                INSERT OR REPLACE INTO TagEmbedding (tag_id, embedding, model, updated_at)
                VALUES (?, ?, ?, ?)
            """, [tag.tag_id, result.embedding, result.model, datetime.now()])

            logger.debug(f"Updated embedding for tag: {tag.name}")

        except Exception as e:
            logger.warning(f"Failed to update embedding for tag {tag.name}: {e}")

    @staticmethod
    def _cosine_similarity(a: List[float], b: List[float]) -> float:
        """Compute cosine similarity between two vectors."""
        import math

        if len(a) != len(b):
            return 0.0

        dot_product = sum(x * y for x, y in zip(a, b))
        norm_a = math.sqrt(sum(x * x for x in a))
        norm_b = math.sqrt(sum(x * x for x in b))

        if norm_a == 0 or norm_b == 0:
            return 0.0

        return dot_product / (norm_a * norm_b)

    # -------------------------------------------------------------------------
    # Tag Merging
    # -------------------------------------------------------------------------

    def merge_tags(
        self,
        source_id: int,
        target_id: int,
        delete_source: bool = True,
    ) -> int:
        """
        Merge source tag into target tag.

        Transfers all assignments and relationships from source to target.

        Args:
            source_id: Tag to merge from
            target_id: Tag to merge into
            delete_source: Whether to delete source after merge

        Returns:
            Number of assignments transferred

        Raises:
            TagNotFoundError: If source or target doesn't exist
            TagMergeError: If merge is not valid
        """
        source = self.get_tag(source_id)
        if not source:
            raise TagNotFoundError(tag_id=source_id)

        target = self.get_tag(target_id)
        if not target:
            raise TagNotFoundError(tag_id=target_id)

        if source_id == target_id:
            raise TagMergeError(source.name, target.name, "Cannot merge tag with itself")

        # Transfer assignments
        result = self.conn.execute("""
            UPDATE MarketTagAssignment
            SET tag_id = ?
            WHERE tag_id = ?
            AND market_id NOT IN (
                SELECT market_id FROM MarketTagAssignment WHERE tag_id = ?
            )
        """, [target_id, source_id, target_id])

        transferred = result.rowcount if hasattr(result, 'rowcount') else 0

        # Delete duplicate assignments
        self.conn.execute("""
            DELETE FROM MarketTagAssignment WHERE tag_id = ?
        """, [source_id])

        # Transfer parent edges (source's parents become target's parents)
        self.conn.execute("""
            INSERT OR IGNORE INTO TagEdge (edge_id, parent_tag_id, child_tag_id, relationship, weight, created_at)
            SELECT (SELECT COALESCE(MAX(edge_id), 0) + ROW_NUMBER() OVER () FROM TagEdge),
                   parent_tag_id, ?, relationship, weight, ?
            FROM TagEdge WHERE child_tag_id = ?
        """, [target_id, datetime.now(), source_id])

        # Transfer child edges (source's children become target's children)
        self.conn.execute("""
            INSERT OR IGNORE INTO TagEdge (edge_id, parent_tag_id, child_tag_id, relationship, weight, created_at)
            SELECT (SELECT COALESCE(MAX(edge_id), 0) + ROW_NUMBER() OVER () FROM TagEdge),
                   ?, child_tag_id, relationship, weight, ?
            FROM TagEdge WHERE parent_tag_id = ?
        """, [target_id, datetime.now(), source_id])

        # Update target usage count
        self.conn.execute("""
            UPDATE TagNode SET usage_count = usage_count + ?
            WHERE tag_id = ?
        """, [source.usage_count, target_id])

        # Delete source if requested
        if delete_source:
            self.delete_tag(source_id, force=True)

        logger.info(f"Merged tag {source.name} into {target.name} ({transferred} assignments)")
        return transferred

    # -------------------------------------------------------------------------
    # Market Tag Assignment
    # -------------------------------------------------------------------------

    def assign_tag_to_market(
        self,
        market_id: int,
        tag_id: int,
        confidence: float = 1.0,
        source: str = "manual",
        assigned_by: Optional[str] = None,
    ) -> int:
        """
        Assign a tag to a market.

        Args:
            market_id: Market to tag
            tag_id: Tag to assign
            confidence: Confidence score (0-1)
            source: Source of assignment (manual, model, unanimous, etc.)
            assigned_by: Who/what made the assignment

        Returns:
            Assignment ID
        """
        tag = self.get_tag(tag_id)
        if not tag:
            raise TagNotFoundError(tag_id=tag_id)

        # Check for existing assignment
        existing = self.conn.execute("""
            SELECT assignment_id FROM MarketTagAssignment
            WHERE market_id = ? AND tag_id = ?
        """, [market_id, tag_id]).fetchone()

        if existing:
            return existing[0]

        # Get next assignment ID
        result = self.conn.execute(
            "SELECT COALESCE(MAX(assignment_id), 0) + 1 FROM MarketTagAssignment"
        ).fetchone()
        assignment_id = result[0] if result else 1

        # Insert assignment
        self.conn.execute("""
            INSERT INTO MarketTagAssignment (
                assignment_id, market_id, tag_id, confidence, source, assigned_by, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """, [assignment_id, market_id, tag_id, confidence, source, assigned_by, datetime.now()])

        # Update usage count
        self.conn.execute("""
            UPDATE TagNode SET usage_count = usage_count + 1 WHERE tag_id = ?
        """, [tag_id])

        if tag_id in self._tag_cache:
            self._tag_cache[tag_id].usage_count += 1

        return assignment_id

    def remove_tag_from_market(self, market_id: int, tag_id: int) -> bool:
        """Remove a tag assignment from a market."""
        result = self.conn.execute("""
            DELETE FROM MarketTagAssignment WHERE market_id = ? AND tag_id = ?
        """, [market_id, tag_id])

        if hasattr(result, 'rowcount') and result.rowcount > 0:
            # Update usage count
            self.conn.execute("""
                UPDATE TagNode SET usage_count = usage_count - 1
                WHERE tag_id = ? AND usage_count > 0
            """, [tag_id])

            if tag_id in self._tag_cache:
                self._tag_cache[tag_id].usage_count = max(0, self._tag_cache[tag_id].usage_count - 1)

            return True

        return False

    def get_market_tags(self, market_id: int) -> List[Tuple[Tag, float, str]]:
        """
        Get all tags assigned to a market.

        Returns:
            List of (Tag, confidence, source) tuples
        """
        rows = self.conn.execute("""
            SELECT tag_id, confidence, source
            FROM MarketTagAssignment
            WHERE market_id = ?
            ORDER BY confidence DESC
        """, [market_id]).fetchall()

        results = []
        for tag_id, confidence, source in rows:
            tag = self.get_tag(tag_id)
            if tag:
                results.append((tag, confidence, source))

        return results

    def get_markets_with_tag(self, tag_id: int, limit: int = 100) -> List[int]:
        """Get all market IDs that have a specific tag."""
        rows = self.conn.execute("""
            SELECT market_id FROM MarketTagAssignment
            WHERE tag_id = ?
            ORDER BY confidence DESC
            LIMIT ?
        """, [tag_id, limit]).fetchall()

        return [row[0] for row in rows]

    # -------------------------------------------------------------------------
    # Utility Methods
    # -------------------------------------------------------------------------

    def refresh_all_embeddings(self) -> int:
        """
        Regenerate embeddings for all tags.

        Returns:
            Number of tags updated
        """
        if not self.embedding_service:
            logger.warning("No embedding service - cannot refresh embeddings")
            return 0

        tags = self.get_all_tags()
        count = 0

        for tag in tags:
            try:
                self._update_tag_embedding(tag)
                count += 1
            except Exception as e:
                logger.warning(f"Failed to update embedding for {tag.name}: {e}")

        logger.info(f"Refreshed embeddings for {count} tags")
        return count

    def get_stats(self) -> Dict[str, Any]:
        """Get tag graph statistics."""
        tag_count = self.conn.execute("SELECT COUNT(*) FROM TagNode").fetchone()
        edge_count = self.conn.execute("SELECT COUNT(*) FROM TagEdge").fetchone()
        assignment_count = self.conn.execute("SELECT COUNT(*) FROM MarketTagAssignment").fetchone()
        embedding_count = self.conn.execute("SELECT COUNT(*) FROM TagEmbedding").fetchone()

        return {
            "total_tags": tag_count[0] if tag_count else 0,
            "total_edges": edge_count[0] if edge_count else 0,
            "total_assignments": assignment_count[0] if assignment_count else 0,
            "tags_with_embeddings": embedding_count[0] if embedding_count else 0,
            "cache_size": len(self._tag_cache),
        }
