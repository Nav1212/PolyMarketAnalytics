"""
Embedding Service

A high-level service for generating and managing embeddings with:
- Multi-level caching (memory + database)
- Batch processing for efficiency
- Similarity search capabilities
- Automatic normalization

This service sits on top of OllamaClient and provides:
1. Automatic caching to avoid recomputation
2. Batch processing for multiple texts
3. Persistence to DuckDB for durability
4. Similarity search using cosine distance

Usage:
    from nlp_enrichment.core import EmbeddingService, OllamaClient
    from fetcher.config import EmbeddingConfig

    client = OllamaClient()
    service = EmbeddingService(client, EmbeddingConfig())

    # Generate embedding (cached)
    embedding = service.embed("Hello world")

    # Batch embedding
    embeddings = service.embed_batch(["Hello", "World", "Test"])

    # Find similar texts
    similar = service.find_similar(embedding, top_k=5)
"""

import hashlib
import math
import threading
import time
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple, Union

import duckdb

from fetcher.config import EmbeddingConfig, EmbeddingCacheConfig
from fetcher.utils.logging_config import get_logger
from nlp_enrichment.core.exceptions import (
    EmbeddingCacheError,
    EmbeddingDimensionError,
    EmbeddingError,
    EmbeddingGenerationError,
)
from nlp_enrichment.core.ollama_client import OllamaClient

logger = get_logger("embedding_service")


# =============================================================================
# Data Classes
# =============================================================================

@dataclass
class EmbeddingResult:
    """Result of an embedding operation."""

    text: str
    embedding: List[float]
    text_hash: str
    model: str
    from_cache: bool = False
    cache_level: Optional[str] = None  # "memory", "database", None

    @property
    def dimension(self) -> int:
        """Get embedding dimension."""
        return len(self.embedding)


@dataclass
class SimilarityResult:
    """Result of a similarity search."""

    text: str
    text_hash: str
    embedding: List[float]
    similarity: float  # Cosine similarity (0 to 1)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class CacheStats:
    """Statistics about cache performance."""

    memory_hits: int = 0
    memory_misses: int = 0
    database_hits: int = 0
    database_misses: int = 0
    generations: int = 0
    memory_size: int = 0
    database_size: int = 0

    @property
    def total_requests(self) -> int:
        return self.memory_hits + self.memory_misses

    @property
    def memory_hit_rate(self) -> float:
        if self.total_requests == 0:
            return 0.0
        return self.memory_hits / self.total_requests

    @property
    def overall_hit_rate(self) -> float:
        if self.total_requests == 0:
            return 0.0
        return (self.memory_hits + self.database_hits) / self.total_requests


# =============================================================================
# LRU Cache Implementation
# =============================================================================

class LRUCache:
    """
    Thread-safe LRU cache for embeddings.

    Uses OrderedDict to maintain insertion order for LRU eviction.
    """

    def __init__(self, max_size: int = 10000):
        self._cache: OrderedDict[str, List[float]] = OrderedDict()
        self._max_size = max_size
        self._lock = threading.RLock()

    def get(self, key: str) -> Optional[List[float]]:
        """Get item from cache, moving it to end (most recently used)."""
        with self._lock:
            if key in self._cache:
                # Move to end (most recently used)
                self._cache.move_to_end(key)
                return self._cache[key]
            return None

    def put(self, key: str, value: List[float]) -> None:
        """Add item to cache, evicting oldest if necessary."""
        with self._lock:
            if key in self._cache:
                # Update existing and move to end
                self._cache.move_to_end(key)
                self._cache[key] = value
            else:
                # Add new item
                self._cache[key] = value
                # Evict oldest if over capacity
                while len(self._cache) > self._max_size:
                    self._cache.popitem(last=False)

    def contains(self, key: str) -> bool:
        """Check if key is in cache without updating order."""
        with self._lock:
            return key in self._cache

    def clear(self) -> None:
        """Clear all items from cache."""
        with self._lock:
            self._cache.clear()

    def size(self) -> int:
        """Get current cache size."""
        with self._lock:
            return len(self._cache)

    def keys(self) -> List[str]:
        """Get all keys in cache."""
        with self._lock:
            return list(self._cache.keys())


# =============================================================================
# Embedding Service
# =============================================================================

class EmbeddingService:
    """
    High-level service for embedding generation and management.

    Features:
    - Two-level caching: fast memory cache + persistent database cache
    - Batch processing for efficiency
    - Automatic embedding normalization
    - Similarity search using cosine distance
    - Thread-safe operations

    Architecture:
        Request -> Memory Cache -> Database Cache -> Ollama Generation

    The service maintains a write buffer for database persistence,
    flushing periodically to avoid excessive database writes.

    Attributes:
        client: OllamaClient for embedding generation
        config: EmbeddingConfig with service settings
        model: Model name for embeddings
        dimension: Expected embedding dimension
    """

    def __init__(
        self,
        client: OllamaClient,
        config: Optional[EmbeddingConfig] = None,
        conn: Optional[duckdb.DuckDBPyConnection] = None,
    ):
        """
        Initialize the embedding service.

        Args:
            client: OllamaClient for API calls
            config: Embedding configuration
            conn: DuckDB connection for cache persistence
        """
        self.client = client
        self.config = config or EmbeddingConfig()
        self.model = client.config.embedding_model
        self.dimension = self.config.dimension
        self.conn = conn

        # Memory cache (LRU)
        self._memory_cache = LRUCache(
            max_size=self.config.cache.max_memory_items
        )

        # Write buffer for batched database writes
        self._write_buffer: Dict[str, Tuple[str, List[float]]] = {}
        self._write_buffer_lock = threading.Lock()

        # Statistics
        self._stats = CacheStats()
        self._stats_lock = threading.Lock()

        # Initialize database table if persistence is enabled
        if self.conn and self.config.cache.persist_to_db:
            self._ensure_cache_table()

        logger.info(
            f"EmbeddingService initialized: model={self.model}, "
            f"dimension={self.dimension}, cache_enabled={self.config.cache.enabled}"
        )

    def _ensure_cache_table(self) -> None:
        """Create the embedding cache table if it doesn't exist."""
        try:
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS EmbeddingCache (
                    text_hash VARCHAR PRIMARY KEY,
                    text VARCHAR NOT NULL,
                    embedding DOUBLE[] NOT NULL,
                    model VARCHAR NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            self.conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_embedding_cache_model
                ON EmbeddingCache(model)
            """)
            logger.debug("EmbeddingCache table ensured")
        except Exception as e:
            logger.warning(f"Failed to create EmbeddingCache table: {e}")

    # -------------------------------------------------------------------------
    # Core Embedding Methods
    # -------------------------------------------------------------------------

    def embed(
        self,
        text: str,
        use_cache: bool = True,
    ) -> EmbeddingResult:
        """
        Generate embedding for a single text.

        Checks memory cache first, then database cache, then generates new.

        Args:
            text: Text to embed
            use_cache: Whether to use caching (default True)

        Returns:
            EmbeddingResult with embedding and metadata

        Raises:
            EmbeddingGenerationError: If embedding generation fails
        """
        text_hash = self._compute_hash(text)

        # Try caches if enabled
        if use_cache and self.config.cache.enabled:
            # Level 1: Memory cache
            cached = self._memory_cache.get(text_hash)
            if cached is not None:
                with self._stats_lock:
                    self._stats.memory_hits += 1
                return EmbeddingResult(
                    text=text,
                    embedding=cached,
                    text_hash=text_hash,
                    model=self.model,
                    from_cache=True,
                    cache_level="memory",
                )
            else:
                with self._stats_lock:
                    self._stats.memory_misses += 1

            # Level 2: Database cache
            if self.conn and self.config.cache.persist_to_db:
                db_cached = self._get_from_database(text_hash)
                if db_cached is not None:
                    # Promote to memory cache
                    self._memory_cache.put(text_hash, db_cached)
                    with self._stats_lock:
                        self._stats.database_hits += 1
                    return EmbeddingResult(
                        text=text,
                        embedding=db_cached,
                        text_hash=text_hash,
                        model=self.model,
                        from_cache=True,
                        cache_level="database",
                    )
                else:
                    with self._stats_lock:
                        self._stats.database_misses += 1

        # Generate new embedding
        try:
            embedding = self.client.embed(self.model, text)

            # Validate dimension
            if len(embedding) != self.dimension:
                raise EmbeddingDimensionError(
                    expected_dimension=self.dimension,
                    actual_dimension=len(embedding),
                    model=self.model,
                )

            # Normalize if configured
            if self.config.normalize:
                embedding = self._normalize(embedding)

            # Update caches
            if use_cache and self.config.cache.enabled:
                self._memory_cache.put(text_hash, embedding)
                if self.conn and self.config.cache.persist_to_db:
                    self._add_to_write_buffer(text_hash, text, embedding)

            with self._stats_lock:
                self._stats.generations += 1

            return EmbeddingResult(
                text=text,
                embedding=embedding,
                text_hash=text_hash,
                model=self.model,
                from_cache=False,
                cache_level=None,
            )

        except Exception as e:
            raise EmbeddingGenerationError(
                message=f"Failed to generate embedding: {e}",
                text_preview=text[:100],
                model=self.model,
                original_error=e,
            )

    def embed_batch(
        self,
        texts: List[str],
        use_cache: bool = True,
        show_progress: bool = False,
    ) -> List[EmbeddingResult]:
        """
        Generate embeddings for multiple texts efficiently.

        Processes texts in batches, using cache where available
        and only generating for uncached texts.

        Args:
            texts: List of texts to embed
            use_cache: Whether to use caching
            show_progress: Whether to log progress

        Returns:
            List of EmbeddingResult objects (same order as input)
        """
        results: Dict[str, EmbeddingResult] = {}
        texts_to_generate: List[Tuple[int, str, str]] = []  # (idx, text, hash)

        # Check cache for all texts
        for idx, text in enumerate(texts):
            text_hash = self._compute_hash(text)

            if use_cache and self.config.cache.enabled:
                # Check memory cache
                cached = self._memory_cache.get(text_hash)
                if cached is not None:
                    with self._stats_lock:
                        self._stats.memory_hits += 1
                    results[text_hash] = EmbeddingResult(
                        text=text,
                        embedding=cached,
                        text_hash=text_hash,
                        model=self.model,
                        from_cache=True,
                        cache_level="memory",
                    )
                    continue

                with self._stats_lock:
                    self._stats.memory_misses += 1

                # Check database cache
                if self.conn and self.config.cache.persist_to_db:
                    db_cached = self._get_from_database(text_hash)
                    if db_cached is not None:
                        self._memory_cache.put(text_hash, db_cached)
                        with self._stats_lock:
                            self._stats.database_hits += 1
                        results[text_hash] = EmbeddingResult(
                            text=text,
                            embedding=db_cached,
                            text_hash=text_hash,
                            model=self.model,
                            from_cache=True,
                            cache_level="database",
                        )
                        continue
                    with self._stats_lock:
                        self._stats.database_misses += 1

            texts_to_generate.append((idx, text, text_hash))

        # Generate embeddings in batches
        if texts_to_generate:
            if show_progress:
                logger.info(
                    f"Generating {len(texts_to_generate)} embeddings "
                    f"({len(texts) - len(texts_to_generate)} from cache)"
                )

            batch_size = self.config.batch_size
            for batch_start in range(0, len(texts_to_generate), batch_size):
                batch = texts_to_generate[batch_start:batch_start + batch_size]
                batch_texts = [t[1] for t in batch]

                try:
                    # Ollama supports batch embedding
                    embeddings = self.client.embed(self.model, batch_texts)

                    for (idx, text, text_hash), embedding in zip(batch, embeddings):
                        # Validate and normalize
                        if len(embedding) != self.dimension:
                            logger.warning(
                                f"Dimension mismatch for text hash {text_hash}: "
                                f"expected {self.dimension}, got {len(embedding)}"
                            )
                            continue

                        if self.config.normalize:
                            embedding = self._normalize(embedding)

                        # Update caches
                        if use_cache and self.config.cache.enabled:
                            self._memory_cache.put(text_hash, embedding)
                            if self.conn and self.config.cache.persist_to_db:
                                self._add_to_write_buffer(text_hash, text, embedding)

                        with self._stats_lock:
                            self._stats.generations += 1

                        results[text_hash] = EmbeddingResult(
                            text=text,
                            embedding=embedding,
                            text_hash=text_hash,
                            model=self.model,
                            from_cache=False,
                            cache_level=None,
                        )

                except Exception as e:
                    logger.error(f"Batch embedding failed: {e}")
                    # Try individual embedding as fallback
                    for idx, text, text_hash in batch:
                        try:
                            result = self.embed(text, use_cache=use_cache)
                            results[text_hash] = result
                        except Exception as inner_e:
                            logger.error(f"Failed to embed text: {inner_e}")

        # Flush write buffer if threshold reached
        self._maybe_flush_write_buffer()

        # Return results in original order
        return [
            results.get(self._compute_hash(text))
            for text in texts
            if self._compute_hash(text) in results
        ]

    # -------------------------------------------------------------------------
    # Similarity Search
    # -------------------------------------------------------------------------

    def find_similar(
        self,
        query_embedding: List[float],
        top_k: int = 10,
        min_similarity: float = 0.0,
    ) -> List[SimilarityResult]:
        """
        Find texts with similar embeddings using cosine similarity.

        Searches the database cache for similar embeddings.

        Args:
            query_embedding: Query embedding vector
            top_k: Maximum number of results
            min_similarity: Minimum similarity threshold (0 to 1)

        Returns:
            List of SimilarityResult sorted by similarity (descending)

        Note:
            Requires database persistence to be enabled.
        """
        if not self.conn or not self.config.cache.persist_to_db:
            logger.warning("Similarity search requires database persistence")
            return []

        # Normalize query if needed
        if self.config.normalize:
            query_embedding = self._normalize(query_embedding)

        try:
            # Use DuckDB's array operations for cosine similarity
            # DuckDB supports list_cosine_similarity starting from 0.10.0
            # For compatibility, we'll compute it manually

            # Get all embeddings
            rows = self.conn.execute("""
                SELECT text_hash, text, embedding
                FROM EmbeddingCache
                WHERE model = ?
            """, [self.model]).fetchall()

            results = []
            for text_hash, text, embedding in rows:
                # Convert to list if needed
                if hasattr(embedding, 'tolist'):
                    embedding = embedding.tolist()

                similarity = self._cosine_similarity(query_embedding, embedding)

                if similarity >= min_similarity:
                    results.append(SimilarityResult(
                        text=text,
                        text_hash=text_hash,
                        embedding=embedding,
                        similarity=similarity,
                    ))

            # Sort by similarity descending
            results.sort(key=lambda x: x.similarity, reverse=True)

            return results[:top_k]

        except Exception as e:
            logger.error(f"Similarity search failed: {e}")
            return []

    def find_similar_to_text(
        self,
        text: str,
        top_k: int = 10,
        min_similarity: float = 0.0,
        exclude_self: bool = True,
    ) -> List[SimilarityResult]:
        """
        Find texts similar to the given text.

        Embeds the query text and performs similarity search.

        Args:
            text: Query text
            top_k: Maximum number of results
            min_similarity: Minimum similarity threshold
            exclude_self: Whether to exclude the query text from results

        Returns:
            List of SimilarityResult
        """
        result = self.embed(text)

        results = self.find_similar(
            result.embedding,
            top_k=top_k + (1 if exclude_self else 0),
            min_similarity=min_similarity,
        )

        if exclude_self:
            results = [r for r in results if r.text_hash != result.text_hash]

        return results[:top_k]

    # -------------------------------------------------------------------------
    # Cache Management
    # -------------------------------------------------------------------------

    def _get_from_database(self, text_hash: str) -> Optional[List[float]]:
        """Get embedding from database cache."""
        try:
            row = self.conn.execute("""
                SELECT embedding FROM EmbeddingCache
                WHERE text_hash = ? AND model = ?
            """, [text_hash, self.model]).fetchone()

            if row:
                embedding = row[0]
                # Convert to list if numpy array
                if hasattr(embedding, 'tolist'):
                    return embedding.tolist()
                return list(embedding)
            return None
        except Exception as e:
            logger.debug(f"Database cache lookup failed: {e}")
            return None

    def _add_to_write_buffer(
        self,
        text_hash: str,
        text: str,
        embedding: List[float],
    ) -> None:
        """Add embedding to write buffer for batched database persistence."""
        with self._write_buffer_lock:
            self._write_buffer[text_hash] = (text, embedding)

    def _maybe_flush_write_buffer(self) -> None:
        """Flush write buffer if threshold reached."""
        with self._write_buffer_lock:
            if len(self._write_buffer) >= self.config.cache.batch_persist_threshold:
                self._flush_write_buffer_locked()

    def _flush_write_buffer_locked(self) -> None:
        """Flush write buffer to database (must hold lock)."""
        if not self._write_buffer or not self.conn:
            return

        try:
            for text_hash, (text, embedding) in self._write_buffer.items():
                self.conn.execute("""
                    INSERT OR REPLACE INTO EmbeddingCache
                    (text_hash, text, embedding, model)
                    VALUES (?, ?, ?, ?)
                """, [text_hash, text, embedding, self.model])

            logger.debug(f"Flushed {len(self._write_buffer)} embeddings to database")
            self._write_buffer.clear()
        except Exception as e:
            logger.error(f"Failed to flush write buffer: {e}")

    def flush_cache(self) -> None:
        """Force flush of write buffer to database."""
        with self._write_buffer_lock:
            self._flush_write_buffer_locked()

    def clear_memory_cache(self) -> None:
        """Clear the memory cache."""
        self._memory_cache.clear()
        logger.info("Memory cache cleared")

    def clear_database_cache(self) -> None:
        """Clear the database cache for this model."""
        if self.conn:
            try:
                self.conn.execute("""
                    DELETE FROM EmbeddingCache WHERE model = ?
                """, [self.model])
                logger.info(f"Database cache cleared for model {self.model}")
            except Exception as e:
                logger.error(f"Failed to clear database cache: {e}")

    def get_cache_stats(self) -> CacheStats:
        """Get cache performance statistics."""
        with self._stats_lock:
            stats = CacheStats(
                memory_hits=self._stats.memory_hits,
                memory_misses=self._stats.memory_misses,
                database_hits=self._stats.database_hits,
                database_misses=self._stats.database_misses,
                generations=self._stats.generations,
                memory_size=self._memory_cache.size(),
                database_size=self._get_database_cache_size(),
            )
        return stats

    def _get_database_cache_size(self) -> int:
        """Get number of entries in database cache."""
        if not self.conn:
            return 0
        try:
            row = self.conn.execute("""
                SELECT COUNT(*) FROM EmbeddingCache WHERE model = ?
            """, [self.model]).fetchone()
            return row[0] if row else 0
        except Exception:
            return 0

    # -------------------------------------------------------------------------
    # Utility Methods
    # -------------------------------------------------------------------------

    @staticmethod
    def _compute_hash(text: str) -> str:
        """Compute SHA256 hash of text."""
        return hashlib.sha256(text.encode()).hexdigest()

    @staticmethod
    def _normalize(embedding: List[float]) -> List[float]:
        """L2-normalize an embedding vector."""
        norm = math.sqrt(sum(x * x for x in embedding))
        if norm == 0:
            return embedding
        return [x / norm for x in embedding]

    @staticmethod
    def _cosine_similarity(a: List[float], b: List[float]) -> float:
        """Compute cosine similarity between two vectors."""
        if len(a) != len(b):
            return 0.0

        dot_product = sum(x * y for x, y in zip(a, b))
        norm_a = math.sqrt(sum(x * x for x in a))
        norm_b = math.sqrt(sum(x * x for x in b))

        if norm_a == 0 or norm_b == 0:
            return 0.0

        return dot_product / (norm_a * norm_b)

    def preload_from_database(self, limit: int = 1000) -> int:
        """
        Preload recent embeddings from database into memory cache.

        Args:
            limit: Maximum number of embeddings to preload

        Returns:
            Number of embeddings loaded
        """
        if not self.conn or not self.config.cache.persist_to_db:
            return 0

        try:
            rows = self.conn.execute("""
                SELECT text_hash, embedding
                FROM EmbeddingCache
                WHERE model = ?
                ORDER BY created_at DESC
                LIMIT ?
            """, [self.model, limit]).fetchall()

            count = 0
            for text_hash, embedding in rows:
                if hasattr(embedding, 'tolist'):
                    embedding = embedding.tolist()
                self._memory_cache.put(text_hash, embedding)
                count += 1

            logger.info(f"Preloaded {count} embeddings into memory cache")
            return count

        except Exception as e:
            logger.error(f"Failed to preload embeddings: {e}")
            return 0


# =============================================================================
# Convenience Functions
# =============================================================================

def create_embedding_service(
    client: Optional[OllamaClient] = None,
    config: Optional[EmbeddingConfig] = None,
    conn: Optional[duckdb.DuckDBPyConnection] = None,
) -> EmbeddingService:
    """
    Create an EmbeddingService with default configuration.

    Args:
        client: OllamaClient (creates new if not provided)
        config: EmbeddingConfig (uses defaults if not provided)
        conn: DuckDB connection for persistence

    Returns:
        Configured EmbeddingService
    """
    if client is None:
        client = OllamaClient()

    return EmbeddingService(client, config, conn)


def compute_similarity(
    embedding1: List[float],
    embedding2: List[float],
) -> float:
    """
    Compute cosine similarity between two embeddings.

    Args:
        embedding1: First embedding vector
        embedding2: Second embedding vector

    Returns:
        Cosine similarity (0 to 1 for normalized vectors)
    """
    return EmbeddingService._cosine_similarity(embedding1, embedding2)
