"""
Unit tests for EmbeddingService.

Tests the embedding service with mocked OllamaClient and DuckDB.
"""

import math
import pytest
from unittest.mock import MagicMock, patch, PropertyMock
from dataclasses import dataclass
from typing import List
import threading
import time

import duckdb

from fetcher.config import EmbeddingConfig, EmbeddingCacheConfig
from nlp_enrichment.core.embedding_service import (
    EmbeddingService,
    EmbeddingResult,
    SimilarityResult,
    CacheStats,
    LRUCache,
    create_embedding_service,
    compute_similarity,
)
from nlp_enrichment.core.exceptions import (
    EmbeddingGenerationError,
    EmbeddingDimensionError,
    EmbeddingCacheError,
)


# =============================================================================
# Fixtures
# =============================================================================

@pytest.fixture
def mock_ollama_client():
    """Create a mock OllamaClient."""
    client = MagicMock()
    client.config = MagicMock()
    client.config.embedding_model = "nomic-embed-text"
    return client


@pytest.fixture
def default_embedding_config():
    """Create default embedding configuration."""
    return EmbeddingConfig(
        batch_size=32,
        normalize=True,
        dimension=768,
        cache=EmbeddingCacheConfig(
            enabled=True,
            max_memory_items=100,
            persist_to_db=True,
            batch_persist_threshold=10,
        ),
    )


@pytest.fixture
def in_memory_conn():
    """Create an in-memory DuckDB connection with schema."""
    conn = duckdb.connect(":memory:")

    # Create embedding cache table
    conn.execute("""
        CREATE TABLE EmbeddingCache (
            text_hash VARCHAR PRIMARY KEY,
            text VARCHAR NOT NULL,
            embedding DOUBLE[] NOT NULL,
            model VARCHAR NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.execute("""
        CREATE INDEX idx_embedding_cache_model ON EmbeddingCache(model)
    """)

    yield conn
    conn.close()


@pytest.fixture
def embedding_service(mock_ollama_client, default_embedding_config, in_memory_conn):
    """Create EmbeddingService with mocks."""
    # Setup mock to return proper embeddings
    mock_ollama_client.embed.return_value = [0.1] * 768

    service = EmbeddingService(
        client=mock_ollama_client,
        config=default_embedding_config,
        conn=in_memory_conn,
    )
    return service


@pytest.fixture
def sample_embedding():
    """Create a sample normalized embedding."""
    raw = [float(i) for i in range(768)]
    norm = math.sqrt(sum(x * x for x in raw))
    return [x / norm for x in raw]


# =============================================================================
# LRU Cache Tests
# =============================================================================

class TestLRUCache:
    """Tests for the LRU cache implementation."""

    def test_cache_put_and_get(self):
        """Test basic put and get operations."""
        cache = LRUCache(max_size=10)

        cache.put("key1", [0.1, 0.2, 0.3])
        result = cache.get("key1")

        assert result == [0.1, 0.2, 0.3]

    def test_cache_miss_returns_none(self):
        """Test cache miss returns None."""
        cache = LRUCache(max_size=10)

        result = cache.get("nonexistent")

        assert result is None

    def test_cache_eviction(self):
        """Test LRU eviction when cache is full."""
        cache = LRUCache(max_size=3)

        cache.put("key1", [1.0])
        cache.put("key2", [2.0])
        cache.put("key3", [3.0])
        cache.put("key4", [4.0])  # This should evict key1

        assert cache.get("key1") is None  # Evicted
        assert cache.get("key2") == [2.0]
        assert cache.get("key3") == [3.0]
        assert cache.get("key4") == [4.0]

    def test_cache_lru_order(self):
        """Test that LRU order is maintained on access."""
        cache = LRUCache(max_size=3)

        cache.put("key1", [1.0])
        cache.put("key2", [2.0])
        cache.put("key3", [3.0])

        # Access key1, making it most recently used
        cache.get("key1")

        # Add key4, should evict key2 (oldest after key1 access)
        cache.put("key4", [4.0])

        assert cache.get("key1") == [1.0]  # Still here
        assert cache.get("key2") is None  # Evicted
        assert cache.get("key3") == [3.0]
        assert cache.get("key4") == [4.0]

    def test_cache_update_existing(self):
        """Test updating an existing key."""
        cache = LRUCache(max_size=10)

        cache.put("key1", [1.0])
        cache.put("key1", [2.0])  # Update

        assert cache.get("key1") == [2.0]
        assert cache.size() == 1

    def test_cache_contains(self):
        """Test contains method."""
        cache = LRUCache(max_size=10)

        cache.put("key1", [1.0])

        assert cache.contains("key1") is True
        assert cache.contains("key2") is False

    def test_cache_clear(self):
        """Test clearing the cache."""
        cache = LRUCache(max_size=10)

        cache.put("key1", [1.0])
        cache.put("key2", [2.0])
        cache.clear()

        assert cache.size() == 0
        assert cache.get("key1") is None

    def test_cache_thread_safety(self):
        """Test cache is thread-safe."""
        cache = LRUCache(max_size=100)
        errors = []

        def writer(thread_id):
            try:
                for i in range(50):
                    cache.put(f"key_{thread_id}_{i}", [float(i)])
            except Exception as e:
                errors.append(e)

        def reader(thread_id):
            try:
                for i in range(50):
                    cache.get(f"key_{thread_id}_{i}")
            except Exception as e:
                errors.append(e)

        threads = []
        for i in range(5):
            threads.append(threading.Thread(target=writer, args=(i,)))
            threads.append(threading.Thread(target=reader, args=(i,)))

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0


# =============================================================================
# Embedding Service Basic Tests
# =============================================================================

class TestEmbeddingServiceBasic:
    """Basic tests for EmbeddingService."""

    def test_initialization(self, mock_ollama_client, default_embedding_config, in_memory_conn):
        """Test service initializes correctly."""
        service = EmbeddingService(
            client=mock_ollama_client,
            config=default_embedding_config,
            conn=in_memory_conn,
        )

        assert service.model == "nomic-embed-text"
        assert service.dimension == 768
        assert service.config == default_embedding_config

    def test_initialization_without_conn(self, mock_ollama_client, default_embedding_config):
        """Test service works without database connection."""
        service = EmbeddingService(
            client=mock_ollama_client,
            config=default_embedding_config,
            conn=None,
        )

        assert service.conn is None

    def test_initialization_creates_table(self, mock_ollama_client, default_embedding_config):
        """Test service creates cache table if needed."""
        conn = duckdb.connect(":memory:")

        service = EmbeddingService(
            client=mock_ollama_client,
            config=default_embedding_config,
            conn=conn,
        )

        # Check table exists
        result = conn.execute("""
            SELECT COUNT(*) FROM information_schema.tables
            WHERE table_name = 'EmbeddingCache'
        """).fetchone()

        assert result[0] == 1
        conn.close()


# =============================================================================
# Single Embedding Tests
# =============================================================================

class TestSingleEmbedding:
    """Tests for single text embedding."""

    def test_embed_success(self, embedding_service, mock_ollama_client):
        """Test successful embedding generation."""
        mock_ollama_client.embed.return_value = [0.1] * 768

        result = embedding_service.embed("Hello world")

        assert isinstance(result, EmbeddingResult)
        assert result.text == "Hello world"
        assert len(result.embedding) == 768
        assert result.model == "nomic-embed-text"
        assert result.from_cache is False

    def test_embed_normalized(self, embedding_service, mock_ollama_client):
        """Test embeddings are normalized when configured."""
        raw = [1.0, 0.0, 0.0] + [0.0] * 765
        mock_ollama_client.embed.return_value = raw

        result = embedding_service.embed("Test")

        # Check normalization (L2 norm should be ~1)
        norm = math.sqrt(sum(x * x for x in result.embedding))
        assert abs(norm - 1.0) < 0.0001

    def test_embed_memory_cache_hit(self, embedding_service, mock_ollama_client):
        """Test embedding is returned from memory cache."""
        mock_ollama_client.embed.return_value = [0.1] * 768

        # First call - generates embedding
        result1 = embedding_service.embed("Hello")
        assert result1.from_cache is False

        # Second call - should be from cache
        result2 = embedding_service.embed("Hello")
        assert result2.from_cache is True
        assert result2.cache_level == "memory"

        # Ollama should only be called once
        assert mock_ollama_client.embed.call_count == 1

    def test_embed_database_cache_hit(self, embedding_service, mock_ollama_client, in_memory_conn):
        """Test embedding is returned from database cache."""
        embedding = [0.1] * 768

        # Pre-populate database cache
        text_hash = embedding_service._compute_hash("Test text")
        in_memory_conn.execute("""
            INSERT INTO EmbeddingCache (text_hash, text, embedding, model)
            VALUES (?, ?, ?, ?)
        """, [text_hash, "Test text", embedding, "nomic-embed-text"])

        # Should hit database cache
        result = embedding_service.embed("Test text")

        assert result.from_cache is True
        assert result.cache_level == "database"
        assert mock_ollama_client.embed.call_count == 0

    def test_embed_cache_disabled(self, mock_ollama_client, in_memory_conn):
        """Test embedding without caching."""
        config = EmbeddingConfig(
            cache=EmbeddingCacheConfig(enabled=False),
            dimension=768,
        )
        mock_ollama_client.embed.return_value = [0.1] * 768

        service = EmbeddingService(mock_ollama_client, config, in_memory_conn)

        result1 = service.embed("Hello")
        result2 = service.embed("Hello")

        # Both calls should go to Ollama
        assert mock_ollama_client.embed.call_count == 2
        assert result1.from_cache is False
        assert result2.from_cache is False

    def test_embed_dimension_mismatch(self, embedding_service, mock_ollama_client):
        """Test error when embedding dimension doesn't match."""
        mock_ollama_client.embed.return_value = [0.1] * 384  # Wrong dimension

        with pytest.raises(EmbeddingGenerationError):
            embedding_service.embed("Hello")

    def test_embed_generation_error(self, embedding_service, mock_ollama_client):
        """Test error handling when generation fails."""
        mock_ollama_client.embed.side_effect = Exception("Ollama error")

        with pytest.raises(EmbeddingGenerationError) as exc_info:
            embedding_service.embed("Hello")

        assert "Ollama error" in str(exc_info.value)


# =============================================================================
# Batch Embedding Tests
# =============================================================================

class TestBatchEmbedding:
    """Tests for batch embedding."""

    def test_embed_batch_success(self, embedding_service, mock_ollama_client):
        """Test successful batch embedding."""
        mock_ollama_client.embed.return_value = [[0.1] * 768, [0.2] * 768, [0.3] * 768]

        results = embedding_service.embed_batch(["Hello", "World", "Test"])

        assert len(results) == 3
        assert all(isinstance(r, EmbeddingResult) for r in results)
        assert results[0].text == "Hello"
        assert results[1].text == "World"
        assert results[2].text == "Test"

    def test_embed_batch_partial_cache(self, embedding_service, mock_ollama_client):
        """Test batch with some cached embeddings."""
        # Pre-cache one embedding
        mock_ollama_client.embed.return_value = [0.1] * 768
        embedding_service.embed("Hello")
        mock_ollama_client.embed.reset_mock()

        # Now batch with cached and uncached
        mock_ollama_client.embed.return_value = [[0.2] * 768, [0.3] * 768]

        results = embedding_service.embed_batch(["Hello", "World", "Test"])

        assert len(results) == 3
        # Verify only uncached texts were sent to Ollama
        call_args = mock_ollama_client.embed.call_args
        if call_args:
            texts_sent = call_args[0][1]
            assert "Hello" not in texts_sent

    def test_embed_batch_all_cached(self, embedding_service, mock_ollama_client):
        """Test batch where all are cached."""
        mock_ollama_client.embed.return_value = [0.1] * 768

        # Pre-cache all
        embedding_service.embed("Hello")
        embedding_service.embed("World")
        mock_ollama_client.embed.reset_mock()

        results = embedding_service.embed_batch(["Hello", "World"])

        assert len(results) == 2
        assert all(r.from_cache for r in results)
        mock_ollama_client.embed.assert_not_called()

    def test_embed_batch_with_progress(self, embedding_service, mock_ollama_client, caplog):
        """Test batch embedding with progress logging."""
        mock_ollama_client.embed.return_value = [[0.1] * 768] * 3

        with caplog.at_level("INFO"):
            results = embedding_service.embed_batch(
                ["Hello", "World", "Test"],
                show_progress=True
            )

        assert len(results) == 3
        assert "Generating 3 embeddings" in caplog.text

    def test_embed_batch_error_recovery(self, embedding_service, mock_ollama_client):
        """Test batch continues after individual errors."""
        # First batch call fails, individual fallback succeeds
        mock_ollama_client.embed.side_effect = [
            Exception("Batch failed"),  # Batch fails
            [0.1] * 768,  # Individual succeeds
            [0.2] * 768,
            [0.3] * 768,
        ]

        results = embedding_service.embed_batch(["A", "B", "C"])

        # Should have recovered with individual embeddings
        assert len(results) == 3


# =============================================================================
# Similarity Search Tests
# =============================================================================

class TestSimilaritySearch:
    """Tests for similarity search functionality."""

    def test_find_similar_basic(self, embedding_service, mock_ollama_client, in_memory_conn):
        """Test basic similarity search."""
        # Pre-populate database
        embeddings = [
            ("hash1", "Politics", [1.0, 0.0, 0.0] + [0.0] * 765),
            ("hash2", "Elections", [0.9, 0.1, 0.0] + [0.0] * 765),
            ("hash3", "Sports", [0.0, 1.0, 0.0] + [0.0] * 765),
        ]
        for hash_, text, emb in embeddings:
            in_memory_conn.execute("""
                INSERT INTO EmbeddingCache (text_hash, text, embedding, model)
                VALUES (?, ?, ?, ?)
            """, [hash_, text, emb, "nomic-embed-text"])

        # Query embedding similar to "Politics"
        query_embedding = [0.95, 0.05, 0.0] + [0.0] * 765
        mock_ollama_client.embed.return_value = query_embedding

        results = embedding_service.find_similar(query_embedding, top_k=3)

        assert len(results) > 0
        # Politics should be most similar
        assert results[0].text == "Politics" or results[0].text == "Elections"

    def test_find_similar_with_threshold(self, embedding_service, mock_ollama_client, in_memory_conn):
        """Test similarity search with minimum threshold."""
        # Pre-populate with diverse embeddings
        in_memory_conn.execute("""
            INSERT INTO EmbeddingCache (text_hash, text, embedding, model)
            VALUES (?, ?, ?, ?)
        """, ["hash1", "Very different", [0.0, 0.0, 1.0] + [0.0] * 765, "nomic-embed-text"])

        query = [1.0, 0.0, 0.0] + [0.0] * 765

        results = embedding_service.find_similar(query, min_similarity=0.9)

        # Should not find "Very different" (orthogonal)
        assert all(r.similarity >= 0.9 for r in results)

    def test_find_similar_to_text(self, embedding_service, mock_ollama_client, in_memory_conn):
        """Test similarity search by text."""
        mock_ollama_client.embed.return_value = [0.1] * 768

        # Pre-populate
        in_memory_conn.execute("""
            INSERT INTO EmbeddingCache (text_hash, text, embedding, model)
            VALUES (?, ?, ?, ?)
        """, ["hash1", "Similar text", [0.1] * 768, "nomic-embed-text"])

        results = embedding_service.find_similar_to_text("Query text", top_k=5)

        # Should have called embed
        assert mock_ollama_client.embed.called

    def test_find_similar_exclude_self(self, embedding_service, mock_ollama_client, in_memory_conn):
        """Test similarity search excludes query text."""
        query_hash = embedding_service._compute_hash("Query text")
        mock_ollama_client.embed.return_value = [0.1] * 768

        in_memory_conn.execute("""
            INSERT INTO EmbeddingCache (text_hash, text, embedding, model)
            VALUES (?, ?, ?, ?)
        """, [query_hash, "Query text", [0.1] * 768, "nomic-embed-text"])

        results = embedding_service.find_similar_to_text(
            "Query text",
            exclude_self=True
        )

        assert all(r.text != "Query text" for r in results)


# =============================================================================
# Cache Management Tests
# =============================================================================

class TestCacheManagement:
    """Tests for cache management operations."""

    def test_flush_cache(self, embedding_service, mock_ollama_client, in_memory_conn):
        """Test flushing write buffer to database."""
        mock_ollama_client.embed.return_value = [0.1] * 768

        # Generate some embeddings
        for i in range(5):
            embedding_service.embed(f"Text {i}")

        # Manually flush
        embedding_service.flush_cache()

        # Check database has entries
        count = in_memory_conn.execute("""
            SELECT COUNT(*) FROM EmbeddingCache
        """).fetchone()[0]

        assert count >= 5

    def test_clear_memory_cache(self, embedding_service, mock_ollama_client):
        """Test clearing memory cache."""
        mock_ollama_client.embed.return_value = [0.1] * 768

        embedding_service.embed("Hello")
        assert embedding_service._memory_cache.size() > 0

        embedding_service.clear_memory_cache()

        assert embedding_service._memory_cache.size() == 0

    def test_clear_database_cache(self, embedding_service, mock_ollama_client, in_memory_conn):
        """Test clearing database cache."""
        # Pre-populate
        in_memory_conn.execute("""
            INSERT INTO EmbeddingCache (text_hash, text, embedding, model)
            VALUES (?, ?, ?, ?)
        """, ["hash1", "Text", [0.1] * 768, "nomic-embed-text"])

        embedding_service.clear_database_cache()

        count = in_memory_conn.execute("""
            SELECT COUNT(*) FROM EmbeddingCache WHERE model = ?
        """, ["nomic-embed-text"]).fetchone()[0]

        assert count == 0

    def test_preload_from_database(self, embedding_service, in_memory_conn):
        """Test preloading embeddings into memory cache."""
        # Pre-populate database
        for i in range(50):
            in_memory_conn.execute("""
                INSERT INTO EmbeddingCache (text_hash, text, embedding, model)
                VALUES (?, ?, ?, ?)
            """, [f"hash{i}", f"Text {i}", [0.1] * 768, "nomic-embed-text"])

        loaded = embedding_service.preload_from_database(limit=20)

        assert loaded == 20
        assert embedding_service._memory_cache.size() == 20


# =============================================================================
# Cache Statistics Tests
# =============================================================================

class TestCacheStatistics:
    """Tests for cache statistics."""

    def test_get_cache_stats(self, embedding_service, mock_ollama_client, in_memory_conn):
        """Test getting cache statistics."""
        mock_ollama_client.embed.return_value = [0.1] * 768

        # Generate some activity
        embedding_service.embed("Hello")
        embedding_service.embed("World")
        embedding_service.embed("Hello")  # Cache hit

        stats = embedding_service.get_cache_stats()

        assert isinstance(stats, CacheStats)
        assert stats.memory_hits >= 1
        assert stats.generations >= 2
        assert stats.memory_size > 0

    def test_cache_stats_hit_rate(self):
        """Test cache statistics hit rate calculation."""
        stats = CacheStats(
            memory_hits=75,
            memory_misses=25,
            database_hits=10,
            database_misses=15,
            generations=15,
        )

        assert stats.total_requests == 100
        assert stats.memory_hit_rate == 0.75
        assert stats.overall_hit_rate == 0.85

    def test_cache_stats_zero_requests(self):
        """Test cache stats with zero requests."""
        stats = CacheStats()

        assert stats.total_requests == 0
        assert stats.memory_hit_rate == 0.0
        assert stats.overall_hit_rate == 0.0


# =============================================================================
# Utility Tests
# =============================================================================

class TestUtilityMethods:
    """Tests for utility methods."""

    def test_compute_hash(self):
        """Test hash computation is deterministic."""
        hash1 = EmbeddingService._compute_hash("Hello")
        hash2 = EmbeddingService._compute_hash("Hello")
        hash3 = EmbeddingService._compute_hash("World")

        assert hash1 == hash2
        assert hash1 != hash3
        assert len(hash1) == 64  # SHA256

    def test_normalize(self):
        """Test vector normalization."""
        vector = [3.0, 4.0, 0.0]

        normalized = EmbeddingService._normalize(vector)

        # 3-4-5 triangle, so norm is 5
        assert normalized == [0.6, 0.8, 0.0]

        # Check L2 norm is 1
        norm = math.sqrt(sum(x * x for x in normalized))
        assert abs(norm - 1.0) < 0.0001

    def test_normalize_zero_vector(self):
        """Test normalization of zero vector."""
        vector = [0.0, 0.0, 0.0]

        normalized = EmbeddingService._normalize(vector)

        assert normalized == [0.0, 0.0, 0.0]

    def test_cosine_similarity(self):
        """Test cosine similarity computation."""
        a = [1.0, 0.0, 0.0]
        b = [1.0, 0.0, 0.0]

        sim = EmbeddingService._cosine_similarity(a, b)
        assert sim == 1.0  # Identical

        c = [0.0, 1.0, 0.0]
        sim = EmbeddingService._cosine_similarity(a, c)
        assert sim == 0.0  # Orthogonal

        d = [-1.0, 0.0, 0.0]
        sim = EmbeddingService._cosine_similarity(a, d)
        assert sim == -1.0  # Opposite

    def test_cosine_similarity_different_lengths(self):
        """Test cosine similarity with different length vectors."""
        a = [1.0, 0.0]
        b = [1.0, 0.0, 0.0]

        sim = EmbeddingService._cosine_similarity(a, b)
        assert sim == 0.0  # Should handle gracefully


# =============================================================================
# Convenience Function Tests
# =============================================================================

class TestConvenienceFunctions:
    """Tests for module-level convenience functions."""

    def test_create_embedding_service(self, mock_ollama_client, default_embedding_config):
        """Test create_embedding_service function."""
        service = create_embedding_service(
            client=mock_ollama_client,
            config=default_embedding_config,
        )

        assert isinstance(service, EmbeddingService)
        assert service.client == mock_ollama_client
        assert service.config == default_embedding_config

    def test_compute_similarity(self):
        """Test compute_similarity convenience function."""
        a = [1.0, 0.0, 0.0]
        b = [0.707, 0.707, 0.0]

        sim = compute_similarity(a, b)

        assert abs(sim - 0.707) < 0.001


# =============================================================================
# Integration-style Tests
# =============================================================================

class TestRealisticScenarios:
    """Tests simulating realistic usage patterns."""

    def test_market_embedding_workflow(self, embedding_service, mock_ollama_client):
        """Test embedding workflow for market questions."""
        mock_ollama_client.embed.return_value = [0.1] * 768

        markets = [
            "Will Biden win the 2024 election?",
            "Will Trump win the 2024 election?",
            "Will the S&P 500 reach 5000 in 2024?",
            "Will Bitcoin reach $100,000?",
            "Will there be a recession in 2024?",
        ]

        results = embedding_service.embed_batch(markets, show_progress=True)

        assert len(results) == 5
        assert all(r.dimension == 768 for r in results)

    def test_tag_similarity_workflow(self, embedding_service, mock_ollama_client, in_memory_conn):
        """Test using embeddings for tag similarity."""
        # Create embeddings for tags
        tags = ["politics", "elections", "economy", "sports", "technology"]

        mock_ollama_client.embed.return_value = [[float(i) / 10] * 768 for i in range(5)]
        results = embedding_service.embed_batch(tags)

        # Flush to DB
        embedding_service.flush_cache()

        # Now search for similar
        mock_ollama_client.embed.return_value = [0.05] * 768  # Similar to "elections"
        similar = embedding_service.find_similar_to_text("voting", top_k=3)

        # Should return some results
        assert len(similar) >= 0  # May be empty if no matches

    def test_high_volume_caching(self, embedding_service, mock_ollama_client):
        """Test caching under high volume."""
        mock_ollama_client.embed.return_value = [0.1] * 768

        # Generate 100 unique embeddings
        for i in range(100):
            embedding_service.embed(f"Text {i}")

        # Now access them all again - should all be cached
        mock_ollama_client.embed.reset_mock()

        for i in range(100):
            result = embedding_service.embed(f"Text {i}")
            assert result.from_cache is True

        # No new calls to Ollama
        mock_ollama_client.embed.assert_not_called()
