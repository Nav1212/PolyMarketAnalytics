"""
Unit tests for OllamaClient.

Tests the Ollama API wrapper with mocked HTTP responses.
"""

import json
import pytest
from unittest.mock import MagicMock, patch, PropertyMock
from dataclasses import dataclass
from typing import List

import httpx

from fetcher.config import OllamaConfig
from nlp_enrichment.core.ollama_client import (
    OllamaClient,
    GenerationResponse,
    ModelInfo,
    EmbeddingResponse,
    retry_ollama,
    create_client,
    quick_generate,
    quick_embed,
)
from nlp_enrichment.core.exceptions import (
    OllamaConnectionError,
    OllamaModelNotFoundError,
    OllamaGenerationError,
    OllamaTimeoutError,
)


# =============================================================================
# Fixtures
# =============================================================================

@pytest.fixture
def default_config():
    """Create default Ollama configuration."""
    return OllamaConfig(
        base_url="http://localhost:11434",
        timeout=30.0,
        connect_timeout=5.0,
        embedding_model="nomic-embed-text",
        generation_model="llama3.2",
        retry_max_attempts=3,
        retry_base_delay=0.1,
        retry_max_delay=1.0,
    )


@pytest.fixture
def mock_client(default_config):
    """Create OllamaClient with mocked HTTP client."""
    client = OllamaClient(default_config)
    client._client = MagicMock()
    return client


@pytest.fixture
def sample_models_response():
    """Sample response from /api/tags endpoint."""
    return {
        "models": [
            {
                "name": "llama3.2:latest",
                "modified_at": "2024-01-01T00:00:00Z",
                "size": 4000000000,
                "digest": "abc123",
                "details": {
                    "family": "llama",
                    "parameter_size": "7B",
                },
            },
            {
                "name": "nomic-embed-text:latest",
                "modified_at": "2024-01-01T00:00:00Z",
                "size": 500000000,
                "digest": "def456",
                "details": {
                    "family": "nomic",
                },
            },
        ]
    }


@pytest.fixture
def sample_generation_response():
    """Sample response from /api/generate endpoint."""
    return {
        "model": "llama3.2",
        "created_at": "2024-01-01T00:00:00Z",
        "response": "Hello! I'm an AI assistant.",
        "done": True,
        "total_duration": 1000000000,
        "load_duration": 100000000,
        "prompt_eval_count": 10,
        "prompt_eval_duration": 200000000,
        "eval_count": 20,
        "eval_duration": 500000000,
        "context": [1, 2, 3, 4, 5],
    }


@pytest.fixture
def sample_embedding_response():
    """Sample response from /api/embed endpoint."""
    return {
        "embeddings": [[0.1, 0.2, 0.3, 0.4, 0.5] * 153 + [0.1, 0.2, 0.3]],  # 768 dims
    }


# =============================================================================
# Configuration Tests
# =============================================================================

class TestOllamaConfig:
    """Tests for OllamaConfig dataclass."""

    def test_default_values(self):
        """Test default configuration values."""
        config = OllamaConfig()

        assert config.base_url == "http://localhost:11434"
        assert config.timeout == 300.0
        assert config.connect_timeout == 10.0
        assert config.embedding_model == "nomic-embed-text"
        assert config.generation_model == "llama3.2"
        assert config.retry_max_attempts == 3

    def test_custom_values(self):
        """Test custom configuration values."""
        config = OllamaConfig(
            base_url="http://custom:8080",
            timeout=60.0,
            embedding_model="mxbai-embed-large",
        )

        assert config.base_url == "http://custom:8080"
        assert config.timeout == 60.0
        assert config.embedding_model == "mxbai-embed-large"


# =============================================================================
# Client Initialization Tests
# =============================================================================

class TestOllamaClientInit:
    """Tests for OllamaClient initialization."""

    def test_initialization_with_default_config(self):
        """Test client initializes with default config."""
        with patch.object(httpx.Client, '__init__', return_value=None):
            client = OllamaClient()

        assert client.config is not None
        assert client.base_url == "http://localhost:11434"

    def test_initialization_with_custom_config(self, default_config):
        """Test client initializes with custom config."""
        with patch.object(httpx.Client, '__init__', return_value=None):
            client = OllamaClient(default_config)

        assert client.config == default_config
        assert client.base_url == default_config.base_url

    def test_base_url_trailing_slash_removed(self):
        """Test trailing slash is removed from base_url."""
        config = OllamaConfig(base_url="http://localhost:11434/")

        with patch.object(httpx.Client, '__init__', return_value=None):
            client = OllamaClient(config)

        assert client.base_url == "http://localhost:11434"

    def test_context_manager(self, mock_client):
        """Test client works as context manager."""
        mock_client._client.close = MagicMock()

        with mock_client as client:
            assert client is mock_client

        mock_client._client.close.assert_called_once()


# =============================================================================
# Health Check Tests
# =============================================================================

class TestHealthCheck:
    """Tests for health check functionality."""

    def test_is_healthy_success(self, mock_client):
        """Test health check returns True when server responds."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_client._client.get.return_value = mock_response

        assert mock_client.is_healthy() is True
        mock_client._client.get.assert_called_with("/", timeout=5.0)

    def test_is_healthy_failure(self, mock_client):
        """Test health check returns False when server doesn't respond."""
        mock_client._client.get.side_effect = httpx.ConnectError("Connection refused")

        assert mock_client.is_healthy() is False

    def test_is_healthy_non_200(self, mock_client):
        """Test health check returns False for non-200 status."""
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_client._client.get.return_value = mock_response

        assert mock_client.is_healthy() is False

    def test_get_version_success(self, mock_client):
        """Test getting server version."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"version": "0.1.23"}
        mock_response.raise_for_status = MagicMock()
        mock_client._client.get.return_value = mock_response

        version = mock_client.get_version()

        assert version == "0.1.23"

    def test_get_version_failure(self, mock_client):
        """Test version returns None on failure."""
        mock_client._client.get.side_effect = httpx.ConnectError("Connection refused")

        version = mock_client.get_version()

        assert version is None


# =============================================================================
# Model Management Tests
# =============================================================================

class TestModelManagement:
    """Tests for model listing and availability checks."""

    def test_list_models_success(self, mock_client, sample_models_response):
        """Test listing available models."""
        mock_response = MagicMock()
        mock_response.json.return_value = sample_models_response
        mock_response.raise_for_status = MagicMock()
        mock_client._client.get.return_value = mock_response

        models = mock_client.list_models()

        assert len(models) == 2
        assert models[0].name == "llama3.2:latest"
        assert models[0].size == 4000000000
        assert models[0].size_gb == pytest.approx(3.725, rel=0.01)
        assert models[0].family == "llama"
        assert models[1].name == "nomic-embed-text:latest"

    def test_list_models_connection_error(self, mock_client):
        """Test list_models raises OllamaConnectionError on connection failure."""
        mock_client._client.get.side_effect = httpx.ConnectError("Connection refused")

        with pytest.raises(OllamaConnectionError) as exc_info:
            mock_client.list_models()

        assert "Cannot connect to Ollama server" in str(exc_info.value)

    def test_list_models_timeout(self, mock_client):
        """Test list_models raises OllamaTimeoutError on timeout."""
        mock_client._client.get.side_effect = httpx.TimeoutException("Timeout")

        with pytest.raises(OllamaTimeoutError) as exc_info:
            mock_client.list_models()

        assert "Timeout" in str(exc_info.value)

    def test_is_model_available_from_cache(self, mock_client, sample_models_response):
        """Test model availability check uses cache."""
        mock_response = MagicMock()
        mock_response.json.return_value = sample_models_response
        mock_response.raise_for_status = MagicMock()
        mock_client._client.get.return_value = mock_response

        # First call populates cache
        mock_client.list_models()

        # Second call should use cache (no additional HTTP call)
        call_count = mock_client._client.get.call_count
        assert mock_client.is_model_available("llama3.2") is True
        assert mock_client._client.get.call_count == call_count  # No new calls

    def test_is_model_available_not_found(self, mock_client, sample_models_response):
        """Test model availability returns False for unknown model."""
        mock_response = MagicMock()
        mock_response.json.return_value = sample_models_response
        mock_response.raise_for_status = MagicMock()
        mock_client._client.get.return_value = mock_response

        mock_client.list_models()

        assert mock_client.is_model_available("unknown-model") is False

    def test_ensure_model_available_raises(self, mock_client, sample_models_response):
        """Test ensure_model_available raises for missing model."""
        mock_response = MagicMock()
        mock_response.json.return_value = sample_models_response
        mock_response.raise_for_status = MagicMock()
        mock_client._client.get.return_value = mock_response

        mock_client.list_models()

        with pytest.raises(OllamaModelNotFoundError) as exc_info:
            mock_client.ensure_model_available("unknown-model")

        assert "unknown-model" in str(exc_info.value)


# =============================================================================
# Generation Tests
# =============================================================================

class TestGeneration:
    """Tests for text generation functionality."""

    def test_generate_success(self, mock_client, sample_generation_response):
        """Test successful text generation."""
        mock_response = MagicMock()
        mock_response.json.return_value = sample_generation_response
        mock_response.raise_for_status = MagicMock()
        mock_client._client.post.return_value = mock_response

        result = mock_client.generate("llama3.2", "Hello")

        assert isinstance(result, GenerationResponse)
        assert result.text == "Hello! I'm an AI assistant."
        assert result.model == "llama3.2"
        assert result.done is True
        assert result.tokens_per_second is not None

    def test_generate_with_options(self, mock_client, sample_generation_response):
        """Test generation with custom options."""
        mock_response = MagicMock()
        mock_response.json.return_value = sample_generation_response
        mock_response.raise_for_status = MagicMock()
        mock_client._client.post.return_value = mock_response

        result = mock_client.generate(
            "llama3.2",
            "Hello",
            system="You are helpful.",
            options={"temperature": 0.7},
        )

        # Verify the request payload
        call_args = mock_client._client.post.call_args
        payload = call_args.kwargs.get('json', call_args[1].get('json'))

        assert payload["model"] == "llama3.2"
        assert payload["prompt"] == "Hello"
        assert payload["system"] == "You are helpful."
        assert payload["options"]["temperature"] == 0.7

    def test_generate_connection_error(self, mock_client):
        """Test generate raises OllamaConnectionError on connection failure."""
        mock_client._client.post.side_effect = httpx.ConnectError("Connection refused")

        with pytest.raises(OllamaConnectionError):
            mock_client.generate("llama3.2", "Hello")

    def test_generate_timeout_error(self, mock_client):
        """Test generate raises OllamaTimeoutError on timeout."""
        mock_client._client.post.side_effect = httpx.TimeoutException("Timeout")

        with pytest.raises(OllamaTimeoutError) as exc_info:
            mock_client.generate("llama3.2", "Hello")

        assert "timed out" in str(exc_info.value).lower()

    def test_generate_model_not_found(self, mock_client):
        """Test generate raises OllamaModelNotFoundError for 404."""
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_client._client.post.side_effect = httpx.HTTPStatusError(
            "Not Found",
            request=MagicMock(),
            response=mock_response,
        )

        with pytest.raises(OllamaModelNotFoundError):
            mock_client.generate("unknown-model", "Hello")

    def test_generate_json_success(self, mock_client):
        """Test JSON generation parses response."""
        json_response = {"colors": ["red", "blue", "green"]}
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "model": "llama3.2",
            "created_at": "2024-01-01T00:00:00Z",
            "response": json.dumps(json_response),
            "done": True,
        }
        mock_response.raise_for_status = MagicMock()
        mock_client._client.post.return_value = mock_response

        result = mock_client.generate_json("llama3.2", "List 3 colors in JSON")

        assert result == json_response

    def test_generate_json_parse_error(self, mock_client):
        """Test generate_json raises on invalid JSON."""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "model": "llama3.2",
            "created_at": "2024-01-01T00:00:00Z",
            "response": "This is not JSON",
            "done": True,
        }
        mock_response.raise_for_status = MagicMock()
        mock_client._client.post.return_value = mock_response

        with pytest.raises(OllamaGenerationError) as exc_info:
            mock_client.generate_json("llama3.2", "Bad prompt")

        assert "parse JSON" in str(exc_info.value)


# =============================================================================
# Embedding Tests
# =============================================================================

class TestEmbedding:
    """Tests for embedding generation functionality."""

    def test_embed_single_text(self, mock_client, sample_embedding_response):
        """Test embedding a single text."""
        mock_response = MagicMock()
        mock_response.json.return_value = sample_embedding_response
        mock_response.raise_for_status = MagicMock()
        mock_client._client.post.return_value = mock_response

        embedding = mock_client.embed("nomic-embed-text", "Hello world")

        assert isinstance(embedding, list)
        assert len(embedding) == 768
        assert all(isinstance(x, float) for x in embedding)

    def test_embed_batch_texts(self, mock_client):
        """Test embedding multiple texts."""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "embeddings": [
                [0.1] * 768,
                [0.2] * 768,
                [0.3] * 768,
            ]
        }
        mock_response.raise_for_status = MagicMock()
        mock_client._client.post.return_value = mock_response

        embeddings = mock_client.embed("nomic-embed-text", ["Hello", "World", "Test"])

        assert isinstance(embeddings, list)
        assert len(embeddings) == 3
        assert all(len(e) == 768 for e in embeddings)

    def test_embed_connection_error(self, mock_client):
        """Test embed raises OllamaConnectionError on connection failure."""
        mock_client._client.post.side_effect = httpx.ConnectError("Connection refused")

        with pytest.raises(OllamaConnectionError):
            mock_client.embed("nomic-embed-text", "Hello")

    def test_embed_model_not_found(self, mock_client):
        """Test embed raises OllamaModelNotFoundError for 404."""
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_client._client.post.side_effect = httpx.HTTPStatusError(
            "Not Found",
            request=MagicMock(),
            response=mock_response,
        )

        with pytest.raises(OllamaModelNotFoundError):
            mock_client.embed("unknown-model", "Hello")

    def test_embed_with_info(self, mock_client, sample_embedding_response):
        """Test embed_with_info returns EmbeddingResponse."""
        mock_response = MagicMock()
        mock_response.json.return_value = sample_embedding_response
        mock_response.raise_for_status = MagicMock()
        mock_client._client.post.return_value = mock_response

        result = mock_client.embed_with_info("nomic-embed-text", "Hello")

        assert isinstance(result, EmbeddingResponse)
        assert result.model == "nomic-embed-text"
        assert result.dimension == 768


# =============================================================================
# Retry Logic Tests
# =============================================================================

class TestRetryLogic:
    """Tests for retry decorator and logic."""

    def test_retry_on_timeout(self):
        """Test retry decorator retries on timeout."""
        call_count = 0

        @retry_ollama(max_attempts=3, base_delay=0.01, jitter=False)
        def flaky_function():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise httpx.TimeoutException("Timeout")
            return "success"

        result = flaky_function()

        assert result == "success"
        assert call_count == 3

    def test_retry_exhausted(self):
        """Test retry decorator raises after max attempts."""
        call_count = 0

        @retry_ollama(max_attempts=3, base_delay=0.01, jitter=False)
        def always_fails():
            nonlocal call_count
            call_count += 1
            raise httpx.TimeoutException("Timeout")

        with pytest.raises(httpx.TimeoutException):
            always_fails()

        assert call_count == 3

    def test_retry_on_connection_error(self):
        """Test retry on connection errors."""
        call_count = 0

        @retry_ollama(max_attempts=2, base_delay=0.01, jitter=False)
        def flaky_connection():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise httpx.ConnectError("Connection refused")
            return "connected"

        result = flaky_connection()

        assert result == "connected"
        assert call_count == 2

    def test_no_retry_on_non_retriable_error(self):
        """Test non-retriable errors are not retried."""
        call_count = 0

        @retry_ollama(max_attempts=3, base_delay=0.01)
        def raises_value_error():
            nonlocal call_count
            call_count += 1
            raise ValueError("Not retriable")

        with pytest.raises(ValueError):
            raises_value_error()

        assert call_count == 1  # No retry

    def test_retry_callback(self):
        """Test on_retry callback is called."""
        callback_calls = []

        def on_retry(exc, attempt, delay):
            callback_calls.append((type(exc).__name__, attempt, delay))

        @retry_ollama(max_attempts=3, base_delay=0.01, jitter=False, on_retry=on_retry)
        def flaky_with_callback():
            if len(callback_calls) < 2:
                raise httpx.TimeoutException("Timeout")
            return "done"

        result = flaky_with_callback()

        assert result == "done"
        assert len(callback_calls) == 2
        assert callback_calls[0][0] == "TimeoutException"
        assert callback_calls[0][1] == 1


# =============================================================================
# Data Class Tests
# =============================================================================

class TestDataClasses:
    """Tests for response data classes."""

    def test_generation_response_properties(self):
        """Test GenerationResponse computed properties."""
        response = GenerationResponse(
            text="Hello",
            model="llama3.2",
            created_at="2024-01-01T00:00:00Z",
            done=True,
            total_duration=1000000000,  # 1 second in ns
            eval_count=100,
            eval_duration=500000000,  # 0.5 seconds in ns
        )

        assert response.tokens_per_second == 200.0  # 100 tokens / 0.5 seconds
        assert response.total_duration_seconds == 1.0

    def test_generation_response_none_properties(self):
        """Test GenerationResponse with None durations."""
        response = GenerationResponse(
            text="Hello",
            model="llama3.2",
            created_at="2024-01-01T00:00:00Z",
            done=True,
        )

        assert response.tokens_per_second is None
        assert response.total_duration_seconds is None

    def test_model_info_properties(self):
        """Test ModelInfo computed properties."""
        info = ModelInfo(
            name="llama3.2:7b",
            modified_at="2024-01-01T00:00:00Z",
            size=7000000000,  # 7GB
            digest="abc123",
            details={"family": "llama", "parameter_size": "7B"},
        )

        assert info.size_gb == pytest.approx(6.52, rel=0.01)
        assert info.family == "llama"
        assert info.parameter_size == "7B"

    def test_embedding_response_dimension(self):
        """Test EmbeddingResponse dimension property."""
        response = EmbeddingResponse(
            embedding=[0.1] * 768,
            model="nomic-embed-text",
        )

        assert response.dimension == 768


# =============================================================================
# Utility Function Tests
# =============================================================================

class TestUtilityFunctions:
    """Tests for utility functions."""

    def test_compute_text_hash(self):
        """Test text hash computation."""
        hash1 = OllamaClient.compute_text_hash("Hello")
        hash2 = OllamaClient.compute_text_hash("Hello")
        hash3 = OllamaClient.compute_text_hash("World")

        assert hash1 == hash2  # Same input -> same hash
        assert hash1 != hash3  # Different input -> different hash
        assert len(hash1) == 64  # SHA256 hex length

    def test_create_client(self, default_config):
        """Test create_client convenience function."""
        with patch.object(httpx.Client, '__init__', return_value=None):
            client = create_client(default_config)

        assert isinstance(client, OllamaClient)
        assert client.config == default_config


# =============================================================================
# Integration-style Tests (still mocked, but more realistic)
# =============================================================================

class TestRealisticScenarios:
    """Tests simulating realistic usage scenarios."""

    def test_full_generation_workflow(self, mock_client, sample_generation_response, sample_models_response):
        """Test a complete generation workflow."""
        # Setup mock responses
        def mock_get(endpoint, **kwargs):
            mock_resp = MagicMock()
            mock_resp.status_code = 200
            if endpoint == "/":
                return mock_resp
            elif endpoint == "/api/tags":
                mock_resp.json.return_value = sample_models_response
                mock_resp.raise_for_status = MagicMock()
                return mock_resp
            raise ValueError(f"Unexpected endpoint: {endpoint}")

        def mock_post(endpoint, **kwargs):
            mock_resp = MagicMock()
            mock_resp.json.return_value = sample_generation_response
            mock_resp.raise_for_status = MagicMock()
            return mock_resp

        mock_client._client.get = mock_get
        mock_client._client.post = mock_post

        # Full workflow
        assert mock_client.is_healthy()
        models = mock_client.list_models()
        assert len(models) == 2

        result = mock_client.generate("llama3.2", "What is 2+2?")
        assert result.done is True

    def test_batch_embedding_workflow(self, mock_client):
        """Test batch embedding workflow."""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "embeddings": [[0.1] * 768 for _ in range(5)]
        }
        mock_response.raise_for_status = MagicMock()
        mock_client._client.post.return_value = mock_response

        texts = [
            "Hello world",
            "How are you?",
            "Machine learning",
            "Natural language processing",
            "Embeddings are useful",
        ]

        embeddings = mock_client.embed("nomic-embed-text", texts)

        assert len(embeddings) == 5
        assert all(len(e) == 768 for e in embeddings)
