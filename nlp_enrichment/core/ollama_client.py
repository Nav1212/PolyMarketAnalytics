"""
Ollama API Client

A robust wrapper around the Ollama REST API with:
- Retry logic with exponential backoff
- Connection pooling via httpx
- Streaming and non-streaming generation
- Embedding generation
- Model management (list, check availability)

Ollama API Reference: https://github.com/ollama/ollama/blob/main/docs/api.md

Usage:
    from nlp_enrichment.core import OllamaClient
    from fetcher.config import OllamaConfig

    client = OllamaClient(OllamaConfig())

    # Generate text
    response = client.generate("llama3.2", "What is 2+2?")
    print(response.text)

    # Generate embeddings
    embedding = client.embed("nomic-embed-text", "Hello world")
    print(len(embedding))  # 768

    # List available models
    models = client.list_models()
    for model in models:
        print(f"{model.name}: {model.size}")
"""

import functools
import hashlib
import json
import random
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple, Type, Union

import httpx

from fetcher.config import OllamaConfig
from fetcher.utils.logging_config import get_logger
from nlp_enrichment.core.exceptions import (
    OllamaConnectionError,
    OllamaError,
    OllamaGenerationError,
    OllamaModelNotFoundError,
    OllamaTimeoutError,
    RetriableOllamaError,
)

logger = get_logger("ollama_client")


# =============================================================================
# Data Classes for API Responses
# =============================================================================

@dataclass
class GenerationResponse:
    """Response from a text generation request."""

    text: str
    model: str
    created_at: str
    done: bool
    total_duration: Optional[int] = None  # nanoseconds
    load_duration: Optional[int] = None
    prompt_eval_count: Optional[int] = None
    prompt_eval_duration: Optional[int] = None
    eval_count: Optional[int] = None
    eval_duration: Optional[int] = None
    context: Optional[List[int]] = None  # For conversation context

    @property
    def tokens_per_second(self) -> Optional[float]:
        """Calculate tokens per second from generation stats."""
        if self.eval_count and self.eval_duration:
            # Convert nanoseconds to seconds
            duration_seconds = self.eval_duration / 1e9
            if duration_seconds > 0:
                return self.eval_count / duration_seconds
        return None

    @property
    def total_duration_seconds(self) -> Optional[float]:
        """Get total duration in seconds."""
        if self.total_duration:
            return self.total_duration / 1e9
        return None


@dataclass
class ModelInfo:
    """Information about an available Ollama model."""

    name: str
    modified_at: str
    size: int  # bytes
    digest: str
    details: Dict[str, Any] = field(default_factory=dict)

    @property
    def size_gb(self) -> float:
        """Get model size in gigabytes."""
        return self.size / (1024 ** 3)

    @property
    def family(self) -> Optional[str]:
        """Get model family from details."""
        return self.details.get("family")

    @property
    def parameter_size(self) -> Optional[str]:
        """Get parameter size (e.g., '7B', '13B')."""
        return self.details.get("parameter_size")


@dataclass
class EmbeddingResponse:
    """Response from an embedding request."""

    embedding: List[float]
    model: str

    @property
    def dimension(self) -> int:
        """Get embedding dimension."""
        return len(self.embedding)


# =============================================================================
# Retry Decorator (Ollama-specific)
# =============================================================================

# Exceptions that should trigger retry
RETRIABLE_EXCEPTIONS: Tuple[Type[Exception], ...] = (
    httpx.TimeoutException,
    httpx.NetworkError,
    httpx.RemoteProtocolError,
    RetriableOllamaError,
    OllamaConnectionError,
)


def retry_ollama(
    max_attempts: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    exponential_base: float = 2.0,
    jitter: bool = True,
    retriable_exceptions: Optional[Tuple[Type[Exception], ...]] = None,
    on_retry: Optional[Callable[[Exception, int, float], None]] = None,
):
    """
    Decorator that retries Ollama API calls with exponential backoff.

    This follows the pattern from fetcher/utils/retry.py but is specialized
    for Ollama operations.

    Args:
        max_attempts: Maximum number of attempts (including first try)
        base_delay: Initial delay between retries in seconds
        max_delay: Maximum delay between retries in seconds
        exponential_base: Base for exponential backoff calculation
        jitter: Add random jitter to delay to avoid thundering herd
        retriable_exceptions: Tuple of exception types to retry on
        on_retry: Optional callback called before each retry

    Example:
        >>> @retry_ollama(max_attempts=3)
        ... def generate(prompt: str) -> str:
        ...     return client.generate("llama3.2", prompt)
    """
    if retriable_exceptions is None:
        retriable_exceptions = RETRIABLE_EXCEPTIONS

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            last_exception = None

            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)

                except retriable_exceptions as e:
                    last_exception = e

                    if attempt >= max_attempts:
                        logger.error(
                            f"All {max_attempts} attempts failed for {func.__name__}",
                            exc_info=True
                        )
                        raise

                    # Calculate delay with exponential backoff
                    delay = min(
                        base_delay * (exponential_base ** (attempt - 1)),
                        max_delay
                    )

                    # Add jitter (+-25% of delay)
                    if jitter:
                        delay = delay * (0.75 + random.random() * 0.5)

                    logger.warning(
                        f"Attempt {attempt}/{max_attempts} failed for {func.__name__}: {e}. "
                        f"Retrying in {delay:.2f}s..."
                    )

                    if on_retry:
                        on_retry(e, attempt, delay)

                    time.sleep(delay)

                except httpx.HTTPStatusError as e:
                    # Check if this HTTP status is retriable
                    if e.response.status_code in {500, 502, 503, 504}:
                        last_exception = e

                        if attempt >= max_attempts:
                            logger.error(
                                f"All {max_attempts} attempts failed for {func.__name__}",
                                exc_info=True
                            )
                            raise

                        delay = min(
                            base_delay * (exponential_base ** (attempt - 1)),
                            max_delay
                        )

                        if jitter:
                            delay = delay * (0.75 + random.random() * 0.5)

                        logger.warning(
                            f"HTTP {e.response.status_code} on attempt {attempt}/{max_attempts} "
                            f"for {func.__name__}. Retrying in {delay:.2f}s..."
                        )

                        if on_retry:
                            on_retry(e, attempt, delay)

                        time.sleep(delay)
                    else:
                        # Non-retriable HTTP error, raise immediately
                        raise

            # Should not reach here, but just in case
            if last_exception:
                raise last_exception

        return wrapper
    return decorator


# =============================================================================
# Ollama Client Implementation
# =============================================================================

class OllamaClient:
    """
    Client for interacting with the Ollama API.

    Provides methods for:
    - Text generation (generate, chat)
    - Embedding generation
    - Model management (list, pull status, info)

    The client uses connection pooling via httpx for efficiency and
    includes automatic retry logic for transient failures.

    Attributes:
        config: OllamaConfig with connection settings
        base_url: Ollama API base URL
        client: httpx.Client for HTTP requests

    Example:
        >>> config = OllamaConfig(base_url="http://localhost:11434")
        >>> client = OllamaClient(config)
        >>>
        >>> # Check if Ollama is running
        >>> if client.is_healthy():
        ...     models = client.list_models()
        ...     print(f"Available models: {[m.name for m in models]}")
        >>>
        >>> # Generate text
        >>> response = client.generate("llama3.2", "Explain quantum computing")
        >>> print(response.text)
    """

    def __init__(self, config: Optional[OllamaConfig] = None):
        """
        Initialize the Ollama client.

        Args:
            config: Ollama configuration. If None, uses defaults.
        """
        self.config = config or OllamaConfig()
        self.base_url = self.config.base_url.rstrip("/")

        # Create HTTP client with connection pooling
        self._client = httpx.Client(
            base_url=self.base_url,
            timeout=httpx.Timeout(
                connect=self.config.connect_timeout,
                read=self.config.timeout,
                write=self.config.timeout,
                pool=self.config.connect_timeout,
            ),
            # Connection pooling settings
            limits=httpx.Limits(
                max_keepalive_connections=5,
                max_connections=10,
                keepalive_expiry=30.0,
            ),
        )

        # Cache for model availability checks
        self._model_cache: Dict[str, bool] = {}
        self._model_cache_time: float = 0
        self._model_cache_ttl: float = 60.0  # Cache for 60 seconds

        logger.info(f"OllamaClient initialized with base_url={self.base_url}")

    def __enter__(self) -> "OllamaClient":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    def close(self) -> None:
        """Close the HTTP client and release resources."""
        self._client.close()
        logger.debug("OllamaClient closed")

    # -------------------------------------------------------------------------
    # Health & Status
    # -------------------------------------------------------------------------

    def is_healthy(self) -> bool:
        """
        Check if Ollama server is running and healthy.

        Returns:
            True if server responds, False otherwise.
        """
        try:
            response = self._client.get("/", timeout=5.0)
            return response.status_code == 200
        except Exception as e:
            logger.debug(f"Health check failed: {e}")
            return False

    def get_version(self) -> Optional[str]:
        """
        Get Ollama server version.

        Returns:
            Version string or None if unavailable.
        """
        try:
            response = self._client.get("/api/version")
            response.raise_for_status()
            return response.json().get("version")
        except Exception as e:
            logger.debug(f"Failed to get version: {e}")
            return None

    # -------------------------------------------------------------------------
    # Model Management
    # -------------------------------------------------------------------------

    @retry_ollama(max_attempts=2, base_delay=0.5)
    def list_models(self) -> List[ModelInfo]:
        """
        List all available models in Ollama.

        Returns:
            List of ModelInfo objects with model details.

        Raises:
            OllamaConnectionError: If cannot connect to server.
        """
        try:
            response = self._client.get("/api/tags")
            response.raise_for_status()
            data = response.json()

            models = []
            for model_data in data.get("models", []):
                models.append(ModelInfo(
                    name=model_data["name"],
                    modified_at=model_data.get("modified_at", ""),
                    size=model_data.get("size", 0),
                    digest=model_data.get("digest", ""),
                    details=model_data.get("details", {}),
                ))

            # Update model cache
            self._model_cache = {m.name: True for m in models}
            # Also cache base names (without tags)
            for m in models:
                base_name = m.name.split(":")[0]
                self._model_cache[base_name] = True
            self._model_cache_time = time.time()

            return models

        except httpx.ConnectError as e:
            raise OllamaConnectionError(
                "Cannot connect to Ollama server",
                base_url=self.base_url,
                original_error=e,
            )
        except httpx.TimeoutException as e:
            raise OllamaTimeoutError(
                "Timeout listing models",
                timeout_seconds=self.config.connect_timeout,
                operation="list_models",
            )

    def is_model_available(self, model: str) -> bool:
        """
        Check if a specific model is available.

        Uses caching to avoid repeated API calls.

        Args:
            model: Model name (e.g., "llama3.2", "llama3.2:7b")

        Returns:
            True if model is available, False otherwise.
        """
        # Check cache first
        if time.time() - self._model_cache_time < self._model_cache_ttl:
            if model in self._model_cache:
                return self._model_cache[model]
            # Check base name
            base_name = model.split(":")[0]
            if base_name in self._model_cache:
                return self._model_cache[base_name]

        # Refresh cache
        try:
            models = self.list_models()
            return model in self._model_cache or model.split(":")[0] in self._model_cache
        except OllamaError:
            return False

    def ensure_model_available(self, model: str) -> None:
        """
        Ensure a model is available, raising an error if not.

        Args:
            model: Model name to check.

        Raises:
            OllamaModelNotFoundError: If model is not available.
        """
        if not self.is_model_available(model):
            available = list(self._model_cache.keys()) if self._model_cache else None
            raise OllamaModelNotFoundError(
                model=model,
                available_models=available,
            )

    # -------------------------------------------------------------------------
    # Text Generation
    # -------------------------------------------------------------------------

    @retry_ollama()
    def generate(
        self,
        model: str,
        prompt: str,
        system: Optional[str] = None,
        template: Optional[str] = None,
        context: Optional[List[int]] = None,
        options: Optional[Dict[str, Any]] = None,
        format: Optional[str] = None,
        raw: bool = False,
        stream: bool = False,
    ) -> Union[GenerationResponse, Iterator[GenerationResponse]]:
        """
        Generate text using a specified model.

        This is the core text generation method. For JSON output, use
        format="json" which instructs the model to respond in JSON.

        Args:
            model: Model name (e.g., "llama3.2", "mistral")
            prompt: The prompt to generate from
            system: System message to set context
            template: Custom prompt template (advanced)
            context: Previous conversation context for continuation
            options: Model parameters like temperature, top_p, etc.
            format: Response format ("json" for JSON mode)
            raw: If True, don't apply prompt template
            stream: If True, return iterator for streaming responses

        Returns:
            GenerationResponse with generated text (or iterator if streaming)

        Raises:
            OllamaModelNotFoundError: If model doesn't exist
            OllamaGenerationError: If generation fails
            OllamaTimeoutError: If request times out

        Example:
            >>> response = client.generate(
            ...     model="llama3.2",
            ...     prompt="List 3 colors",
            ...     format="json",
            ...     options={"temperature": 0.7}
            ... )
            >>> print(response.text)
            {"colors": ["red", "blue", "green"]}
        """
        # Build request payload
        payload: Dict[str, Any] = {
            "model": model,
            "prompt": prompt,
            "stream": stream,
        }

        if system:
            payload["system"] = system
        if template:
            payload["template"] = template
        if context:
            payload["context"] = context
        if options:
            payload["options"] = options
        if format:
            payload["format"] = format
        if raw:
            payload["raw"] = raw

        try:
            if stream:
                return self._generate_stream(payload)
            else:
                return self._generate_sync(payload)

        except httpx.ConnectError as e:
            raise OllamaConnectionError(
                "Cannot connect to Ollama server",
                base_url=self.base_url,
                original_error=e,
            )
        except httpx.TimeoutException as e:
            raise OllamaTimeoutError(
                "Generation request timed out",
                timeout_seconds=self.config.timeout,
                model=model,
                operation="generate",
            )
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                raise OllamaModelNotFoundError(model=model)
            raise OllamaGenerationError(
                f"Generation failed with status {e.response.status_code}",
                model=model,
                prompt_preview=prompt[:200],
                status_code=e.response.status_code,
            )

    def _generate_sync(self, payload: Dict[str, Any]) -> GenerationResponse:
        """Execute synchronous generation request."""
        response = self._client.post("/api/generate", json=payload)
        response.raise_for_status()
        data = response.json()

        return GenerationResponse(
            text=data.get("response", ""),
            model=data.get("model", payload["model"]),
            created_at=data.get("created_at", ""),
            done=data.get("done", True),
            total_duration=data.get("total_duration"),
            load_duration=data.get("load_duration"),
            prompt_eval_count=data.get("prompt_eval_count"),
            prompt_eval_duration=data.get("prompt_eval_duration"),
            eval_count=data.get("eval_count"),
            eval_duration=data.get("eval_duration"),
            context=data.get("context"),
        )

    def _generate_stream(self, payload: Dict[str, Any]) -> Iterator[GenerationResponse]:
        """Execute streaming generation request."""
        with self._client.stream("POST", "/api/generate", json=payload) as response:
            response.raise_for_status()
            for line in response.iter_lines():
                if line:
                    data = json.loads(line)
                    yield GenerationResponse(
                        text=data.get("response", ""),
                        model=data.get("model", payload["model"]),
                        created_at=data.get("created_at", ""),
                        done=data.get("done", False),
                        total_duration=data.get("total_duration"),
                        load_duration=data.get("load_duration"),
                        prompt_eval_count=data.get("prompt_eval_count"),
                        prompt_eval_duration=data.get("prompt_eval_duration"),
                        eval_count=data.get("eval_count"),
                        eval_duration=data.get("eval_duration"),
                        context=data.get("context"),
                    )

    def generate_json(
        self,
        model: str,
        prompt: str,
        system: Optional[str] = None,
        options: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Generate JSON response from the model.

        Convenience method that sets format="json" and parses the response.

        Args:
            model: Model name
            prompt: Prompt that should result in JSON output
            system: Optional system message
            options: Model parameters

        Returns:
            Parsed JSON dictionary

        Raises:
            OllamaGenerationError: If generation or parsing fails
        """
        response = self.generate(
            model=model,
            prompt=prompt,
            system=system,
            format="json",
            options=options,
        )

        try:
            return json.loads(response.text)
        except json.JSONDecodeError as e:
            raise OllamaGenerationError(
                f"Failed to parse JSON response: {e}",
                model=model,
                prompt_preview=prompt,
            )

    # -------------------------------------------------------------------------
    # Embeddings
    # -------------------------------------------------------------------------

    @retry_ollama()
    def embed(
        self,
        model: str,
        text: Union[str, List[str]],
    ) -> Union[List[float], List[List[float]]]:
        """
        Generate embeddings for text.

        Supports both single text and batch embedding generation.

        Args:
            model: Embedding model name (e.g., "nomic-embed-text")
            text: Single text string or list of texts

        Returns:
            Single embedding vector or list of embeddings (for batch)

        Raises:
            OllamaModelNotFoundError: If model doesn't exist
            OllamaGenerationError: If embedding generation fails

        Example:
            >>> # Single embedding
            >>> embedding = client.embed("nomic-embed-text", "Hello world")
            >>> print(len(embedding))  # 768
            >>>
            >>> # Batch embedding
            >>> embeddings = client.embed("nomic-embed-text", ["Hello", "World"])
            >>> print(len(embeddings))  # 2
            >>> print(len(embeddings[0]))  # 768
        """
        # Handle single text vs batch
        is_batch = isinstance(text, list)
        texts = text if is_batch else [text]

        payload = {
            "model": model,
            "input": texts,
        }

        try:
            response = self._client.post("/api/embed", json=payload)
            response.raise_for_status()
            data = response.json()

            embeddings = data.get("embeddings", [])

            if is_batch:
                return embeddings
            else:
                return embeddings[0] if embeddings else []

        except httpx.ConnectError as e:
            raise OllamaConnectionError(
                "Cannot connect to Ollama server",
                base_url=self.base_url,
                original_error=e,
            )
        except httpx.TimeoutException as e:
            raise OllamaTimeoutError(
                "Embedding request timed out",
                timeout_seconds=self.config.timeout,
                model=model,
                operation="embed",
            )
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                raise OllamaModelNotFoundError(model=model)
            raise OllamaGenerationError(
                f"Embedding generation failed with status {e.response.status_code}",
                model=model,
                text_preview=str(text)[:100],
                status_code=e.response.status_code,
            )

    def embed_with_info(
        self,
        model: str,
        text: str,
    ) -> EmbeddingResponse:
        """
        Generate embedding with metadata.

        Args:
            model: Embedding model name
            text: Text to embed

        Returns:
            EmbeddingResponse with embedding and metadata
        """
        embedding = self.embed(model, text)
        return EmbeddingResponse(
            embedding=embedding,
            model=model,
        )

    # -------------------------------------------------------------------------
    # Utility Methods
    # -------------------------------------------------------------------------

    def get_embedding_dimension(self, model: str) -> int:
        """
        Get the embedding dimension for a model.

        Generates a test embedding to determine the dimension.

        Args:
            model: Embedding model name

        Returns:
            Embedding dimension (e.g., 768 for nomic-embed-text)
        """
        test_embedding = self.embed(model, "test")
        return len(test_embedding)

    @staticmethod
    def compute_text_hash(text: str) -> str:
        """
        Compute a hash for text (useful for caching).

        Args:
            text: Text to hash

        Returns:
            SHA256 hash of the text
        """
        return hashlib.sha256(text.encode()).hexdigest()


# =============================================================================
# Convenience Functions
# =============================================================================

def create_client(config: Optional[OllamaConfig] = None) -> OllamaClient:
    """
    Create an OllamaClient with the given configuration.

    Args:
        config: Optional OllamaConfig, uses defaults if not provided

    Returns:
        Configured OllamaClient instance
    """
    return OllamaClient(config)


def quick_generate(prompt: str, model: str = "llama3.2") -> str:
    """
    Quick one-off text generation.

    Creates a client, generates text, and cleans up.
    Not recommended for repeated use (creates new connection each time).

    Args:
        prompt: Text prompt
        model: Model to use

    Returns:
        Generated text
    """
    with OllamaClient() as client:
        response = client.generate(model, prompt)
        return response.text


def quick_embed(text: str, model: str = "nomic-embed-text") -> List[float]:
    """
    Quick one-off embedding generation.

    Args:
        text: Text to embed
        model: Embedding model to use

    Returns:
        Embedding vector
    """
    with OllamaClient() as client:
        return client.embed(model, text)
