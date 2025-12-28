"""
NLP Enrichment Exception Hierarchy

Provides specific exception types for the NLP enrichment system,
following the pattern established in fetcher/utils/exceptions.py.

Exception Hierarchy:
    NLPEnrichmentError (base)
    ├── OllamaError
    │   ├── OllamaConnectionError
    │   ├── OllamaModelNotFoundError
    │   ├── OllamaGenerationError
    │   └── OllamaTimeoutError
    ├── EmbeddingError
    │   ├── EmbeddingGenerationError
    │   ├── EmbeddingCacheError
    │   └── EmbeddingDimensionError
    └── TagGraphError
        ├── TagNotFoundError
        ├── TagCycleError
        └── TagMergeError
"""

from typing import Optional, Dict, Any, List


class NLPEnrichmentError(Exception):
    """
    Base exception for all NLP enrichment-related errors.

    Provides consistent error structure with message and optional details.
    """

    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.message = message
        self.details = details or {}

    def __str__(self) -> str:
        if self.details:
            return f"{self.message} | Details: {self.details}"
        return self.message

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.message!r}, details={self.details!r})"


# =============================================================================
# Ollama Errors
# =============================================================================

class OllamaError(NLPEnrichmentError):
    """Base exception for Ollama-related errors."""

    def __init__(
        self,
        message: str,
        base_url: Optional[str] = None,
        model: Optional[str] = None,
        **kwargs
    ):
        details = {
            "base_url": base_url,
            "model": model,
            **kwargs
        }
        # Remove None values for cleaner output
        details = {k: v for k, v in details.items() if v is not None}
        super().__init__(message, details)
        self.base_url = base_url
        self.model = model


class OllamaConnectionError(OllamaError):
    """
    Raised when connection to Ollama server fails.

    Common causes:
    - Ollama not running
    - Wrong host/port configuration
    - Network issues
    """

    def __init__(
        self,
        message: str = "Failed to connect to Ollama server",
        base_url: Optional[str] = None,
        original_error: Optional[Exception] = None,
        **kwargs
    ):
        super().__init__(message, base_url=base_url, **kwargs)
        self.original_error = original_error
        if original_error:
            self.details["original_error"] = str(original_error)


class OllamaModelNotFoundError(OllamaError):
    """
    Raised when requested model is not available in Ollama.

    The user needs to pull the model first with `ollama pull <model>`.
    """

    def __init__(
        self,
        model: str,
        available_models: Optional[List[str]] = None,
        **kwargs
    ):
        message = f"Model '{model}' not found in Ollama"
        super().__init__(message, model=model, **kwargs)
        self.available_models = available_models
        if available_models:
            self.details["available_models"] = available_models
            self.details["suggestion"] = f"Run 'ollama pull {model}' to download the model"


class OllamaGenerationError(OllamaError):
    """
    Raised when text generation fails.

    This could be due to:
    - Invalid prompt
    - Model error during generation
    - Resource exhaustion
    """

    def __init__(
        self,
        message: str = "Text generation failed",
        model: Optional[str] = None,
        prompt_preview: Optional[str] = None,
        status_code: Optional[int] = None,
        **kwargs
    ):
        super().__init__(message, model=model, **kwargs)
        self.status_code = status_code
        if prompt_preview:
            # Truncate long prompts for error message
            self.details["prompt_preview"] = prompt_preview[:200] + "..." if len(prompt_preview) > 200 else prompt_preview
        if status_code:
            self.details["status_code"] = status_code


class OllamaTimeoutError(OllamaError):
    """
    Raised when Ollama request times out.

    Generation can be slow for large models or complex prompts.
    Consider increasing timeout or using a faster model.
    """

    def __init__(
        self,
        message: str = "Ollama request timed out",
        timeout_seconds: Optional[float] = None,
        model: Optional[str] = None,
        operation: Optional[str] = None,
        **kwargs
    ):
        super().__init__(message, model=model, **kwargs)
        self.timeout_seconds = timeout_seconds
        self.operation = operation
        if timeout_seconds:
            self.details["timeout_seconds"] = timeout_seconds
        if operation:
            self.details["operation"] = operation


# =============================================================================
# Embedding Errors
# =============================================================================

class EmbeddingError(NLPEnrichmentError):
    """Base exception for embedding-related errors."""

    def __init__(
        self,
        message: str,
        model: Optional[str] = None,
        **kwargs
    ):
        details = {"model": model, **kwargs}
        details = {k: v for k, v in details.items() if v is not None}
        super().__init__(message, details)
        self.model = model


class EmbeddingGenerationError(EmbeddingError):
    """
    Raised when embedding generation fails.

    This wraps lower-level Ollama errors with embedding context.
    """

    def __init__(
        self,
        message: str = "Failed to generate embedding",
        text_preview: Optional[str] = None,
        model: Optional[str] = None,
        original_error: Optional[Exception] = None,
        **kwargs
    ):
        super().__init__(message, model=model, **kwargs)
        self.original_error = original_error
        if text_preview:
            self.details["text_preview"] = text_preview[:100] + "..." if len(text_preview) > 100 else text_preview
        if original_error:
            self.details["original_error"] = str(original_error)


class EmbeddingCacheError(EmbeddingError):
    """
    Raised when embedding cache operations fail.

    Could be due to:
    - Database connection issues
    - Cache corruption
    - Serialization errors
    """

    def __init__(
        self,
        message: str = "Embedding cache operation failed",
        operation: Optional[str] = None,
        cache_key: Optional[str] = None,
        **kwargs
    ):
        super().__init__(message, **kwargs)
        self.operation = operation
        self.cache_key = cache_key
        if operation:
            self.details["operation"] = operation
        if cache_key:
            self.details["cache_key"] = cache_key[:50] + "..." if len(cache_key) > 50 else cache_key


class EmbeddingDimensionError(EmbeddingError):
    """
    Raised when embedding dimension doesn't match expected value.

    This usually indicates a model mismatch or configuration error.
    """

    def __init__(
        self,
        expected_dimension: int,
        actual_dimension: int,
        model: Optional[str] = None,
        **kwargs
    ):
        message = f"Embedding dimension mismatch: expected {expected_dimension}, got {actual_dimension}"
        super().__init__(message, model=model, **kwargs)
        self.expected_dimension = expected_dimension
        self.actual_dimension = actual_dimension
        self.details["expected_dimension"] = expected_dimension
        self.details["actual_dimension"] = actual_dimension


# =============================================================================
# Tag Graph Errors
# =============================================================================

class TagGraphError(NLPEnrichmentError):
    """Base exception for tag graph-related errors."""
    pass


class TagNotFoundError(TagGraphError):
    """Raised when a referenced tag doesn't exist."""

    def __init__(
        self,
        tag_id: Optional[int] = None,
        tag_name: Optional[str] = None,
        **kwargs
    ):
        if tag_id:
            message = f"Tag with ID {tag_id} not found"
        elif tag_name:
            message = f"Tag '{tag_name}' not found"
        else:
            message = "Tag not found"

        super().__init__(message, kwargs)
        self.tag_id = tag_id
        self.tag_name = tag_name


class TagCycleError(TagGraphError):
    """
    Raised when an operation would create a cycle in the tag hierarchy.

    Tag hierarchies must be DAGs (Directed Acyclic Graphs).
    """

    def __init__(
        self,
        parent_tag: str,
        child_tag: str,
        cycle_path: Optional[List[str]] = None,
        **kwargs
    ):
        message = f"Adding '{parent_tag}' as parent of '{child_tag}' would create a cycle"
        super().__init__(message, kwargs)
        self.parent_tag = parent_tag
        self.child_tag = child_tag
        self.cycle_path = cycle_path
        if cycle_path:
            self.details["cycle_path"] = " -> ".join(cycle_path)


class TagMergeError(TagGraphError):
    """
    Raised when tag merge operation fails.

    Could be due to:
    - Trying to merge a tag with itself
    - Source tag doesn't exist
    - Target tag doesn't exist
    - Merge would create conflicts
    """

    def __init__(
        self,
        source_tag: str,
        target_tag: str,
        reason: str,
        **kwargs
    ):
        message = f"Cannot merge tag '{source_tag}' into '{target_tag}': {reason}"
        super().__init__(message, kwargs)
        self.source_tag = source_tag
        self.target_tag = target_tag
        self.reason = reason


# =============================================================================
# Retriable Error Mixin
# =============================================================================

class RetriableNLPError:
    """
    Mixin to mark NLP exceptions as retriable.

    Used by retry decorators to determine if an error should trigger retry.
    """

    @property
    def is_retriable(self) -> bool:
        return True


class RetriableOllamaError(OllamaError, RetriableNLPError):
    """Ollama error that should be retried (e.g., temporary connection issues)."""
    pass


class RetriableEmbeddingError(EmbeddingError, RetriableNLPError):
    """Embedding error that should be retried."""
    pass
