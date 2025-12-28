"""
Core NLP infrastructure module.

Contains:
    - OllamaClient: Wrapper for Ollama API with retry logic
    - EmbeddingService: Embedding generation with caching
    - Exceptions: NLP-specific exception types
"""

from nlp_enrichment.core.ollama_client import OllamaClient
from nlp_enrichment.core.embedding_service import EmbeddingService
from nlp_enrichment.core.exceptions import (
    NLPEnrichmentError,
    OllamaConnectionError,
    OllamaModelNotFoundError,
    OllamaGenerationError,
    EmbeddingError,
    EmbeddingCacheError,
)

__all__ = [
    "OllamaClient",
    "EmbeddingService",
    "NLPEnrichmentError",
    "OllamaConnectionError",
    "OllamaModelNotFoundError",
    "OllamaGenerationError",
    "EmbeddingError",
    "EmbeddingCacheError",
]
