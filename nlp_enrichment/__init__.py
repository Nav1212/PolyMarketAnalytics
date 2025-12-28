"""
NLP Enrichment Package for PolyMarket

This package provides NLP-based enrichment capabilities for market data,
including tag extraction, category classification, and embedding generation.

Modules:
    core/       - Core infrastructure (Ollama client, embedding service)
    tags/       - Tag management and graph operations
    council/    - Model council for multi-model voting (Phase 3)
    enrichment/ - Background enrichment service (Phase 4)
    review/     - Human review queue (Phase 5)
    finetuning/ - Fine-tuning data collection (Phase 5)
    api/        - FastAPI REST endpoints (Phase 6)
"""

__version__ = "0.1.0"
