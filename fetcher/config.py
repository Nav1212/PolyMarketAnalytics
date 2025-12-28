"""
Configuration loader for Polymarket fetcher.
Loads settings from config.json with fallback defaults.
"""

import json
from pathlib import Path
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class RateLimitsConfig:
    """Rate limiting configuration."""
    trade: int = 70
    market: int = 100
    price: int = 100
    leaderboard: int = 70
    gamma_market: int = 100
    window_seconds: float = 10.0


@dataclass
class QueuesConfig:
    """Queue threshold configuration."""
    trade_threshold: int = 10000
    market_threshold: int = 1000
    market_token_threshold: int = 5000
    price_threshold: int = 10000
    leaderboard_threshold: int = 5000
    gamma_market_threshold: int = 1000
    gamma_event_threshold: int = 1000
    gamma_category_threshold: int = 1000


@dataclass
class BatchSizesConfig:
    """Batch size configuration (aliases queue thresholds for persistence)."""
    trade: int = 10000
    market: int = 1000
    market_token: int = 5000
    price: int = 10000


@dataclass
class OutputDirsConfig:
    """Output directory configuration."""
    trade: str = "data/trades"
    market: str = "data/markets"
    market_token: str = "data/market_tokens"
    price: str = "data/prices"
    leaderboard: str = "data/leaderboard"
    gamma_market: str = "data/gamma_markets"
    gamma_event: str = "data/gamma_events"
    gamma_category: str = "data/gamma_categories"


@dataclass
class ApiConfig:
    """API configuration."""
    data_api_base: str = "https://data-api.polymarket.com"
    clob_api_base: str = "https://clob.polymarket.com"
    price_api_base: str = "https://clob.polymarket.com"
    gamma_api_base: str = "https://gamma-api.polymarket.com"
    timeout: float = 30.0
    connect_timeout: float = 10.0


@dataclass
class WorkersConfig:
    """Worker configuration per function type."""
    trade: int = 2
    market: int = 3
    price: int = 2
    leaderboard: int = 1
    gamma_market: int = 1


@dataclass
class CursorsConfig:
    """Cursor persistence configuration."""
    enabled: bool = True
    filename: str = "cursor.json"


@dataclass
class RetryConfig:
    """Retry configuration for failed worker executions."""
    max_attempts: int = 3
    base_delay: float = 1.0
    max_delay: float = 30.0
    exponential_base: float = 2.0


# =============================================================================
# NLP Enrichment Configuration
# =============================================================================

@dataclass
class OllamaConfig:
    """
    Configuration for Ollama LLM server connection.

    Ollama runs locally and provides access to various open-source models
    like Llama, Mistral, etc. for text generation and embedding.

    Attributes:
        base_url: Ollama API base URL (default: localhost:11434)
        timeout: Request timeout in seconds for generation (can be long)
        connect_timeout: Connection establishment timeout
        embedding_model: Model to use for generating embeddings
        generation_model: Model to use for text generation tasks
        retry_max_attempts: Maximum retry attempts for failed requests
        retry_base_delay: Base delay between retries (exponential backoff)
        retry_max_delay: Maximum delay between retries
    """
    base_url: str = "http://localhost:11434"
    timeout: float = 300.0  # LLM generation can be slow
    connect_timeout: float = 10.0
    embedding_model: str = "nomic-embed-text"  # 768 dimensions, good quality
    generation_model: str = "llama3.2"  # Default generation model
    retry_max_attempts: int = 3
    retry_base_delay: float = 1.0
    retry_max_delay: float = 60.0


@dataclass
class EmbeddingCacheConfig:
    """
    Configuration for embedding cache behavior.

    Embeddings are expensive to compute, so we cache them to avoid
    recomputation. This config controls cache behavior.

    Attributes:
        enabled: Whether to use caching
        max_memory_items: Maximum items to keep in memory cache (LRU eviction)
        persist_to_db: Whether to persist embeddings to DuckDB
        batch_persist_threshold: Number of new embeddings before batch persist
    """
    enabled: bool = True
    max_memory_items: int = 10000
    persist_to_db: bool = True
    batch_persist_threshold: int = 100


@dataclass
class EmbeddingConfig:
    """
    Configuration for the embedding service.

    Attributes:
        batch_size: Number of texts to embed in a single batch
        normalize: Whether to L2-normalize embedding vectors
        cache: Cache configuration
        dimension: Expected embedding dimension (model-dependent)
    """
    batch_size: int = 32
    normalize: bool = True
    cache: EmbeddingCacheConfig = field(default_factory=EmbeddingCacheConfig)
    dimension: int = 768  # nomic-embed-text default


@dataclass
class ModelCouncilConfig:
    """
    Configuration for the model council voting system.

    The council runs multiple models on the same task and aggregates
    their responses using voting. This improves accuracy and reliability.

    Attributes:
        models: List of model names to use in the council
        require_unanimous: Whether unanimous agreement is required
        min_votes_for_majority: Minimum votes needed for majority decision
        timeout_per_model: Timeout for each model's response
        parallel_execution: Whether to run models in parallel
    """
    models: list = field(default_factory=lambda: ["llama3.2", "mistral", "phi3"])
    require_unanimous: bool = False
    min_votes_for_majority: int = 2
    timeout_per_model: float = 120.0
    parallel_execution: bool = True


@dataclass
class NLPEnrichmentConfig:
    """
    Master configuration for the NLP enrichment system.

    Aggregates all NLP-related configurations.

    Attributes:
        enabled: Whether NLP enrichment is active
        ollama: Ollama server configuration
        embedding: Embedding service configuration
        council: Model council configuration
        enrichment_batch_size: Number of markets to enrich in one batch
        poll_interval_seconds: Seconds between polling for new markets
    """
    enabled: bool = True
    ollama: OllamaConfig = field(default_factory=OllamaConfig)
    embedding: EmbeddingConfig = field(default_factory=EmbeddingConfig)
    council: ModelCouncilConfig = field(default_factory=ModelCouncilConfig)
    enrichment_batch_size: int = 10
    poll_interval_seconds: float = 30.0


@dataclass
class Config:
    """Main configuration class."""
    rate_limits: RateLimitsConfig = field(default_factory=RateLimitsConfig)
    queues: QueuesConfig = field(default_factory=QueuesConfig)
    output_dirs: OutputDirsConfig = field(default_factory=OutputDirsConfig)
    api: ApiConfig = field(default_factory=ApiConfig)
    workers: WorkersConfig = field(default_factory=WorkersConfig)
    cursors: CursorsConfig = field(default_factory=CursorsConfig)
    retry: RetryConfig = field(default_factory=RetryConfig)
    nlp_enrichment: NLPEnrichmentConfig = field(default_factory=NLPEnrichmentConfig)
    
    @property
    def batch_sizes(self) -> BatchSizesConfig:
        """Return batch sizes derived from queue thresholds."""
        return BatchSizesConfig(
            trade=self.queues.trade_threshold,
            market=self.queues.market_threshold,
            market_token=self.queues.market_token_threshold,
            price=self.queues.price_threshold
        )
    
    @classmethod
    def from_dict(cls, data: dict) -> "Config":
        """Create Config from dictionary."""
        # Parse NLP enrichment config with nested structures
        nlp_data = data.get("nlp_enrichment", {})
        nlp_config = NLPEnrichmentConfig(
            enabled=nlp_data.get("enabled", True),
            ollama=OllamaConfig(**nlp_data.get("ollama", {})),
            embedding=EmbeddingConfig(
                batch_size=nlp_data.get("embedding", {}).get("batch_size", 32),
                normalize=nlp_data.get("embedding", {}).get("normalize", True),
                cache=EmbeddingCacheConfig(**nlp_data.get("embedding", {}).get("cache", {})),
                dimension=nlp_data.get("embedding", {}).get("dimension", 768),
            ),
            council=ModelCouncilConfig(**nlp_data.get("council", {})),
            enrichment_batch_size=nlp_data.get("enrichment_batch_size", 10),
            poll_interval_seconds=nlp_data.get("poll_interval_seconds", 30.0),
        )

        return cls(
            rate_limits=RateLimitsConfig(**data.get("rate_limits", {})),
            queues=QueuesConfig(**data.get("queues", {})),
            output_dirs=OutputDirsConfig(**data.get("output_dirs", {})),
            api=ApiConfig(**data.get("api", {})),
            workers=WorkersConfig(**data.get("workers", {})),
            cursors=CursorsConfig(**data.get("cursors", {})),
            retry=RetryConfig(**data.get("retry", {})),
            nlp_enrichment=nlp_config,
        )
    
    def to_dict(self) -> dict:
        """Convert Config to dictionary."""
        return {
            "rate_limits": {
                "trade": self.rate_limits.trade,
                "market": self.rate_limits.market,
                "price": self.rate_limits.price,
                "leaderboard": self.rate_limits.leaderboard,
                "window_seconds": self.rate_limits.window_seconds
            },
            "queues": {
                "trade_threshold": self.queues.trade_threshold,
                "market_threshold": self.queues.market_threshold,
                "market_token_threshold": self.queues.market_token_threshold,
                "price_threshold": self.queues.price_threshold
            },
            "output_dirs": {
                "trade": self.output_dirs.trade,
                "market": self.output_dirs.market,
                "market_token": self.output_dirs.market_token,
                "price": self.output_dirs.price
            },
            "api": {
                "data_api_base": self.api.data_api_base,
                "timeout": self.api.timeout,
                "connect_timeout": self.api.connect_timeout
            },
            "workers": {
                "trade": self.workers.trade,
                "market": self.workers.market,
                "price": self.workers.price,
                "leaderboard": self.workers.leaderboard
            },
            "cursors": {
                "enabled": self.cursors.enabled,
                "filename": self.cursors.filename
            },
            "retry": {
                "max_attempts": self.retry.max_attempts,
                "base_delay": self.retry.base_delay,
                "max_delay": self.retry.max_delay,
                "exponential_base": self.retry.exponential_base
            },
            "nlp_enrichment": {
                "enabled": self.nlp_enrichment.enabled,
                "ollama": {
                    "base_url": self.nlp_enrichment.ollama.base_url,
                    "timeout": self.nlp_enrichment.ollama.timeout,
                    "connect_timeout": self.nlp_enrichment.ollama.connect_timeout,
                    "embedding_model": self.nlp_enrichment.ollama.embedding_model,
                    "generation_model": self.nlp_enrichment.ollama.generation_model,
                    "retry_max_attempts": self.nlp_enrichment.ollama.retry_max_attempts,
                    "retry_base_delay": self.nlp_enrichment.ollama.retry_base_delay,
                    "retry_max_delay": self.nlp_enrichment.ollama.retry_max_delay,
                },
                "embedding": {
                    "batch_size": self.nlp_enrichment.embedding.batch_size,
                    "normalize": self.nlp_enrichment.embedding.normalize,
                    "dimension": self.nlp_enrichment.embedding.dimension,
                    "cache": {
                        "enabled": self.nlp_enrichment.embedding.cache.enabled,
                        "max_memory_items": self.nlp_enrichment.embedding.cache.max_memory_items,
                        "persist_to_db": self.nlp_enrichment.embedding.cache.persist_to_db,
                        "batch_persist_threshold": self.nlp_enrichment.embedding.cache.batch_persist_threshold,
                    },
                },
                "council": {
                    "models": self.nlp_enrichment.council.models,
                    "require_unanimous": self.nlp_enrichment.council.require_unanimous,
                    "min_votes_for_majority": self.nlp_enrichment.council.min_votes_for_majority,
                    "timeout_per_model": self.nlp_enrichment.council.timeout_per_model,
                    "parallel_execution": self.nlp_enrichment.council.parallel_execution,
                },
                "enrichment_batch_size": self.nlp_enrichment.enrichment_batch_size,
                "poll_interval_seconds": self.nlp_enrichment.poll_interval_seconds,
            }
        }


# Global config instance
_config: Optional[Config] = None


def load_config(config_path: Optional[str] = None) -> Config:
    """
    Load configuration from JSON file.
    
    Args:
        config_path: Path to config.json. If None, looks in same directory as this file.
    
    Returns:
        Config instance with loaded or default values.
    """
    global _config
    
    if config_path is None:
        resolved_path = Path(__file__).parent / "config.json"
    else:
        resolved_path = Path(config_path)
    
    if resolved_path.exists():
        try:
            with open(resolved_path, 'r') as f:
                data = json.load(f)
            _config = Config.from_dict(data)
            print(f"[Config] Loaded configuration from {resolved_path}")
        except Exception as e:
            print(f"[Config] Error loading {resolved_path}: {e}. Using defaults.")
            _config = Config()
    else:
        print(f"[Config] {resolved_path} not found. Using defaults.")
        _config = Config()
    
    return _config


def get_config() -> Config:
    """
    Get the current configuration. Loads from file if not already loaded.
    
    Returns:
        Config instance.
    """
    global _config
    if _config is None:
        _config = load_config()
    return _config


def set_config(config: Config) -> None:
    """
    Set a custom configuration.
    
    Args:
        config: Config instance to use.
    """
    global _config
    _config = config


def save_config(config: Config, config_path: Optional[str] = None) -> None:
    """
    Save configuration to JSON file.
    
    Args:
        config: Config instance to save.
        config_path: Path to save to. If None, saves to default location.
    """
    if config_path is None:
        resolved_path = Path(__file__).parent / "config.json"
    else:
        resolved_path = Path(config_path)
    
    with open(resolved_path, 'w') as f:
        json.dump(config.to_dict(), f, indent=4)
    
    print(f"[Config] Saved configuration to {resolved_path}")
