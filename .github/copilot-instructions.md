# Polymarket Data Pipeline - AI Agent Instructions

## Architecture Overview

This is a **multi-threaded data pipeline** for fetching Polymarket prediction market data with cursor-based resume capability.

### Core Components
- **`fetcher/`** - Main fetcher module with workers, coordination, and persistence
- **`Ingestion/`** - Silver layer DuckDB schema creation (`silver_create.py`)
- **`ProofofConcept/scripts/`** - Legacy 3-layer pipeline (staging → gold → parquet archive)
- **`data/`** - Parquet output organized by `dt=YYYY-MM-DD/` partitions

### Data Flow
```
FetcherCoordinator → Workers (TradeFetcher, MarketFetcher, PriceFetcher, LeaderboardFetcher)
                          ↓
                   SwappableQueue (thread-safe batching)
                          ↓
                   ParquetPersister → data/{type}/dt=YYYY-MM-DD/*.parquet
```

### Load Order Pattern
Markets fetch first, then feed downstream fetchers via inter-fetcher queues:
- `MarketFetcher` → `condition_id` to TradeFetcher, LeaderboardFetcher
- `MarketFetcher` → `token_id` to PriceFetcher

## Key Patterns

### Configuration (Dataclass-based)
All config flows from `fetcher/config.json` through `fetcher/config.py` dataclasses:
```python
config = get_config()
config.rate_limits.trade  # 70 req/10sec window
config.queues.trade_threshold  # 10000 items triggers parquet flush
```

### Cursor Persistence for Resume
The `CursorManager` enables interrupt/resume via `cursor.json`:
```python
# Cursors saved on shutdown, loaded on startup
cursor_manager.save_cursors()  # On KeyboardInterrupt
cursor_manager.load_cursors()  # On startup
```
Use `--fresh` flag to ignore existing cursors.

### SwappableQueue for Non-Blocking Writes
Workers use atomic swap to avoid blocking during parquet writes:
```python
queue = SwappableQueue(threshold=10000)
if queue.should_swap():
    items = queue.swap()  # Atomic: returns all items, clears queue
```

### Rate Limiting via TokenBucket
`WorkerManager` provides centralized rate limiting per worker type:
```python
self._manager.acquire_trade(loop_start)  # Blocks until token available
```

## Developer Workflows

### Running the Pipeline
```bash
# Full pipeline (markets → trades/prices/leaderboard)
python -m fetcher.main --mode=all

# Single mode
python -m fetcher.main --mode=trades --limit=10

# Resume from interrupt
python -m fetcher.main --mode=all  # Automatically resumes

# Fresh start (ignore cursors)
python -m fetcher.main --mode=all --fresh
```

### Testing
```bash
# All tests
pytest

# Integration tests (makes real API calls)
pytest -m integration

# Unit tests only
pytest -m "not integration"

# Coverage
pytest --cov=fetcher --cov-report=term-missing
```

### DuckDB Silver Layer
```python
from Ingestion.silver_create import create_all_tables
create_all_tables(db_path)  # Creates MarketDim, MarketTokenDim, TraderDim, etc.
```

## Code Conventions

### Adding a New Fetcher
1. Create worker in `fetcher/workers/{type}_fetcher.py` following `TradeFetcher` pattern
2. Add rate limit in `config.json` under `rate_limits`
3. Add queue threshold in `config.json` under `queues`
4. Add parquet schema in `parquet_persister.py` with `pa.schema([...])`
5. Register in `fetcher/workers/__init__.py`
6. Add cursor dataclass in `cursors/manager.py` if resumable

### Parquet Schema Pattern
Schemas defined in `fetcher/persistence/parquet_persister.py`:
```python
TRADE_SCHEMA = pa.schema([
    ('conditionId', pa.string()),
    ('timestamp', pa.int64()),
    ('price', pa.float64()),
    # ...
])
```

### Retry Decorator
Use `@retry` from `fetcher/utils/retry.py` for API calls:
```python
@retry(max_attempts=3, base_delay=1.0)
def fetch_data(self):
    # API call here
```

## External Dependencies
- **Polymarket APIs**: `data-api.polymarket.com`, `clob.polymarket.com`, `gamma-api.polymarket.com`
- **DuckDB**: Silver layer storage at `PolyMarketData/silver.duckdb`
- **httpx**: HTTP client with timeout handling
- **pyarrow**: Parquet serialization

## Tag Manager (LLM Classification)

### Architecture
The Tag Manager uses Ollama for LLM-based market classification with a multi-model voting system.

```
BackgroundClassifier (Coordinator) → Work Queue → Worker Pool (N threads)
                                          ↓
                                    JudgeService (per worker, own DB connection)
                                          ↓
                                    JudgePool → ThreadPoolExecutor (parallel models)
                                          ↓
                                    GPU Semaphore (throttles Ollama requests)
```

### Parallelization Settings
Configured via `SettingsService` (persisted in DuckDB `Settings` table):

| Setting | Key | Default | Description |
|---------|-----|---------|-------------|
| Parallel Workers | `parallel_workers` | 2 | Worker threads processing markets |
| GPU Concurrency | `ollama_concurrency` | 2 | Max concurrent Ollama requests |

```python
from tag_manager.services import SettingsService
settings = SettingsService(conn)
settings.get_parallel_workers()  # Default: 2
settings.get_ollama_concurrency()  # Default: 2
```

### Key Classes
- **`BackgroundClassifier`** (`judge_service.py`): Queue-based worker pool for background classification
- **`JudgePool`** (`llm/judge_pool.py`): Multi-model voting with GPU semaphore throttling
- **`SettingsService`** (`services/settings_service.py`): Persistent settings storage

### Graceful Shutdown
Workers finish their current classification before stopping:
```python
classifier.stop()  # Sets stop event, waits for workers to complete current task
```
