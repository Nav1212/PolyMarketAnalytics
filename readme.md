# PolyMarketAnalytics

A distributed ETL data pipeline for collecting, storing, and analyzing Polymarket data using a medallion architecture (Bronze → Silver → Gold layers).

## Overview

PolyMarketAnalytics is designed to efficiently fetch, transform, and store Polymarket prediction market data. The system uses a multi-threaded architecture with resumable execution, rate limiting, and a three-layer data warehouse design.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         EXTERNAL APIs                                    │
│   Data API • CLOB API • Gamma API                                       │
└────────────────────────────────┬────────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                         FETCHER LAYER                                    │
│   MarketFetcher → TradeFetcher, PriceFetcher, LeaderboardFetcher        │
│   (Parallel workers with rate limiting & cursor-based resumption)       │
└────────────────────────────────┬────────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                      MEDALLION ARCHITECTURE                              │
│  ┌──────────────┐   ┌──────────────┐   ┌──────────────┐                 │
│  │ BRONZE       │ → │ SILVER       │ → │ GOLD         │                 │
│  │ Raw Parquet  │   │ DuckDB       │   │ Analytics    │                 │
│  │ Immutable    │   │ Normalized   │   │ Aggregations │                 │
│  └──────────────┘   └──────────────┘   └──────────────┘                 │
└─────────────────────────────────────────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                       TAG MANAGER UI                                     │
│   Streamlit app for market classification with LLM integration          │
└─────────────────────────────────────────────────────────────────────────┘
```

For detailed diagrams, see [docs/diagrams/architecture.md](docs/diagrams/architecture.md).

## Features

- **Distributed Data Fetching**: Multi-threaded workers with configurable parallelism
- **Rate Limiting**: Token bucket algorithm with per-API rate limits
- **Resumable Execution**: Cursor-based progress tracking survives interruptions
- **Medallion Architecture**: Bronze (Parquet) → Silver (DuckDB) → Gold (Analytics)
- **Hive Partitioning**: Date-partitioned Parquet files for efficient querying
- **Tag Manager UI**: Streamlit app for market classification with Ollama LLM integration

## Data Sources

| API | Base URL | Data Collected |
|-----|----------|----------------|
| Data API | `data-api.polymarket.com` | Markets, trades, volumes |
| CLOB API | `clob.polymarket.com` | Price history, leaderboard |
| Gamma API | `gamma-api.polymarket.com` | Extended market data, events |

## Project Structure

```
PolyMarketAnalytics/
├── fetcher/                    # Core data fetching system
│   ├── main.py                 # CLI entry point
│   ├── config.py               # Configuration management
│   ├── workers/                # Fetcher implementations
│   │   ├── market_fetcher.py   # Markets & tokens
│   │   ├── trade_fetcher.py    # Trade data
│   │   ├── price_fetcher.py    # Price history
│   │   └── leaderboard_fetcher.py
│   ├── coordination/           # Orchestration
│   │   └── coordinator.py
│   ├── persistence/            # Data persistence
│   │   ├── swappable_queue.py  # Atomic buffer swap
│   │   └── parquet_persister.py
│   └── cursors/                # Progress tracking
│       └── manager.py
├── Ingestion/                  # Bronze → Silver transformation
│   ├── silver_loader.py        # Orchestrates loading
│   ├── silver_create.py        # Schema definitions
│   └── transformers/           # Data transformers
├── tag_manager/                # Streamlit UI
│   ├── app.py                  # Main application
│   ├── pages/                  # UI pages
│   └── services/               # Business logic
├── data/                       # Bronze layer (Parquet files)
│   ├── trades/dt=YYYY-MM-DD/
│   ├── markets/dt=YYYY-MM-DD/
│   ├── prices/dt=YYYY-MM-DD/
│   └── leaderboard/dt=YYYY-MM-DD/
├── docs/                       # Documentation
│   └── diagrams/               # System diagrams
├── scripts/                    # Utility scripts
├── tests/                      # Unit tests
└── notebooks/                  # Jupyter notebooks
```

## Prerequisites

- Python 3.10+
- DuckDB
- Required packages (see `requirements.txt`)

## Installation

```bash
# Clone the repository
git clone https://github.com/Nav1212/PolyMarketAnalytics.git
cd PolyMarketAnalytics

# Install dependencies
pip install -r requirements.txt
```

## Usage

### Data Fetching

```bash
# Full pipeline - fetch all data types
python fetcher/main.py --mode all

# Fetch specific data types
python fetcher/main.py --mode markets    # Only markets
python fetcher/main.py --mode trades     # Only trades
python fetcher/main.py --mode prices     # Only prices
python fetcher/main.py --mode leaderboard

# Options
python fetcher/main.py --fresh           # Ignore saved cursors (start fresh)
python fetcher/main.py --limit 100       # Process only 100 markets
python fetcher/main.py --timeout 3600    # 1-hour timeout
```

The fetcher supports **graceful shutdown** - press `Ctrl+C` to stop. Progress is saved to `cursor.json` and automatically resumed on next run.

### Data Transformation (Bronze → Silver)

```bash
# Load Bronze Parquet files into Silver DuckDB
python -m Ingestion.silver_loader
```

### Tag Manager UI

```bash
# Start the Streamlit application
streamlit run tag_manager/app.py
```

The Tag Manager provides:
- **Tags Page**: Create and manage market classification tags
- **Examples Page**: Add training examples for classifications
- **Judge Page**: Human review queue for LLM classification results
- **History Page**: View and edit past classifications
- **Settings Page**: Configure LLM models (Ollama integration)

## Data Schema

### Bronze Layer (Parquet)
Raw, immutable data partitioned by date (`dt=YYYY-MM-DD/`).

### Silver Layer (DuckDB)

**Dimension Tables:**
- `MarketDim` - Markets with metadata (question, volume, dates)
- `MarketTokenDim` - Tokens per market (outcomes, prices, winners)
- `TraderDim` - Unique traders/wallets
- `EventDim` - Events grouping markets

**Fact Tables:**
- `TradeFact` - Individual trades
- `PriceHistoryFact` - Historical price data

## Configuration

Configuration is managed in `fetcher/config.py` with defaults:

| Setting | Default | Description |
|---------|---------|-------------|
| Rate Limits | 70-100 req/10s | Per-API rate limiting |
| Workers | 1-3 per type | Parallel worker count |
| Queue Thresholds | 1K-10K | Items before Parquet write |
| Timeout | 30s | API request timeout |

## Key Design Patterns

- **Token Bucket Rate Limiting**: Thread-safe per-API rate limiting
- **Atomic Buffer Swap**: Non-blocking queue persistence via `SwappableQueue`
- **Cursor-Based Resumption**: Save/restore progress on interruption
- **Hive Partitioning**: Date-based organization for efficient querying
- **Medallion Architecture**: Bronze → Silver → Gold data layers

## Development

```bash
# Run tests
pytest tests/

# Run with coverage
pytest --cov=fetcher tests/
```

## License

MIT License
