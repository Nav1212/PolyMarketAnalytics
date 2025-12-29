# PolyMarket Data Pipeline

A high-performance, multi-threaded data pipeline for scraping and warehousing Polymarket prediction market data.

## Overview

This project implements a production-grade ETL pipeline that:
- Fetches market, trade, price, and leaderboard data from multiple Polymarket APIs
- Handles rate limiting with token bucket algorithm
- Persists data to Parquet files with Hive partitioning
- Supports cursor-based resumable fetching
- Transforms raw data into a structured DuckDB data warehouse (Silver layer)
- Provides Bronze-to-Silver ETL transformers for dimension and fact tables

## Architecture

For detailed system diagrams, see [SYSTEM_DIAGRAM.md](SYSTEM_DIAGRAM.md).

```
+----------------+     +----------------+     +----------------+
|   Data API     |     |   CLOB API     |     |   Gamma API    |
| (markets/trades)|     | (prices/lead.) |     | (ext. markets) |
+-------+--------+     +-------+--------+     +-------+--------+
        |                      |                      |
        v                      v                      v
+------------------------------------------------------------------+
|                     FETCHER COORDINATOR                           |
|  MarketFetcher -> TradeFetcher, PriceFetcher, LeaderboardFetcher |
+------------------------------------------------------------------+
        |                      |                      |
        v                      v                      v
+------------------------------------------------------------------+
|                     RATE LIMITER (Token Bucket)                   |
|   Trade: 70/10s | Market: 100/10s | Price: 100/10s | Gamma: 100/10s  |
+------------------------------------------------------------------+
        |
        v
+------------------------------------------------------------------+
|   SwappableQueue -> ParquetPersister -> Parquet Files (Bronze)   |
+------------------------------------------------------------------+
        |
        v
+------------------------------------------------------------------+
|                  DuckDB Data Warehouse (Silver)                   |
|     MarketDim | TraderDim | TradeFact | PriceHistoryFact         |
+------------------------------------------------------------------+
```

## Quick Start

### Installation

```bash
# Clone the repository
git clone <repo-url>
cd PolyMarketScrapping

# Create virtual environment
python -m venv .venv
.venv\Scripts\activate  # Windows
# source .venv/bin/activate  # Linux/Mac

# Install dependencies
pip install -r requirements.txt
```

### Running the Pipeline

```bash
# Full pipeline - fetches markets, trades, prices, and leaderboard
python -m fetcher.main --mode all

# Fetch only specific data types
python -m fetcher.main --mode trades      # Trades for inactive markets
python -m fetcher.main --mode markets     # Market data only
python -m fetcher.main --mode leaderboard # Leaderboard only

# Options
python -m fetcher.main --fresh            # Ignore saved cursors, start fresh
python -m fetcher.main --limit 100        # Process only 100 markets (testing)
python -m fetcher.main --timeout 3600     # Set 1-hour timeout
```

### Resumable Fetching

The pipeline automatically saves progress on interruption (Ctrl+C) and resumes from the last position:

```bash
# Start fetching (can be interrupted)
python -m fetcher.main --mode all

# Resume from last position (automatic)
python -m fetcher.main --mode all

# Force fresh start
python -m fetcher.main --mode all --fresh
```

## Project Structure

```
PolyMarketScrapping/
├── fetcher/                    # Main fetcher package
│   ├── main.py                 # Entry point
│   ├── config.py               # Configuration management
│   ├── config.json             # Runtime configuration
│   ├── coordination/
│   │   └── coordinator.py      # FetcherCoordinator - orchestrates all fetchers
│   ├── workers/
│   │   ├── worker_manager.py   # Rate limiting (token bucket)
│   │   ├── market_fetcher.py   # Fetches markets from Data API
│   │   ├── trade_fetcher.py    # Fetches trades per market
│   │   ├── price_fetcher.py    # Fetches price history per token
│   │   ├── leaderboard_fetcher.py
│   │   └── gamma_market_fetcher.py
│   ├── persistence/
│   │   ├── swappable_queue.py  # Non-blocking queue with atomic swap
│   │   └── parquet_persister.py # Batch writes to Parquet
│   ├── cursors/
│   │   └── manager.py          # Cursor persistence for resumption
│   └── utils/
│       ├── exceptions.py
│       ├── retry.py
│       └── logging_config.py
├── Ingestion/                  # Bronze-to-Silver transformations
│   ├── silver_create.py        # Create Silver layer schema in DuckDB
│   ├── silver_loader.py        # Orchestrates transformation pipeline
│   ├── load_silver.py          # Entry point for loading Silver layer
│   └── transformers/           # ETL transformers
│       ├── base.py             # Base transformer class
│       ├── market_dim.py       # MarketDim transformer (market metadata)
│       ├── market_token_dim.py # MarketTokenDim transformer (outcome tokens)
│       └── trader_dim.py       # TraderDim transformer (wallet addresses)
├── scripts/                    # Utility scripts
│   ├── reload_from_staging.py  # Load Bronze -> Silver
│   ├── recreate_marketdim.py   # Recreate MarketDim table
│   ├── reload_marketdim.py     # Reload MarketDim from parquet
│   ├── load_market_tokens.py   # Load market tokens from Bronze
│   └── debug_clear.py          # Debug utilities
├── data/                       # Bronze layer (Parquet files)
│   ├── trades/dt=YYYY-MM-DD/
│   ├── markets/dt=YYYY-MM-DD/
│   ├── market_tokens/
│   ├── prices/
│   ├── leaderboard/
│   ├── gamma_markets/
│   ├── gamma_events/
│   └── gamma_categories/
├── tests/                      # Unit and integration tests
│   ├── unit/                   # Unit tests
│   └── integration/            # Integration tests
├── ProofofConcept/             # Legacy prototype scripts
├── SYSTEM_DIAGRAM.md           # Detailed architecture diagrams
└── README.md                   # This file
```

## Data Layers (Medallion Architecture)

### Bronze Layer (Raw Parquet)
Raw data from APIs stored in Hive-partitioned Parquet files:
```
data/trades/dt=2024-12-26/trades_20241226_143022_123456.parquet
```
<img width="1566" height="702" alt="image" src="https://github.com/user-attachments/assets/c89a7ef8-ee2f-48cb-9b82-4b8f22419261" />
<img width="1295" height="662" alt="image" src="https://github.com/user-attachments/assets/2cf3651e-2073-48ac-8a71-8ec7a045c3ed" />
vc  <img width="1391" height="594" alt="image" src="https://github.com/user-attachments/assets/d3726982-5138-43e3-90d6-98e1daece318" />

### Silver Layer (DuckDB)
Prototype Implemented transformation untested
Structured data warehouse with dimension and fact tables:
- **MarketDim** - Market metadata (merged from CLOB and Gamma APIs)
- **MarketTokenDim** - Outcome tokens per market
- **TraderDim** - Normalized wallet addresses
- **TradeFact** - Individual trades with foreign keys
- **PriceHistoryFact** - Historical prices

Load Silver layer from Bronze:
```bash
# Default paths
python -m Ingestion.load_silver

# Custom paths and options
python -m Ingestion.load_silver --bronze data --silver path/to/silver.duckdb

# Adjust worker scaling
python -m Ingestion.load_silver --min-workers 2 --max-workers 16

# Preview what would be loaded
python -m Ingestion.load_silver --dry-run
```

### Gold Layer (Analytics)
Aggregated views and reports for analysis (in development).

## Configuration

Edit `fetcher/config.json` to customize:

```json
{
  "rate_limits": {
    "trade": 70,
    "market": 100,
    "price": 100,
    "leaderboard": 70,
    "gamma_market": 100,
    "window_seconds": 10.0
  },
  "workers": {
    "trade": 2,
    "market": 1,
    "price": 2,
    "leaderboard": 1,
    "gamma_market": 1
  },
  "queues": {
    "trade_threshold": 10000,
    "market_threshold": 10000,
    "market_token_threshold": 5000,
    "price_threshold": 10000,
    "leaderboard_threshold": 5000,
    "gamma_market_threshold": 1000,
    "gamma_event_threshold": 1000,
    "gamma_category_threshold": 1000
  },
  "retry": {
    "max_attempts": 3,
    "base_delay": 1.0,
    "max_delay": 30.0,
    "exponential_base": 2.0
  }
}
```

## External APIs

| API | Base URL | Data | Rate Limit |
|-----|----------|------|------------|
| Data API | https://data-api.polymarket.com | Markets, Trades | 100 req/10s |
| CLOB API | https://clob.polymarket.com | Prices, Leaderboard | 70-100 req/10s |
| Gamma API | https://gamma-api.polymarket.com | Extended Markets | 100 req/10s |

## Key Features

- **Multi-threaded fetching** - Configurable worker counts per data type
- **Token bucket rate limiting** - Respects API limits per endpoint
- **Non-blocking persistence** - SwappableQueue with atomic buffer swap
- **Cursor-based resumption** - Never lose progress on interruption
- **Hive partitioning** - Efficient date-based file organization
- **Retry with backoff** - Automatic retry for transient failures

## Testing

```bash
# Run all tests
pytest

# Run specific test categories
pytest tests/unit/
pytest tests/integration/
```

## License

MIT
