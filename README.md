# Polymarket Data Pipeline

3-layer data pipeline for Polymarket prediction market analytics.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    STAGING LAYER (DuckDB)                    │
│             Raw JSON from API, append-only                   │
├─────────────────────────────────────────────────────────────┤
│  stg_markets_raw    stg_prices_raw    stg_trades_raw        │
└──────────────────────────┬──────────────────────────────────┘
                           │ transform_gold.py
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                  GOLD LAYER (DuckDB)                         │
│           Snowflake schema, storage-optimized                │
├─────────────────────────────────────────────────────────────┤
│  dim_category (lookup)   dim_outcome (lookup: YES/NO)       │
│  dim_event → dim_market → dim_token                          │
│  fact_price_snapshot (hourly)    fact_trade                 │
└──────────────────────────┬──────────────────────────────────┘
                           │ parquet_backup.py (monthly)
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                   ARCHIVE LAYER (Parquet)                    │
│           Compressed backups, portable                       │
├─────────────────────────────────────────────────────────────┤
│  archive/gold/2024-12/*.parquet                             │
└─────────────────────────────────────────────────────────────┘
```

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Create database with schema
cd scripts
python create_database.py

# Run historical backfill (markets + hourly prices + Nov 2024 trades)
python backfill_historical.py

# Transform staging → gold
python transform_gold.py

# Daily scrape (run via cron)
python daily_scraper.py

# Monthly backup to Parquet
python parquet_backup.py backup
```

## Scripts

| Script | Purpose |
|--------|---------|
| `create_database.py` | Create DuckDB with staging + gold tables |
| `api_client.py` | Polymarket API client with rate limiting |
| `backfill_historical.py` | Load all historical data into staging |
| `daily_scraper.py` | Daily job to scrape yesterday's prices |
| `transform_gold.py` | Transform staging JSON → star schema |
| `parquet_backup.py` | Monthly Parquet export/restore |

## Schema

### Staging Layer (Raw JSON)
- `stg_markets_raw` - Market metadata from Gamma API
- `stg_prices_raw` - Hourly price history from CLOB API  
- `stg_trades_raw` - Trade data (Nov 2024 initially)

### Gold Layer (Snowflake Schema)

Storage-optimized with normalized lookup tables:

- **Lookup Tables** (few rows, eliminate repeated text)
  - `dim_category` - Category names (Politics, Sports, Crypto, etc.)
  - `dim_outcome` - Outcome names (YES=1, NO=2)

- **Dimension Tables** (use FK to lookups)
  - `dim_event` - Events with category_id FK
  - `dim_market` - Markets with event_id FK
  - `dim_token` - Tokens with market_id + outcome_id FKs

- **Fact Tables** (no dim_date FK, timestamps stored directly)
  - `fact_price_snapshot` - Hourly prices with snapshot_hour TIMESTAMP
  - `fact_trade` - Trades with trade_ts TIMESTAMP, side as TINYINT

## File Locations

```
VibeCoding/
├── PolyMarketScrapping/   # This repo (code only)
│   ├── scripts/           # Pipeline code (tracked)
│   │   ├── create_database.py
│   │   ├── api_client.py
│   │   ├── backfill_historical.py
│   │   ├── daily_scraper.py
│   │   ├── transform_gold.py
│   │   └── parquet_backup.py
│   ├── requirements.txt
│   └── README.md
│
└── PolyMarketData/        # Data directory (outside repo, auto-created)
    ├── polymarket.duckdb  # DuckDB database
    └── archive/           # Parquet backups
        └── gold/
            └── 2024-12/
                ├── dim_market.parquet
                └── fact_price_snapshot.parquet
```

## Data Retention

- **Staging**: Deleted 30 days after gold transformation
- **Gold**: Retained indefinitely in DuckDB
- **Archive**: Monthly Parquet backups (portable, compressed)

## Rate Limits

| API | Endpoint | Limit | Our Setting |
|-----|----------|-------|-------------|
| Gamma | /markets | 12.5/s | 10/s |
| CLOB | /prices-history | 10/s | 8/s |
| Data | /trades | 7.5/s | 6/s |

## Expansion Plan

1. **Phase 1** (Current): Nov 2024 trades only
2. **Phase 2**: Expand to all 2024 trades
3. **Phase 3**: Expand to all historical trades
