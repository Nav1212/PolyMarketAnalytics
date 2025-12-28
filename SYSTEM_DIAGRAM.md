# PolyMarket Data Pipeline - System Architecture

## High-Level Overview

```
+-----------------------------------------------------------------------------------+
|                           POLYMARKET DATA PIPELINE                                 |
+-----------------------------------------------------------------------------------+

                              EXTERNAL APIs (Data Sources)
    +------------------+    +------------------+    +----------------------+
    |   Data API       |    |    CLOB API      |    |     Gamma API        |
    | data-api.        |    | clob.            |    | gamma-api.           |
    | polymarket.com   |    | polymarket.com   |    | polymarket.com       |
    +--------+---------+    +--------+---------+    +----------+-----------+
             |                       |                         |
             | /markets              | /prices-history         | /markets
             | /trades               | /leaderboard            |
             v                       v                         v
+-----------------------------------------------------------------------------------+
|                              FETCHER LAYER                                         |
|  +-----------------------------------------------------------------------------+  |
|  |                        FetcherCoordinator                                   |  |
|  |  - Orchestrates all fetchers with load ordering                             |  |
|  |  - Manages queue wiring between fetchers                                    |  |
|  |  - Handles cursor persistence for resumable fetching                        |  |
|  +-----------------------------------------------------------------------------+  |
|                                      |                                            |
|     +----------------+---------------+---------------+----------------+           |
|     |                |               |               |                |           |
|     v                v               v               v                v           |
| +--------+     +----------+    +----------+   +-------------+  +-------------+    |
| | Market |     |  Trade   |    |  Price   |   | Leaderboard |  |   Gamma     |    |
| | Fetcher|---->| Fetcher  |    | Fetcher  |   |   Fetcher   |  |   Market    |    |
| | (1 wkr)|     | (2 wkrs) |    | (2 wkrs) |   |  (1 wkr)    |  |   Fetcher   |    |
| +---+----+     +----+-----+    +----+-----+   +------+------+  +------+------+    |
|     |               |               |               |                |            |
|     | condition_ids | token_ids     |               |                |            |
|     +-------+-------+               |               |                |            |
|             |                       |               |                |            |
+-----------------------------------------------------------------------------------+
              |                       |               |                |
              v                       v               v                v
+-----------------------------------------------------------------------------------+
|                           RATE LIMITING LAYER                                      |
|  +-----------------------------------------------------------------------------+  |
|  |                         WorkerManager                                        |  |
|  |  +------------+  +------------+  +------------+  +---------------+           |  |
|  |  | Trade      |  | Market     |  | Price      |  | Leaderboard   |  | Gamma      |           |  |
|  |  | Bucket     |  | Bucket     |  | Bucket     |  | Bucket        |  | Bucket |           |  |
|  |  | 70/10s     |  | 100/10s    |  | 100/10s    |  | 70/10s        |  | 100/10s|           |  |
|  |  +------------+  +------------+  +------------+  +---------------+  +-------+           |  |
|  |  Token Bucket Algorithm - Thread-safe rate limiting per API                  |  |
|  +-----------------------------------------------------------------------------+  |
+-----------------------------------------------------------------------------------+
              |                       |               |                |
              v                       v               v                v
+-----------------------------------------------------------------------------------+
|                           QUEUE & PERSISTENCE LAYER                                |
|                                                                                    |
|  +------------------------+          +----------------------------------+          |
|  |    SwappableQueue      |          |       ParquetPersister           |          |
|  | - Non-blocking queue   |  swap    | - Daemon thread monitors queue   |          |
|  | - Atomic buffer swap   |--------->| - Writes batches when threshold  |          |
|  | - Threshold-triggered  |          | - Hive partitioning (dt=YYYY-MM) |          |
|  +------------------------+          +----------------------------------+          |
|                                                                                    |
|  Queues:                             Thresholds:                                   |
|  - trade_output_queue                - trades: 10,000 items                        |
|  - market_output_queue               - markets: 10,000 items                       |
|  - market_token_output_queue         - market_tokens: 5,000 items                  |
|  - price_output_queue                - prices: 10,000 items                        |
|  - leaderboard_output_queue          - leaderboard: 5,000 items                    |
|  - gamma_market_output_queue         - gamma_markets: 1,000 items                  |
|  - gamma_event_output_queue          - gamma_events: 1,000 items                   |
|  - gamma_category_output_queue       - gamma_categories: 1,000 items               |
+-----------------------------------------------------------------------------------+
                                       |
                                       v
+-----------------------------------------------------------------------------------+
|                              STORAGE LAYER                                         |
|                                                                                    |
|  +---------------------------+    +---------------------------+                    |
|  |      BRONZE LAYER         |    |      SILVER LAYER         |                    |
|  |   (Raw Parquet Files)     |    |       (DuckDB)            |                    |
|  +---------------------------+    +---------------------------+                    |
|  |                           |    |                           |                    |
|  | data/                     |    | polymarket.duckdb         |                    |
|  |  +-- trades/              |    |                           |                    |
|  |  |   +-- dt=2024-12-26/   |--->| Dimension Tables:         |                    |
|  |  |       +-- *.parquet    |    |  - MarketDim              |                    |
|  |  +-- markets/             |    |  - MarketTokenDim         |                    |
|  |  |   +-- dt=2024-12-26/   |    |  - TraderDim              |                    |
|  |  +-- market_tokens/       |    |  - EventDim               |                    |
|  |  +-- prices/              |    |                           |                    |
|  |  +-- leaderboard/         |    | Fact Tables:              |                    |
|  |  +-- gamma_markets/       |    |  - TradeFact              |                    |
|  |  +-- gamma_events/        |    |  - PriceHistoryFact       |                    |
|  |  +-- gamma_categories/    |    |                           |                    |
|  +---------------------------+    +---------------------------+                    |
|                                                  |                                 |
|                                                  v                                 |
|                                   +---------------------------+                    |
|                                   |      GOLD LAYER           |                    |
|                                   |   (Aggregated Analytics)  |                    |
|                                   +---------------------------+                    |
|                                   | - Daily scraper results   |                    |
|                                   | - Historical backfill     |                    |
|                                   | - Database statistics     |                    |
|                                   +---------------------------+                    |
+-----------------------------------------------------------------------------------+
                                       |
                                       v
+-----------------------------------------------------------------------------------+
|                           CURSOR MANAGEMENT                                        |
|  +-----------------------------------------------------------------------------+  |
|  |                         CursorManager                                        |  |
|  |  - Saves progress to cursor.json on shutdown/interrupt                       |  |
|  |  - Resumes from last position on startup                                     |  |
|  |  - Supports --fresh flag for clean start                                     |  |
|  +-----------------------------------------------------------------------------+  |
|  |  Cursor Types:                                                               |  |
|  |  +----------------+  +----------------+  +------------------+                 |  |
|  |  | TradeCursor    |  | PriceCursor    |  | LeaderboardCursor|                 |  |
|  |  | - market       |  | - token_id     |  | - category_index |                 |  |
|  |  | - offset       |  | - start_ts     |  | - period_index   |                 |  |
|  |  | - pending_mkts |  | - end_ts       |  | - offset         |                 |  |
|  |  +----------------+  +----------------+  +------------------+                 |  |
|  +-----------------------------------------------------------------------------+  |
+-----------------------------------------------------------------------------------+
```

## Data Flow Diagram

```
                                    START
                                      |
                                      v
                    +----------------------------------+
                    |        MarketFetcher             |
                    |   GET /markets?cursor=...        |
                    +----------------------------------+
                                      |
              +-----------------------+-----------------------+
              |                       |                       |
              v                       v                       v
    +------------------+    +------------------+    +------------------+
    | Extract Markets  |    | Extract Tokens   |    | Extract IDs      |
    | -> market_queue  |    | -> token_queue   |    | -> trade_queue   |
    +------------------+    +------------------+    +------------------+
              |                       |                       |
              v                       v                       v
    +------------------+    +------------------+    +------------------+
    |  MarketPersister |    |  PriceFetcher    |    |  TradeFetcher    |
    |  -> parquet      |    | GET /prices-hist |    | GET /trades      |
    +------------------+    +------------------+    +------------------+
                                      |                       |
                                      v                       v
                            +------------------+    +------------------+
                            |  PricePersister  |    |  TradePersister  |
                            |  -> parquet      |    |  -> parquet      |
                            +------------------+    +------------------+
                                      |                       |
                                      +-----------+-----------+
                                                  |
                                                  v
                            +----------------------------------+
                            |         BRONZE LAYER             |
                            |      (Parquet Files)             |
                            +----------------------------------+
                                                  |
                                                  v
                            +----------------------------------+
                            |    Ingestion Scripts             |
                            |  (reload_from_staging.py, etc.)  |
                            +----------------------------------+
                                                  |
                                                  v
                            +----------------------------------+
                            |         SILVER LAYER             |
                            |   (DuckDB - polymarket.duckdb)   |
                            +----------------------------------+
                                                  |
                                                  v
                            +----------------------------------+
                            |         GOLD LAYER               |
                            |    (Analytics & Reports)         |
                            +----------------------------------+
```

## Component Details

### External APIs

| API | Base URL | Endpoints | Rate Limit |
|-----|----------|-----------|------------|
| **Data API** | https://data-api.polymarket.com | `/markets`, `/trades` | 100 req/10s |
| **CLOB API** | https://clob.polymarket.com | `/prices-history`, `/leaderboard` | 70-100 req/10s |
| **Gamma API** | https://gamma-api.polymarket.com | `/markets` | 100 req/10s |

### Fetcher Components

| Fetcher | Workers | Input | Output | Description |
|---------|---------|-------|--------|-------------|
| **MarketFetcher** | 1 | API cursor | Markets, Tokens, IDs | Fetches all markets, feeds downstream |
| **TradeFetcher** | 2 | condition_ids | Trades | Fetches trades per market |
| **PriceFetcher** | 2 | token_ids | Price history | Fetches historical prices |
| **LeaderboardFetcher** | 1 | Categories | Rankings | Iterates all category/period combos |
| **GammaMarketFetcher** | 1 | None | Markets, Events, Categories | Extended market data |

### Data Schemas

```
TRADE:          proxyWallet, side, price, size, conditionId, timestamp, transactionHash, outcome, name
MARKET:         condition_Id, end_date_iso, description, question, maker_base_fee, fpmm, closed, active, volume
MARKET_TOKEN:   condition_Id, price, token_id, winner, outcome
PRICE:          timestamp, token_id, price
LEADERBOARD:    rank, proxyWallet, userName, xUsername, verifiedBadge, vol, pnl, profileImage
GAMMA_MARKET:   id, conditionId, question, slug, category, liquidity, volume, active, closed, outcomes...
```

### Database Schema (Silver Layer)

```sql
-- Dimension Tables
MarketDim:      market_id (PK), external_id, event_id (FK), question, active, start_date_iso, end_date_iso, VolumeNum
MarketTokenDim: token_id (PK), token_hash, market_id (FK), outcome, price, winner
TraderDim:      trader_id (PK), wallet_address
EventDim:       event_id (PK), slug, title

-- Fact Tables
TradeFact:         trade_id (PK), token_id (FK), timestamp, price, size, side, maker_id (FK), taker_id (FK)
PriceHistoryFact:  id (PK), token_id (FK), timestamp, price
```

## Execution Modes

```bash
# Full pipeline - all fetchers
python fetcher/main.py --mode all

# Specific fetchers
python fetcher/main.py --mode trades      # Only trades for inactive markets
python fetcher/main.py --mode markets     # Only market fetching
python fetcher/main.py --mode prices      # Only price fetching
python fetcher/main.py --mode leaderboard # Only leaderboard

# Options
python fetcher/main.py --fresh            # Ignore saved cursors, start fresh
python fetcher/main.py --limit 100        # Process only 100 markets
python fetcher/main.py --timeout 3600     # 1-hour timeout
```

## Key Design Patterns

1. **Token Bucket Rate Limiting** - Thread-safe per-API rate limiting
2. **Atomic Buffer Swap** - Non-blocking queue persistence via SwappableQueue
3. **Cursor-Based Resumption** - Save/restore progress on interruption
4. **Hive Partitioning** - Date-based parquet file organization (`dt=YYYY-MM-DD/`)
5. **Multi-threaded Workers** - Parallel fetching with configurable worker counts
6. **Medallion Architecture** - Bronze (raw) -> Silver (structured) -> Gold (analytics)
