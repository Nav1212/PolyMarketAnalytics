# System Architecture Diagrams

## High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              POLYMARKET ANALYTICS                                │
│                         Distributed ETL Data Pipeline                            │
└─────────────────────────────────────────────────────────────────────────────────┘

                              EXTERNAL DATA SOURCES
┌─────────────────────────────────────────────────────────────────────────────────┐
│                                                                                  │
│   ┌───────────────────┐  ┌───────────────────┐  ┌───────────────────┐          │
│   │    Data API       │  │    CLOB API       │  │    Gamma API      │          │
│   │ data-api.poly...  │  │ clob.polymarket   │  │ gamma-api.poly... │          │
│   │                   │  │                   │  │                   │          │
│   │ • /markets        │  │ • /prices-history │  │ • /markets        │          │
│   │ • /trades         │  │ • /leaderboard    │  │ • /events         │          │
│   └─────────┬─────────┘  └─────────┬─────────┘  └─────────┬─────────┘          │
│             │                      │                      │                     │
└─────────────┼──────────────────────┼──────────────────────┼─────────────────────┘
              │                      │                      │
              ▼                      ▼                      ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                             FETCHER LAYER                                        │
│                        (fetcher/main.py orchestrator)                            │
│  ┌─────────────────────────────────────────────────────────────────────────┐    │
│  │                     FetcherCoordinator                                   │    │
│  │                  (coordination/coordinator.py)                           │    │
│  │                                                                          │    │
│  │  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐ ┌─────────────────┐    │    │
│  │  │   Market    │ │   Trade     │ │   Price     │ │   Leaderboard   │    │    │
│  │  │   Fetcher   │ │   Fetcher   │ │   Fetcher   │ │    Fetcher      │    │    │
│  │  │  (1 worker) │ │ (2 workers) │ │ (2 workers) │ │   (1 worker)    │    │    │
│  │  └──────┬──────┘ └──────┬──────┘ └──────┬──────┘ └────────┬────────┘    │    │
│  │         │               │               │                  │             │    │
│  └─────────┼───────────────┼───────────────┼──────────────────┼─────────────┘    │
│            │               │               │                  │                  │
│  ┌─────────▼───────────────▼───────────────▼──────────────────▼─────────────┐    │
│  │                      WorkerManager (Rate Limiting)                        │    │
│  │                    Token Bucket Algorithm per API                         │    │
│  │          Trade: 70 req/10s │ Market: 100 req/10s │ Price: 100 req/10s    │    │
│  └──────────────────────────────────────────────────────────────────────────┘    │
│                                                                                   │
│  ┌──────────────────────────────────────────────────────────────────────────┐    │
│  │                       CursorManager (Progress Tracking)                   │    │
│  │                     Persists state to cursor.json                         │    │
│  │                     Enables resumable execution                           │    │
│  └──────────────────────────────────────────────────────────────────────────┘    │
└──────────────────────────────────────────┬──────────────────────────────────────┘
                                           │
                                           ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           PERSISTENCE LAYER                                      │
│                                                                                  │
│  ┌──────────────────────────────────────────────────────────────────────────┐   │
│  │                        SwappableQueue                                     │   │
│  │              Non-blocking queue with atomic buffer swap                   │   │
│  │                  Threshold-triggered persistence                          │   │
│  └─────────────────────────────────┬────────────────────────────────────────┘   │
│                                    │                                             │
│                                    ▼                                             │
│  ┌──────────────────────────────────────────────────────────────────────────┐   │
│  │                       ParquetPersister                                    │   │
│  │               Daemon thread for Parquet file writing                      │   │
│  │              Hive partitioning: dt=YYYY-MM-DD/                            │   │
│  └──────────────────────────────────────────────────────────────────────────┘   │
└──────────────────────────────────────┬──────────────────────────────────────────┘
                                       │
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                            MEDALLION ARCHITECTURE                                │
│                                                                                  │
│  ┌────────────────────┐  ┌────────────────────┐  ┌────────────────────┐        │
│  │   BRONZE LAYER     │  │   SILVER LAYER     │  │    GOLD LAYER      │        │
│  │   (Raw Parquet)    │  │    (DuckDB)        │  │   (Analytics)      │        │
│  │                    │  │                    │  │                    │        │
│  │  data/             │  │  polymarket.duckdb │  │  • Aggregations    │        │
│  │  ├─ trades/        │  │                    │  │  • Daily summaries │        │
│  │  ├─ markets/       │  │  ┌──────────────┐  │  │  • Custom reports  │        │
│  │  ├─ prices/        │─▶│  │  MarketDim   │  │─▶│                    │        │
│  │  ├─ leaderboard/   │  │  │  TraderDim   │  │  │                    │        │
│  │  └─ gamma_*/       │  │  │  TradeFact   │  │  │                    │        │
│  │                    │  │  │  PriceFact   │  │  │                    │        │
│  │  Immutable, append │  │  └──────────────┘  │  │                    │        │
│  │  only, partitioned │  │  Normalized, SCD   │  │  Denormalized      │        │
│  └────────────────────┘  └────────────────────┘  └────────────────────┘        │
│                                    ▲                                             │
│                                    │                                             │
│                    ┌───────────────┴───────────────┐                            │
│                    │     INGESTION LAYER           │                            │
│                    │  (Ingestion/silver_loader.py) │                            │
│                    │   Bronze → Silver transforms   │                            │
│                    └───────────────────────────────┘                            │
└─────────────────────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           TAG MANAGER UI                                         │
│                      (Streamlit Application)                                     │
│                                                                                  │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐        │
│  │    Tags      │  │   Examples   │  │    Judge     │  │   History    │        │
│  │   Page       │  │    Page      │  │    Page      │  │    Page      │        │
│  │              │  │              │  │              │  │              │        │
│  │ Create/view  │  │ Add training │  │ Human review │  │ View/edit    │        │
│  │ market tags  │  │ examples     │  │ decisions    │  │ past results │        │
│  └──────────────┘  └──────────────┘  └──────────────┘  └──────────────┘        │
│                                                                                  │
│                    ┌───────────────────────────────┐                            │
│                    │      LLM Classification       │                            │
│                    │     (Ollama Integration)      │                            │
│                    │   Background batch processing  │                            │
│                    └───────────────────────────────┘                            │
└─────────────────────────────────────────────────────────────────────────────────┘
```

## Data Flow Diagram

```
                                    DATA FLOW
═══════════════════════════════════════════════════════════════════════════════════

    PHASE 1: Market Discovery
    ─────────────────────────

    ┌─────────────────┐     GET /markets      ┌─────────────────┐
    │  MarketFetcher  │ ◀────────────────────▶│    Data API     │
    │   (1 worker)    │                       └─────────────────┘
    └────────┬────────┘
             │
             │ Extracts & distributes
             ▼
    ┌────────────────────────────────────────────────────────────┐
    │                                                            │
    │  ┌─────────────┐  ┌─────────────┐  ┌─────────────────┐   │
    │  │  Markets    │  │  Tokens     │  │  condition_ids  │   │
    │  │   Queue     │  │   Queue     │  │  & token_ids    │   │
    │  └──────┬──────┘  └──────┬──────┘  └────────┬────────┘   │
    │         │                │                   │            │
    └─────────┼────────────────┼───────────────────┼────────────┘
              │                │                   │
              ▼                ▼                   ▼

    PHASE 2: Parallel Data Collection (after warmup delay)
    ──────────────────────────────────────────────────────

    ┌─────────────────┐                ┌─────────────────┐
    │  TradeFetcher   │                │  PriceFetcher   │
    │  (2 workers)    │                │  (2 workers)    │
    │                 │                │                 │
    │ GET /trades     │                │ GET /prices-    │
    │ ?market=...     │                │ history?token=  │
    └────────┬────────┘                └────────┬────────┘
             │                                  │
             │         ┌─────────────────┐     │
             │         │ LeaderboardFetch│     │
             │         │  (1 worker)     │     │
             │         │                 │     │
             │         │ GET /leaderboard│     │
             │         └────────┬────────┘     │
             │                  │              │
             ▼                  ▼              ▼
    ┌─────────────────────────────────────────────────────────────┐
    │                    SwappableQueues                          │
    │  ┌─────────────┐  ┌─────────────┐  ┌─────────────────┐     │
    │  │trade_queue  │  │leaderboard  │  │  price_queue    │     │
    │  │threshold:10K│  │queue: 5K    │  │  threshold:10K  │     │
    │  └──────┬──────┘  └──────┬──────┘  └────────┬────────┘     │
    └─────────┼────────────────┼──────────────────┼───────────────┘
              │                │                  │
              │                │                  │
              ▼                ▼                  ▼
    ┌─────────────────────────────────────────────────────────────┐
    │                  ParquetPersisters (Daemon Threads)          │
    │                                                              │
    │     When threshold reached → Atomic buffer swap → Write      │
    └─────────────────────────────────────────────────────────────┘
              │                │                  │
              ▼                ▼                  ▼
    ┌─────────────────────────────────────────────────────────────┐
    │                      BRONZE LAYER                            │
    │                                                              │
    │   data/trades/           data/leaderboard/    data/prices/  │
    │   └─dt=2024-12-26/       └─dt=2024-12-26/     └─dt=2024-12-│
    │     └─*.parquet            └─*.parquet          └─*.parquet │
    │                                                              │
    │              Hive-Partitioned Parquet Files                  │
    └─────────────────────────────────────────────────────────────┘
              │
              │  python -m Ingestion.silver_loader
              ▼
    ┌─────────────────────────────────────────────────────────────┐
    │                   TRANSFORMATION PHASE                       │
    │                                                              │
    │   ┌─────────────────────────────────────────────────────┐   │
    │   │  1. Load MarketDim (BLOCKING - runs first)          │   │
    │   │     markets/*.parquet → MarketDim table             │   │
    │   └─────────────────────────────────────────────────────┘   │
    │                          │                                   │
    │                          ▼                                   │
    │   ┌──────────────────────┴────────────────────────┐         │
    │   │           2. Load in PARALLEL                  │         │
    │   │  ┌────────────────┐    ┌────────────────────┐ │         │
    │   │  │ MarketTokenDim │    │     TraderDim      │ │         │
    │   │  │ Extract tokens │    │ Extract wallets    │ │         │
    │   │  └────────────────┘    └────────────────────┘ │         │
    │   └────────────────────────────────────────────────┘         │
    └─────────────────────────────────────────────────────────────┘
              │
              ▼
    ┌─────────────────────────────────────────────────────────────┐
    │                      SILVER LAYER                            │
    │                    polymarket.duckdb                         │
    │                                                              │
    │  ┌─────────────────────────────────────────────────────┐    │
    │  │                 DIMENSION TABLES                     │    │
    │  │  ┌─────────────┐ ┌───────────────┐ ┌─────────────┐  │    │
    │  │  │  MarketDim  │ │ MarketTokenDim│ │  TraderDim  │  │    │
    │  │  │             │ │               │ │             │  │    │
    │  │  │ market_id   │ │ token_id      │ │ trader_id   │  │    │
    │  │  │ external_id │ │ market_id(FK) │ │ wallet_addr │  │    │
    │  │  │ question    │ │ outcome       │ │             │  │    │
    │  │  │ volume      │ │ price         │ │             │  │    │
    │  │  │ active      │ │ winner        │ │             │  │    │
    │  │  └─────────────┘ └───────────────┘ └─────────────┘  │    │
    │  └─────────────────────────────────────────────────────┘    │
    │                                                              │
    │  ┌─────────────────────────────────────────────────────┐    │
    │  │                    FACT TABLES                       │    │
    │  │  ┌─────────────────────┐  ┌─────────────────────┐   │    │
    │  │  │     TradeFact       │  │   PriceHistoryFact  │   │    │
    │  │  │                     │  │                     │   │    │
    │  │  │ trade_id, token_id  │  │ id, token_id        │   │    │
    │  │  │ timestamp, price    │  │ timestamp, price    │   │    │
    │  │  │ size, side          │  │                     │   │    │
    │  │  │ maker_id, taker_id  │  │                     │   │    │
    │  │  └─────────────────────┘  └─────────────────────┘   │    │
    │  └─────────────────────────────────────────────────────┘    │
    └─────────────────────────────────────────────────────────────┘
```

## Component Interaction Diagram

```
                         COMPONENT INTERACTIONS
═══════════════════════════════════════════════════════════════════════════════════

┌──────────────────────────────────────────────────────────────────────────────────┐
│                              CLI Entry Point                                      │
│                           fetcher/main.py                                        │
│                                                                                   │
│   python fetcher/main.py --mode all --fresh --limit 100 --timeout 3600          │
└───────────────────────────────────────┬──────────────────────────────────────────┘
                                        │
                                        ▼
┌───────────────────────────────────────────────────────────────────────────────────┐
│                              Config (Singleton)                                   │
│                            fetcher/config.py                                      │
│                                                                                   │
│  ┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐│
│  │  RateLimits     │ │   Workers       │ │    Queues       │ │   OutputDirs    ││
│  │  Config         │ │   Config        │ │    Config       │ │    Config       ││
│  │                 │ │                 │ │                 │ │                 ││
│  │ trade: 70/10s   │ │ market: 3       │ │ trade: 10000    │ │ trades: data/   ││
│  │ market: 100/10s │ │ trade: 2        │ │ market: 1000    │ │ markets: data/  ││
│  │ price: 100/10s  │ │ price: 2        │ │ price: 10000    │ │ prices: data/   ││
│  │ leaderboard: 70 │ │ leaderboard: 1  │ │ leaderboard: 5K │ │                 ││
│  └─────────────────┘ └─────────────────┘ └─────────────────┘ └─────────────────┘│
└───────────────────────────────────────┬───────────────────────────────────────────┘
                                        │
                    ┌───────────────────┼───────────────────┐
                    │                   │                   │
                    ▼                   ▼                   ▼
┌───────────────────────┐ ┌─────────────────────┐ ┌─────────────────────────────────┐
│   WorkerManager       │ │   CursorManager     │ │    FetcherCoordinator           │
│   (Rate Limiting)     │ │   (State Tracking)  │ │    (Orchestration)              │
│                       │ │                     │ │                                 │
│ ┌───────────────────┐ │ │ ┌─────────────────┐ │ │  LoadOrder Enum:                │
│ │   TokenBucket     │ │ │ │  TradeCursor    │ │ │  ┌──────────────────────────┐  │
│ │                   │ │ │ │  market, offset │ │ │  │ 1. MarketFetcher         │  │
│ │ • tokens: float   │ │ │ │  pending_mkts   │ │ │  │    (PRIMARY - runs first)│  │
│ │ • capacity: int   │ │ │ └─────────────────┘ │ │  │                          │  │
│ │ • fill_rate: float│ │ │ ┌─────────────────┐ │ │  │ 2. TradeFetcher ─────┐   │  │
│ │ • last_time: ts   │ │ │ │  PriceCursor    │ │ │  │ 3. PriceFetcher ──┐  │   │  │
│ │                   │ │ │ │  token_id       │ │ │  │ 4. LeaderboardF ─┐│  │   │  │
│ │ acquire() → bool  │ │ │ │  start_ts       │ │ │  │                  ││  │   │  │
│ │ wait_for_token()  │ │ │ │  pending_tokens │ │ │  │    (PARALLEL)────┘└──┘   │  │
│ └───────────────────┘ │ │ └─────────────────┘ │ │  └──────────────────────────┘  │
│                       │ │                     │ │                                 │
│ Per-API buckets:      │ │ save_cursors()      │ │  Coordinates shutdown signals  │
│ • trade_bucket        │ │ load_cursors()      │ │  Manages worker lifecycles     │
│ • market_bucket       │ │ clear_cursors()     │ │                                 │
│ • price_bucket        │ │                     │ │                                 │
│ • leaderboard_bucket  │ │ File: cursor.json   │ │                                 │
└───────────┬───────────┘ └──────────┬──────────┘ └────────────────┬────────────────┘
            │                        │                              │
            │                        │                              │
            └────────────────────────┼──────────────────────────────┘
                                     │
                                     ▼
┌──────────────────────────────────────────────────────────────────────────────────┐
│                               FETCHER WORKERS                                     │
│                                                                                   │
│  ┌────────────────────────────────────────────────────────────────────────────┐  │
│  │                         MarketFetcher                                       │  │
│  │                    fetcher/workers/market_fetcher.py                        │  │
│  │                                                                             │  │
│  │  Input:  None (starts fresh or from cursor)                                 │  │
│  │  Output: markets → output_queue                                             │  │
│  │          tokens → market_token_queue                                        │  │
│  │          condition_ids → trade_market_queue (feeds TradeFetcher)            │  │
│  │          token_ids → price_token_queue (feeds PriceFetcher)                 │  │
│  │                                                                             │  │
│  │  API: GET https://data-api.polymarket.com/markets?cursor=...                │  │
│  └────────────────────────────────────────────────────────────────────────────┘  │
│                           │                                                       │
│         ┌─────────────────┼─────────────────┐                                    │
│         ▼                 ▼                 ▼                                    │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────┐                       │
│  │ TradeFetcher │  │ PriceFetcher │  │LeaderboardFetcher│                       │
│  │              │  │              │  │                  │                       │
│  │ Input:       │  │ Input:       │  │ Input:           │                       │
│  │ condition_ids│  │ token_ids    │  │ category/period  │                       │
│  │              │  │              │  │ combinations     │                       │
│  │ Output:      │  │ Output:      │  │                  │                       │
│  │ trade_queue  │  │ price_queue  │  │ Output:          │                       │
│  │              │  │              │  │ leaderboard_queue│                       │
│  └──────────────┘  └──────────────┘  └──────────────────┘                       │
└──────────────────────────────────────────────────────────────────────────────────┘
                                     │
                                     ▼
┌──────────────────────────────────────────────────────────────────────────────────┐
│                            PERSISTENCE COMPONENTS                                 │
│                                                                                   │
│  ┌────────────────────────────────────────────────────────────────────────────┐  │
│  │                          SwappableQueue                                     │  │
│  │                  fetcher/persistence/swappable_queue.py                     │  │
│  │                                                                             │  │
│  │   ┌─────────────────┐         ┌─────────────────┐                          │  │
│  │   │  Active Buffer  │ ──swap──▶│  Passive Buffer │                          │  │
│  │   │  (write target) │ ◀──────│  (read target)  │                          │  │
│  │   └─────────────────┘         └─────────────────┘                          │  │
│  │                                                                             │  │
│  │   • Thread-safe enqueue                                                     │  │
│  │   • Atomic buffer swap when threshold reached                               │  │
│  │   • Non-blocking for workers                                                │  │
│  └───────────────────────────────────────┬────────────────────────────────────┘  │
│                                          │                                        │
│                                          ▼                                        │
│  ┌────────────────────────────────────────────────────────────────────────────┐  │
│  │                         ParquetPersister                                    │  │
│  │                 fetcher/persistence/parquet_persister.py                    │  │
│  │                                                                             │  │
│  │   • Daemon thread (runs continuously)                                       │  │
│  │   • Monitors SwappableQueue                                                 │  │
│  │   • Writes to Parquet when buffer available                                 │  │
│  │   • Hive partitioning: data/{type}/dt=YYYY-MM-DD/batch_{n}.parquet         │  │
│  │   • Uses PyArrow for efficient columnar writes                              │  │
│  └────────────────────────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────────────────────┘


                         GRACEFUL SHUTDOWN SEQUENCE
═══════════════════════════════════════════════════════════════════════════════════

    User presses Ctrl+C
           │
           ▼
    ┌──────────────────┐
    │ signal_shutdown()│
    │ called           │
    └────────┬─────────┘
             │
             ▼
    ┌──────────────────┐     ┌──────────────────┐
    │ Send SENTINEL to │────▶│ Workers detect   │
    │ all input queues │     │ SENTINEL, exit   │
    └────────┬─────────┘     └────────┬─────────┘
             │                        │
             ▼                        ▼
    ┌──────────────────┐     ┌──────────────────┐
    │ CursorManager    │     │ Join all worker  │
    │ .save_cursors()  │     │ threads          │
    └────────┬─────────┘     └────────┬─────────┘
             │                        │
             ▼                        ▼
    ┌──────────────────┐     ┌──────────────────┐
    │ cursor.json      │     │ ParquetPersisters│
    │ written to disk  │     │ flush & stop     │
    └──────────────────┘     └──────────────────┘
             │                        │
             └───────────┬────────────┘
                         ▼
              ┌──────────────────────┐
              │ Safe to resume with  │
              │ python fetcher/main  │
              │ (reads cursor.json)  │
              └──────────────────────┘
```

## Tag Manager UI Architecture

```
                         TAG MANAGER STREAMLIT APP
═══════════════════════════════════════════════════════════════════════════════════

┌──────────────────────────────────────────────────────────────────────────────────┐
│                          streamlit run tag_manager/app.py                         │
└───────────────────────────────────────┬──────────────────────────────────────────┘
                                        │
                                        ▼
┌──────────────────────────────────────────────────────────────────────────────────┐
│                              STREAMLIT PAGES                                      │
│                                                                                   │
│  ┌────────────────┐  ┌────────────────┐  ┌────────────────┐  ┌────────────────┐ │
│  │   1_tags.py    │  │  2_examples.py │  │   3_judge.py   │  │  4_history.py  │ │
│  │                │  │                │  │                │  │                │ │
│  │ • View tags    │  │ • Add training │  │ • Human review │  │ • View past    │ │
│  │ • Create tags  │  │   examples     │  │   queue        │  │   classifications│
│  │ • Delete tags  │  │ • Link markets │  │ • Accept/reject│  │ • Edit results │ │
│  │ • Classification│  │   to tags      │  │   LLM results  │  │ • Filter/sort  │ │
│  │   count display│  │                │  │ • Select judges│  │                │ │
│  └───────┬────────┘  └───────┬────────┘  └───────┬────────┘  └───────┬────────┘ │
│          │                   │                   │                   │          │
└──────────┼───────────────────┼───────────────────┼───────────────────┼──────────┘
           │                   │                   │                   │
           └───────────────────┴───────────────────┴───────────────────┘
                                        │
                                        ▼
┌──────────────────────────────────────────────────────────────────────────────────┐
│                               SERVICES LAYER                                      │
│                          tag_manager/services/                                    │
│                                                                                   │
│  ┌─────────────────────┐  ┌─────────────────────┐  ┌─────────────────────────┐  │
│  │   tag_service.py    │  │  market_service.py  │  │    judge_service.py     │  │
│  │                     │  │                     │  │                         │  │
│  │ • get_all_tags()    │  │ • get_markets()     │  │ • get_pending_reviews() │  │
│  │ • create_tag()      │  │ • search_markets()  │  │ • submit_judgment()     │  │
│  │ • delete_tag()      │  │ • get_market_by_id()│  │ • get_judges()          │  │
│  │ • add_example()     │  │                     │  │ • select_judges()       │  │
│  └──────────┬──────────┘  └──────────┬──────────┘  └────────────┬────────────┘  │
│             │                        │                          │               │
│             └────────────────────────┼──────────────────────────┘               │
│                                      │                                           │
│  ┌───────────────────────────────────┴───────────────────────────────────────┐  │
│  │                        settings_service.py                                 │  │
│  │                                                                            │  │
│  │  • get_llm_settings()           • save_llm_settings()                     │  │
│  │  • get_available_models()       • test_ollama_connection()                │  │
│  └────────────────────────────────────────────────────────────────────────────┘  │
└───────────────────────────────────────┬──────────────────────────────────────────┘
                                        │
                    ┌───────────────────┴───────────────────┐
                    │                                       │
                    ▼                                       ▼
┌───────────────────────────────────────┐  ┌───────────────────────────────────────┐
│          DATABASE LAYER               │  │         LLM INTEGRATION               │
│     tag_manager/db/connection.py      │  │        tag_manager/llm/               │
│                                       │  │                                       │
│  ┌─────────────────────────────────┐  │  │  ┌─────────────────────────────────┐ │
│  │       polymarket.duckdb         │  │  │  │      Ollama Integration         │ │
│  │                                 │  │  │  │                                 │ │
│  │  • MarketDim                    │  │  │  │  • Auto-start Ollama if down    │ │
│  │  • MarketTokenDim               │  │  │  │  • Background classification    │ │
│  │  • Tags                         │  │  │  │  • Batch processing             │ │
│  │  • TagExamples                  │  │  │  │  • Model selection              │ │
│  │  • ClassificationHistory        │  │  │  │  • Prompt templates             │ │
│  │  • JudgeDecisions               │  │  │  │                                 │ │
│  │                                 │  │  │  └─────────────────────────────────┘ │
│  └─────────────────────────────────┘  │  │                                       │
└───────────────────────────────────────┘  └───────────────────────────────────────┘
```
