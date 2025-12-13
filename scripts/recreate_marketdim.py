"""Recreate MarketDim and dependent tables using ProofofConcept schema"""
import duckdb
import sys

print("Starting...", flush=True)

db_path = r"C:\Users\User\Desktop\VibeCoding\PolyMarketData\polymarket.duckdb"
conn = duckdb.connect(db_path)
print("Connected", flush=True)

# Drop tables in FK order
tables_to_drop = ['TradeFact', 'HourlyPriceFact', 'TokenDim', 'MarketDim']
print("Dropping tables...", flush=True)
for table in tables_to_drop:
    try:
        conn.execute(f'DROP TABLE IF EXISTS {table}')
        print(f"  Dropped {table}", flush=True)
    except Exception as e:
        print(f"  {table}: {e}", flush=True)

# Recreate MarketDim with ProofofConcept schema (includes VolumeNum)
print("\nCreating MarketDim...", flush=True)
conn.execute("""
    CREATE TABLE MarketDim (
        market_id       INTEGER PRIMARY KEY,
        external_id     TEXT NOT NULL UNIQUE,
        event_id        INTEGER REFERENCES EventDim(event_id),
        question        TEXT NOT NULL,
        active          BOOLEAN DEFAULT TRUE,
        end_date_iso    TIMESTAMP,
        start_date_iso  TIMESTAMP,
        VolumeNum       TEXT,  
        created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
""")
print("  MarketDim created", flush=True)

# Recreate TokenDim
print("Creating TokenDim...", flush=True)
conn.execute("""
    CREATE TABLE TokenDim (
        token_id        INTEGER PRIMARY KEY,
        token_address   TEXT NOT NULL UNIQUE,
        market_id       INTEGER REFERENCES MarketDim(market_id),
        outcome         BIT,
        created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
""")
print("  TokenDim created", flush=True)

# Recreate HourlyPriceFact
print("Creating HourlyPriceFact...", flush=True)
conn.execute("""
    CREATE TABLE HourlyPriceFact (
        token_id        INTEGER REFERENCES TokenDim(token_id),
        timestamp       TIMESTAMP NOT NULL,
        price           REAL NOT NULL,
        PRIMARY KEY (token_id, timestamp)
    )
""")
print("  HourlyPriceFact created", flush=True)

# Recreate TradeFact
print("Creating TradeFact...", flush=True)
conn.execute("""
    CREATE TABLE TradeFact (
        trade_id        BIGINT PRIMARY KEY,
        token_id        INTEGER REFERENCES TokenDim(token_id),
        timestamp       TIMESTAMP NOT NULL,
        side            BIT,
        price           REAL NOT NULL,
        size            REAL NOT NULL,
        maker_id        INTEGER REFERENCES TraderDim(trader_id),
        taker_id        INTEGER REFERENCES TraderDim(trader_id)
    )
""")
print("  TradeFact created", flush=True)

# Show schema
print("\nMarketDim schema:", flush=True)
for row in conn.execute('DESCRIBE MarketDim').fetchall():
    print(f"  {row[0]}: {row[1]}", flush=True)

conn.close()
print("\nDone", flush=True)
