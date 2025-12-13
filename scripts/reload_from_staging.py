import duckdb
import sys

print("Starting reload...", flush=True)

db_path = r"C:\Users\User\Desktop\VibeCoding\PolyMarketData\polymarket.duckdb"
conn = duckdb.connect(db_path)
print("Connected", flush=True)

# Check stg_markets_raw
count = conn.execute('SELECT COUNT(*) FROM stg_markets_raw').fetchone()[0]
print(f"stg_markets_raw has {count} rows", flush=True)

# Check current MarketDim
count = conn.execute('SELECT COUNT(*) FROM MarketDim').fetchone()[0]
print(f"MarketDim has {count} rows", flush=True)

# Get max market_id
max_id = conn.execute("SELECT COALESCE(MAX(market_id), 0) FROM MarketDim").fetchone()[0]
print(f"Max market_id: {max_id}", flush=True)

# Insert into MarketDim from stg_markets_raw
print("Inserting into MarketDim...", flush=True)
conn.execute("""
    truncate table MarketDim;
    INSERT INTO MarketDim (market_id, external_id, event_id, question, active, start_date_iso, end_date_iso, VolumeNum)
    SELECT 
        ? + ROW_NUMBER() OVER (ORDER BY s.condition_id) as market_id,
        s.condition_id as external_id,
        e.event_id,
        COALESCE(
            json_extract_string(s.raw_json, '$.question'),
            json_extract_string(s.raw_json, '$.title'),
            'Unknown'
        ) as question,
        COALESCE(
            json_extract(s.raw_json, '$.active')::BOOLEAN,
            NOT json_extract(s.raw_json, '$.closed')::BOOLEAN,
            TRUE
        ) as active,
        TRY_CAST(
            json_extract_string(s.raw_json, '$.startDate') AS TIMESTAMP
        ) as start_date_iso,
        TRY_CAST(
            json_extract_string(s.raw_json, '$.endDate') AS TIMESTAMP
        ) as end_date_iso,
        json_extract_string(s.raw_json, '$.volumeNum') as VolumeNum
    FROM stg_markets_raw s
    LEFT JOIN EventDim e ON e.slug = COALESCE(
        json_extract_string(s.raw_json, '$.event.slug'),
        json_extract_string(s.raw_json, '$.eventSlug'),
        'unknown-' || s.condition_id
    )
""", [max_id])

count = conn.execute('SELECT COUNT(*) FROM MarketDim').fetchone()[0]
print(f"MarketDim now has {count} rows", flush=True)

# Show sample
print("\nSample data:", flush=True)
sample = conn.execute('SELECT market_id, external_id, question FROM MarketDim LIMIT 3').fetchall()
for row in sample:
    print(f"  {row[0]}: {row[2][:50]}...", flush=True)

conn.close()
print("\nDone", flush=True)
