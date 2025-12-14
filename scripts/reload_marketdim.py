"""Clear and reload MarketDim from stg_markets_raw"""
import duckdb

db_path = r"C:\Users\User\Desktop\VibeCoding\PolyMarketData\polymarket.duckdb"
conn = duckdb.connect(db_path)

print('Connected to database')

# Clear tables in FK order
print('Clearing tables...')
try:
    conn.execute('DELETE FROM TradeFact')
    print('  TradeFact cleared')
except Exception as e:
    print(f'  TradeFact: {e}')

try:
    conn.execute('DELETE FROM HourlyPriceFact')
    print('  HourlyPriceFact cleared')
except Exception as e:
    print(f'  HourlyPriceFact: {e}')

try:
    conn.execute('DELETE FROM TokenDim')
    print('  TokenDim cleared')
except Exception as e:
    print(f'  TokenDim: {e}')
    import traceback
    traceback.print_exc()

# Clear MarketDim
try:
    conn.execute('DELETE FROM MarketDim')
    print('  MarketDim cleared')
except Exception as e:
    print(f'  MarketDim: {e}')

# Check staging table
print('\nChecking stg_markets_raw...')
try:
    cols = conn.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'stg_markets_raw'").fetchall()
    print(f'  Columns: {[c[0] for c in cols]}')
    count = conn.execute('SELECT COUNT(*) FROM stg_markets_raw').fetchone()[0]
    print(f'  Rows: {count}')
except Exception as e:
    print(f'  Error: {e}')

# Check MarketDim schema
print('\nChecking MarketDim schema...')
try:
    cols = conn.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'MarketDim'").fetchall()
    print(f'  Columns: {[c[0] for c in cols]}')
except Exception as e:
    print(f'  Error: {e}')

# Reload from stg_markets_raw (parsing raw_json)
print('\nReloading MarketDim from stg_markets_raw...')
try:
    max_id = conn.execute("SELECT COALESCE(MAX(market_id), 0) FROM MarketDim").fetchone()[0]
    
    conn.execute("""
        INSERT INTO MarketDim (market_id, external_id, event_id, question, active, start_date_iso, end_date_iso, VolumeNum, Outcome)
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
            json_extract_string(s.raw_json, '$.volume') as VolumeNum,
            JSON_EXTRACT_string(s.raw_json, '$.outcomes') as Outcome
        FROM stg_markets_raw s
        LEFT JOIN EventDim e ON e.slug = COALESCE(
            json_extract_string(s.raw_json, '$.event.slug'),
            json_extract_string(s.raw_json, '$.eventSlug'),
            'unknown-' || s.condition_id
        )
    """, [max_id])
    
    count = conn.execute('SELECT COUNT(*) FROM MarketDim').fetchone()[0]
    print(f'  Rows in MarketDim: {count}')
except Exception as e:
    print(f'  Error: {e}')

conn.close()
print('\nDone')
