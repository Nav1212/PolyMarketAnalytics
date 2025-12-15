import duckdb
import sys

print("Starting...", flush=True)
sys.stdout.flush()

db_path = r"C:\Users\User\Desktop\VibeCoding\PolyMarketData\polymarket.duckdb"
print(f"Connecting to {db_path}", flush=True)

conn = duckdb.connect(db_path)
print("Connected", flush=True)

print("Deleting TradeFact...", flush=True)
conn.execute('DELETE FROM TradeFact')
print("TradeFact deleted", flush=True)

print("Deleting HourlyPriceFact...", flush=True)  
conn.execute('DELETE FROM HourlyPriceFact')
print("HourlyPriceFact deleted", flush=True)

print("Checking TokenDim count...", flush=True)
count = conn.execute('SELECT COUNT(*) FROM TokenDim').fetchone()[0]
print(f"TokenDim has {count} rows", flush=True)

print("Deleting TokenDim in batches...", flush=True)
while True:
    result = conn.execute('DELETE FROM TokenDim WHERE token_id IN (SELECT token_id FROM TokenDim LIMIT 10000)')
    deleted = result.fetchone()
    remaining = conn.execute('SELECT COUNT(*) FROM TokenDim').fetchone()[0]
    print(f"  Remaining: {remaining}", flush=True)
    if remaining == 0:
        break
print("TokenDim deleted", flush=True)

print("Deleting MarketDim...", flush=True)
conn.execute('DELETE FROM MarketDim')
print("MarketDim deleted", flush=True)

conn.close()
print("Done", flush=True)
