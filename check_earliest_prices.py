"""
Check earliest possible prices using market start dates.
"""
import duckdb
import httpx
from datetime import datetime

def main():
    # First, let's check what data we have
    conn = duckdb.connect(':memory:')
    
    print("=" * 70)
    print("CHECKING DATA AVAILABILITY")
    print("=" * 70)
    
    # Check markets
    markets = conn.execute('''
        SELECT condition_Id, game_start_time, end_date_iso, question
        FROM read_parquet('fetcher/data/markets/**/*.parquet', hive_partitioning=true)
        LIMIT 5
    ''').fetchall()
    print(f"\nMarkets found: {len(markets)}")
    for m in markets[:2]:
        cond_id = m[0] if m[0] else 'NULL'
        print(f"  condition_Id: {cond_id[:30] if isinstance(cond_id, str) else cond_id}...")
        print(f"  game_start_time: {m[1]}")
        print(f"  end_date_iso: {m[2]}")
        print()
    
    # Check market tokens
    tokens = conn.execute('''
        SELECT condition_Id, token_id
        FROM read_parquet('fetcher/data/market_tokens/**/*.parquet', hive_partitioning=true)
        LIMIT 5
    ''').fetchall()
    print(f"Market tokens found: {len(tokens)}")
    for t in tokens[:2]:
        print(f"  condition_Id: {t[0][:30] if t[0] else 'NULL'}...")
        print(f"  token_id: {t[1][:30]}...")
        print()
    
    # Try joining - check if condition_Id matches
    print("Attempting join...")
    result = conn.execute('''
        SELECT 
            mt.token_id,
            m.game_start_time,
            m.end_date_iso,
            m.condition_Id as market_cond,
            mt.condition_Id as token_cond
        FROM read_parquet('fetcher/data/market_tokens/**/*.parquet', hive_partitioning=true) mt
        LEFT JOIN read_parquet('fetcher/data/markets/**/*.parquet', hive_partitioning=true) m
            ON mt.condition_Id = m.condition_Id
        LIMIT 5
    ''').fetchall()
    
    print(f"Join result: {len(result)} rows")
    for r in result[:2]:
        print(f"  token: {r[0][:30]}...")
        print(f"  game_start_time: {r[1]}")
        print(f"  market_cond: {r[3][:20] if r[3] else 'NULL'}...")
        print(f"  token_cond: {r[4][:20] if r[4] else 'NULL'}...")
        print()
    
    conn.close()
    
    print("\n" + "=" * 70)
    print("TESTING PRICE API WITH SAMPLE TOKEN")
    print("=" * 70)
    
    # Just pick a token and test various time ranges
    if tokens:
        token_id = tokens[0][1]
        print(f"\nUsing token: {token_id[:50]}...")
        
        # Try different time ranges to find when data exists
        now = int(datetime.now().timestamp())
        
        test_ranges = [
            ("Last 1 hour", now - 3600, now),
            ("Last 24 hours", now - 86400, now),
            ("Dec 1-2, 2025", int(datetime(2025, 12, 1).timestamp()), int(datetime(2025, 12, 2).timestamp())),
            ("Nov 1-2, 2025", int(datetime(2025, 11, 1).timestamp()), int(datetime(2025, 11, 2).timestamp())),
            ("Oct 1-2, 2025", int(datetime(2025, 10, 1).timestamp()), int(datetime(2025, 10, 2).timestamp())),
        ]
        
        for name, start_ts, end_ts in test_ranges:
            try:
                response = httpx.get(
                    'https://clob.polymarket.com/prices-history',
                    params={'market': token_id, 'startTs': start_ts, 'endTs': end_ts, 'fidelity': 60},
                    timeout=10
                )
                data = response.json()
                history = data.get('history', [])
                print(f"  {name}: {len(history)} price points")
                if history:
                    first = history[0]
                    print(f"    First: t={datetime.fromtimestamp(first.get('t', 0))}, p={first.get('p')}")
                    break  # Found data, stop searching
            except Exception as e:
                print(f"  {name}: Error - {e}")
    
    # Now let's find a token with ACTUAL price data - use DuckDB silver layer
    print("\n" + "=" * 70)
    print("FINDING TOKENS WITH PRICE DATA (from DuckDB)")
    print("=" * 70)
    
    # Use the main database which has better data
    try:
        db_conn = duckdb.connect('C:/Users/User/Desktop/VibeCoding/PolyMarketData/polymarket.duckdb', read_only=True)
        
        # Get active markets with token info
        market_info = db_conn.execute('''
            SELECT external_id, end_date_iso, question, VolumeNum
            FROM MarketDim
            WHERE end_date_iso > '2025-12-01'
            AND VolumeNum != '0'
            ORDER BY CAST(REPLACE(VolumeNum, ',', '') AS DOUBLE) DESC
            LIMIT 5
        ''').fetchall()
        
        print(f"Found {len(market_info)} active markets with volume")
        for m in market_info[:2]:
            print(f"  {m[0][:30]}... vol={m[3]} ends={m[1][:10]}")
        
        db_conn.close()
    except Exception as e:
        print(f"DuckDB error: {e}")
        market_info = []
    
    # Test some known tokens directly via API
    print("\nTesting known active tokens with wider time ranges...")
    
    # These are tokens we found had data in previous tests
    known_tokens = [
        "96379884029689670451952458845117392813277706269906082414099623746046014652168",
        "21742633143463906290569050155826241533067272736897614950488156847949938836455",
    ]
    
    now = int(datetime.now().timestamp())
    week_ago = now - (7 * 86400)  # Try 7 days
    
    tokens_with_data = []
    
    for token_id in known_tokens:
        # Try a 1-day chunk from a week ago (to avoid API limit)
        try:
            response = httpx.get(
                'https://clob.polymarket.com/prices-history',
                params={'market': token_id, 'startTs': week_ago, 'endTs': week_ago + 86400, 'fidelity': 60},
                timeout=10
            )
            data = response.json()
            history = data.get('history', [])
            if history:
                tokens_with_data.append((token_id, len(history), history[0]))
                print(f"✓ Token {token_id[:30]}... has {len(history)} price points (1 week ago)")
            else:
                # Try December 15
                dec15_start = int(datetime(2025, 12, 15).timestamp())
                response = httpx.get(
                    'https://clob.polymarket.com/prices-history',
                    params={'market': token_id, 'startTs': dec15_start, 'endTs': dec15_start + 86400, 'fidelity': 60},
                    timeout=10
                )
                data = response.json()
                history = data.get('history', [])
                if history:
                    tokens_with_data.append((token_id, len(history), history[0]))
                    print(f"✓ Token {token_id[:30]}... has {len(history)} price points (Dec 15)")
                else:
                    print(f"✗ Token {token_id[:30]}... no data")
        except Exception as e:
            print(f"  Token {token_id[:20]}... error: {e}")
    
    if tokens_with_data:
        print(f"\nFound {len(tokens_with_data)} tokens with price data!")
        
        # For the first token with data, find the EARLIEST price
        token_id = tokens_with_data[0][0]
        print(f"\n--- Finding EARLIEST price for token {token_id[:40]}... ---")
        
        # Check progressively earlier months to find where data starts
        test_months = [
            datetime(2025, 12, 1),
            datetime(2025, 11, 1),
            datetime(2025, 10, 1),
            datetime(2025, 9, 1),
            datetime(2025, 6, 1),
            datetime(2025, 3, 1),
            datetime(2025, 1, 1),
            datetime(2024, 10, 1),
            datetime(2024, 7, 1),
            datetime(2024, 4, 1),
            datetime(2024, 1, 1),
            datetime(2023, 10, 1),
            datetime(2023, 7, 1),
            datetime(2023, 1, 1),
        ]
        
        earliest_found = None
        earliest_date = None
        
        for month_start in test_months:
            start_ts = int(month_start.timestamp())
            end_ts = start_ts + 86400  # 1 day window
            
            try:
                response = httpx.get(
                    'https://clob.polymarket.com/prices-history',
                    params={'market': token_id, 'startTs': start_ts, 'endTs': end_ts, 'fidelity': 60},
                    timeout=5
                )
                data = response.json()
                history = data.get('history', [])
                status = f"{len(history)} pts" if history else "empty"
                print(f"  {month_start.strftime('%Y-%m')}: {status}")
                if history:
                    earliest_found = history[0]
                    earliest_date = month_start
            except Exception as e:
                print(f"  {month_start.strftime('%Y-%m')}: error - {e}")
        
        if earliest_found:
            print(f"\n*** EARLIEST PRICE DATA AVAILABLE ***")
            print(f"  Month with data: {earliest_date.strftime('%Y-%m')}")
            print(f"  First timestamp: {datetime.fromtimestamp(earliest_found.get('t', 0))}")
            print(f"  First price: {earliest_found.get('p')}")
        else:
            print("\nNo earlier data found - token may be very new")
    else:
        print("\nNo tokens with price data found!")
    
    # Also check some active markets that we know have data
    print("\n" + "=" * 70)
    print("CHECKING KNOWN ACTIVE TOKEN")
    print("=" * 70)
    
    # Get a token that had prices in previous test
    known_token = "9637988402968967045195245884511739281327"
    
    # Find full token ID
    conn = duckdb.connect(':memory:')
    result = conn.execute(f'''
        SELECT token_id, m.game_start_time, m.end_date_iso
        FROM read_parquet('fetcher/data/market_tokens/**/*.parquet', hive_partitioning=true) mt
        JOIN read_parquet('fetcher/data/markets/**/*.parquet', hive_partitioning=true) m
            ON mt.condition_Id = m.condition_Id
        WHERE mt.token_id LIKE '{known_token}%'
        LIMIT 1
    ''').fetchall()
    conn.close()
    
    if result:
        token_id, start, end = result[0]
        print(f"\nToken: {token_id[:50]}...")
        print(f"Start: {start}, End: {end}")
        
        if start:
            dt = datetime.fromisoformat(start.replace('Z', '+00:00'))
            start_ts = int(dt.timestamp())
            
            # Get first 24 hours
            end_ts = start_ts + (24 * 3600)
            print(f"\nFetching first 24 hours from market start...")
            print(f"Range: {datetime.fromtimestamp(start_ts)} to {datetime.fromtimestamp(end_ts)}")
            
            response = httpx.get(
                'https://clob.polymarket.com/prices-history',
                params={'market': token_id, 'startTs': start_ts, 'endTs': end_ts, 'fidelity': 60},
                timeout=10
            )
            data = response.json()
            history = data.get('history', [])
            print(f"Result: {len(history)} price points")
            
            if history:
                first = history[0]
                last = history[-1]
                print(f"First: t={datetime.fromtimestamp(first.get('t', 0))}, p={first.get('p')}")
                print(f"Last:  t={datetime.fromtimestamp(last.get('t', 0))}, p={last.get('p')}")

if __name__ == "__main__":
    main()
