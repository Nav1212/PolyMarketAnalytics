"""Quick API test to get tokens with price data."""
import httpx
from datetime import datetime
import json

# Get events with markets from Gamma API (better for recent data)
print("Fetching events from Gamma API...")
response = httpx.get(
    'https://gamma-api.polymarket.com/events',
    params={'limit': 20, 'active': True, 'closed': False},
    timeout=30
)
events = response.json()
print(f"Events found: {len(events)}")

# Collect all tokens from events
all_tokens = []
for event in events[:10]:
    markets = event.get('markets', [])
    for m in markets:
        tokens = m.get('clobTokenIds', '').split(',') if m.get('clobTokenIds') else []
        question = m.get('question', 'Unknown')[:40]
        for token_id in tokens:
            if token_id and token_id.strip():
                all_tokens.append((token_id.strip(), question))

print(f"Total tokens found: {len(all_tokens)}")

if markets:
    print("\nFirst market structure:")
    print(json.dumps(markets[0], indent=2)[:800])
    print("...")

# Find tokens with price data
print("\n" + "="*60)
print("TESTING TOKENS FOR PRICE DATA")
print("="*60)

now = int(datetime.now().timestamp())
day_ago = now - 86400

found_with_data = []

for m in markets[:5]:
    tokens = m.get('tokens', [])
    question = m.get('question', 'Unknown')[:40]
    
    for t in tokens[:2]:
        token_id = t.get('token_id')
        if not token_id:
            continue
            
        try:
            price_resp = httpx.get(
                'https://clob.polymarket.com/prices-history',
                params={'market': token_id, 'startTs': day_ago, 'endTs': now, 'fidelity': 60},
                timeout=10
            )
            history = price_resp.json().get('history', [])
            
            if history:
                print(f"✓ {question}...")
                print(f"  Token: {token_id[:40]}...")
                print(f"  Points: {len(history)}")
                found_with_data.append((token_id, history))
                break
        except Exception as e:
            print(f"  Error: {e}")
    
    if len(found_with_data) >= 2:
        break

# Now find earliest data for first token
if found_with_data:
    token_id = found_with_data[0][0]
    print(f"\n" + "="*60)
    print(f"FINDING EARLIEST PRICE FOR TOKEN")
    print(f"Token: {token_id}")
    print("="*60)
    
    test_dates = [
        datetime(2025, 12, 15),
        datetime(2025, 12, 1),
        datetime(2025, 11, 1),
        datetime(2025, 10, 1),
        datetime(2025, 7, 1),
        datetime(2025, 4, 1),
        datetime(2025, 1, 1),
        datetime(2024, 10, 1),
        datetime(2024, 7, 1),
        datetime(2024, 4, 1),
        datetime(2024, 1, 1),
        datetime(2023, 7, 1),
        datetime(2023, 1, 1),
    ]
    
    earliest_data = None
    earliest_month = None
    
    for d in test_dates:
        start_ts = int(d.timestamp())
        end_ts = start_ts + 86400
        
        try:
            resp = httpx.get(
                'https://clob.polymarket.com/prices-history',
                params={'market': token_id, 'startTs': start_ts, 'endTs': end_ts, 'fidelity': 60},
                timeout=5
            )
            history = resp.json().get('history', [])
            status = f"{len(history)} pts" if history else "empty"
            print(f"  {d.strftime('%Y-%m-%d')}: {status}")
            
            if history:
                earliest_data = history
                earliest_month = d
        except Exception as e:
            print(f"  {d.strftime('%Y-%m-%d')}: error")
    
    if earliest_data:
        print(f"\n*** EARLIEST DATA FOUND ***")
        print(f"  Date: {earliest_month.strftime('%Y-%m-%d')}")
        print(f"  First point: t={datetime.fromtimestamp(earliest_data[0].get('t', 0))}, p={earliest_data[0].get('p')}")
else:
    print("\nNo tokens with price data found!")
