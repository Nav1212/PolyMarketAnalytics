"""
Endpoint and Worker Testing Script
Tests each API endpoint, measures response times, and saves responses to separate text files.
"""

import os
import time
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Tuple

from config import get_config, Config
from worker_manager import WorkerManager
from market_fetcher import MarketFetcher
from trade_fetcher import TradeFetcher
from price_fetcher import PriceFetcher


# Output directory for test results
OUTPUT_DIR = Path("test_results")


def ensure_output_dir():
    """Create output directory if it doesn't exist."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def format_response(response: Any) -> str:
    """Format response data for saving to file."""
    if isinstance(response, (dict, list)):
        return json.dumps(response, indent=2, default=str)
    return str(response)


def save_response(filename: str, content: str, timing_info: Dict[str, float]):
    """Save response content and timing info to a file."""
    filepath = OUTPUT_DIR / filename
    
    header = f"""{'='*60}
ENDPOINT TEST RESULTS
Generated: {datetime.now().isoformat()}
{'='*60}

TIMING INFORMATION:
-------------------
Total Time: {timing_info.get('total_time', 0):.4f} seconds
Request Time: {timing_info.get('request_time', 0):.4f} seconds
Rate Limit Wait: {timing_info.get('rate_limit_wait', 0):.4f} seconds

RESPONSE DATA:
--------------
"""
    
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(header)
        f.write(content)
    
    print(f"  -> Saved to: {filepath}")


def test_market_endpoint() -> Tuple[Any, Dict[str, float]]:
    """
    Test the market fetcher endpoint.
    Returns response data and timing information.
    """
    print("\n" + "="*60)
    print("TESTING: Market Endpoint")
    print("="*60)
    
    config = get_config()
    worker_manager = WorkerManager(config=config)
    
    timing = {}
    total_start = time.time()
    
    with MarketFetcher(worker_manager=worker_manager, config=config) as fetcher:
        # Test fetching just one page of markets (for testing purposes)
        print("  Fetching markets (first batch)...")
        
        rate_limit_start = time.time()
        worker_manager.acquire_market(rate_limit_start)
        rate_limit_end = time.time()
        timing['rate_limit_wait'] = rate_limit_end - rate_limit_start
        
        request_start = time.time()
        try:
            # Fetch first batch using the client directly
            response = fetcher.client.get_markets(next_cursor="MA==")  # First page
            request_end = time.time()
            timing['request_time'] = request_end - request_start
            
            data = response
            markets = data.get("data", [])
            print(f"  Fetched {len(markets)} markets")
            
            # Save first 5 markets as sample
            sample = {
                "total_in_batch": len(markets),
                "sample_markets": markets[:5] if len(markets) > 5 else markets,
                "next_cursor": data.get("next_cursor")
            }
            
        except Exception as e:
            request_end = time.time()
            timing['request_time'] = request_end - request_start
            sample = {"error": str(e)}
            print(f"  Error: {e}")
    
    total_end = time.time()
    timing['total_time'] = total_end - total_start
    
    print(f"  Total time: {timing['total_time']:.4f}s")
    print(f"  Request time: {timing['request_time']:.4f}s")
    print(f"  Rate limit wait: {timing['rate_limit_wait']:.4f}s")
    
    return sample, timing


def test_trade_endpoint() -> Tuple[Any, Dict[str, float]]:
    """
    Test the trade fetcher endpoint.
    Returns response data and timing information.
    """
    print("\n" + "="*60)
    print("TESTING: Trade Endpoint")
    print("="*60)
    
    config = get_config()
    worker_manager = WorkerManager(config=config)
    
    timing = {}
    total_start = time.time()
    
    # Use a known active market for testing
    # You can replace this with any valid condition_id
    test_market = "0xbd31dc8a20211944f6b70f31557f1001557b59905b7738480ca09bd4532f84af"
    
    with TradeFetcher(worker_manager=worker_manager, config=config) as fetcher:
        print(f"  Fetching trades for market: {test_market[:20]}...")
        
        rate_limit_start = time.time()
        worker_manager.acquire_trade(rate_limit_start)
        rate_limit_end = time.time()
        timing['rate_limit_wait'] = rate_limit_end - rate_limit_start
        
        request_start = time.time()
        try:
            trades = fetcher.fetch_trades(
                market=test_market,
                limit=50
            )
            request_end = time.time()
            timing['request_time'] = request_end - request_start
            
            print(f"  Fetched {len(trades)} trades")
            
            # Save first 5 trades as sample
            sample = {
                "market": test_market,
                "total_trades": len(trades),
                "sample_trades": trades[:5] if len(trades) > 5 else trades
            }
            
        except Exception as e:
            request_end = time.time()
            timing['request_time'] = request_end - request_start
            sample = {"error": str(e), "market": test_market}
            print(f"  Error: {e}")
    
    total_end = time.time()
    timing['total_time'] = total_end - total_start
    
    print(f"  Total time: {timing['total_time']:.4f}s")
    print(f"  Request time: {timing['request_time']:.4f}s")
    print(f"  Rate limit wait: {timing['rate_limit_wait']:.4f}s")
    
    return sample, timing


def test_price_endpoint() -> Tuple[Any, Dict[str, float]]:
    """
    Test the price fetcher endpoint.
    Returns response data and timing information.
    """
    print("\n" + "="*60)
    print("TESTING: Price Endpoint")
    print("="*60)
    
    config = get_config()
    worker_manager = WorkerManager(config=config)
    
    timing = {}
    total_start = time.time()
    
    # Use a known token ID for testing
    # You can replace this with any valid token_id
    test_token = "21742633143463906290569050155826241533067272736897614950488156847949938836455"
    
    with PriceFetcher(worker_manager=worker_manager, config=config) as fetcher:
        print(f"  Fetching price history for token: {test_token[:20]}...")
        
        # Calculate time range (last 24 hours)
        end_ts = int(datetime.now().timestamp())
        start_ts = end_ts - (24 * 60 * 60)  # 24 hours ago
        
        rate_limit_start = time.time()
        worker_manager.acquire_price(rate_limit_start)
        rate_limit_end = time.time()
        timing['rate_limit_wait'] = rate_limit_end - rate_limit_start
        
        request_start = time.time()
        try:
            prices = fetcher.fetch_price_history(
                token_id=test_token,
                start_ts=start_ts,
                end_ts=end_ts,
                resolution="1h"
            )
            request_end = time.time()
            timing['request_time'] = request_end - request_start
            
            print(f"  Fetched {len(prices)} price points")
            
            # Save first 10 prices as sample
            sample = {
                "token_id": test_token,
                "start_ts": start_ts,
                "end_ts": end_ts,
                "resolution": "1h",
                "total_prices": len(prices),
                "sample_prices": prices[:10] if len(prices) > 10 else prices
            }
            
        except Exception as e:
            request_end = time.time()
            timing['request_time'] = request_end - request_start
            sample = {"error": str(e), "token_id": test_token}
            print(f"  Error: {e}")
    
    total_end = time.time()
    timing['total_time'] = total_end - total_start
    
    print(f"  Total time: {timing['total_time']:.4f}s")
    print(f"  Request time: {timing['request_time']:.4f}s")
    print(f"  Rate limit wait: {timing['rate_limit_wait']:.4f}s")
    
    return sample, timing


def test_leaderboard_endpoint() -> Tuple[Any, Dict[str, float]]:
    """
    Test the leaderboard endpoint.
    Returns response data and timing information.
    """
    print("\n" + "="*60)
    print("TESTING: Leaderboard Endpoint")
    print("="*60)
    
    config = get_config()
    worker_manager = WorkerManager(config=config)
    
    timing = {}
    total_start = time.time()
    
    # Use a known market for testing
    test_market = "0xbd31dc8a20211944f6b70f31557f1001557b59905b7738480ca09bd4532f84af"
    
    with TradeFetcher(worker_manager=worker_manager, config=config) as fetcher:
        print(f"  Fetching leaderboard for market: {test_market[:20]}...")
        
        rate_limit_start = time.time()
        # Note: leaderboard uses trade rate limit
        rate_limit_end = time.time()
        timing['rate_limit_wait'] = rate_limit_end - rate_limit_start
        
        request_start = time.time()
        try:
            # Get first batch from leaderboard generator
            leaderboard_gen = fetcher.fetch_leaderboard(
                market=test_market,
                category="all",
                timePeriod="all"
            )
            
            # Get first response only
            first_response = next(leaderboard_gen, None)
            request_end = time.time()
            timing['request_time'] = request_end - request_start
            
            if first_response is not None:
                data = first_response.json() if hasattr(first_response, 'json') else first_response
                print(f"  Fetched leaderboard response")
                
                sample = {
                    "market": test_market,
                    "response": data if isinstance(data, (dict, list)) else str(data)[:1000]
                }
            else:
                sample = {"market": test_market, "response": "No data returned"}
                
        except Exception as e:
            request_end = time.time()
            timing['request_time'] = request_end - request_start
            sample = {"error": str(e), "market": test_market}
            print(f"  Error: {e}")
    
    total_end = time.time()
    timing['total_time'] = total_end - total_start
    
    print(f"  Total time: {timing['total_time']:.4f}s")
    print(f"  Request time: {timing['request_time']:.4f}s")
    print(f"  Rate limit wait: {timing['rate_limit_wait']:.4f}s")
    
    return sample, timing


def generate_summary(results: Dict[str, Dict[str, Any]]):
    """Generate a summary of all test results."""
    print("\n" + "="*60)
    print("TEST SUMMARY")
    print("="*60)
    
    # Rate limits from config (10 second window)
    config = get_config()
    rate_limits = {
        'market': config.rate_limits.market,
        'trade': config.rate_limits.trade,
        'price': config.rate_limits.price,
        'leaderboard': config.rate_limits.trade  # leaderboard uses trade rate limit
    }
    window_seconds = config.rate_limits.window_seconds
    
    summary_lines = [
        f"Test Run: {datetime.now().isoformat()}",
        "",
        "TIMING SUMMARY:",
        "-" * 90,
        f"{'Endpoint':<15} {'Total (s)':<12} {'Request (s)':<12} {'Rate Limit (s)':<14} {'Req/10s (1 worker)':<18} {'Workers to Hit Limit':<20}",
        "-" * 90
    ]
    
    for name, data in results.items():
        timing = data.get('timing', {})
        request_time = timing.get('request_time', 0)
        
        # Calculate requests per 10 seconds for 1 worker
        if request_time > 0:
            requests_per_window = window_seconds / request_time
            rate_limit = rate_limits.get(name, 100)
            workers_needed = max(1, int(rate_limit / requests_per_window) + 1)
        else:
            requests_per_window = 0
            workers_needed = 0
        
        summary_lines.append(
            f"{name:<15} "
            f"{timing.get('total_time', 0):<12.4f} "
            f"{timing.get('request_time', 0):<12.4f} "
            f"{timing.get('rate_limit_wait', 0):<14.4f} "
            f"{requests_per_window:<18.1f} "
            f"{workers_needed:<20}"
        )
    
    summary_lines.extend([
        "-" * 90,
        "",
        f"RATE LIMIT CONFIG (per {window_seconds}s window):",
        "-" * 40,
        f"  Market:      {rate_limits['market']} requests",
        f"  Trade:       {rate_limits['trade']} requests",
        f"  Price:       {rate_limits['price']} requests",
        f"  Leaderboard: {rate_limits['leaderboard']} requests (uses trade limit)",
        "",
        f"CONFIGURED WORKERS:",
        "-" * 40,
        f"  Market:      {config.workers.market} workers",
        f"  Trade:       {config.workers.trade} workers",
        f"  Price:       {config.workers.price} workers",
        f"  Leaderboard: {config.workers.leaderboard} workers",
        "",
        "WORKER ESTIMATE EXPLANATION:",
        "-" * 40,
        "  'Req/10s (1 worker)' = How many requests 1 worker can make in 10 seconds",
        "  'Workers to Hit Limit' = Minimum workers needed to saturate the rate limit",
        "  Formula: ceil(rate_limit / requests_per_worker)",
        "",
        "STATUS:",
        "-" * 40
    ])
    
    for name, data in results.items():
        response = data.get('response', {})
        status = "✓ SUCCESS" if 'error' not in response else f"✗ FAILED: {response.get('error', 'Unknown error')[:50]}"
        summary_lines.append(f"{name:<20} {status}")
    
    summary = "\n".join(summary_lines)
    print(summary)
    
    # Save summary
    summary_path = OUTPUT_DIR / "test_summary.txt"
    with open(summary_path, 'w', encoding='utf-8') as f:
        f.write(summary)
    print(f"\nSummary saved to: {summary_path}")


def run_warmup():
    """Run a quick warmup to exclude compile/import time from measurements."""
    print("\n" + "-"*60)
    print("  WARMUP RUN (not measured)")
    print("-"*60)
    
    config = get_config()
    worker_manager = WorkerManager(config=config)
    
    # Quick warmup calls - just initialize and make one request each
    try:
        with MarketFetcher(worker_manager=worker_manager, config=config) as fetcher:
            fetcher.client.get_markets(next_cursor="MA==")
        print("  ✓ Market warmup complete")
    except Exception as e:
        print(f"  ✗ Market warmup failed: {e}")
    
    try:
        with TradeFetcher(worker_manager=worker_manager, config=config) as fetcher:
            fetcher.fetch_trades(
                market="0xbd31dc8a20211944f6b70f31557f1001557b59905b7738480ca09bd4532f84af",
                limit=10
            )
        print("  ✓ Trade warmup complete")
    except Exception as e:
        print(f"  ✗ Trade warmup failed: {e}")
    
    try:
        with PriceFetcher(worker_manager=worker_manager, config=config) as fetcher:
            end_ts = int(datetime.now().timestamp())
            start_ts = end_ts - 3600
            fetcher.fetch_price_history(
                token_id="21742633143463906290569050155826241533067272736897614950488156847949938836455",
                start_ts=start_ts,
                end_ts=end_ts,
                resolution="1h"
            )
        print("  ✓ Price warmup complete")
    except Exception as e:
        print(f"  ✗ Price warmup failed: {e}")
    
    print("  Warmup complete, starting measured run...\n")
    # Small delay to let rate limits reset
    time.sleep(1)


def main():
    """Run all endpoint tests."""
    print("\n" + "#"*60)
    print("#  POLYMARKET API ENDPOINT TESTER")
    print("#  Testing endpoints and measuring worker times")
    print("#"*60)
    
    ensure_output_dir()
    
    # Run warmup first to exclude compile/import time
    run_warmup()
    
    results = {}
    
    # Test Market Endpoint
    try:
        response, timing = test_market_endpoint()
        results['market'] = {'response': response, 'timing': timing}
        save_response("market_response.txt", format_response(response), timing)
    except Exception as e:
        print(f"  Market test failed: {e}")
        results['market'] = {'response': {'error': str(e)}, 'timing': {}}
    
    # Test Trade Endpoint
    try:
        response, timing = test_trade_endpoint()
        results['trade'] = {'response': response, 'timing': timing}
        save_response("trade_response.txt", format_response(response), timing)
    except Exception as e:
        print(f"  Trade test failed: {e}")
        results['trade'] = {'response': {'error': str(e)}, 'timing': {}}
    
    # Test Price Endpoint
    try:
        response, timing = test_price_endpoint()
        results['price'] = {'response': response, 'timing': timing}
        save_response("price_response.txt", format_response(response), timing)
    except Exception as e:
        print(f"  Price test failed: {e}")
        results['price'] = {'response': {'error': str(e)}, 'timing': {}}
    
    # Test Leaderboard Endpoint
    try:
        response, timing = test_leaderboard_endpoint()
        results['leaderboard'] = {'response': response, 'timing': timing}
        save_response("leaderboard_response.txt", format_response(response), timing)
    except Exception as e:
        print(f"  Leaderboard test failed: {e}")
        results['leaderboard'] = {'response': {'error': str(e)}, 'timing': {}}
    
    # Generate summary
    generate_summary(results)
    
    print("\n" + "#"*60)
    print("#  ALL TESTS COMPLETE")
    print(f"#  Results saved to: {OUTPUT_DIR.absolute()}")
    print("#"*60 + "\n")


if __name__ == "__main__":
    main()
