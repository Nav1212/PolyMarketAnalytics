"""
Market Token Loader with Expanded Schema

This script fetches all markets and saves market tokens with the full schema including:
- condition_Id: The market's condition ID
- token_id: The unique token identifier
- price: Current token price
- winner: Whether this token won (for resolved markets)
- outcome: The outcome label (e.g., "Yes", "No", "Team A", etc.)

Usage:
    python -m scripts.load_market_tokens [--output-dir data/market_tokens] [--active-only]
"""

import argparse
import httpx
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional
import time


# Expanded Market Token Schema (includes outcome)
MARKET_TOKEN_EXPANDED_SCHEMA = pa.schema([
    ('condition_Id', pa.string()),
    ('token_id', pa.string()),
    ('price', pa.float64()),
    ('winner', pa.bool_()),
    ('outcome', pa.string()),
    # Additional useful fields
    ('question', pa.string()),  # Market question for context
    ('closed', pa.bool_()),     # Whether market is closed
])


class MarketTokenLoader:
    """Loads market tokens with expanded schema from Polymarket API."""
    
    CLOB_API_BASE = "https://clob.polymarket.com"
    
    def __init__(
        self,
        output_dir: str = "data/market_tokens",
        timeout: float = 30.0,
        active_only: bool = False
    ):
        """
        Initialize the loader.
        
        Args:
            output_dir: Directory to save parquet files
            timeout: Request timeout in seconds
            active_only: If True, only load tokens from active (non-closed) markets
        """
        self.output_dir = Path(output_dir)
        self.timeout = timeout
        self.active_only = active_only
        self.client = httpx.Client(timeout=timeout)
        
    def close(self):
        """Close HTTP client."""
        self.client.close()
        
    def __enter__(self):
        return self
        
    def __exit__(self, *args):
        self.close()
    
    def fetch_all_markets(self) -> List[Dict[str, Any]]:
        """Fetch all markets from the CLOB API with pagination."""
        all_markets = []
        next_cursor = None
        page = 0
        
        print("Fetching markets from CLOB API...")
        
        while True:
            page += 1
            params = {}
            if next_cursor:
                params["next_cursor"] = next_cursor
                
            response = self.client.get(
                f"{self.CLOB_API_BASE}/markets",
                params=params
            )
            response.raise_for_status()
            data = response.json()
            
            # Handle response format
            if isinstance(data, dict):
                markets = data.get("data", data.get("markets", []))
                next_cursor = data.get("next_cursor")
            else:
                markets = data
                next_cursor = None
            
            if not markets:
                break
                
            all_markets.extend(markets)
            print(f"  Page {page}: {len(markets)} markets (total: {len(all_markets)})")
            
            # Check for end of pagination
            if not next_cursor or next_cursor == "LTE=":
                break
                
            time.sleep(0.1)  # Rate limiting
            
        return all_markets
    
    def extract_tokens(self, markets: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Extract tokens from markets with expanded schema.
        
        Args:
            markets: List of market dictionaries from API
            
        Returns:
            List of token records with expanded schema
        """
        tokens = []
        skipped_closed = 0
        
        for market in markets:
            is_closed = market.get("closed", False)
            
            # Skip closed markets if active_only is set
            if self.active_only and is_closed:
                skipped_closed += 1
                continue
                
            condition_id = market.get("condition_id")
            question = market.get("question", "")
            market_tokens = market.get("tokens", [])
            
            for token in market_tokens:
                token_id = token.get("token_id")
                if not token_id:
                    continue
                    
                token_record = {
                    'condition_Id': condition_id,
                    'token_id': token_id,
                    'price': float(token.get('price', 0.0)) if token.get('price') else 0.0,
                    'winner': bool(token.get('winner', False)),
                    'outcome': token.get('outcome', ''),
                    'question': question[:200] if question else '',  # Truncate for storage
                    'closed': is_closed,
                }
                tokens.append(token_record)
        
        if self.active_only and skipped_closed > 0:
            print(f"  Skipped {skipped_closed} closed markets (active_only=True)")
            
        return tokens
    
    def save_to_parquet(
        self, 
        tokens: List[Dict[str, Any]], 
        partition_date: Optional[str] = None
    ) -> Path:
        """
        Save tokens to parquet file with date partitioning.
        
        Args:
            tokens: List of token records
            partition_date: Date string for partition (default: today)
            
        Returns:
            Path to saved file
        """
        if not partition_date:
            partition_date = datetime.now().strftime("%Y-%m-%d")
            
        # Create output directory
        output_path = self.output_dir / f"dt={partition_date}"
        output_path.mkdir(parents=True, exist_ok=True)
        
        # Create PyArrow table
        table = pa.Table.from_pylist(tokens, schema=MARKET_TOKEN_EXPANDED_SCHEMA)
        
        # Write to parquet
        file_path = output_path / "market_tokens.parquet"
        pq.write_table(table, file_path)
        
        return file_path
    
    def run(self, partition_date: Optional[str] = None) -> Dict[str, Any]:
        """
        Run the full load process.
        
        Args:
            partition_date: Optional date for partitioning
            
        Returns:
            Summary statistics
        """
        start_time = time.time()
        
        # Fetch markets
        markets = self.fetch_all_markets()
        
        # Count statistics
        open_markets = sum(1 for m in markets if not m.get("closed", False))
        closed_markets = len(markets) - open_markets
        
        print(f"\nMarket summary:")
        print(f"  Total markets: {len(markets)}")
        print(f"  Open markets: {open_markets}")
        print(f"  Closed markets: {closed_markets}")
        
        # Extract tokens
        print("\nExtracting tokens...")
        tokens = self.extract_tokens(markets)
        print(f"  Extracted {len(tokens)} tokens")
        
        # Save to parquet
        print("\nSaving to parquet...")
        file_path = self.save_to_parquet(tokens, partition_date)
        print(f"  Saved to: {file_path}")
        
        elapsed = time.time() - start_time
        
        summary = {
            "total_markets": len(markets),
            "open_markets": open_markets,
            "closed_markets": closed_markets,
            "total_tokens": len(tokens),
            "output_file": str(file_path),
            "elapsed_seconds": round(elapsed, 2)
        }
        
        print(f"\nCompleted in {elapsed:.2f} seconds")
        
        return summary


def main():
    parser = argparse.ArgumentParser(
        description="Load market tokens with expanded schema (includes outcome)"
    )
    parser.add_argument(
        "--output-dir", 
        default="data/market_tokens",
        help="Output directory for parquet files (default: data/market_tokens)"
    )
    parser.add_argument(
        "--active-only",
        action="store_true",
        help="Only load tokens from active (non-closed) markets"
    )
    parser.add_argument(
        "--date",
        default=None,
        help="Partition date (default: today, format: YYYY-MM-DD)"
    )
    
    args = parser.parse_args()
    
    print("=" * 60)
    print("Market Token Loader (Expanded Schema)")
    print("=" * 60)
    print(f"Output directory: {args.output_dir}")
    print(f"Active only: {args.active_only}")
    print(f"Partition date: {args.date or 'today'}")
    print()
    
    with MarketTokenLoader(
        output_dir=args.output_dir,
        active_only=args.active_only
    ) as loader:
        summary = loader.run(partition_date=args.date)
        
    print("\n" + "=" * 60)
    print("Summary")
    print("=" * 60)
    for key, value in summary.items():
        print(f"  {key}: {value}")


if __name__ == "__main__":
    main()
