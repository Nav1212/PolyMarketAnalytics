"""
PolyMarket Silver Layer Database Schema

This module defines the Silver Layer schema for DuckDB, providing:
1. Table creation DDL for all dimension and fact tables
2. ID mapping utilities for foreign key relationships
3. Schema validation and initialization functions

Schema Overview:
    MarketDim       - Market dimension (condition_id -> market_id)
    MarketTokenDim  - Token dimension with market FK
    TraderDim       - Trader/wallet dimension for FK normalization
    PriceHistoryFact - Price snapshots over time
    TradeFact       - Individual trade records

Storage Optimizations:
    - REAL (4 bytes) for prices instead of DOUBLE (8 bytes)
    - INTEGER FKs for trader wallets (saves ~76 bytes per trade)
    - Composite primary keys where appropriate
"""

import duckdb
from pathlib import Path
from typing import Optional, Dict, Tuple
from datetime import datetime

# Default database path
DEFAULT_SILVER_DB_PATH = Path(r"C:\Users\User\Desktop\VibeCoding\PolyMarketData\silver.duckdb")


# =============================================================================
# TABLE DEFINITIONS (DDL)
# =============================================================================

CREATE_MARKET_DIM = """
CREATE TABLE IF NOT EXISTS MarketDim (
    market_id       INTEGER PRIMARY KEY,
    condition_id    TEXT NOT NULL UNIQUE,
    question        TEXT,
    description     TEXT,
    start_dt        TIMESTAMP,
    end_dt          TIMESTAMP,
    volume          REAL,
    liquidity       REAL,
    active          BOOLEAN DEFAULT TRUE,
    closed          BOOLEAN DEFAULT FALSE,
    category        TEXT,
    tags            TEXT,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""

CREATE_MARKET_TOKEN_DIM = """
CREATE TABLE IF NOT EXISTS MarketTokenDim (
    token_id            INTEGER PRIMARY KEY,
    token_hash          TEXT NOT NULL UNIQUE,
    market_id           INTEGER NOT NULL REFERENCES MarketDim(market_id),
    outcome             TEXT,
    price               REAL,
    winner              BOOLEAN DEFAULT FALSE,
    ingestion_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
)
"""

CREATE_TRADER_DIM = """
CREATE TABLE IF NOT EXISTS TraderDim (
    trader_id       INTEGER PRIMARY KEY,
    wallet_address  TEXT NOT NULL UNIQUE,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""

CREATE_PRICE_HISTORY_FACT = """
CREATE TABLE IF NOT EXISTS PriceHistoryFact (
    id                  TEXT PRIMARY KEY,
    token_id            INTEGER NOT NULL REFERENCES MarketTokenDim(token_id),
    timestamp           TIMESTAMP NOT NULL,
    price               REAL NOT NULL,
    ingestion_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
)
"""

CREATE_TRADE_FACT = """
CREATE TABLE IF NOT EXISTS TradeFact (
    trade_id            TEXT PRIMARY KEY,
    token_id            INTEGER NOT NULL REFERENCES MarketTokenDim(token_id),
    timestamp           TIMESTAMP NOT NULL,
    price               REAL NOT NULL,
    size                REAL NOT NULL,
    side                TEXT,
    maker_id            INTEGER REFERENCES TraderDim(trader_id),
    taker_id            INTEGER REFERENCES TraderDim(trader_id),
    ingestion_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
)
"""

# Indexes for query performance
CREATE_INDEXES = """
CREATE INDEX IF NOT EXISTS idx_market_token_market ON MarketTokenDim(market_id);
CREATE INDEX IF NOT EXISTS idx_price_token ON PriceHistoryFact(token_id);
CREATE INDEX IF NOT EXISTS idx_price_timestamp ON PriceHistoryFact(timestamp);
CREATE INDEX IF NOT EXISTS idx_trade_token ON TradeFact(token_id);
CREATE INDEX IF NOT EXISTS idx_trade_timestamp ON TradeFact(timestamp);
CREATE INDEX IF NOT EXISTS idx_trade_maker ON TradeFact(maker_id);
CREATE INDEX IF NOT EXISTS idx_trade_taker ON TradeFact(taker_id);
"""


# =============================================================================
# USER-DEFINED TAGS TABLES
# =============================================================================

CREATE_TAGS = """
CREATE TABLE IF NOT EXISTS Tags (
    tag_id                  INTEGER PRIMARY KEY,
    name                    VARCHAR NOT NULL UNIQUE,
    description             TEXT,
    is_active               BOOLEAN DEFAULT TRUE,
    all_checked             BOOLEAN DEFAULT FALSE,
    last_checked_market_id  INTEGER,
    created_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (last_checked_market_id) REFERENCES MarketDim(market_id)
)
"""

CREATE_MARKET_TAG_DIM = """
CREATE TABLE IF NOT EXISTS MarketTagDim (
    market_tag_id   INTEGER PRIMARY KEY,
    market_id       INTEGER NOT NULL,
    tag_id          INTEGER NOT NULL,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (market_id) REFERENCES MarketDim(market_id),
    FOREIGN KEY (tag_id) REFERENCES Tags(tag_id),
    UNIQUE (market_id, tag_id)
)
"""

CREATE_TAG_EXAMPLES = """
CREATE TABLE IF NOT EXISTS TagExamples (
    example_id      INTEGER PRIMARY KEY,
    tag_id          INTEGER NOT NULL,
    market_id       INTEGER NOT NULL,
    is_positive     BOOLEAN NOT NULL,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (tag_id) REFERENCES Tags(tag_id),
    FOREIGN KEY (market_id) REFERENCES MarketDim(market_id),
    UNIQUE (tag_id, market_id)
)
"""

CREATE_JUDGE_HISTORY = """
CREATE TABLE IF NOT EXISTS JudgeHistory (
    history_id      INTEGER PRIMARY KEY,
    tag_id          INTEGER NOT NULL,
    market_id       INTEGER NOT NULL,
    judge_votes     TEXT NOT NULL,
    consensus       BOOLEAN,
    human_decision  BOOLEAN,
    decided_by      VARCHAR,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (tag_id) REFERENCES Tags(tag_id),
    FOREIGN KEY (market_id) REFERENCES MarketDim(market_id)
)
"""

# Tag-related indexes
CREATE_TAG_INDEXES = """
CREATE INDEX IF NOT EXISTS idx_tags_active ON Tags(is_active);
CREATE INDEX IF NOT EXISTS idx_tags_cursor ON Tags(last_checked_market_id);
CREATE INDEX IF NOT EXISTS idx_market_tag_market ON MarketTagDim(market_id);
CREATE INDEX IF NOT EXISTS idx_market_tag_tag ON MarketTagDim(tag_id);
CREATE INDEX IF NOT EXISTS idx_tag_examples_tag ON TagExamples(tag_id);
CREATE INDEX IF NOT EXISTS idx_tag_examples_market ON TagExamples(market_id);
CREATE INDEX IF NOT EXISTS idx_judge_history_tag ON JudgeHistory(tag_id);
CREATE INDEX IF NOT EXISTS idx_judge_history_market ON JudgeHistory(market_id);
CREATE INDEX IF NOT EXISTS idx_judge_history_consensus ON JudgeHistory(consensus);
"""

# All table creation statements in dependency order
ALL_TABLES = [
    ("MarketDim", CREATE_MARKET_DIM),
    ("MarketTokenDim", CREATE_MARKET_TOKEN_DIM),
    ("TraderDim", CREATE_TRADER_DIM),
    ("PriceHistoryFact", CREATE_PRICE_HISTORY_FACT),
    ("TradeFact", CREATE_TRADE_FACT),
    ("Tags", CREATE_TAGS),
    ("MarketTagDim", CREATE_MARKET_TAG_DIM),
    ("TagExamples", CREATE_TAG_EXAMPLES),
    ("JudgeHistory", CREATE_JUDGE_HISTORY),
]


# =============================================================================
# SCHEMA INITIALIZATION
# =============================================================================

def create_silver_schema(
    conn: Optional[duckdb.DuckDBPyConnection] = None,
    db_path: Optional[Path] = None,
) -> duckdb.DuckDBPyConnection:
    """
    Create all Silver Layer tables if they don't exist.

    Args:
        conn: Existing DuckDB connection (optional)
        db_path: Path to DuckDB file (uses default if not provided)

    Returns:
        DuckDB connection with schema initialized
    """
    if conn is None:
        db_path = db_path or DEFAULT_SILVER_DB_PATH
        db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = duckdb.connect(str(db_path))

    print("Creating Silver Layer schema...")

    # Create all tables
    for table_name, ddl in ALL_TABLES:
        try:
            conn.execute(ddl)
            print(f"  ✓ {table_name}")
        except Exception as e:
            print(f"  ✗ {table_name}: {e}")
            raise

    # Create core indexes
    for stmt in CREATE_INDEXES.strip().split(';'):
        if stmt.strip():
            conn.execute(stmt)
    print("  ✓ Core indexes created")

    # Create tag indexes
    for stmt in CREATE_TAG_INDEXES.strip().split(';'):
        if stmt.strip():
            conn.execute(stmt)
    print("  ✓ Tag indexes created")

    return conn


def drop_silver_schema(
    conn: duckdb.DuckDBPyConnection,
    confirm: bool = False
) -> None:
    """
    Drop all Silver Layer tables (use with caution).

    Args:
        conn: DuckDB connection
        confirm: Must be True to actually drop tables
    """
    if not confirm:
        print("Set confirm=True to drop tables")
        return

    # Drop in reverse dependency order (dependent tables first)
    tables = [
        "JudgeHistory", "TagExamples", "MarketTagDim", "Tags",
        "TradeFact", "PriceHistoryFact", "MarketTokenDim", "TraderDim", "MarketDim"
    ]

    for table in tables:
        try:
            conn.execute(f"DROP TABLE IF EXISTS {table}")
            print(f"  Dropped {table}")
        except Exception as e:
            print(f"  {table}: {e}")


def get_table_counts(conn: duckdb.DuckDBPyConnection) -> Dict[str, int]:
    """Get row counts for all Silver Layer tables."""
    counts = {}
    for table_name, _ in ALL_TABLES:
        try:
            count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            counts[table_name] = count
        except:
            counts[table_name] = -1
    return counts


# =============================================================================
# ID MAPPING UTILITIES
# =============================================================================

class IdMapper:
    """
    Utility class for mapping external identifiers to integer primary keys.
    
    Maintains in-memory caches and provides methods to:
    - Look up existing IDs
    - Create new IDs for new entities
    - Batch resolve IDs for bulk inserts
    
    Usage:
        mapper = IdMapper(conn)
        market_id = mapper.get_or_create_market_id("0xabc123...")
        token_id = mapper.get_or_create_token_id("0xdef456...", market_id)
    """
    
    def __init__(self, conn: duckdb.DuckDBPyConnection):
        self.conn = conn
        self._market_cache: Dict[str, int] = {}
        self._token_cache: Dict[str, int] = {}
        self._trader_cache: Dict[str, int] = {}
        self._load_caches()
    
    def _load_caches(self) -> None:
        """Load existing mappings from database into memory."""
        try:
            # Load market mappings
            rows = self.conn.execute(
                "SELECT condition_id, market_id FROM MarketDim"
            ).fetchall()
            self._market_cache = {row[0]: row[1] for row in rows}
            
            # Load token mappings
            rows = self.conn.execute(
                "SELECT token_hash, token_id FROM MarketTokenDim"
            ).fetchall()
            self._token_cache = {row[0]: row[1] for row in rows}
            
            # Load trader mappings
            rows = self.conn.execute(
                "SELECT wallet_address, trader_id FROM TraderDim"
            ).fetchall()
            self._trader_cache = {row[0]: row[1] for row in rows}
            
        except Exception:
            # Tables may not exist yet
            pass
    
    def refresh_caches(self) -> None:
        """Refresh all caches from database."""
        self._market_cache.clear()
        self._token_cache.clear()
        self._trader_cache.clear()
        self._load_caches()
    
    # -------------------------------------------------------------------------
    # Market ID Mapping
    # -------------------------------------------------------------------------
    
    def get_market_id(self, condition_id: str) -> Optional[int]:
        """Get market_id for a condition_id, or None if not found."""
        return self._market_cache.get(condition_id)
    
    def get_or_create_market_id(self, condition_id: str) -> int:
        """Get existing market_id or create a new one."""
        if condition_id in self._market_cache:
            return self._market_cache[condition_id]
        
        # Get next ID
        max_id = self.conn.execute(
            "SELECT COALESCE(MAX(market_id), 0) FROM MarketDim"
        ).fetchone()[0]
        new_id = max_id + 1
        
        # Insert minimal record (will be updated with full data later)
        self.conn.execute(
            "INSERT INTO MarketDim (market_id, condition_id) VALUES (?, ?)",
            [new_id, condition_id]
        )
        
        self._market_cache[condition_id] = new_id
        return new_id
    
    def bulk_resolve_market_ids(self, condition_ids: list) -> Dict[str, int]:
        """
        Resolve multiple condition_ids to market_ids.
        Creates new entries for unknown IDs.
        
        Returns:
            Dict mapping condition_id -> market_id
        """
        result = {}
        new_ids = []
        
        for cid in condition_ids:
            if cid in self._market_cache:
                result[cid] = self._market_cache[cid]
            else:
                new_ids.append(cid)
        
        if new_ids:
            max_id = self.conn.execute(
                "SELECT COALESCE(MAX(market_id), 0) FROM MarketDim"
            ).fetchone()[0]
            
            for i, cid in enumerate(new_ids, start=1):
                new_market_id = max_id + i
                self.conn.execute(
                    "INSERT INTO MarketDim (market_id, condition_id) VALUES (?, ?)",
                    [new_market_id, cid]
                )
                self._market_cache[cid] = new_market_id
                result[cid] = new_market_id
        
        return result
    
    # -------------------------------------------------------------------------
    # Token ID Mapping
    # -------------------------------------------------------------------------
    
    def get_token_id(self, token_hash: str) -> Optional[int]:
        """Get token_id for a token_hash, or None if not found."""
        return self._token_cache.get(token_hash)
    
    def get_or_create_token_id(
        self, 
        token_hash: str, 
        market_id: int,
        outcome: Optional[str] = None
    ) -> int:
        """Get existing token_id or create a new one."""
        if token_hash in self._token_cache:
            return self._token_cache[token_hash]
        
        max_id = self.conn.execute(
            "SELECT COALESCE(MAX(token_id), 0) FROM MarketTokenDim"
        ).fetchone()[0]
        new_id = max_id + 1
        
        self.conn.execute(
            """INSERT INTO MarketTokenDim 
               (token_id, token_hash, market_id, outcome, ingestion_timestamp) 
               VALUES (?, ?, ?, ?, ?)""",
            [new_id, token_hash, market_id, outcome, datetime.now()]
        )
        
        self._token_cache[token_hash] = new_id
        return new_id
    
    def bulk_resolve_token_ids(
        self, 
        tokens: list  # List of (token_hash, market_id, outcome) tuples
    ) -> Dict[str, int]:
        """
        Resolve multiple token_hashes to token_ids.
        Creates new entries for unknown tokens.
        
        Args:
            tokens: List of (token_hash, market_id, outcome) tuples
            
        Returns:
            Dict mapping token_hash -> token_id
        """
        result = {}
        new_tokens = []
        
        for token_hash, market_id, outcome in tokens:
            if token_hash in self._token_cache:
                result[token_hash] = self._token_cache[token_hash]
            else:
                new_tokens.append((token_hash, market_id, outcome))
        
        if new_tokens:
            max_id = self.conn.execute(
                "SELECT COALESCE(MAX(token_id), 0) FROM MarketTokenDim"
            ).fetchone()[0]
            
            now = datetime.now()
            for i, (token_hash, market_id, outcome) in enumerate(new_tokens, start=1):
                new_token_id = max_id + i
                self.conn.execute(
                    """INSERT INTO MarketTokenDim 
                       (token_id, token_hash, market_id, outcome, ingestion_timestamp) 
                       VALUES (?, ?, ?, ?, ?)""",
                    [new_token_id, token_hash, market_id, outcome, now]
                )
                self._token_cache[token_hash] = new_token_id
                result[token_hash] = new_token_id
        
        return result
    
    # -------------------------------------------------------------------------
    # Trader ID Mapping
    # -------------------------------------------------------------------------
    
    def get_trader_id(self, wallet_address: str) -> Optional[int]:
        """Get trader_id for a wallet_address, or None if not found."""
        return self._trader_cache.get(wallet_address)
    
    def get_or_create_trader_id(self, wallet_address: str) -> int:
        """Get existing trader_id or create a new one."""
        if wallet_address in self._trader_cache:
            return self._trader_cache[wallet_address]
        
        max_id = self.conn.execute(
            "SELECT COALESCE(MAX(trader_id), 0) FROM TraderDim"
        ).fetchone()[0]
        new_id = max_id + 1
        
        self.conn.execute(
            "INSERT INTO TraderDim (trader_id, wallet_address) VALUES (?, ?)",
            [new_id, wallet_address]
        )
        
        self._trader_cache[wallet_address] = new_id
        return new_id
    
    def bulk_resolve_trader_ids(self, wallet_addresses: list) -> Dict[str, int]:
        """
        Resolve multiple wallet_addresses to trader_ids.
        Creates new entries for unknown wallets.
        
        Returns:
            Dict mapping wallet_address -> trader_id
        """
        result = {}
        new_wallets = []
        
        for wallet in wallet_addresses:
            if wallet in self._trader_cache:
                result[wallet] = self._trader_cache[wallet]
            else:
                new_wallets.append(wallet)
        
        if new_wallets:
            max_id = self.conn.execute(
                "SELECT COALESCE(MAX(trader_id), 0) FROM TraderDim"
            ).fetchone()[0]
            
            for i, wallet in enumerate(new_wallets, start=1):
                new_trader_id = max_id + i
                self.conn.execute(
                    "INSERT INTO TraderDim (trader_id, wallet_address) VALUES (?, ?)",
                    [new_trader_id, wallet]
                )
                self._trader_cache[wallet] = new_trader_id
                result[wallet] = new_trader_id
        
        return result


# =============================================================================
# CONVENIENCE FUNCTIONS
# =============================================================================

def connect_silver(db_path: Optional[Path] = None) -> Tuple[duckdb.DuckDBPyConnection, IdMapper]:
    """
    Connect to Silver Layer database and return connection with ID mapper.
    
    Usage:
        conn, mapper = connect_silver()
        # Use conn for queries, mapper for ID resolution
    """
    db_path = db_path or DEFAULT_SILVER_DB_PATH
    conn = create_silver_schema(db_path=db_path)
    mapper = IdMapper(conn)
    return conn, mapper


def print_schema_info(conn: duckdb.DuckDBPyConnection) -> None:
    """Print information about all Silver Layer tables."""
    print("\n" + "="*60)
    print("Silver Layer Schema")
    print("="*60)
    
    counts = get_table_counts(conn)
    
    for table_name, _ in ALL_TABLES:
        print(f"\n{table_name} ({counts.get(table_name, 0):,} rows)")
        print("-" * 40)
        try:
            for row in conn.execute(f"DESCRIBE {table_name}").fetchall():
                print(f"  {row[0]:25} {row[1]}")
        except:
            print("  (table not found)")


# =============================================================================
# MAIN ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    import sys
    
    # Allow custom DB path from command line
    if len(sys.argv) > 1:
        db_path = Path(sys.argv[1])
    else:
        db_path = DEFAULT_SILVER_DB_PATH
    
    print(f"Initializing Silver Layer at: {db_path}")
    
    conn = create_silver_schema(db_path=db_path)
    print_schema_info(conn)
    
    print("\n✓ Silver Layer ready")
    conn.close()