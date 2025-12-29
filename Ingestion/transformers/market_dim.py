"""
MarketDim Transformer

Transforms bronze market data into the silver MarketDim table.

Sources:
- Primary: data/markets/ (CLOB API) - Source of Truth for core fields
- Enrichment: data/gamma_markets/ (Gamma API) - category and tags

Logic:
1. Load all markets from CLOB API parquet files
2. Enrich with category/tags from Gamma API
3. Deduplicate by condition_id
4. Upsert into MarketDim table
"""

from pathlib import Path
from typing import Dict, Any, Optional, List
from datetime import datetime
import duckdb

from Ingestion.transformers.base import BaseTransformer
from fetcher.config import get_config


class MarketDimTransformer(BaseTransformer):
    """
    Transformer for populating the MarketDim silver table.

    Combines data from CLOB API markets (primary) and Gamma API markets
    (enrichment) with CLOB as the Source of Truth for conflicts.
    """

    def __init__(
        self,
        conn: duckdb.DuckDBPyConnection,
        bronze_base_path: Path,
    ):
        """
        Initialize the MarketDim transformer.

        Args:
            conn: DuckDB connection to silver database
            bronze_base_path: Base path to bronze data directory
        """
        super().__init__(conn, bronze_base_path, "MarketDimTransformer")

    def get_bronze_path(self) -> Path:
        """Return path to CLOB markets (primary source)."""
        return self.bronze_base_path / "markets"

    def get_gamma_path(self) -> Path:
        """Return path to Gamma markets (enrichment source)."""
        return self.bronze_base_path / "gamma_markets"

    def get_table_name(self) -> str:
        return "MarketDim"

    def transform(self) -> int:
        """
        Execute the MarketDim transformation.

        Steps:
        1. Load CLOB markets (primary source)
        2. Load Gamma markets for enrichment
        3. Merge sources (CLOB as SOT)
        4. Upsert into MarketDim

        Returns:
            Number of records processed
        """
        self.reset_stats()
        self.logger.info("Starting MarketDim transformation")

        # Step 1: Load CLOB markets (primary)
        clob_markets = self._load_clob_markets()
        self.logger.info(f"Loaded {len(clob_markets)} markets from CLOB API")

        # Step 2: Load Gamma markets (enrichment)
        gamma_markets = self._load_gamma_markets()
        self.logger.info(f"Loaded {len(gamma_markets)} markets from Gamma API")

        # Step 3: Merge sources (CLOB as SOT)
        merged_markets = self._merge_sources(clob_markets, gamma_markets)
        self.logger.info(f"Merged into {len(merged_markets)} unique markets")

        # Step 4: Upsert into MarketDim
        self._upsert_markets(merged_markets)

        self._records_processed = len(merged_markets)
        stats = self.get_stats()
        self.logger.info(
            f"MarketDim transformation complete: "
            f"{stats['records_inserted']} inserted, "
            f"{stats['records_updated']} updated, "
            f"{stats['records_skipped']} skipped"
        )

        return self._records_processed

    def _load_clob_markets(self) -> Dict[str, Dict[str, Any]]:
        """
        Load markets from CLOB API parquet files.

        Returns:
            Dict mapping condition_id -> market data
        """
        bronze_path = self.get_bronze_path()

        if not bronze_path.exists():
            self.logger.warning(f"CLOB markets path does not exist: {bronze_path}")
            return {}

        try:
            read_conn = duckdb.connect(":memory:")
            glob_pattern = str(bronze_path / "**" / "*.parquet")

            # Read and deduplicate by condition_id, keeping latest values
            query = f"""
                SELECT DISTINCT ON (condition_id)
                    condition_id,
                    question,
                    description,
                    end_date_iso,
                    volume,
                    liquidity,
                    active,
                    closed
                FROM read_parquet('{glob_pattern}', hive_partitioning=true)
                WHERE condition_id IS NOT NULL
                ORDER BY condition_id
            """

            result = read_conn.execute(query).fetchdf()
            read_conn.close()

            # Convert to dict keyed by condition_id
            markets = {}
            for record in result.to_dict('records'):
                cid = record.get('condition_id')
                if cid:
                    markets[cid] = {
                        'condition_id': cid,
                        'question': record.get('question'),
                        'description': record.get('description'),
                        'end_dt': self._parse_datetime(record.get('end_date_iso')),
                        'start_dt': None,  # Not available in CLOB data
                        'volume': self._safe_float(record.get('volume')),
                        'liquidity': self._safe_float(record.get('liquidity')),
                        'active': record.get('active', True),
                        'closed': record.get('closed', False),
                        'category': None,  # Will be enriched from Gamma
                        'tags': None,  # Will be enriched from Gamma/NLP
                    }

            return markets

        except Exception as e:
            self.logger.error(f"Error loading CLOB markets: {e}")
            return {}

    def _load_gamma_markets(self) -> Dict[str, Dict[str, Any]]:
        """
        Load markets from Gamma API parquet files for enrichment.

        Returns:
            Dict mapping conditionId -> enrichment data (category, etc.)
        """
        gamma_path = self.get_gamma_path()

        if not gamma_path.exists():
            self.logger.warning(f"Gamma markets path does not exist: {gamma_path}")
            return {}

        try:
            read_conn = duckdb.connect(":memory:")
            glob_pattern = str(gamma_path / "**" / "*.parquet")

            query = f"""
                SELECT DISTINCT ON (conditionId)
                    conditionId,
                    category,
                    question,
                    description,
                    startDate,
                    endDate,
                    volumeNum,
                    liquidityNum,
                    active,
                    closed
                FROM read_parquet('{glob_pattern}', hive_partitioning=true)
                WHERE conditionId IS NOT NULL
                ORDER BY conditionId
            """

            result = read_conn.execute(query).fetchdf()
            read_conn.close()

            # Convert to dict keyed by conditionId
            markets = {}
            for record in result.to_dict('records'):
                cid = record.get('conditionId')
                if cid:
                    markets[cid] = {
                        'condition_id': cid,
                        'category': record.get('category'),
                        'question': record.get('question'),
                        'description': record.get('description'),
                        'start_dt': self._parse_datetime(record.get('startDate')),
                        'end_dt': self._parse_datetime(record.get('endDate')),
                        'volume': self._safe_float(record.get('volumeNum')),
                        'liquidity': self._safe_float(record.get('liquidityNum')),
                        'active': record.get('active', True),
                        'closed': record.get('closed', False),
                    }

            return markets

        except Exception as e:
            self.logger.error(f"Error loading Gamma markets: {e}")
            return {}

    def _merge_sources(
        self,
        clob_markets: Dict[str, Dict[str, Any]],
        gamma_markets: Dict[str, Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Merge CLOB and Gamma market data.

        CLOB is Source of Truth for all core fields.
        Gamma provides category enrichment.

        Args:
            clob_markets: Markets from CLOB API (keyed by condition_id)
            gamma_markets: Markets from Gamma API (keyed by condition_id)

        Returns:
            List of merged market records
        """
        merged = []
        all_condition_ids = set(clob_markets.keys()) | set(gamma_markets.keys())

        for cid in all_condition_ids:
            clob_data = clob_markets.get(cid, {})
            gamma_data = gamma_markets.get(cid, {})

            if clob_data:
                # CLOB exists - use as primary, enrich with Gamma
                market = clob_data.copy()

                # Enrich with Gamma category (only if CLOB doesn't have it)
                if not market.get('category') and gamma_data.get('category'):
                    market['category'] = gamma_data['category']

                # Use Gamma start_dt if CLOB doesn't have it
                if not market.get('start_dt') and gamma_data.get('start_dt'):
                    market['start_dt'] = gamma_data['start_dt']

            else:
                # Only in Gamma - use Gamma data
                market = {
                    'condition_id': cid,
                    'question': gamma_data.get('question'),
                    'description': gamma_data.get('description'),
                    'start_dt': gamma_data.get('start_dt'),
                    'end_dt': gamma_data.get('end_dt'),
                    'volume': gamma_data.get('volume'),
                    'liquidity': gamma_data.get('liquidity'),
                    'active': gamma_data.get('active', True),
                    'closed': gamma_data.get('closed', False),
                    'category': gamma_data.get('category'),
                    'tags': None,
                }

            merged.append(market)

        return merged

    def _upsert_markets(self, markets: List[Dict[str, Any]]) -> None:
        """
        Upsert markets into the MarketDim table using batch operations.

        Uses batch INSERT and UPDATE for efficient upserts wrapped in a transaction.

        Args:
            markets: List of market records to upsert
        """
        if not markets:
            return

        now = datetime.now()
        config = get_config()
        batch_size = config.batch_sizes.market * 10  # 10x config batch size

        try:
            # Start transaction
            self.conn.execute("BEGIN TRANSACTION")

            # Load all existing condition_ids in one query
            existing_result = self.conn.execute(
                "SELECT condition_id, market_id FROM MarketDim"
            ).fetchall()
            existing_map = {row[0]: row[1] for row in existing_result}

            # Get current max market_id once
            max_id_result = self.conn.execute(
                "SELECT COALESCE(MAX(market_id), 0) FROM MarketDim"
            ).fetchone()
            next_id = max_id_result[0] + 1 if max_id_result else 1

            # Separate markets into updates and inserts
            updates = []
            inserts = []

            for market in markets:
                condition_id = market['condition_id']
                if condition_id in existing_map:
                    updates.append((
                        market.get('question'),
                        market.get('description'),
                        market.get('start_dt'),
                        market.get('end_dt'),
                        market.get('volume'),
                        market.get('liquidity'),
                        market.get('active', True),
                        market.get('closed', False),
                        market.get('category'),
                        market.get('tags'),
                        now,
                        condition_id,
                    ))
                else:
                    inserts.append((
                        next_id,
                        condition_id,
                        market.get('question'),
                        market.get('description'),
                        market.get('start_dt'),
                        market.get('end_dt'),
                        market.get('volume'),
                        market.get('liquidity'),
                        market.get('active', True),
                        market.get('closed', False),
                        market.get('category'),
                        market.get('tags'),
                        now,
                        now,
                    ))
                    next_id += 1

            # Batch UPDATE using executemany
            if updates:
                for i in range(0, len(updates), batch_size):
                    batch = updates[i:i + batch_size]
                    self.conn.executemany("""
                        UPDATE MarketDim SET
                            question = COALESCE(?, question),
                            description = COALESCE(?, description),
                            start_dt = COALESCE(?, start_dt),
                            end_dt = COALESCE(?, end_dt),
                            volume = COALESCE(?, volume),
                            liquidity = COALESCE(?, liquidity),
                            active = ?,
                            closed = ?,
                            category = COALESCE(?, category),
                            tags = COALESCE(?, tags),
                            updated_at = ?
                        WHERE condition_id = ?
                    """, batch)
                self._records_updated = len(updates)

            # Batch INSERT using executemany
            if inserts:
                for i in range(0, len(inserts), batch_size):
                    batch = inserts[i:i + batch_size]
                    self.conn.executemany("""
                        INSERT INTO MarketDim (
                            market_id, condition_id, question, description,
                            start_dt, end_dt, volume, liquidity,
                            active, closed, category, tags,
                            created_at, updated_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, batch)
                self._records_inserted = len(inserts)

            # Commit transaction
            self.conn.execute("COMMIT")

        except Exception as e:
            self.logger.error(f"Error in batch upsert, rolling back: {e}")
            self.conn.execute("ROLLBACK")
            self._records_skipped = len(markets)
            raise

    def _parse_datetime(self, value: Any) -> Optional[datetime]:
        """Parse a datetime value from various formats."""
        if value is None:
            return None

        if isinstance(value, datetime):
            return value

        if isinstance(value, str):
            # Try common formats
            for fmt in [
                "%Y-%m-%dT%H:%M:%S.%fZ",
                "%Y-%m-%dT%H:%M:%SZ",
                "%Y-%m-%dT%H:%M:%S",
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%d",
            ]:
                try:
                    return datetime.strptime(value, fmt)
                except ValueError:
                    continue

        return None

    def _safe_float(self, value: Any) -> Optional[float]:
        """Safely convert a value to float."""
        if value is None:
            return None

        try:
            return float(value)
        except (ValueError, TypeError):
            return None
