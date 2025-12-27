"""
MarketTokenDim Transformer

Transforms bronze market_tokens data into the silver MarketTokenDim table.

Source: data/market_tokens/
Mapping:
- token_id (bronze) -> token_hash (silver)
- condition_Id (bronze) -> market_id lookup from MarketDim
- outcome, price, winner -> direct mapping
"""

from pathlib import Path
from typing import Dict, Any, Optional, List
from datetime import datetime
import duckdb

from Ingestion.transformers.base import BaseTransformer


class MarketTokenDimTransformer(BaseTransformer):
    """
    Transformer for populating the MarketTokenDim silver table.

    Requires MarketDim to be populated first for market_id lookups.
    """

    def __init__(
        self,
        conn: duckdb.DuckDBPyConnection,
        bronze_base_path: Path,
        market_id_cache: Optional[Dict[str, int]] = None
    ):
        """
        Initialize the MarketTokenDim transformer.

        Args:
            conn: DuckDB connection to silver database
            bronze_base_path: Base path to bronze data directory
            market_id_cache: Optional pre-loaded cache of condition_id -> market_id
        """
        super().__init__(conn, bronze_base_path, "MarketTokenDimTransformer")
        self._market_id_cache = market_id_cache or {}

    def get_bronze_path(self) -> Path:
        """Return path to market_tokens parquet files."""
        return self.bronze_base_path / "market_tokens"

    def get_table_name(self) -> str:
        return "MarketTokenDim"

    def transform(self) -> int:
        """
        Execute the MarketTokenDim transformation.

        Steps:
        1. Load market_id cache from MarketDim if not provided
        2. Read all tokens from bronze
        3. Map condition_Id to market_id
        4. Insert new tokens (skip existing)

        Returns:
            Number of records processed
        """
        self.reset_stats()
        self.logger.info("Starting MarketTokenDim transformation")

        # Step 1: Load market_id cache if not provided
        if not self._market_id_cache:
            self._load_market_id_cache()

        if not self._market_id_cache:
            self.logger.warning("No markets found in MarketDim - cannot process tokens")
            return 0

        self.logger.info(f"Market ID cache loaded with {len(self._market_id_cache)} entries")

        # Step 2: Load existing token hashes to avoid duplicates
        existing_tokens = self._load_existing_tokens()
        self.logger.info(f"Found {len(existing_tokens)} existing tokens in silver")

        # Step 3: Read tokens from bronze
        tokens = self._load_bronze_tokens()
        self.logger.info(f"Loaded {len(tokens)} tokens from bronze")

        # Step 4: Process and insert tokens
        self._process_tokens(tokens, existing_tokens)

        stats = self.get_stats()
        self.logger.info(
            f"MarketTokenDim transformation complete: "
            f"{stats['records_inserted']} inserted, "
            f"{stats['records_skipped']} skipped"
        )

        return self._records_processed

    def _load_market_id_cache(self) -> None:
        """Load condition_id -> market_id mapping from MarketDim."""
        try:
            result = self.conn.execute(
                "SELECT condition_id, market_id FROM MarketDim"
            ).fetchall()

            self._market_id_cache = {row[0]: row[1] for row in result}

        except Exception as e:
            self.logger.error(f"Error loading market ID cache: {e}")
            self._market_id_cache = {}

    def _load_existing_tokens(self) -> set:
        """Load existing token_hash values from MarketTokenDim."""
        try:
            result = self.conn.execute(
                "SELECT token_hash FROM MarketTokenDim"
            ).fetchall()

            return {row[0] for row in result}

        except Exception as e:
            self.logger.error(f"Error loading existing tokens: {e}")
            return set()

    def _load_bronze_tokens(self) -> List[Dict[str, Any]]:
        """
        Load tokens from bronze market_tokens parquet files.

        Returns:
            List of token dictionaries
        """
        bronze_path = self.get_bronze_path()

        if not bronze_path.exists():
            self.logger.warning(f"Bronze path does not exist: {bronze_path}")
            return []

        try:
            read_conn = duckdb.connect(":memory:")
            glob_pattern = str(bronze_path / "**" / "*.parquet")

            # Read all tokens, deduplicating by token_id
            query = f"""
                SELECT DISTINCT ON (token_id)
                    condition_Id,
                    token_id,
                    price,
                    winner,
                    outcome
                FROM read_parquet('{glob_pattern}', hive_partitioning=true)
                WHERE token_id IS NOT NULL
                ORDER BY token_id
            """

            result = read_conn.execute(query).fetchdf()
            read_conn.close()

            return result.to_dict('records')

        except Exception as e:
            self.logger.error(f"Error loading bronze tokens: {e}")
            return []

    def _process_tokens(
        self,
        tokens: List[Dict[str, Any]],
        existing_tokens: set
    ) -> None:
        """
        Process and insert tokens into MarketTokenDim.

        Args:
            tokens: List of token records from bronze
            existing_tokens: Set of existing token_hash values
        """
        now = datetime.now()

        # Get current max token_id for auto-increment
        max_id_result = self.conn.execute(
            "SELECT COALESCE(MAX(token_id), 0) FROM MarketTokenDim"
        ).fetchone()
        next_id = max_id_result[0] + 1

        for token in tokens:
            self._records_processed += 1

            token_hash = token.get('token_id')  # bronze token_id -> silver token_hash
            condition_id = token.get('condition_Id')

            # Skip if already exists
            if token_hash in existing_tokens:
                self._records_skipped += 1
                continue

            # Lookup market_id
            market_id = self._market_id_cache.get(condition_id)
            if market_id is None:
                self.logger.warning(
                    f"No market_id found for condition_id: {condition_id}"
                )
                self._records_skipped += 1
                continue

            try:
                self.conn.execute("""
                    INSERT INTO MarketTokenDim (
                        token_id, token_hash, market_id, outcome, price, winner,
                        ingestion_timestamp
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """, [
                    next_id,
                    token_hash,
                    market_id,
                    token.get('outcome'),
                    self._safe_float(token.get('price')),
                    token.get('winner', False),
                    now,
                ])

                existing_tokens.add(token_hash)
                next_id += 1
                self._records_inserted += 1

            except Exception as e:
                self.logger.error(f"Error inserting token {token_hash}: {e}")
                self._records_skipped += 1

    def _safe_float(self, value: Any) -> Optional[float]:
        """Safely convert a value to float."""
        if value is None:
            return None

        try:
            return float(value)
        except (ValueError, TypeError):
            return None
