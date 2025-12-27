"""
TraderDim Transformer

Transforms bronze trade and leaderboard data into the silver TraderDim table.

Sources:
- data/trades/ - proxyWallet field
- data/leaderboard/ - proxyWallet field

Logic:
1. Extract unique wallet addresses from both sources
2. Union and deduplicate
3. Insert new wallets into TraderDim
"""

from pathlib import Path
from typing import Dict, Any, Set, List
from datetime import datetime
import duckdb

from Ingestion.transformers.base import BaseTransformer


class TraderDimTransformer(BaseTransformer):
    """
    Transformer for populating the TraderDim silver table.

    Combines unique wallet addresses from trades and leaderboard data.
    """

    def __init__(
        self,
        conn: duckdb.DuckDBPyConnection,
        bronze_base_path: Path
    ):
        """
        Initialize the TraderDim transformer.

        Args:
            conn: DuckDB connection to silver database
            bronze_base_path: Base path to bronze data directory
        """
        super().__init__(conn, bronze_base_path, "TraderDimTransformer")

    def get_bronze_path(self) -> Path:
        """Return path to trades (primary source for wallets)."""
        return self.bronze_base_path / "trades"

    def get_leaderboard_path(self) -> Path:
        """Return path to leaderboard parquet files."""
        return self.bronze_base_path / "leaderboard"

    def get_table_name(self) -> str:
        return "TraderDim"

    def transform(self) -> int:
        """
        Execute the TraderDim transformation.

        Steps:
        1. Load existing wallet addresses from TraderDim
        2. Extract unique wallets from trades
        3. Extract unique wallets from leaderboard
        4. Union and deduplicate
        5. Insert new wallets

        Returns:
            Number of records processed
        """
        self.reset_stats()
        self.logger.info("Starting TraderDim transformation")

        # Step 1: Load existing wallets
        existing_wallets = self._load_existing_wallets()
        self.logger.info(f"Found {len(existing_wallets)} existing wallets in silver")

        # Step 2: Extract wallets from trades
        trade_wallets = self._extract_trade_wallets()
        self.logger.info(f"Found {len(trade_wallets)} unique wallets in trades")

        # Step 3: Extract wallets from leaderboard
        leaderboard_wallets = self._extract_leaderboard_wallets()
        self.logger.info(f"Found {len(leaderboard_wallets)} unique wallets in leaderboard")

        # Step 4: Union and find new wallets
        all_wallets = trade_wallets | leaderboard_wallets
        new_wallets = all_wallets - existing_wallets
        self.logger.info(f"Total unique wallets: {len(all_wallets)}, new: {len(new_wallets)}")

        # Step 5: Insert new wallets
        self._insert_wallets(new_wallets)

        self._records_processed = len(all_wallets)
        stats = self.get_stats()
        self.logger.info(
            f"TraderDim transformation complete: "
            f"{stats['records_inserted']} inserted, "
            f"{stats['records_skipped']} skipped (already exist)"
        )

        return self._records_processed

    def _load_existing_wallets(self) -> Set[str]:
        """Load existing wallet addresses from TraderDim."""
        try:
            result = self.conn.execute(
                "SELECT wallet_address FROM TraderDim"
            ).fetchall()

            return {row[0] for row in result if row[0]}

        except Exception as e:
            self.logger.error(f"Error loading existing wallets: {e}")
            return set()

    def _extract_trade_wallets(self) -> Set[str]:
        """
        Extract unique wallet addresses from trades parquet files.

        Returns:
            Set of unique wallet addresses
        """
        trades_path = self.get_bronze_path()

        if not trades_path.exists():
            self.logger.warning(f"Trades path does not exist: {trades_path}")
            return set()

        try:
            read_conn = duckdb.connect(":memory:")
            glob_pattern = str(trades_path / "**" / "*.parquet")

            query = f"""
                SELECT DISTINCT proxyWallet
                FROM read_parquet('{glob_pattern}', hive_partitioning=true)
                WHERE proxyWallet IS NOT NULL AND proxyWallet != ''
            """

            result = read_conn.execute(query).fetchall()
            read_conn.close()

            return {row[0] for row in result}

        except Exception as e:
            self.logger.error(f"Error extracting trade wallets: {e}")
            return set()

    def _extract_leaderboard_wallets(self) -> Set[str]:
        """
        Extract unique wallet addresses from leaderboard parquet files.

        Returns:
            Set of unique wallet addresses
        """
        leaderboard_path = self.get_leaderboard_path()

        if not leaderboard_path.exists():
            self.logger.warning(f"Leaderboard path does not exist: {leaderboard_path}")
            return set()

        try:
            read_conn = duckdb.connect(":memory:")
            glob_pattern = str(leaderboard_path / "**" / "*.parquet")

            query = f"""
                SELECT DISTINCT proxyWallet
                FROM read_parquet('{glob_pattern}', hive_partitioning=true)
                WHERE proxyWallet IS NOT NULL AND proxyWallet != ''
            """

            result = read_conn.execute(query).fetchall()
            read_conn.close()

            return {row[0] for row in result}

        except Exception as e:
            self.logger.error(f"Error extracting leaderboard wallets: {e}")
            return set()

    def _insert_wallets(self, wallets: Set[str]) -> None:
        """
        Insert new wallet addresses into TraderDim.

        Args:
            wallets: Set of wallet addresses to insert
        """
        if not wallets:
            return

        now = datetime.now()

        # Get current max trader_id for auto-increment
        max_id_result = self.conn.execute(
            "SELECT COALESCE(MAX(trader_id), 0) FROM TraderDim"
        ).fetchone()
        next_id = max_id_result[0] + 1

        for wallet in wallets:
            try:
                self.conn.execute("""
                    INSERT INTO TraderDim (trader_id, wallet_address, created_at)
                    VALUES (?, ?, ?)
                """, [next_id, wallet, now])

                next_id += 1
                self._records_inserted += 1

            except Exception as e:
                self.logger.error(f"Error inserting wallet {wallet}: {e}")
                self._records_skipped += 1
