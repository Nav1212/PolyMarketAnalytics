"""
Run Page - Start and monitor fetcher runs
"""

import streamlit as st
import sys
import time
from pathlib import Path
from datetime import datetime

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from fetcher_gui.services.run_manager import RunManager, RunMode, RunStatus
from fetcher_gui.services.progress_tracker import ProgressTracker
from fetcher.config import get_config

st.set_page_config(page_title="Run", page_icon="R", layout="wide")


def init_session_state():
    """Initialize session state."""
    if "run_manager" not in st.session_state:
        st.session_state.run_manager = RunManager()

    if "progress_tracker" not in st.session_state:
        st.session_state.progress_tracker = ProgressTracker()

    if "config" not in st.session_state:
        st.session_state.config = get_config()


def main():
    init_session_state()

    st.title("Run Fetcher")

    run_manager = st.session_state.run_manager
    progress_tracker = st.session_state.progress_tracker
    config = st.session_state.config

    # Check cursor status for resume info
    cursor_status = progress_tracker.get_cursor_status()
    has_pending = cursor_status.get("has_progress", False)

    # Current status section
    st.subheader("Current Status")

    status_cols = st.columns([2, 1, 1])

    with status_cols[0]:
        if run_manager.is_running:
            progress = run_manager.progress
            st.error(f"Status: {progress.status.value.upper()}")
            st.text(f"Mode: {progress.mode.value if progress.mode else 'N/A'}")
            st.text(f"Started: {progress.start_time.strftime('%H:%M:%S') if progress.start_time else 'N/A'}")
            if progress.current_operation:
                st.text(f"Current: {progress.current_operation}")
        else:
            st.success("Status: READY")
            if has_pending:
                st.warning("Pending progress available - can resume from previous run")

    with status_cols[1]:
        if run_manager.is_running:
            if st.button("Stop Run", type="primary", use_container_width=True):
                run_manager.stop()
                st.rerun()

    with status_cols[2]:
        if st.button("Refresh", use_container_width=True):
            st.rerun()

    st.divider()

    # Run options section
    st.subheader("Start New Run")

    if run_manager.is_running:
        st.warning("A run is already in progress. Stop it first to start a new one.")
    else:
        # Run mode selection with descriptions
        st.markdown("### Select Run Mode")

        run_tabs = st.tabs([
            "Everything",
            "Markets Only",
            "Trades (Market Dim)",
            "Trades (Leaderboard)",
            "Prices (New Markets)"
        ])

        # Tab: Everything
        with run_tabs[0]:
            st.markdown("""
            **Full Pipeline Run**

            Runs the complete fetching pipeline:
            1. Markets from CLOB API (runs first)
            2. Trades, Prices, Leaderboard (run in parallel after markets)

            This is the most comprehensive option but takes the longest.
            """)

            col1, col2 = st.columns(2)
            with col1:
                all_fresh = st.checkbox("Fresh Start", key="all_fresh",
                    help="Clear all cursors and start from scratch")
            with col2:
                all_limit = st.number_input("Limit (optional)", min_value=0, value=0,
                    key="all_limit", help="Limit number of markets (0 = no limit)")

            if st.button("Run Everything", type="primary", use_container_width=True, key="run_all"):
                limit = all_limit if all_limit > 0 else None
                if run_manager.start(RunMode.ALL, fresh=all_fresh, limit=limit):
                    st.success("Started full pipeline run!")
                    st.rerun()
                else:
                    st.error("Failed to start run")

        # Tab: Markets Only
        with run_tabs[1]:
            st.markdown("""
            **Markets Only**

            Fetches only market data from the CLOB API.
            Does not fetch trades, prices, or leaderboard data.

            Use this to update the market dimension table.
            """)

            col1, col2 = st.columns(2)
            with col1:
                markets_fresh = st.checkbox("Fresh Start", key="markets_fresh",
                    help="Clear market cursor and start from scratch")

            if st.button("Run Markets", type="primary", use_container_width=True, key="run_markets"):
                if run_manager.start(RunMode.MARKETS, fresh=markets_fresh):
                    st.success("Started markets run!")
                    st.rerun()
                else:
                    st.error("Failed to start run")

        # Tab: Trades (Market Dim)
        with run_tabs[2]:
            st.markdown("""
            **Trades for Markets in Market Dimension**

            Fetches trade history for inactive/closed markets found in the
            MarketDim table in DuckDB.

            Prerequisites:
            - Markets must have been loaded previously
            - Silver layer must be ingested (MarketDim populated)
            """)

            col1, col2 = st.columns(2)
            with col1:
                trades_fresh = st.checkbox("Fresh Start", key="trades_fresh",
                    help="Clear trade cursor and start from scratch")
            with col2:
                trades_limit = st.number_input("Limit (optional)", min_value=0, value=0,
                    key="trades_limit", help="Limit number of markets (0 = no limit)")

            if st.button("Run Trades (Market Dim)", type="primary", use_container_width=True, key="run_trades"):
                limit = trades_limit if trades_limit > 0 else None
                if run_manager.start(RunMode.TRADES, fresh=trades_fresh, limit=limit):
                    st.success("Started trades run!")
                    st.rerun()
                else:
                    st.error("Failed to start run")

        # Tab: Trades (Leaderboard)
        with run_tabs[3]:
            st.markdown("""
            **Trades for Leaderboard Members**

            Fetches trade history for markets where leaderboard members
            have traded.

            This helps capture high-value trader activity.

            Prerequisites:
            - Leaderboard data must be loaded first
            """)

            col1, col2 = st.columns(2)
            with col1:
                trades_lb_fresh = st.checkbox("Fresh Start", key="trades_lb_fresh",
                    help="Clear cursor and start from scratch")

            if st.button("Run Trades (Leaderboard)", type="primary", use_container_width=True, key="run_trades_lb"):
                if run_manager.start(RunMode.TRADES_LEADERBOARD, fresh=trades_lb_fresh):
                    st.success("Started leaderboard trades run!")
                    st.rerun()
                else:
                    st.error("Failed to start run")

        # Tab: Prices (New Markets)
        with run_tabs[4]:
            st.markdown("""
            **Prices for New Markets**

            Fetches price history for tokens from recently added markets.

            Uses volume-aware resolution:
            - Low volume (<$10K): Hourly resolution
            - High volume (>$100K): Minute resolution
            - Mid volume: Adaptive based on price changes

            Prerequisites:
            - Markets must have been loaded with tokens
            """)

            col1, col2 = st.columns(2)
            with col1:
                prices_fresh = st.checkbox("Fresh Start", key="prices_fresh",
                    help="Clear price cursor and start from scratch")

            if st.button("Run Prices", type="primary", use_container_width=True, key="run_prices"):
                if run_manager.start(RunMode.PRICES, fresh=prices_fresh):
                    st.success("Started prices run!")
                    st.rerun()
                else:
                    st.error("Failed to start run")

    st.divider()

    # Log output section
    st.subheader("Run Log")

    log_container = st.container()

    with log_container:
        log_lines = run_manager.get_log_lines(100)

        if log_lines:
            # Display logs in a scrollable area
            log_text = "\n".join(log_lines)
            st.code(log_text, language="text")

            if st.button("Clear Logs"):
                run_manager.clear_logs()
                st.rerun()
        else:
            st.info("No log output yet. Start a run to see logs here.")

    # Auto-refresh when running
    if run_manager.is_running:
        time.sleep(2)
        st.rerun()

    st.divider()

    # Worker summary
    st.subheader("Worker Configuration Summary")

    worker_cols = st.columns(5)

    with worker_cols[0]:
        st.metric("Market Workers", config.workers.market)

    with worker_cols[1]:
        st.metric("Trade Workers", config.workers.trade)

    with worker_cols[2]:
        st.metric("Price Workers", config.workers.price)

    with worker_cols[3]:
        st.metric("Leaderboard Workers", config.workers.leaderboard)

    with worker_cols[4]:
        st.metric("Gamma Workers", config.workers.gamma_market)

    st.caption("Configure workers on the Workers page")


if __name__ == "__main__":
    main()
