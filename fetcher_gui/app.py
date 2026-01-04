"""
Fetcher Control Panel - Streamlit Application

Main entry point for the fetcher monitoring and control UI.

Run with:
    streamlit run fetcher_gui/app.py
"""

import streamlit as st

# Page config must be first Streamlit command
st.set_page_config(
    page_title="Fetcher Control Panel",
    page_icon="F",
    layout="wide",
    initial_sidebar_state="expanded",
)

import sys
from pathlib import Path
from datetime import datetime
import json

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from fetcher_gui.services.progress_tracker import ProgressTracker
from fetcher_gui.services.run_manager import RunManager
from fetcher.config import get_config, save_config, Config


def init_session_state():
    """Initialize session state variables."""
    if "progress_tracker" not in st.session_state:
        st.session_state.progress_tracker = ProgressTracker()

    if "run_manager" not in st.session_state:
        st.session_state.run_manager = RunManager()

    if "config" not in st.session_state:
        st.session_state.config = get_config()

    if "last_refresh" not in st.session_state:
        st.session_state.last_refresh = datetime.now()


def format_timestamp(ts_str: str) -> str:
    """Format an ISO timestamp for display."""
    if not ts_str:
        return "Never"
    try:
        dt = datetime.fromisoformat(ts_str)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except:
        return ts_str


def main():
    """Main application entry point."""
    init_session_state()

    st.title("Fetcher Control Panel")

    # Sidebar status
    with st.sidebar:
        st.header("System Status")

        # Refresh button
        if st.button("Refresh Status", use_container_width=True):
            st.session_state.last_refresh = datetime.now()
            st.session_state.progress_tracker.refresh()
            st.rerun()

        st.caption(f"Last refresh: {st.session_state.last_refresh.strftime('%H:%M:%S')}")
        st.divider()

        # Load status
        progress = st.session_state.progress_tracker
        load_status = progress.get_load_status()

        st.subheader("Data Status")

        # Markets
        market_info = load_status.get("markets", {})
        if market_info.get("count", 0) > 0:
            st.success(f"Markets: {market_info.get('count', 0):,}")
            st.caption(f"Last: {market_info.get('last_date', 'N/A')}")
        else:
            st.warning("Markets: No data")

        # Trades
        trade_info = load_status.get("trades", {})
        if trade_info.get("count", 0) > 0:
            st.success(f"Trades: {trade_info.get('count', 0):,}")
            st.caption(f"Last: {trade_info.get('last_date', 'N/A')}")
        else:
            st.warning("Trades: No data")

        # Prices
        price_info = load_status.get("prices", {})
        if price_info.get("count", 0) > 0:
            st.success(f"Prices: {price_info.get('count', 0):,}")
            st.caption(f"Last: {price_info.get('last_date', 'N/A')}")
        else:
            st.warning("Prices: No data")

        # Leaderboard
        lb_info = load_status.get("leaderboard", {})
        if lb_info.get("count", 0) > 0:
            st.success(f"Leaderboard: {lb_info.get('count', 0):,}")
            st.caption(f"Last: {lb_info.get('last_date', 'N/A')}")
        else:
            st.warning("Leaderboard: No data")

        st.divider()

        # Run status
        run_manager = st.session_state.run_manager
        if run_manager.is_running:
            st.error("Run in progress")
            st.caption(f"Mode: {run_manager.current_mode}")
            if st.button("Stop Run", use_container_width=True):
                run_manager.stop()
                st.rerun()
        else:
            st.success("Ready")

    # Main content - Quick overview
    st.markdown("""
    Welcome to the Fetcher Control Panel! Use the sidebar to navigate:

    - **Progress**: View loading progress and cursor status
    - **Workers**: Configure worker distribution and view rate statistics
    - **Run**: Start fetching runs for different data types
    """)

    # Quick stats in columns
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric(
            "Markets Loaded",
            f"{load_status.get('markets', {}).get('count', 0):,}",
            help="Total number of market records in parquet files"
        )

    with col2:
        st.metric(
            "Trades Loaded",
            f"{load_status.get('trades', {}).get('count', 0):,}",
            help="Total number of trade records in parquet files"
        )

    with col3:
        st.metric(
            "Prices Loaded",
            f"{load_status.get('prices', {}).get('count', 0):,}",
            help="Total number of price records in parquet files"
        )

    with col4:
        st.metric(
            "Leaderboard Entries",
            f"{load_status.get('leaderboard', {}).get('count', 0):,}",
            help="Total number of leaderboard records"
        )

    st.divider()

    # Cursor status
    st.subheader("Current Cursor Status")

    cursor_status = progress.get_cursor_status()

    if cursor_status.get("has_progress", False):
        st.info("There is pending progress that can be resumed.")

        cursor_cols = st.columns(4)

        with cursor_cols[0]:
            markets_cursor = cursor_status.get("markets", {})
            if not markets_cursor.get("is_empty", True):
                st.warning("Markets: In Progress")
                st.caption(f"Completed: {markets_cursor.get('completed', False)}")
            else:
                st.success("Markets: Complete")

        with cursor_cols[1]:
            trades_cursor = cursor_status.get("trades", {})
            if not trades_cursor.get("is_empty", True):
                pending = trades_cursor.get("pending_markets", 0)
                st.warning(f"Trades: {pending} pending")
            else:
                st.success("Trades: Complete")

        with cursor_cols[2]:
            prices_cursor = cursor_status.get("prices", {})
            if not prices_cursor.get("is_empty", True):
                pending = prices_cursor.get("pending_tokens", 0)
                st.warning(f"Prices: {pending} pending")
            else:
                st.success("Prices: Complete")

        with cursor_cols[3]:
            lb_cursor = cursor_status.get("leaderboard", {})
            if not lb_cursor.get("is_empty", True):
                st.warning("Leaderboard: In Progress")
                st.caption(f"Cat: {lb_cursor.get('category_index', 0)}, Period: {lb_cursor.get('time_period_index', 0)}")
            else:
                st.success("Leaderboard: Complete")

        st.caption(f"Last updated: {format_timestamp(cursor_status.get('last_updated', ''))}")
    else:
        st.success("No pending progress. All cursors are clear.")

    st.divider()

    # Quick actions
    st.subheader("Quick Actions")

    action_cols = st.columns(5)

    run_manager = st.session_state.run_manager

    with action_cols[0]:
        if st.button("Run Everything", use_container_width=True, disabled=run_manager.is_running):
            st.switch_page("pages/3_run.py")

    with action_cols[1]:
        if st.button("Run Markets", use_container_width=True, disabled=run_manager.is_running):
            st.switch_page("pages/3_run.py")

    with action_cols[2]:
        if st.button("Run Trades", use_container_width=True, disabled=run_manager.is_running):
            st.switch_page("pages/3_run.py")

    with action_cols[3]:
        if st.button("Run Leaderboard", use_container_width=True, disabled=run_manager.is_running):
            st.switch_page("pages/3_run.py")

    with action_cols[4]:
        if st.button("Configure Workers", use_container_width=True):
            st.switch_page("pages/2_workers.py")

    # Worker summary
    st.divider()
    st.subheader("Worker Configuration")

    config = st.session_state.config

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

    # Rate limit summary
    st.caption(f"Rate Limits: Trade={config.rate_limits.trade}/10s, Market={config.rate_limits.market}/10s, Price={config.rate_limits.price}/10s")


if __name__ == "__main__":
    main()
