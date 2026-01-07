"""
Progress Page - View loading progress and data status
"""

import streamlit as st
import sys
from pathlib import Path
from datetime import datetime

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from fetcher_gui.services.progress_tracker import ProgressTracker

st.set_page_config(page_title="Progress", page_icon="P", layout="wide")


def init_session_state():
    """Initialize session state."""
    if "progress_tracker" not in st.session_state:
        st.session_state.progress_tracker = ProgressTracker()


def format_count(count: int) -> str:
    """Format a count with commas."""
    return f"{count:,}"


def format_date(date_str: str) -> str:
    """Format an ISO date for display."""
    if not date_str:
        return "N/A"
    return date_str


def main():
    init_session_state()

    st.title("Loading Progress")

    # Refresh button
    col1, col2 = st.columns([6, 1])
    with col2:
        if st.button("Refresh", use_container_width=True):
            st.session_state.progress_tracker.refresh()
            st.rerun()

    progress = st.session_state.progress_tracker

    # Last load date
    last_load = progress.get_last_load_date()
    st.info(f"**Last Data Load:** {format_date(last_load)}")

    st.divider()

    # Data counts section
    st.subheader("Data Counts")

    load_status = progress.get_load_status()

    # Main data types
    cols = st.columns(4)

    data_types = [
        ("markets", "Markets", "Primary market data from CLOB API"),
        ("trades", "Trades", "Trade history for markets"),
        ("prices", "Prices", "Price history for tokens"),
        ("leaderboard", "Leaderboard", "Leaderboard rankings"),
    ]

    for i, (key, label, help_text) in enumerate(data_types):
        with cols[i]:
            info = load_status.get(key, {})
            count = info.get("count", 0)
            files = info.get("file_count", 0)
            last_date = info.get("last_date")

            st.metric(
                label=label,
                value=format_count(count),
                help=help_text
            )
            st.caption(f"{files} files | Last: {format_date(last_date)}")

    st.divider()

    # Secondary data types
    st.subheader("Related Data")

    secondary_cols = st.columns(4)

    secondary_types = [
        ("market_tokens", "Market Tokens", "Token mappings for markets"),
        ("gamma_markets", "Gamma Markets", "Extended market data from Gamma API"),
        ("gamma_events", "Gamma Events", "Events from Gamma API"),
        ("gamma_categories", "Gamma Categories", "Categories from Gamma API"),
    ]

    for i, (key, label, help_text) in enumerate(secondary_types):
        with secondary_cols[i]:
            info = load_status.get(key, {})
            count = info.get("count", 0)
            files = info.get("file_count", 0)

            st.metric(
                label=label,
                value=format_count(count),
                help=help_text
            )
            st.caption(f"{files} files")

    st.divider()

    # Cursor status section
    st.subheader("Cursor Status (Resume Points)")

    cursor_status = progress.get_cursor_status()

    if cursor_status.get("has_progress"):
        st.warning("There is pending progress that can be resumed. Run the fetcher to continue from these positions.")
    else:
        st.success("All cursors are clear. No pending work.")

    # Cursor details in a detailed table format
    st.markdown("### Current Resume Positions")

    # Markets cursor
    markets = cursor_status.get("markets", {})
    with st.container(border=True):
        st.markdown("**Markets Cursor**")
        if markets.get("is_empty"):
            st.success("Status: Complete - No pending work")
        elif markets.get("completed"):
            st.success("Status: Completed")
        else:
            st.warning("Status: In Progress")
            col1, col2 = st.columns(2)
            with col1:
                st.text(f"Next Cursor: {markets.get('next_cursor', 'N/A')}")
            with col2:
                st.text(f"Completed: {markets.get('completed', False)}")

    # Trades cursor
    trades = cursor_status.get("trades", {})
    with st.container(border=True):
        st.markdown("**Trades Cursor**")
        if trades.get("is_empty"):
            st.success("Status: Complete - No pending work")
        else:
            pending = trades.get("pending_markets", 0)
            st.warning(f"Status: In Progress - {pending} markets pending")
            col1, col2, col3 = st.columns(3)
            with col1:
                st.text(f"Current Market: {trades.get('current_market', 'N/A')}")
            with col2:
                st.text(f"Offset: {trades.get('offset', 0)}")
            with col3:
                st.text(f"Filter Amount: {trades.get('filter_amount', 0)}")

    # Prices cursor
    prices = cursor_status.get("prices", {})
    with st.container(border=True):
        st.markdown("**Prices Cursor**")
        if prices.get("is_empty"):
            st.success("Status: Complete - No pending work")
        elif prices.get("completed"):
            st.success("Status: Completed")
        else:
            pending = prices.get("pending_tokens", 0)
            st.warning(f"Status: In Progress - {pending} tokens pending")
            col1, col2 = st.columns(2)
            with col1:
                st.text(f"Current Token: {prices.get('current_token', 'N/A')}")
            with col2:
                st.text(f"Pending Tokens: {pending}")

    # Leaderboard cursor
    lb = cursor_status.get("leaderboard", {})
    with st.container(border=True):
        st.markdown("**Leaderboard Cursor**")
        if lb.get("is_empty"):
            st.success("Status: Complete - No pending work")
        elif lb.get("completed"):
            st.success("Status: Completed")
        else:
            st.warning("Status: In Progress")
            col1, col2, col3 = st.columns(3)
            with col1:
                st.text(f"Category Index: {lb.get('category_index', 0)} / 10")
            with col2:
                st.text(f"Time Period Index: {lb.get('time_period_index', 0)} / 4")
            with col3:
                st.text(f"Offset: {lb.get('offset', 0)}")
            st.caption("Categories: OVERALL, POLITICS, SPORTS, CRYPTO, CULTURE, MENTIONS, WEATHER, ECONOMICS, TECH, FINANCE")
            st.caption("Periods: DAY, WEEK, MONTH, ALL")

    # Gamma cursor
    gamma = cursor_status.get("gamma_markets", {})
    with st.container(border=True):
        st.markdown("**Gamma Markets Cursor**")
        if gamma.get("is_empty"):
            st.info("Status: Not started")
        elif gamma.get("completed"):
            st.success("Status: Completed")
        else:
            st.warning("Status: In Progress")

    # Last updated
    last_updated = cursor_status.get("last_updated")
    if last_updated:
        try:
            dt = datetime.fromisoformat(last_updated)
            st.caption(f"Cursors last updated: {dt.strftime('%Y-%m-%d %H:%M:%S')}")
        except:
            st.caption(f"Cursors last updated: {last_updated}")

    st.divider()

    # Data directory info
    st.subheader("Data Directory")

    data_dir = PROJECT_ROOT / "data"
    if data_dir.exists():
        st.success(f"Data directory: `{data_dir}`")

        # Show directory sizes
        with st.expander("Directory Details"):
            for subdir in sorted(data_dir.iterdir()):
                if subdir.is_dir():
                    # Count files and calculate size
                    files = list(subdir.rglob("*.parquet"))
                    total_size = sum(f.stat().st_size for f in files)
                    size_mb = total_size / (1024 * 1024)
                    st.text(f"{subdir.name}: {len(files)} files, {size_mb:.1f} MB")
    else:
        st.warning(f"Data directory not found: `{data_dir}`")


if __name__ == "__main__":
    main()
