"""
Workers Page - Configure worker distribution and view rate statistics
"""

import streamlit as st
import sys
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from fetcher.config import get_config, save_config, Config, WorkersConfig, RateLimitsConfig

st.set_page_config(page_title="Workers", page_icon="W", layout="wide")


def init_session_state():
    """Initialize session state."""
    if "config" not in st.session_state:
        st.session_state.config = get_config()


def main():
    init_session_state()

    st.title("Worker Configuration")

    config = st.session_state.config

    # Worker distribution section
    st.subheader("Worker Distribution")

    st.markdown("""
    Configure how many worker threads are allocated to each fetcher type.
    More workers means faster fetching but higher API load.
    """)

    with st.form("worker_config"):
        cols = st.columns(5)

        with cols[0]:
            market_workers = st.number_input(
                "Market Workers",
                min_value=1,
                max_value=10,
                value=config.workers.market,
                help="Workers for fetching market data from CLOB API"
            )

        with cols[1]:
            trade_workers = st.number_input(
                "Trade Workers",
                min_value=1,
                max_value=10,
                value=config.workers.trade,
                help="Workers for fetching trade history"
            )

        with cols[2]:
            price_workers = st.number_input(
                "Price Workers",
                min_value=1,
                max_value=10,
                value=config.workers.price,
                help="Workers for fetching price history"
            )

        with cols[3]:
            leaderboard_workers = st.number_input(
                "Leaderboard Workers",
                min_value=1,
                max_value=5,
                value=config.workers.leaderboard,
                help="Workers for fetching leaderboard data"
            )

        with cols[4]:
            gamma_workers = st.number_input(
                "Gamma Workers",
                min_value=1,
                max_value=5,
                value=config.workers.gamma_market,
                help="Workers for fetching Gamma API data"
            )

        submitted = st.form_submit_button("Save Worker Configuration", use_container_width=True)

        if submitted:
            config.workers.market = market_workers
            config.workers.trade = trade_workers
            config.workers.price = price_workers
            config.workers.leaderboard = leaderboard_workers
            config.workers.gamma_market = gamma_workers

            save_config(config)
            st.session_state.config = config
            st.success("Worker configuration saved!")

    st.divider()

    # Rate limits section
    st.subheader("Rate Limits")

    st.markdown("""
    Configure API rate limits (requests per window). These control how fast
    each API can be called to avoid hitting server-side rate limits.
    """)

    with st.form("rate_config"):
        rate_cols = st.columns(5)

        with rate_cols[0]:
            trade_rate = st.number_input(
                "Trade API Rate",
                min_value=10,
                max_value=200,
                value=config.rate_limits.trade,
                help="Requests per window for Trade API"
            )

        with rate_cols[1]:
            market_rate = st.number_input(
                "Market API Rate",
                min_value=10,
                max_value=200,
                value=config.rate_limits.market,
                help="Requests per window for Market API"
            )

        with rate_cols[2]:
            price_rate = st.number_input(
                "Price API Rate",
                min_value=10,
                max_value=200,
                value=config.rate_limits.price,
                help="Requests per window for Price API"
            )

        with rate_cols[3]:
            leaderboard_rate = st.number_input(
                "Leaderboard API Rate",
                min_value=10,
                max_value=200,
                value=config.rate_limits.leaderboard,
                help="Requests per window for Leaderboard API"
            )

        with rate_cols[4]:
            window_seconds = st.number_input(
                "Window (seconds)",
                min_value=1.0,
                max_value=60.0,
                value=config.rate_limits.window_seconds,
                step=1.0,
                help="Time window for rate limiting"
            )

        rate_submitted = st.form_submit_button("Save Rate Limits", use_container_width=True)

        if rate_submitted:
            config.rate_limits.trade = trade_rate
            config.rate_limits.market = market_rate
            config.rate_limits.price = price_rate
            config.rate_limits.leaderboard = leaderboard_rate
            config.rate_limits.window_seconds = window_seconds

            save_config(config)
            st.session_state.config = config
            st.success("Rate limits saved!")

    st.divider()

    # Effective rates display
    st.subheader("Effective Request Rates")

    st.markdown("Combined throughput based on workers and rate limits:")

    eff_cols = st.columns(4)

    with eff_cols[0]:
        trade_eff = (config.rate_limits.trade / config.rate_limits.window_seconds)
        st.metric(
            "Trade Requests/sec",
            f"{trade_eff:.1f}",
            help=f"{config.workers.trade} workers sharing {config.rate_limits.trade} requests/{config.rate_limits.window_seconds}s"
        )

    with eff_cols[1]:
        market_eff = (config.rate_limits.market / config.rate_limits.window_seconds)
        st.metric(
            "Market Requests/sec",
            f"{market_eff:.1f}",
            help=f"{config.workers.market} workers sharing {config.rate_limits.market} requests/{config.rate_limits.window_seconds}s"
        )

    with eff_cols[2]:
        price_eff = (config.rate_limits.price / config.rate_limits.window_seconds)
        st.metric(
            "Price Requests/sec",
            f"{price_eff:.1f}",
            help=f"{config.workers.price} workers sharing {config.rate_limits.price} requests/{config.rate_limits.window_seconds}s"
        )

    with eff_cols[3]:
        lb_eff = (config.rate_limits.leaderboard / config.rate_limits.window_seconds)
        st.metric(
            "Leaderboard Requests/sec",
            f"{lb_eff:.1f}",
            help=f"{config.workers.leaderboard} workers sharing {config.rate_limits.leaderboard} requests/{config.rate_limits.window_seconds}s"
        )

    st.divider()

    # Queue thresholds section
    st.subheader("Queue Thresholds")

    st.markdown("""
    Configure when data is flushed to parquet files. Higher thresholds mean
    fewer but larger writes.
    """)

    with st.form("queue_config"):
        queue_cols = st.columns(4)

        with queue_cols[0]:
            trade_threshold = st.number_input(
                "Trade Queue",
                min_value=1000,
                max_value=100000,
                value=config.queues.trade_threshold,
                step=1000,
                help="Items before flush to parquet"
            )

        with queue_cols[1]:
            market_threshold = st.number_input(
                "Market Queue",
                min_value=100,
                max_value=10000,
                value=config.queues.market_threshold,
                step=100,
                help="Items before flush to parquet"
            )

        with queue_cols[2]:
            price_threshold = st.number_input(
                "Price Queue",
                min_value=1000,
                max_value=100000,
                value=config.queues.price_threshold,
                step=1000,
                help="Items before flush to parquet"
            )

        with queue_cols[3]:
            leaderboard_threshold = st.number_input(
                "Leaderboard Queue",
                min_value=1000,
                max_value=50000,
                value=config.queues.leaderboard_threshold,
                step=1000,
                help="Items before flush to parquet"
            )

        queue_submitted = st.form_submit_button("Save Queue Thresholds", use_container_width=True)

        if queue_submitted:
            config.queues.trade_threshold = trade_threshold
            config.queues.market_threshold = market_threshold
            config.queues.price_threshold = price_threshold
            config.queues.leaderboard_threshold = leaderboard_threshold

            save_config(config)
            st.session_state.config = config
            st.success("Queue thresholds saved!")

    st.divider()

    # Current config summary
    st.subheader("Current Configuration Summary")

    with st.expander("View Full Configuration"):
        config_dict = config.to_dict()

        st.json(config_dict)

    # Reset to defaults button
    st.divider()

    if st.button("Reset to Defaults", type="secondary"):
        default_config = Config()
        save_config(default_config)
        st.session_state.config = default_config
        st.success("Configuration reset to defaults!")
        st.rerun()


if __name__ == "__main__":
    main()
