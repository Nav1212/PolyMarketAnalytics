"""
Tag Manager - Streamlit Application

Main entry point for the tag management UI.

Run with:
    streamlit run tag_manager/app.py
"""

import streamlit as st

# Page config must be first Streamlit command
st.set_page_config(
    page_title="Tag Manager",
    page_icon="🏷️",
    layout="wide",
    initial_sidebar_state="expanded",
)

from pathlib import Path
import duckdb

from tag_manager.db import get_connection, DEFAULT_DB_PATH
from tag_manager.services import TagService, MarketService, BackgroundClassifier
from tag_manager.llm import OllamaClient


def init_session_state():
    """Initialize session state variables."""
    if "db_conn" not in st.session_state:
        st.session_state.db_conn = get_connection()

    if "ollama_available" not in st.session_state:
        client = OllamaClient()
        # Auto-start Ollama if not running
        st.session_state.ollama_available = client.ensure_running(max_wait=30.0)
        st.session_state.available_models = client.list_models() if st.session_state.ollama_available else []
        client.close()

    # Start background classifier if Ollama is available
    if "background_classifier" not in st.session_state:
        if st.session_state.ollama_available:
            classifier = BackgroundClassifier(
                db_path=str(DEFAULT_DB_PATH),
                poll_interval=30,
                batch_size=5,
            )
            classifier.start()
            st.session_state.background_classifier = classifier
        else:
            st.session_state.background_classifier = None


def main():
    """Main application entry point."""
    init_session_state()

    st.title("🏷️ Tag Manager")

    # Sidebar status
    with st.sidebar:
        st.header("System Status")

        # Database status
        try:
            count = st.session_state.db_conn.execute(
                "SELECT COUNT(*) FROM MarketDim"
            ).fetchone()[0]
            st.success(f"✅ Database: {count:,} markets")
        except Exception as e:
            st.error(f"❌ Database error: {e}")

        # Ollama status
        if st.session_state.ollama_available:
            models = st.session_state.available_models
            st.success(f"✅ Ollama: {len(models)} models")
            with st.expander("Available models"):
                for m in models:
                    st.text(m)
        else:
            st.warning("⚠️ Ollama not available")
            st.caption("Run `ollama serve` to enable LLM features")

        # Background classifier status
        classifier = st.session_state.get("background_classifier")
        if classifier and classifier.is_running:
            st.success("✅ Auto-classifier running")
            st.caption(f"Classified: {classifier.markets_classified}")
            if classifier.last_run:
                st.caption(f"Last run: {classifier.last_run.strftime('%H:%M:%S')}")
        elif st.session_state.ollama_available:
            st.warning("⚠️ Auto-classifier stopped")

        st.divider()

        # Quick stats
        tag_service = TagService(st.session_state.db_conn)
        tags = tag_service.list_tags()
        st.metric("Total Tags", len(tags))
        st.metric("Active Tags", len([t for t in tags if t.is_active]))

    # Main content
    st.markdown("""
    Welcome to the Tag Manager! Use the sidebar to navigate between pages:

    - **📋 Tags**: View, create, and manage tags
    - **📝 Examples**: Add example markets for training
    - **⚖️ Judge**: Review markets that need human decision
    - **📜 History**: View and edit recent classifications
    - **⚙️ Settings**: Configure LLM models and classification settings
    """)

    # Quick actions
    st.subheader("Quick Actions")

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        if st.button("➕ Create New Tag", use_container_width=True):
            st.switch_page("pages/1_tags.py")

    with col2:
        pending = len([t for t in tags if not t.all_checked])
        if st.button(f"⚖️ Review Pending ({pending})", use_container_width=True):
            st.switch_page("pages/3_judge.py")

    with col3:
        if st.button("📊 View History", use_container_width=True):
            st.switch_page("pages/4_history.py")

    with col4:
        if st.button("⚙️ Settings", use_container_width=True):
            st.switch_page("pages/5_settings.py")

    # Tags overview
    if tags:
        st.subheader("Tags Overview")

        for tag in tags[:5]:  # Show first 5 tags
            with st.container():
                col1, col2, col3, col4 = st.columns([3, 1, 1, 1])

                with col1:
                    status = "🟢" if tag.is_active else "⚪"
                    st.markdown(f"**{status} {tag.name}**")
                    if tag.description:
                        st.caption(tag.description[:100])

                with col2:
                    st.metric("Examples", tag.example_count)

                with col3:
                    st.metric("Positive", tag.positive_count)

                with col4:
                    st.metric("Negative", tag.negative_count)

        if len(tags) > 5:
            st.caption(f"...and {len(tags) - 5} more tags. See Tags page for full list.")


if __name__ == "__main__":
    main()
