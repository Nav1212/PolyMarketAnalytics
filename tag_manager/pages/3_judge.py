"""
Judge Page - Manual review of markets that need human decision.
"""

import streamlit as st
from tag_manager.db import get_connection
from tag_manager.services import TagService, MarketService, JudgeService
from tag_manager.llm import JudgePool, OllamaClient

st.set_page_config(page_title="Judge", page_icon="⚖️", layout="wide")


def init_state():
    if "db_conn" not in st.session_state:
        st.session_state.db_conn = get_connection()
    if "ollama_available" not in st.session_state:
        client = OllamaClient()
        st.session_state.ollama_available = client.is_available()
        client.close()


def main():
    init_state()
    tag_service = TagService(st.session_state.db_conn)
    market_service = MarketService(st.session_state.db_conn)
    judge_service = JudgeService(st.session_state.db_conn)

    st.title("⚖️ Judge Markets")

    # Check Ollama status
    if not st.session_state.ollama_available:
        st.warning("⚠️ Ollama is not running. LLM classification won't work.")
        st.caption("Start Ollama with `ollama serve` to enable automatic classification.")

    # Tag selection
    tags = tag_service.list_tags(active_only=True)

    if not tags:
        st.warning("No active tags. Create a tag first.")
        return

    selected_tag = st.selectbox(
        "Select Tag to Judge",
        options=tags,
        format_func=lambda t: f"{t.name} ({t.example_count} examples)"
    )

    if not selected_tag:
        return

    tag = tag_service.get_tag(selected_tag.tag_id)
    st.info(f"**{tag.name}**: {tag.description or 'No description'}")

    # Show example counts
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Positive Examples", tag.positive_count)
    with col2:
        st.metric("Negative Examples", tag.negative_count)
    with col3:
        tagged = market_service.count_tagged_markets(tag.tag_id)
        st.metric("Tagged Markets", tagged)

    if tag.example_count < 2:
        st.warning("⚠️ Add at least 2 examples (1 positive, 1 negative) before judging.")
        st.page_link("pages/2_examples.py", label="Go to Examples page")
        return

    st.divider()

    # Mode selection
    mode = st.radio(
        "Mode",
        ["🤖 Auto-classify (LLM)", "👤 Manual review only"],
        horizontal=True
    )

    # Get markets to judge
    tab1, tab2 = st.tabs(["📥 Pending Review", "🆕 New Markets"])

    with tab1:
        st.subheader("Markets Needing Human Review")
        st.caption("These markets had no consensus from LLM judges")

        pending = judge_service.get_pending_reviews(tag_id=tag.tag_id, limit=10)

        if not pending:
            st.success("No pending reviews! All caught up.")
        else:
            for entry in pending:
                with st.container():
                    st.markdown(f"### {entry.market_question}")
                    if entry.market_description:
                        st.caption(entry.market_description[:300])

                    # Show votes
                    st.markdown("**LLM Votes:**")
                    for model, vote in entry.judge_votes.items():
                        emoji = "✅" if vote else "❌"
                        st.text(f"  {model}: {emoji}")

                    # Decision buttons
                    col1, col2, col3 = st.columns([1, 1, 2])
                    with col1:
                        if st.button("✅ Belongs", key=f"yes_{entry.history_id}"):
                            judge_service.record_human_decision(entry.history_id, True)
                            st.success("Marked as belonging to tag")
                            st.rerun()
                    with col2:
                        if st.button("❌ Doesn't belong", key=f"no_{entry.history_id}"):
                            judge_service.record_human_decision(entry.history_id, False)
                            st.success("Marked as NOT belonging to tag")
                            st.rerun()

                    st.divider()

    with tab2:
        st.subheader("Classify New Markets")

        # Get unprocessed markets
        markets = market_service.get_markets_for_tagging(
            tag_id=tag.tag_id,
            limit=5,
            after_market_id=tag.last_checked_market_id
        )

        if not markets:
            st.success("🎉 All markets have been processed for this tag!")
            tag_service.mark_all_checked(tag.tag_id, True)
        else:
            st.caption(f"Found {len(markets)} new markets to classify")

            for market in markets:
                with st.container():
                    st.markdown(f"### {market.question}")
                    if market.description:
                        st.caption(market.description[:300])
                    if market.category:
                        st.text(f"Category: {market.category}")

                    if mode == "🤖 Auto-classify (LLM)":
                        col1, col2 = st.columns([1, 3])
                        with col1:
                            if st.button("🤖 Classify", key=f"classify_{market.market_id}"):
                                with st.spinner("Asking LLM judges..."):
                                    try:
                                        result = judge_service.classify_market(
                                            tag.tag_id,
                                            market.market_id
                                        )
                                        if result.consensus is not None:
                                            emoji = "✅" if result.consensus else "❌"
                                            st.success(f"Consensus: {emoji} ({result.decided_by})")
                                        else:
                                            st.warning("No consensus - needs human review")
                                        st.rerun()
                                    except Exception as e:
                                        st.error(f"Error: {e}")
                        with col2:
                            st.caption("Or manually decide:")
                            c1, c2 = st.columns(2)
                            with c1:
                                if st.button("✅ Yes", key=f"man_yes_{market.market_id}"):
                                    tag_service.add_example(tag.tag_id, market.market_id, True)
                                    st.rerun()
                            with c2:
                                if st.button("❌ No", key=f"man_no_{market.market_id}"):
                                    tag_service.add_example(tag.tag_id, market.market_id, False)
                                    st.rerun()
                    else:
                        # Manual only mode
                        col1, col2, col3 = st.columns([1, 1, 2])
                        with col1:
                            if st.button("✅ Belongs", key=f"yes_{market.market_id}"):
                                tag_service.add_example(tag.tag_id, market.market_id, True)
                                st.rerun()
                        with col2:
                            if st.button("❌ Doesn't", key=f"no_{market.market_id}"):
                                tag_service.add_example(tag.tag_id, market.market_id, False)
                                st.rerun()

                    st.divider()

            # Batch classify button
            if mode == "🤖 Auto-classify (LLM)" and st.session_state.ollama_available:
                st.divider()
                if st.button("🚀 Auto-classify all visible markets", type="primary"):
                    progress = st.progress(0)
                    for i, market in enumerate(markets):
                        with st.spinner(f"Classifying {i+1}/{len(markets)}..."):
                            try:
                                judge_service.classify_market(tag.tag_id, market.market_id)
                            except Exception as e:
                                st.error(f"Error on {market.question[:50]}: {e}")
                        progress.progress((i + 1) / len(markets))
                    st.success("Done!")
                    st.rerun()


if __name__ == "__main__":
    main()
