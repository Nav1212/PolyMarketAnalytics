"""
History Page - View and edit recent classifications for fine-tuning.
"""

import streamlit as st
from tag_manager.db import get_connection
from tag_manager.services import TagService, JudgeService, MarketService

st.set_page_config(page_title="History", page_icon="📜", layout="wide")


def init_state():
    if "db_conn" not in st.session_state:
        st.session_state.db_conn = get_connection()


def main():
    init_state()
    tag_service = TagService(st.session_state.db_conn)
    judge_service = JudgeService(st.session_state.db_conn)
    market_service = MarketService(st.session_state.db_conn)

    st.title("📜 Classification History")
    st.markdown("Review and edit past classifications to improve future results.")

    # Search box
    search_query = st.text_input(
        "🔍 Search",
        placeholder="Search by market question...",
        key="history_search"
    )

    # Filters
    col1, col2, col3, col4, col5 = st.columns([2, 1, 1, 1, 1])

    with col1:
        tags = tag_service.list_tags()
        tag_options = [None] + tags
        selected_tag = st.selectbox(
            "Filter by Tag",
            options=tag_options,
            format_func=lambda t: "All tags" if t is None else t.name
        )

    with col2:
        available_categories = market_service.get_distinct_categories()
        category_options = ["All"] + available_categories
        selected_category = st.selectbox(
            "Category",
            options=category_options,
            index=0
        )

    with col3:
        decision_filter = st.selectbox(
            "Decision",
            options=["All", "Positive", "Negative", "Pending"],
            index=0
        )

    with col4:
        source_filter = st.selectbox(
            "Source",
            options=["All", "LLM Consensus", "Human"],
            index=0
        )

    with col5:
        limit = st.selectbox("Show", [10, 25, 50, 100], index=0)

    tag_id = selected_tag.tag_id if selected_tag else None

    # Get history
    history = judge_service.get_recent_history(
        tag_id=tag_id,
        limit=limit,
        search_query=search_query if search_query else None,
        decision_filter=decision_filter if decision_filter != "All" else None,
        source_filter=source_filter if source_filter != "All" else None,
        category_filter=selected_category if selected_category != "All" else None,
    )

    if not history:
        if search_query or decision_filter != "All" or source_filter != "All":
            st.info("No results match your filters. Try adjusting your search or filter criteria.")
        else:
            st.info("No classification history yet. Start judging markets!")
        return

    st.caption(f"Showing {len(history)} most recent classifications")

    # Display history
    for entry in history:
        with st.container():
            # Header with tag and status
            col1, col2, col3 = st.columns([3, 1, 1])

            with col1:
                st.markdown(f"### {entry.market_question}")
                st.caption(f"Tag: **{entry.tag_name}**")

            with col2:
                # Current decision
                if entry.human_decision is not None:
                    decision = entry.human_decision
                    source = "👤 Human"
                elif entry.consensus is not None:
                    decision = entry.consensus
                    source = f"🤖 {entry.decided_by}"
                else:
                    decision = None
                    source = "⏳ Pending"

                if decision is not None:
                    emoji = "✅" if decision else "❌"
                    st.markdown(f"**Decision:** {emoji}")
                else:
                    st.markdown("**Decision:** ⏳")
                st.caption(source)

            with col3:
                st.caption(f"Updated: {entry.updated_at.strftime('%Y-%m-%d %H:%M')}")

            # LLM votes detail
            with st.expander("View LLM votes"):
                for model, vote in entry.judge_votes.items():
                    emoji = "✅" if vote else "❌"
                    st.text(f"{model}: {emoji}")

            # Edit section
            if st.session_state.get(f"editing_hist_{entry.history_id}", False):
                st.markdown("**Edit Decision:**")
                col1, col2, col3 = st.columns([1, 1, 1])

                with col1:
                    if st.button("✅ Mark as Belongs", key=f"edit_yes_{entry.history_id}"):
                        judge_service.update_decision(entry.history_id, True)
                        st.session_state[f"editing_hist_{entry.history_id}"] = False
                        st.success("Updated!")
                        st.rerun()

                with col2:
                    if st.button("❌ Mark as Doesn't Belong", key=f"edit_no_{entry.history_id}"):
                        judge_service.update_decision(entry.history_id, False)
                        st.session_state[f"editing_hist_{entry.history_id}"] = False
                        st.success("Updated!")
                        st.rerun()

                with col3:
                    if st.button("Cancel", key=f"cancel_{entry.history_id}"):
                        st.session_state[f"editing_hist_{entry.history_id}"] = False
                        st.rerun()
            else:
                if st.button("✏️ Edit Decision", key=f"edit_btn_{entry.history_id}"):
                    st.session_state[f"editing_hist_{entry.history_id}"] = True
                    st.rerun()

            st.divider()

    # Summary stats
    st.subheader("Summary Statistics")

    col1, col2, col3, col4 = st.columns(4)

    total = len(history)
    consensus_count = len([h for h in history if h.consensus is not None and h.human_decision is None])
    human_count = len([h for h in history if h.human_decision is not None])
    pending_count = len([h for h in history if h.consensus is None and h.human_decision is None])

    with col1:
        st.metric("Total", total)
    with col2:
        st.metric("LLM Consensus", consensus_count)
    with col3:
        st.metric("Human Decided", human_count)
    with col4:
        st.metric("Pending Review", pending_count)

    # Fine-tuning info
    st.subheader("Fine-tuning Notes")
    st.markdown("""
    When you edit a decision, it:
    1. Updates the `JudgeHistory` record with your correction
    2. Updates the `MarketTagDim` to reflect the new decision
    3. These corrections help improve future LLM classifications by providing
       better training signal

    **Tip:** Focus on correcting cases where all LLM judges agreed but were wrong.
    These are the most valuable corrections for improving the models.
    """)


if __name__ == "__main__":
    main()
