"""
Examples Page - Manage tag examples (positive and negative).
"""

import streamlit as st
from tag_manager.db import get_connection
from tag_manager.services import TagService, MarketService

st.set_page_config(page_title="Examples", page_icon="📝", layout="wide")


def init_state():
    if "db_conn" not in st.session_state:
        st.session_state.db_conn = get_connection()


def main():
    init_state()
    tag_service = TagService(st.session_state.db_conn)
    market_service = MarketService(st.session_state.db_conn)

    st.title("📝 Tag Examples")
    st.markdown("Add example markets to train the LLM judges.")

    # Tag selection
    tags = tag_service.list_tags(active_only=True)

    if not tags:
        st.warning("No active tags. Create a tag first on the Tags page.")
        return

    selected_tag = st.selectbox(
        "Select Tag",
        options=tags,
        format_func=lambda t: f"{t.name} ({t.example_count} examples)"
    )

    if not selected_tag:
        return

    tag = tag_service.get_tag(selected_tag.tag_id)

    # Tag info
    st.info(f"**{tag.name}**: {tag.description or 'No description'}")

    # Two columns: search markets / view examples
    tab1, tab2 = st.tabs(["🔍 Add Examples", "📋 View Examples"])

    with tab1:
        st.subheader("Search Markets")

        search_query = st.text_input(
            "Search by question",
            placeholder="e.g., Bitcoin, election, World Cup..."
        )

        if search_query:
            markets = market_service.search_markets(search_query, limit=20)

            if not markets:
                st.info("No markets found. Try a different search term.")
            else:
                st.caption(f"Found {len(markets)} markets")

                for market in markets:
                    with st.container():
                        col1, col2 = st.columns([4, 1])

                        with col1:
                            st.markdown(f"**{market.question}**")
                            if market.description:
                                st.caption(market.description[:200] + "..." if len(market.description or "") > 200 else market.description)
                            if market.category:
                                st.caption(f"Category: {market.category}")

                        with col2:
                            # Check if already an example
                            examples = tag_service.get_examples(tag.tag_id)
                            is_example = any(e.market_id == market.market_id for e in examples)

                            if is_example:
                                existing = next(e for e in examples if e.market_id == market.market_id)
                                if existing.is_positive:
                                    st.success("✅ Positive")
                                else:
                                    st.error("❌ Negative")

                                if st.button("Remove", key=f"rem_{market.market_id}"):
                                    tag_service.remove_example(tag.tag_id, market.market_id)
                                    st.rerun()
                            else:
                                col_pos, col_neg = st.columns(2)
                                with col_pos:
                                    if st.button("✅", key=f"pos_{market.market_id}", help="Add as positive example"):
                                        tag_service.add_example(tag.tag_id, market.market_id, is_positive=True)
                                        st.rerun()
                                with col_neg:
                                    if st.button("❌", key=f"neg_{market.market_id}", help="Add as negative example"):
                                        tag_service.add_example(tag.tag_id, market.market_id, is_positive=False)
                                        st.rerun()

                        st.divider()

    with tab2:
        st.subheader("Current Examples")

        examples = tag_service.get_examples(tag.tag_id)

        if not examples:
            st.info("No examples yet. Search and add some markets!")
        else:
            # Separate positive and negative
            positive = [e for e in examples if e.is_positive]
            negative = [e for e in examples if not e.is_positive]

            col1, col2 = st.columns(2)

            with col1:
                st.markdown("### ✅ Positive Examples")
                st.caption(f"{len(positive)} markets that BELONG to this tag")

                for ex in positive:
                    with st.container():
                        st.markdown(f"**{ex.market_question}**")
                        if st.button("Remove", key=f"rem_pos_{ex.example_id}"):
                            tag_service.remove_example(tag.tag_id, ex.market_id)
                            st.rerun()
                        st.divider()

            with col2:
                st.markdown("### ❌ Negative Examples")
                st.caption(f"{len(negative)} markets that DO NOT belong to this tag")

                for ex in negative:
                    with st.container():
                        st.markdown(f"**{ex.market_question}**")
                        if st.button("Remove", key=f"rem_neg_{ex.example_id}"):
                            tag_service.remove_example(tag.tag_id, ex.market_id)
                            st.rerun()
                        st.divider()


if __name__ == "__main__":
    main()
