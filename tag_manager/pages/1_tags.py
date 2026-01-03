"""
Tags Page - View and manage tags.
"""

import streamlit as st
from tag_manager.db import get_connection
from tag_manager.services import TagService, MarketService

st.set_page_config(page_title="Tags", page_icon="📋", layout="wide")


def init_state():
    if "db_conn" not in st.session_state:
        st.session_state.db_conn = get_connection()


def main():
    init_state()
    tag_service = TagService(st.session_state.db_conn)
    market_service = MarketService(st.session_state.db_conn)

    st.title("📋 Tags")

    # Create new tag section
    with st.expander("➕ Create New Tag", expanded=False):
        with st.form("create_tag"):
            name = st.text_input("Tag Name", placeholder="e.g., Crypto, Politics, Sports")
            description = st.text_area(
                "Description",
                placeholder="Describe what kind of markets belong to this tag..."
            )

            if st.form_submit_button("Create Tag", type="primary"):
                if not name:
                    st.error("Tag name is required")
                elif tag_service.get_tag_by_name(name):
                    st.error(f"Tag '{name}' already exists")
                else:
                    tag = tag_service.create_tag(name, description)
                    st.success(f"Created tag: {tag.name}")
                    st.rerun()

    st.divider()

    # List existing tags
    tags = tag_service.list_tags()

    if not tags:
        st.info("No tags yet. Create your first tag above!")
        return

    # Filter options
    col1, col2 = st.columns([2, 1])
    with col1:
        search = st.text_input("🔍 Search tags", placeholder="Type to filter...")
    with col2:
        show_inactive = st.checkbox("Show inactive tags", value=False)

    # Filter tags
    filtered_tags = tags
    if search:
        filtered_tags = [t for t in filtered_tags if search.lower() in t.name.lower()]
    if not show_inactive:
        filtered_tags = [t for t in filtered_tags if t.is_active]

    st.caption(f"Showing {len(filtered_tags)} of {len(tags)} tags")

    # Display tags
    for tag in filtered_tags:
        with st.container():
            col1, col2, col3, col4 = st.columns([3, 2, 2, 1])

            with col1:
                status = "🟢" if tag.is_active else "⚪"
                checked = "✅" if tag.all_checked else "🔄"
                st.markdown(f"### {status} {tag.name} {checked}")

                if tag.description:
                    st.markdown(tag.description)
                else:
                    st.caption("*No description*")

            with col2:
                st.metric("Training Examples", tag.example_count)
                st.caption(f"✅ {tag.positive_count} pos | ❌ {tag.negative_count} neg")

            with col3:
                # Get classification counts
                counts = market_service.get_classification_counts(tag.tag_id)
                total_classified = counts['positive'] + counts['negative']
                st.metric("Classified", total_classified)
                st.caption(f"✅ {counts['positive']} pos | ❌ {counts['negative']} neg")

            with col4:
                # Action buttons
                if st.button("✏️ Edit", key=f"edit_{tag.tag_id}"):
                    st.session_state[f"editing_{tag.tag_id}"] = True

                if tag.is_active:
                    if st.button("🚫 Deactivate", key=f"deact_{tag.tag_id}"):
                        tag_service.update_tag(tag.tag_id, is_active=False)
                        st.rerun()
                else:
                    if st.button("✅ Activate", key=f"act_{tag.tag_id}"):
                        tag_service.update_tag(tag.tag_id, is_active=True)
                        st.rerun()

            # Edit form (shown when editing)
            if st.session_state.get(f"editing_{tag.tag_id}", False):
                with st.form(f"edit_form_{tag.tag_id}"):
                    new_name = st.text_input("Name", value=tag.name)
                    new_desc = st.text_area("Description", value=tag.description or "")

                    col1, col2 = st.columns(2)
                    with col1:
                        if st.form_submit_button("💾 Save"):
                            tag_service.update_tag(
                                tag.tag_id,
                                name=new_name,
                                description=new_desc
                            )
                            st.session_state[f"editing_{tag.tag_id}"] = False
                            st.rerun()
                    with col2:
                        if st.form_submit_button("Cancel"):
                            st.session_state[f"editing_{tag.tag_id}"] = False
                            st.rerun()

        st.divider()


if __name__ == "__main__":
    main()
