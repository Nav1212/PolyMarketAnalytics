"""
Settings Page - Configure LLM models and classification settings.
"""

import streamlit as st
import time
from tag_manager.db import get_connection
from tag_manager.llm import OllamaClient
from tag_manager.services import SettingsService

st.set_page_config(page_title="Settings", page_icon="⚙️", layout="wide")

# Default models that work well for classification
RECOMMENDED_MODELS = [
    ("llama3.2:latest", "Fast, good for classification tasks"),
    ("llama3.1:latest", "Larger, more accurate"),
    ("mistral:latest", "Good balance of speed and accuracy"),
    ("phi3:latest", "Small and fast, Microsoft model"),
    ("gemma2:latest", "Google's efficient model"),
    ("qwen2.5:latest", "Alibaba's multilingual model"),
]


def init_state():
    if "db_conn" not in st.session_state:
        st.session_state.db_conn = get_connection()
    if "ollama_client" not in st.session_state:
        st.session_state.ollama_client = OllamaClient()
    if "settings_service" not in st.session_state:
        st.session_state.settings_service = SettingsService(st.session_state.db_conn)
    if "selected_models" not in st.session_state:
        # Load from persistent storage first, then fall back to installed models
        settings = st.session_state.settings_service
        saved_models = settings.get_selected_models()
        if saved_models:
            st.session_state.selected_models = saved_models
        else:
            client = st.session_state.ollama_client
            installed = client.list_models()
            st.session_state.selected_models = installed[:3] if installed else []
    # Load other settings from persistent storage
    if "require_unanimous" not in st.session_state:
        settings = st.session_state.settings_service
        st.session_state.require_unanimous = settings.get_require_unanimous()
    if "min_votes_for_majority" not in st.session_state:
        settings = st.session_state.settings_service
        st.session_state.min_votes_for_majority = settings.get_min_votes_for_majority()
    # Load parallelization settings
    if "parallel_workers" not in st.session_state:
        settings = st.session_state.settings_service
        st.session_state.parallel_workers = settings.get_parallel_workers()
    if "ollama_concurrency" not in st.session_state:
        settings = st.session_state.settings_service
        st.session_state.ollama_concurrency = settings.get_ollama_concurrency()


def refresh_models():
    """Refresh the list of installed models."""
    client = st.session_state.ollama_client
    return client.list_models()


def save_selected_models(models: list[str]):
    """Save selected models to both session state and persistent storage."""
    st.session_state.selected_models = models
    st.session_state.settings_service.set_selected_models(models)


def save_consensus_settings(require_unanimous: bool, min_votes: int):
    """Save consensus settings to both session state and persistent storage."""
    st.session_state.require_unanimous = require_unanimous
    st.session_state.min_votes_for_majority = min_votes
    st.session_state.settings_service.set_require_unanimous(require_unanimous)
    st.session_state.settings_service.set_min_votes_for_majority(min_votes)


def save_parallelization_settings(parallel_workers: int, ollama_concurrency: int):
    """Save parallelization settings to both session state and persistent storage."""
    st.session_state.parallel_workers = parallel_workers
    st.session_state.ollama_concurrency = ollama_concurrency
    st.session_state.settings_service.set_parallel_workers(parallel_workers)
    st.session_state.settings_service.set_ollama_concurrency(ollama_concurrency)


def install_model(model_name: str) -> bool:
    """Install a model using ollama pull."""
    import subprocess
    try:
        result = subprocess.run(
            ["ollama", "pull", model_name],
            capture_output=True,
            text=True,
            timeout=600,  # 10 minute timeout for large models
        )
        return result.returncode == 0
    except Exception:
        return False


def uninstall_model(model_name: str) -> bool:
    """Remove a model using ollama rm."""
    import subprocess
    try:
        result = subprocess.run(
            ["ollama", "rm", model_name],
            capture_output=True,
            text=True,
            timeout=60,
        )
        return result.returncode == 0
    except Exception:
        return False


def main():
    init_state()
    client = st.session_state.ollama_client

    st.title("⚙️ Settings")

    # Check Ollama status
    ollama_running = client.ensure_running(max_wait=10.0)

    if not ollama_running:
        st.error("Ollama is not running and couldn't be started automatically.")
        st.caption("Please install Ollama from https://ollama.ai and run `ollama serve`")
        return

    st.success("✅ Ollama is running")

    # Tabs for different settings
    tab1, tab2, tab3 = st.tabs(["📦 Installed Models", "⬇️ Install Models", "🎛️ Classification Settings"])

    with tab1:
        st.subheader("Installed Models")
        st.caption("These models are available on your system")

        installed_models = refresh_models()

        if not installed_models:
            st.warning("No models installed. Go to 'Install Models' tab to add some.")
        else:
            # Show which models are selected for classification
            selected = st.session_state.get("selected_models", [])

            for model in installed_models:
                col1, col2, col3 = st.columns([3, 1, 1])

                with col1:
                    is_selected = model in selected
                    icon = "✅" if is_selected else "⬜"
                    st.markdown(f"**{icon} {model}**")

                with col2:
                    if model in selected:
                        if st.button("Deselect", key=f"deselect_{model}"):
                            selected.remove(model)
                            save_selected_models(selected)
                            st.rerun()
                    else:
                        if st.button("Select", key=f"select_{model}"):
                            selected.append(model)
                            save_selected_models(selected)
                            st.rerun()

                with col3:
                    if st.button("🗑️ Remove", key=f"remove_{model}"):
                        with st.spinner(f"Removing {model}..."):
                            if uninstall_model(model):
                                if model in selected:
                                    selected.remove(model)
                                    save_selected_models(selected)
                                st.success(f"Removed {model}")
                                st.rerun()
                            else:
                                st.error(f"Failed to remove {model}")

            st.divider()

            # Show selected models for classification
            st.subheader("Models Used for Classification")
            if selected:
                st.info(f"Using {len(selected)} model(s): {', '.join(selected)}")
                st.caption("Classification requires consensus from these models")
            else:
                st.warning("No models selected! Select at least one model above.")

    with tab2:
        st.subheader("Install New Models")
        st.caption("Download models from Ollama's library")

        installed_models = refresh_models()

        # Recommended models
        st.markdown("### Recommended Models")

        for model_name, description in RECOMMENDED_MODELS:
            col1, col2, col3 = st.columns([2, 2, 1])

            with col1:
                # Check if already installed (handle version tags)
                base_name = model_name.split(":")[0]
                is_installed = any(base_name in m for m in installed_models)

                if is_installed:
                    st.markdown(f"**✅ {model_name}**")
                else:
                    st.markdown(f"**{model_name}**")

            with col2:
                st.caption(description)

            with col3:
                if is_installed:
                    st.success("Installed")
                else:
                    if st.button("Install", key=f"install_{model_name}"):
                        progress_placeholder = st.empty()
                        with progress_placeholder:
                            with st.spinner(f"Installing {model_name}... This may take several minutes."):
                                if install_model(model_name):
                                    st.success(f"Installed {model_name}!")
                                    # Auto-select newly installed model
                                    selected = st.session_state.get("selected_models", [])
                                    new_models = refresh_models()
                                    for m in new_models:
                                        if base_name in m and m not in selected:
                                            selected.append(m)
                                            save_selected_models(selected)
                                            break
                                    time.sleep(1)
                                    st.rerun()
                                else:
                                    st.error(f"Failed to install {model_name}")

        st.divider()

        # Custom model input
        st.markdown("### Install Custom Model")
        st.caption("Enter any model name from [Ollama's library](https://ollama.ai/library)")

        col1, col2 = st.columns([3, 1])
        with col1:
            custom_model = st.text_input(
                "Model name",
                placeholder="e.g., codellama:7b, neural-chat:latest",
                label_visibility="collapsed"
            )
        with col2:
            if st.button("Install", key="install_custom", disabled=not custom_model):
                with st.spinner(f"Installing {custom_model}..."):
                    if install_model(custom_model):
                        st.success(f"Installed {custom_model}!")
                        time.sleep(1)
                        st.rerun()
                    else:
                        st.error(f"Failed to install {custom_model}")

    with tab3:
        st.subheader("Classification Settings")

        # Current selected models
        selected = st.session_state.get("selected_models", [])

        st.markdown("### Active Models")
        if selected:
            for i, model in enumerate(selected):
                col1, col2 = st.columns([4, 1])
                with col1:
                    st.text(f"{i+1}. {model}")
                with col2:
                    if st.button("Remove", key=f"cls_remove_{model}"):
                        selected.remove(model)
                        save_selected_models(selected)
                        st.rerun()
        else:
            st.warning("No models selected for classification")

        st.divider()

        # Consensus settings
        st.markdown("### Consensus Settings")

        # Load current values from session state (already loaded from DB in init_state)
        current_unanimous = st.session_state.get("require_unanimous", False)
        current_min_votes = st.session_state.get("min_votes_for_majority", 2)

        consensus_mode = st.radio(
            "Consensus Mode",
            ["Majority Vote", "Unanimous"],
            index=1 if current_unanimous else 0,
            help="Majority: 2+ models must agree. Unanimous: All models must agree."
        )

        require_unanimous = (consensus_mode == "Unanimous")

        min_votes = st.slider(
            "Minimum votes for majority",
            min_value=1,
            max_value=max(3, len(selected)),
            value=current_min_votes,
            help="How many models must agree for a majority decision"
        )

        # Save button for consensus settings
        if st.button("💾 Save Consensus Settings"):
            save_consensus_settings(require_unanimous, min_votes)
            st.success("Consensus settings saved!")

        st.divider()

        # Parallelization settings
        st.markdown("### Parallelization Settings")
        st.caption("Control how many classifications run in parallel")

        # Load current values from session state
        current_parallel_workers = st.session_state.get("parallel_workers", 2)
        current_ollama_concurrency = st.session_state.get("ollama_concurrency", 2)

        parallel_workers = st.slider(
            "Parallel Workers",
            min_value=1,
            max_value=8,
            value=current_parallel_workers,
            help="Number of worker threads processing markets in parallel. Higher = faster but more resource usage."
        )

        ollama_concurrency = st.slider(
            "Ollama GPU Concurrency",
            min_value=1,
            max_value=4,
            value=current_ollama_concurrency,
            help="Maximum concurrent requests to Ollama. Limited by GPU memory. Start with 2, increase if you have a powerful GPU."
        )

        # Save button for parallelization settings
        if st.button("💾 Save Parallelization Settings"):
            save_parallelization_settings(parallel_workers, ollama_concurrency)
            st.success("Parallelization settings saved! Restart background classifier to apply.")

        st.divider()

        # Test classification
        st.markdown("### Test Classification")
        st.caption("Test if your models are working correctly")

        test_prompt = st.text_area(
            "Test prompt",
            value="Is this a market about cryptocurrency? Answer YES or NO.\n\nMarket: Will Bitcoin reach $100,000 by end of 2025?",
            height=100
        )

        if st.button("🧪 Test Models", disabled=not selected):
            st.markdown("**Results:**")

            for model in selected:
                with st.spinner(f"Testing {model}..."):
                    try:
                        response = client.generate(
                            model=model,
                            prompt=test_prompt,
                            temperature=0.1,
                            max_tokens=10,
                        )
                        st.success(f"**{model}**: {response}")
                    except Exception as e:
                        st.error(f"**{model}**: Error - {e}")


if __name__ == "__main__":
    main()
