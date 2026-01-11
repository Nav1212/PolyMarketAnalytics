"""
Multi-model LLM judge pool for market classification.

Runs the same classification prompt across multiple models and aggregates votes.
"""

import json
import logging
import threading
from typing import Optional
from dataclasses import dataclass, field
from concurrent.futures import ThreadPoolExecutor, as_completed

from tag_manager.llm.ollama_client import OllamaClient, OllamaConfig

logger = logging.getLogger(__name__)


@dataclass
class JudgeResult:
    """Result of a single judge's classification."""
    model: str
    vote: Optional[bool]  # True = belongs to tag, False = doesn't, None = error
    raw_response: str
    error: Optional[str] = None


@dataclass
class PoolResult:
    """Aggregated result from all judges."""
    votes: dict[str, bool]  # model -> vote
    consensus: Optional[bool]  # None if no consensus
    decided_by: str  # 'unanimous', 'majority', 'no_consensus'
    individual_results: list[JudgeResult] = field(default_factory=list)

    @property
    def votes_json(self) -> str:
        """Get votes as JSON string for database storage."""
        return json.dumps(self.votes)


CLASSIFICATION_PROMPT = """You are a market classifier. Given a prediction market question, determine if it belongs to the tag "{tag_name}".

Tag description: {tag_description}

{examples_section}

Market to classify:
Question: {market_question}
Description: {market_description}

Does this market belong to the tag "{tag_name}"? Answer with ONLY "YES" or "NO"."""


class JudgePool:
    """
    Pool of LLM judges for market classification.

    Uses multiple models to vote on whether a market belongs to a tag,
    then aggregates the votes to determine consensus.

    Usage:
        pool = JudgePool(models=["llama3.2", "mistral", "phi3"])
        result = pool.classify(
            tag_name="Crypto",
            tag_description="Markets about cryptocurrency",
            market_question="Will Bitcoin reach $100k?",
            market_description="...",
            positive_examples=[...],
            negative_examples=[...]
        )
        print(result.consensus)  # True, False, or None
    """

    def __init__(
        self,
        models: Optional[list[str]] = None,
        ollama_config: Optional[OllamaConfig] = None,
        require_unanimous: Optional[bool] = None,
        min_votes_for_majority: Optional[int] = None,
        ollama_concurrency: Optional[int] = None,
    ):
        self.client = OllamaClient(ollama_config)

        # Use provided models, or auto-detect available models
        if models:
            self.models = models
        else:
            self.models = self._get_default_models()

        # Load consensus settings from persistent storage if not provided
        if require_unanimous is None or min_votes_for_majority is None:
            saved_unanimous, saved_min_votes = self._get_default_consensus_settings()
            self.require_unanimous = require_unanimous if require_unanimous is not None else saved_unanimous
            self.min_votes_for_majority = min_votes_for_majority if min_votes_for_majority is not None else saved_min_votes
        else:
            self.require_unanimous = require_unanimous
            self.min_votes_for_majority = min_votes_for_majority

        # Load ollama concurrency setting (limits concurrent GPU requests)
        if ollama_concurrency is None:
            ollama_concurrency = self._get_default_ollama_concurrency()
        self.ollama_concurrency = ollama_concurrency
        self._gpu_semaphore = threading.Semaphore(self.ollama_concurrency)

        logger.info(f"JudgePool initialized with models: {self.models}, GPU concurrency: {self.ollama_concurrency}")

    def _get_default_models(self) -> list[str]:
        """Get default models - from persistent storage, session state, or auto-detect."""
        # Try to get from persistent storage first
        try:
            from tag_manager.db import get_connection
            from tag_manager.services import SettingsService
            conn = get_connection()
            settings = SettingsService(conn)
            saved_models = settings.get_selected_models()
            if saved_models:
                return saved_models
        except Exception:
            pass

        # Try to get from Streamlit session state
        try:
            import streamlit as st
            selected = st.session_state.get("selected_models", [])
            if selected:
                return selected
        except Exception:
            pass

        # Fall back to auto-detection
        available = self.client.list_models()
        if available:
            return available[:3]

        # Final fallback
        return ["llama3.2", "mistral", "phi3"]

    def _get_default_consensus_settings(self) -> tuple[bool, int]:
        """Get default consensus settings from persistent storage."""
        try:
            from tag_manager.db import get_connection
            from tag_manager.services import SettingsService
            conn = get_connection()
            settings = SettingsService(conn)
            return settings.get_require_unanimous(), settings.get_min_votes_for_majority()
        except Exception:
            pass
        return False, 2  # Default values

    def _get_default_ollama_concurrency(self) -> int:
        """Get default Ollama GPU concurrency from persistent storage."""
        try:
            from tag_manager.db import get_connection
            from tag_manager.services import SettingsService
            conn = get_connection()
            settings = SettingsService(conn)
            return settings.get_ollama_concurrency()
        except Exception:
            pass
        return 2  # Default value

    def update_settings(
        self,
        models: Optional[list[str]] = None,
        require_unanimous: Optional[bool] = None,
        min_votes_for_majority: Optional[int] = None,
        ollama_concurrency: Optional[int] = None,
    ):
        """Update pool settings dynamically."""
        if models is not None:
            self.models = models
            logger.info(f"Updated models to: {self.models}")
        if require_unanimous is not None:
            self.require_unanimous = require_unanimous
        if min_votes_for_majority is not None:
            self.min_votes_for_majority = min_votes_for_majority
        if ollama_concurrency is not None and ollama_concurrency != self.ollama_concurrency:
            self.ollama_concurrency = ollama_concurrency
            self._gpu_semaphore = threading.Semaphore(ollama_concurrency)
            logger.info(f"Updated GPU concurrency to: {self.ollama_concurrency}")

    def classify(
        self,
        tag_name: str,
        tag_description: str,
        market_question: str,
        market_description: str,
        positive_examples: Optional[list[dict]] = None,
        negative_examples: Optional[list[dict]] = None,
    ) -> PoolResult:
        """
        Classify a market using all judges in the pool.

        Args:
            tag_name: Name of the tag
            tag_description: Description of what the tag represents
            market_question: The market's question text
            market_description: The market's description
            positive_examples: List of example markets that belong to tag
            negative_examples: List of example markets that don't belong

        Returns:
            PoolResult with votes and consensus
        """
        prompt = self._build_prompt(
            tag_name=tag_name,
            tag_description=tag_description,
            market_question=market_question,
            market_description=market_description or "",
            positive_examples=positive_examples or [],
            negative_examples=negative_examples or [],
        )

        # Run all models in parallel
        results = []
        with ThreadPoolExecutor(max_workers=len(self.models)) as executor:
            futures = {
                executor.submit(self._classify_single, model, prompt): model
                for model in self.models
            }

            for future in as_completed(futures):
                model = futures[future]
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    results.append(JudgeResult(
                        model=model,
                        vote=None,
                        raw_response="",
                        error=str(e)
                    ))

        return self._aggregate_results(results)

    def _classify_single(self, model: str, prompt: str) -> JudgeResult:
        """Run classification on a single model (throttled by GPU semaphore)."""
        with self._gpu_semaphore:
            try:
                logger.debug(f"Classifying with model: {model}")
                response = self.client.generate(
                    model=model,
                    prompt=prompt,
                    temperature=0.1,
                    max_tokens=10,
                )

                vote = self._parse_response(response)
                logger.debug(f"Model {model} response: '{response}' -> vote: {vote}")

                return JudgeResult(
                    model=model,
                    vote=vote,
                    raw_response=response,
                )
            except Exception as e:
                logger.error(f"Error with model {model}: {e}")
                return JudgeResult(
                    model=model,
                    vote=None,
                    raw_response="",
                    error=str(e)
                )

    def _build_prompt(
        self,
        tag_name: str,
        tag_description: str,
        market_question: str,
        market_description: str,
        positive_examples: list[dict],
        negative_examples: list[dict],
    ) -> str:
        """Build the classification prompt with examples."""
        examples_section = ""

        if positive_examples:
            examples_section += "Examples of markets that BELONG to this tag:\n"
            for ex in positive_examples[:3]:  # Limit to 3 examples
                examples_section += f"- {ex.get('question', '')}\n"
            examples_section += "\n"

        if negative_examples:
            examples_section += "Examples of markets that DO NOT belong to this tag:\n"
            for ex in negative_examples[:3]:  # Limit to 3 examples
                examples_section += f"- {ex.get('question', '')}\n"
            examples_section += "\n"

        return CLASSIFICATION_PROMPT.format(
            tag_name=tag_name,
            tag_description=tag_description or "No description provided",
            examples_section=examples_section,
            market_question=market_question,
            market_description=market_description[:500] if market_description else "No description",
        )

    def _parse_response(self, response: str) -> Optional[bool]:
        """Parse YES/NO response from model."""
        response_upper = response.upper().strip()

        if "YES" in response_upper:
            return True
        elif "NO" in response_upper:
            return False
        else:
            return None

    def _aggregate_results(self, results: list[JudgeResult]) -> PoolResult:
        """Aggregate individual results into a consensus."""
        votes = {}
        for r in results:
            if r.vote is not None:
                votes[r.model] = r.vote

        if not votes:
            return PoolResult(
                votes={},
                consensus=None,
                decided_by="no_votes",
                individual_results=results,
            )

        yes_votes = sum(1 for v in votes.values() if v)
        no_votes = sum(1 for v in votes.values() if not v)
        total_votes = len(votes)

        # Check for unanimous agreement
        if yes_votes == total_votes:
            return PoolResult(
                votes=votes,
                consensus=True,
                decided_by="unanimous",
                individual_results=results,
            )
        elif no_votes == total_votes:
            return PoolResult(
                votes=votes,
                consensus=False,
                decided_by="unanimous",
                individual_results=results,
            )

        # If unanimous required but not achieved
        if self.require_unanimous:
            return PoolResult(
                votes=votes,
                consensus=None,
                decided_by="no_consensus",
                individual_results=results,
            )

        # Check for majority
        if yes_votes >= self.min_votes_for_majority:
            return PoolResult(
                votes=votes,
                consensus=True,
                decided_by="majority",
                individual_results=results,
            )
        elif no_votes >= self.min_votes_for_majority:
            return PoolResult(
                votes=votes,
                consensus=False,
                decided_by="majority",
                individual_results=results,
            )

        # No consensus
        return PoolResult(
            votes=votes,
            consensus=None,
            decided_by="no_consensus",
            individual_results=results,
        )

    def close(self):
        """Close the underlying Ollama client."""
        self.client.close()
