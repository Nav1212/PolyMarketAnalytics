"""
Multi-model LLM judge pool for market classification.

Runs the same classification prompt across multiple models and aggregates votes.
"""

import json
from typing import Optional
from dataclasses import dataclass, field
from concurrent.futures import ThreadPoolExecutor, as_completed

from tag_manager.llm.ollama_client import OllamaClient, OllamaConfig


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
        require_unanimous: bool = False,
        min_votes_for_majority: int = 2,
    ):
        self.models = models or ["llama3.2", "mistral", "phi3"]
        self.client = OllamaClient(ollama_config)
        self.require_unanimous = require_unanimous
        self.min_votes_for_majority = min_votes_for_majority

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
        """Run classification on a single model."""
        try:
            response = self.client.generate(
                model=model,
                prompt=prompt,
                temperature=0.1,
                max_tokens=10,
            )

            vote = self._parse_response(response)

            return JudgeResult(
                model=model,
                vote=vote,
                raw_response=response,
            )
        except Exception as e:
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
