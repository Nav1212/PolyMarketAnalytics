"""
Ollama HTTP client for LLM interactions.

Simple synchronous client for generating text completions using local Ollama server.
"""

import subprocess
import time
import requests
from typing import Optional
from dataclasses import dataclass


@dataclass
class OllamaConfig:
    """Configuration for Ollama client."""
    base_url: str = "http://localhost:11434"
    timeout: float = 120.0
    default_model: str = "llama3.2"


class OllamaClient:
    """
    Simple HTTP client for Ollama API.

    Usage:
        client = OllamaClient()
        response = client.generate("llama3.2", "What is 2+2?")
        print(response)
    """

    def __init__(self, config: Optional[OllamaConfig] = None):
        self.config = config or OllamaConfig()
        self._session = requests.Session()

    def generate(
        self,
        model: str,
        prompt: str,
        temperature: float = 0.1,
        max_tokens: int = 10,
    ) -> str:
        """
        Generate a completion using the specified model.

        Args:
            model: Model name (e.g., "llama3.2", "mistral", "phi3")
            prompt: The prompt to send to the model
            temperature: Sampling temperature (lower = more deterministic)
            max_tokens: Maximum tokens to generate

        Returns:
            The generated text response

        Raises:
            requests.RequestException: If the API call fails
        """
        url = f"{self.config.base_url}/api/generate"

        payload = {
            "model": model,
            "prompt": prompt,
            "stream": False,
            "options": {
                "temperature": temperature,
                "num_predict": max_tokens,
            }
        }

        response = self._session.post(
            url,
            json=payload,
            timeout=self.config.timeout
        )
        response.raise_for_status()

        data = response.json()
        return data.get("response", "").strip()

    def is_available(self) -> bool:
        """Check if Ollama server is running and accessible."""
        try:
            response = self._session.get(
                f"{self.config.base_url}/api/tags",
                timeout=5.0
            )
            return response.status_code == 200
        except requests.RequestException:
            return False

    def ensure_running(self, max_wait: float = 30.0) -> bool:
        """
        Ensure Ollama is running, starting it if necessary.

        Args:
            max_wait: Maximum seconds to wait for Ollama to start

        Returns:
            True if Ollama is running, False if it couldn't be started
        """
        if self.is_available():
            return True

        # Try to start Ollama
        try:
            subprocess.Popen(
                ["ollama", "serve"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                creationflags=subprocess.CREATE_NO_WINDOW if hasattr(subprocess, 'CREATE_NO_WINDOW') else 0,
            )
        except FileNotFoundError:
            return False
        except Exception:
            return False

        # Wait for Ollama to be ready
        start_time = time.time()
        while time.time() - start_time < max_wait:
            if self.is_available():
                return True
            time.sleep(0.5)

        return False

    def list_models(self) -> list[str]:
        """Get list of available models."""
        try:
            response = self._session.get(
                f"{self.config.base_url}/api/tags",
                timeout=5.0
            )
            response.raise_for_status()
            data = response.json()
            return [m["name"] for m in data.get("models", [])]
        except requests.RequestException:
            return []

    def close(self):
        """Close the HTTP session."""
        self._session.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
