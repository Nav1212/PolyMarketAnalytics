"""
Coordination module - orchestrates all fetchers with proper load ordering.

This module is responsible for:
- Creating and wiring up fetcher queues
- Managing fetcher lifecycle (start, stop, resume)
- Coordinating data flow between fetchers
- Handling cursor persistence for resumption
"""

from fetcher.coordination.coordinator import (
    FetcherCoordinator,
    LoadOrder,
)

__all__ = [
    "FetcherCoordinator",
    "LoadOrder",
]
