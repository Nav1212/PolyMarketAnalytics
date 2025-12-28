"""
Tag management module.

Contains:
    - TagGraph: Tag hierarchy and relationship management
    - Tag: Tag data class
    - TagEdge: Edge data class
    - TagSearchResult: Search result data class
"""

from nlp_enrichment.tags.tag_graph import (
    TagGraph,
    Tag,
    TagEdge,
    TagSearchResult,
)

__all__ = [
    "TagGraph",
    "Tag",
    "TagEdge",
    "TagSearchResult",
]
