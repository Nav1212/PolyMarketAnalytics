"""
Custom exceptions for Polymarket fetcher.

Provides a hierarchy of exceptions for better error handling
and more specific error recovery strategies.
"""

from typing import Optional, Dict, Any


class PolymarketError(Exception):
    """Base exception for all Polymarket-related errors."""
    
    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.message = message
        self.details = details or {}
    
    def __str__(self):
        if self.details:
            return f"{self.message} | Details: {self.details}"
        return self.message


# =============================================================================
# API Errors
# =============================================================================

class PolymarketAPIError(PolymarketError):
    """Base exception for API-related errors."""
    
    def __init__(
        self,
        message: str,
        status_code: Optional[int] = None,
        response_body: Optional[str] = None,
        endpoint: Optional[str] = None,
        **kwargs
    ):
        details = {
            "status_code": status_code,
            "endpoint": endpoint,
            "response_body": response_body[:500] if response_body else None,
            **kwargs
        }
        super().__init__(message, details)
        self.status_code = status_code
        self.endpoint = endpoint


class RateLimitExceededError(PolymarketAPIError):
    """Raised when API rate limit is exceeded (HTTP 429)."""
    
    def __init__(
        self,
        message: str = "Rate limit exceeded",
        retry_after: Optional[int] = None,
        **kwargs
    ):
        super().__init__(message, status_code=429, **kwargs)
        self.retry_after = retry_after
        self.details["retry_after"] = retry_after


class MarketNotFoundError(PolymarketAPIError):
    """Raised when a requested market does not exist (HTTP 404)."""
    
    def __init__(self, market_id: str, **kwargs):
        message = f"Market not found: {market_id}"
        super().__init__(message, status_code=404, **kwargs)
        self.market_id = market_id
        self.details["market_id"] = market_id


class TokenNotFoundError(PolymarketAPIError):
    """Raised when a requested token does not exist."""
    
    def __init__(self, token_id: str, **kwargs):
        message = f"Token not found: {token_id}"
        super().__init__(message, status_code=404, **kwargs)
        self.token_id = token_id
        self.details["token_id"] = token_id


class NetworkTimeoutError(PolymarketAPIError):
    """Raised when an API request times out."""
    
    def __init__(
        self,
        message: str = "Request timed out",
        timeout_seconds: Optional[float] = None,
        **kwargs
    ):
        super().__init__(message, **kwargs)
        self.timeout_seconds = timeout_seconds
        self.details["timeout_seconds"] = timeout_seconds


class ServiceUnavailableError(PolymarketAPIError):
    """Raised when the API service is unavailable (HTTP 503)."""
    
    def __init__(self, message: str = "Service unavailable", **kwargs):
        super().__init__(message, status_code=503, **kwargs)


# =============================================================================
# Data Errors
# =============================================================================

class DataValidationError(PolymarketError):
    """Raised when data fails validation checks."""
    
    def __init__(
        self,
        message: str,
        field: Optional[str] = None,
        value: Any = None,
        expected: Optional[str] = None,
        **kwargs
    ):
        details = {
            "field": field,
            "value": str(value)[:100] if value else None,
            "expected": expected,
            **kwargs
        }
        super().__init__(message, details)
        self.field = field
        self.value = value


class SchemaError(DataValidationError):
    """Raised when data doesn't match expected schema."""
    
    def __init__(self, message: str, missing_fields: Optional[list] = None, **kwargs):
        super().__init__(message, **kwargs)
        self.missing_fields = missing_fields or []
        self.details["missing_fields"] = self.missing_fields


# =============================================================================
# Persistence Errors
# =============================================================================

class PersistenceError(PolymarketError):
    """Base exception for data persistence errors."""
    pass


class ParquetWriteError(PersistenceError):
    """Raised when writing to Parquet fails."""
    
    def __init__(
        self,
        message: str,
        file_path: Optional[str] = None,
        batch_size: Optional[int] = None,
        **kwargs
    ):
        details = {
            "file_path": file_path,
            "batch_size": batch_size,
            **kwargs
        }
        super().__init__(message, details)
        self.file_path = file_path


class CursorError(PersistenceError):
    """Raised when cursor save/load fails."""
    
    def __init__(self, message: str, cursor_path: Optional[str] = None, **kwargs):
        details = {"cursor_path": cursor_path, **kwargs}
        super().__init__(message, details)


class DatabaseError(PersistenceError):
    """Raised when database operations fail."""
    
    def __init__(
        self,
        message: str,
        table: Optional[str] = None,
        operation: Optional[str] = None,
        **kwargs
    ):
        details = {"table": table, "operation": operation, **kwargs}
        super().__init__(message, details)
        self.table = table
        self.operation = operation


# =============================================================================
# Worker/Coordination Errors
# =============================================================================

class WorkerError(PolymarketError):
    """Base exception for worker-related errors."""
    
    def __init__(self, message: str, worker_id: Optional[int] = None, **kwargs):
        details = {"worker_id": worker_id, **kwargs}
        super().__init__(message, details)
        self.worker_id = worker_id


class QueueError(PolymarketError):
    """Raised when queue operations fail."""
    pass


class CoordinatorError(PolymarketError):
    """Raised when coordinator operations fail."""
    pass


# =============================================================================
# Retriable Error Mixin
# =============================================================================

class RetriableError:
    """
    Mixin to mark exceptions as retriable.
    
    Used by retry decorator to determine if an error should trigger a retry.
    """
    
    @property
    def is_retriable(self) -> bool:
        return True


class RetriableAPIError(PolymarketAPIError, RetriableError):
    """API error that should be retried."""
    pass


class RetriableNetworkError(NetworkTimeoutError, RetriableError):
    """Network error that should be retried."""
    pass
