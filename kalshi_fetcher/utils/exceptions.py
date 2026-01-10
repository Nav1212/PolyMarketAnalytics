"""
Custom exceptions for Kalshi fetcher.
"""


class KalshiFetcherError(Exception):
    """Base exception for Kalshi fetcher errors."""
    pass


class KalshiAPIError(KalshiFetcherError):
    """Error from Kalshi API response."""
    
    def __init__(self, message: str, status_code: int = None, endpoint: str = None, response_body: str = None):
        self.status_code = status_code
        self.endpoint = endpoint
        self.response_body = response_body
        super().__init__(message)


class RateLimitExceededError(KalshiAPIError):
    """Rate limit exceeded (HTTP 429)."""
    
    def __init__(self, endpoint: str = None, response_body: str = None):
        super().__init__(
            "Rate limit exceeded",
            status_code=429,
            endpoint=endpoint,
            response_body=response_body
        )


class NetworkTimeoutError(KalshiFetcherError):
    """Network request timed out."""
    
    def __init__(self, endpoint: str = None, timeout: float = None):
        self.endpoint = endpoint
        self.timeout = timeout
        message = f"Request timed out after {timeout}s" if timeout else "Request timed out"
        super().__init__(message)


class AuthenticationError(KalshiFetcherError):
    """Authentication failed."""
    
    def __init__(self, message: str = "Authentication failed"):
        super().__init__(message)


class ParquetWriteError(KalshiFetcherError):
    """Error writing parquet file."""
    
    def __init__(self, filepath: str, reason: str = None):
        self.filepath = filepath
        message = f"Failed to write parquet: {filepath}"
        if reason:
            message += f" - {reason}"
        super().__init__(message)


class CursorError(KalshiFetcherError):
    """Error with cursor persistence."""
    
    def __init__(self, message: str, cursor_file: str = None):
        self.cursor_file = cursor_file
        super().__init__(message)
