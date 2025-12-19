"""
Retry decorator with exponential backoff for Polymarket API calls.

Provides configurable retry logic for transient failures with
jitter to avoid thundering herd problems.
"""

import functools
import random
import time
from typing import Callable, Tuple, Type, Optional, Union, Any
import httpx

from fetcher.utils.logging_config import get_logger
from fetcher.utils.exceptions import (
    PolymarketAPIError,
    RateLimitExceededError,
    NetworkTimeoutError,
    ServiceUnavailableError,
    RetriableError,
)

logger = get_logger("retry")


# Default retriable exceptions
DEFAULT_RETRIABLE_EXCEPTIONS: Tuple[Type[Exception], ...] = (
    httpx.TimeoutException,
    httpx.NetworkError,
    httpx.RemoteProtocolError,
    NetworkTimeoutError,
    RateLimitExceededError,
    ServiceUnavailableError,
)

# HTTP status codes that should trigger a retry
RETRIABLE_STATUS_CODES = {429, 500, 502, 503, 504}


def retry(
    max_attempts: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    exponential_base: float = 2.0,
    jitter: bool = True,
    retriable_exceptions: Optional[Tuple[Type[Exception], ...]] = None,
    on_retry: Optional[Callable[[Exception, int, float], None]] = None,
):
    """
    Decorator that retries a function with exponential backoff.
    
    Args:
        max_attempts: Maximum number of attempts (including first try)
        base_delay: Initial delay between retries in seconds
        max_delay: Maximum delay between retries in seconds
        exponential_base: Base for exponential backoff calculation
        jitter: Add random jitter to delay to avoid thundering herd
        retriable_exceptions: Tuple of exception types to retry on
        on_retry: Optional callback called before each retry with (exception, attempt, delay)
    
    Returns:
        Decorated function with retry logic
    
    Example:
        >>> @retry(max_attempts=3, base_delay=1.0)
        ... def fetch_data(market_id: str) -> dict:
        ...     response = httpx.get(f"/markets/{market_id}")
        ...     response.raise_for_status()
        ...     return response.json()
    """
    if retriable_exceptions is None:
        retriable_exceptions = DEFAULT_RETRIABLE_EXCEPTIONS
    
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            last_exception = None
            
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                
                except retriable_exceptions as e:
                    last_exception = e
                    
                    # Check if it's the last attempt
                    if attempt >= max_attempts:
                        logger.error(
                            f"All {max_attempts} attempts failed for {func.__name__}",
                            exc_info=True
                        )
                        raise
                    
                    # Calculate delay with exponential backoff
                    delay = min(
                        base_delay * (exponential_base ** (attempt - 1)),
                        max_delay
                    )
                    
                    # Add jitter (±25% of delay)
                    if jitter:
                        delay = delay * (0.75 + random.random() * 0.5)
                    
                    # Special handling for rate limit errors
                    if isinstance(e, RateLimitExceededError) and e.retry_after:
                        delay = max(delay, e.retry_after)
                    
                    logger.warning(
                        f"Attempt {attempt}/{max_attempts} failed for {func.__name__}: {e}. "
                        f"Retrying in {delay:.2f}s..."
                    )
                    
                    # Call retry callback if provided
                    if on_retry:
                        on_retry(e, attempt, delay)
                    
                    time.sleep(delay)
                
                except httpx.HTTPStatusError as e:
                    # Check if this HTTP status is retriable
                    if e.response.status_code in RETRIABLE_STATUS_CODES:
                        last_exception = e
                        
                        if attempt >= max_attempts:
                            logger.error(
                                f"All {max_attempts} attempts failed for {func.__name__}",
                                exc_info=True
                            )
                            raise
                        
                        delay = min(
                            base_delay * (exponential_base ** (attempt - 1)),
                            max_delay
                        )
                        
                        if jitter:
                            delay = delay * (0.75 + random.random() * 0.5)
                        
                        # Check for Retry-After header
                        retry_after = e.response.headers.get("Retry-After")
                        if retry_after:
                            try:
                                delay = max(delay, float(retry_after))
                            except ValueError:
                                pass
                        
                        logger.warning(
                            f"HTTP {e.response.status_code} on attempt {attempt}/{max_attempts} "
                            f"for {func.__name__}. Retrying in {delay:.2f}s..."
                        )
                        
                        if on_retry:
                            on_retry(e, attempt, delay)
                        
                        time.sleep(delay)
                    else:
                        # Non-retriable HTTP error, raise immediately
                        raise
            
            # Should not reach here, but just in case
            if last_exception:
                raise last_exception
        
        return wrapper
    return decorator


def retry_async(
    max_attempts: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    exponential_base: float = 2.0,
    jitter: bool = True,
    retriable_exceptions: Optional[Tuple[Type[Exception], ...]] = None,
    on_retry: Optional[Callable[[Exception, int, float], None]] = None,
):
    """
    Async version of retry decorator.
    
    Same parameters as retry(), but works with async functions.
    """
    import asyncio
    
    if retriable_exceptions is None:
        retriable_exceptions = DEFAULT_RETRIABLE_EXCEPTIONS
    
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        async def wrapper(*args, **kwargs) -> Any:
            last_exception = None
            
            for attempt in range(1, max_attempts + 1):
                try:
                    return await func(*args, **kwargs)
                
                except retriable_exceptions as e:
                    last_exception = e
                    
                    if attempt >= max_attempts:
                        logger.error(
                            f"All {max_attempts} attempts failed for {func.__name__}",
                            exc_info=True
                        )
                        raise
                    
                    delay = min(
                        base_delay * (exponential_base ** (attempt - 1)),
                        max_delay
                    )
                    
                    if jitter:
                        delay = delay * (0.75 + random.random() * 0.5)
                    
                    if isinstance(e, RateLimitExceededError) and e.retry_after:
                        delay = max(delay, e.retry_after)
                    
                    logger.warning(
                        f"Attempt {attempt}/{max_attempts} failed for {func.__name__}: {e}. "
                        f"Retrying in {delay:.2f}s..."
                    )
                    
                    if on_retry:
                        on_retry(e, attempt, delay)
                    
                    await asyncio.sleep(delay)
            
            if last_exception:
                raise last_exception
        
        return wrapper
    return decorator


class RetryContext:
    """
    Context manager for retry logic when decorator isn't suitable.
    
    Example:
        >>> async with RetryContext(max_attempts=3) as ctx:
        ...     for attempt in ctx:
        ...         try:
        ...             result = await fetch_data()
        ...             break
        ...         except httpx.TimeoutException:
        ...             await ctx.handle_retry()
    """
    
    def __init__(
        self,
        max_attempts: int = 3,
        base_delay: float = 1.0,
        max_delay: float = 60.0,
        exponential_base: float = 2.0,
        jitter: bool = True,
    ):
        self.max_attempts = max_attempts
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.exponential_base = exponential_base
        self.jitter = jitter
        self.attempt = 0
        self.last_exception: Optional[Exception] = None
    
    def __iter__(self):
        for self.attempt in range(1, self.max_attempts + 1):
            yield self.attempt
    
    def should_retry(self) -> bool:
        """Check if another retry attempt is available."""
        return self.attempt < self.max_attempts
    
    def get_delay(self) -> float:
        """Calculate delay for current attempt."""
        delay = min(
            self.base_delay * (self.exponential_base ** (self.attempt - 1)),
            self.max_delay
        )
        if self.jitter:
            delay = delay * (0.75 + random.random() * 0.5)
        return delay
    
    def handle_retry(self, exception: Optional[Exception] = None) -> float:
        """
        Handle a retry: log and sleep.
        
        Returns the delay that was used.
        """
        self.last_exception = exception
        delay = self.get_delay()
        
        if exception:
            logger.warning(
                f"Attempt {self.attempt}/{self.max_attempts} failed: {exception}. "
                f"Retrying in {delay:.2f}s..."
            )
        
        time.sleep(delay)
        return delay
