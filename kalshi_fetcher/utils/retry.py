"""
Retry decorator for Kalshi API calls.
Implements exponential backoff with jitter.
"""

import functools
import random
import time
from typing import Callable, Type, Tuple, Optional

from kalshi_fetcher.utils.logging_config import get_logger
from kalshi_fetcher.utils.exceptions import RateLimitExceededError, NetworkTimeoutError

logger = get_logger("retry")


def retry(
    max_attempts: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 30.0,
    exponential_base: float = 2.0,
    retryable_exceptions: Tuple[Type[Exception], ...] = (
        RateLimitExceededError,
        NetworkTimeoutError,
        ConnectionError,
        TimeoutError,
    ),
):
    """
    Decorator for retrying failed API calls with exponential backoff.
    
    Args:
        max_attempts: Maximum number of retry attempts
        base_delay: Initial delay between retries in seconds
        max_delay: Maximum delay between retries in seconds
        exponential_base: Base for exponential backoff calculation
        retryable_exceptions: Tuple of exception types to retry on
    
    Returns:
        Decorated function with retry logic
    
    Example:
        @retry(max_attempts=3, base_delay=1.0)
        def fetch_data():
            # API call here
            pass
    """
    def decorator(func: Callable):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except retryable_exceptions as e:
                    last_exception = e
                    
                    if attempt == max_attempts:
                        logger.error(
                            f"Max retries ({max_attempts}) exceeded for {func.__name__}",
                            extra={"exception": str(e)}
                        )
                        raise
                    
                    # Calculate delay with exponential backoff and jitter
                    delay = min(
                        base_delay * (exponential_base ** (attempt - 1)),
                        max_delay
                    )
                    # Add jitter (±25%)
                    jitter = delay * 0.25 * (random.random() * 2 - 1)
                    delay = max(0, delay + jitter)
                    
                    logger.warning(
                        f"Retry {attempt}/{max_attempts} for {func.__name__} "
                        f"after {delay:.2f}s delay",
                        extra={"exception": str(e), "attempt": attempt}
                    )
                    
                    time.sleep(delay)
            
            # Should not reach here, but just in case
            if last_exception:
                raise last_exception
        
        return wrapper
    return decorator
