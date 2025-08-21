"""
Rate limiting utilities to comply with Notion API limits.
"""

import time
from typing import Callable, Any
from functools import wraps


class RateLimiter:
    """
    Rate limiter for Notion API requests.
    
    Notion API allows an average of 3 requests per second with some burst tolerance.
    """
    
    def __init__(self, requests_per_second: float = 2.5):
        """
        Initialize rate limiter.
        
        Args:
            requests_per_second: Target requests per second (slightly below limit for safety)
        """
        self.min_interval = 1.0 / requests_per_second
        self.last_request_time = 0.0
    
    def wait_if_needed(self) -> None:
        """Wait if necessary to maintain rate limit."""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        
        if time_since_last < self.min_interval:
            sleep_time = self.min_interval - time_since_last
            time.sleep(sleep_time)
        
        self.last_request_time = time.time()
    
    def rate_limited_call(self, func: Callable, *args, **kwargs) -> Any:
        """
        Make a rate-limited function call.
        
        Args:
            func: Function to call
            *args: Function arguments
            **kwargs: Function keyword arguments
            
        Returns:
            Function result
        """
        self.wait_if_needed()
        return func(*args, **kwargs)


def rate_limited(requests_per_second: float = 2.5):
    """
    Decorator to rate limit function calls.
    
    Args:
        requests_per_second: Target requests per second
    """
    limiter = RateLimiter(requests_per_second)
    
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            return limiter.rate_limited_call(func, *args, **kwargs)
        return wrapper
    return decorator


