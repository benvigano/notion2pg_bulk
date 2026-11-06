"""
Rate limiting utilities to comply with Notion API limits.
"""

import time
from typing import Callable, Any


class RateLimiter:
    """
    Enforces a maximum request rate for Notion API calls.

    Notion's API allows an average of 3 requests per second with some burst
    tolerance. This limiter targets 2.5 req/s as a conservative default.
    Transient errors (429, 5xx) are handled separately by the notion_client
    SDK's built-in RetryOptions.
    """

    def __init__(self, requests_per_second: float = 2.5):
        self.min_interval = 1.0 / requests_per_second
        self.last_request_time = 0.0

    def wait_if_needed(self) -> None:
        current_time = time.time()
        elapsed = current_time - self.last_request_time
        if elapsed < self.min_interval:
            time.sleep(self.min_interval - elapsed)
        self.last_request_time = time.time()

    def rate_limited_call(self, func: Callable, *args, **kwargs) -> Any:
        self.wait_if_needed()
        return func(*args, **kwargs)

