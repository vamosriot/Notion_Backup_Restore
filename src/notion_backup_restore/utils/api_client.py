"""
Notion API client wrapper with comprehensive error handling and rate limiting.

This module provides a wrapper around the notion-client with retry logic,
rate limiting integration, and circuit breaker functionality.
"""

import time
import random
from typing import Any, Dict, Optional, Callable, TypeVar, Union
from functools import wraps
from notion_client import Client
from notion_client.errors import APIResponseError, RequestTimeoutError
import logging

from .rate_limiter import AdaptiveRateLimiter, RateLimitConfig
from .logger import APICallLogger

T = TypeVar('T')


class CircuitBreakerError(Exception):
    """Raised when circuit breaker is open."""
    pass


class CircuitBreaker:
    """
    Circuit breaker pattern implementation for API calls.
    
    Automatically opens when error rate exceeds threshold,
    preventing cascading failures.
    """
    
    def __init__(self, failure_threshold: int = 5, timeout: int = 60):
        """
        Initialize circuit breaker.
        
        Args:
            failure_threshold: Number of failures before opening circuit
            timeout: Time to wait before attempting to close circuit (seconds)
        """
        self.failure_threshold = failure_threshold
        self.timeout = timeout
        self.failure_count = 0
        self.last_failure_time: Optional[float] = None
        self.state = "closed"  # closed, open, half-open
    
    def call(self, func: Callable[[], T]) -> T:
        """
        Execute function with circuit breaker protection.
        
        Args:
            func: Function to execute
            
        Returns:
            Function result
            
        Raises:
            CircuitBreakerError: If circuit is open
        """
        if self.state == "open":
            if self._should_attempt_reset():
                self.state = "half-open"
            else:
                raise CircuitBreakerError("Circuit breaker is open")
        
        try:
            result = func()
            self._on_success()
            return result
        except Exception as e:
            self._on_failure()
            raise e
    
    def _should_attempt_reset(self) -> bool:
        """Check if enough time has passed to attempt reset."""
        if self.last_failure_time is None:
            return True
        return time.time() - self.last_failure_time >= self.timeout
    
    def _on_success(self) -> None:
        """Handle successful call."""
        self.failure_count = 0
        self.state = "closed"
    
    def _on_failure(self) -> None:
        """Handle failed call."""
        self.failure_count += 1
        self.last_failure_time = time.time()
        
        if self.failure_count >= self.failure_threshold:
            self.state = "open"


class NotionAPIClient:
    """
    Enhanced Notion API client with rate limiting, retries, and error handling.
    
    This wrapper provides comprehensive error handling, automatic retries with
    exponential backoff, rate limiting, and circuit breaker functionality.
    """
    
    def __init__(
        self,
        auth: str,
        rate_limit_config: Optional[RateLimitConfig] = None,
        max_retries: int = 3,
        retry_backoff_factor: float = 2.0,
        retry_max_delay: int = 60,
        circuit_breaker_threshold: int = 5,
        circuit_breaker_timeout: int = 60,
        logger: Optional[logging.Logger] = None
    ):
        """
        Initialize Notion API client.
        
        Args:
            auth: Notion integration token
            rate_limit_config: Rate limiting configuration
            max_retries: Maximum number of retry attempts
            retry_backoff_factor: Exponential backoff factor
            retry_max_delay: Maximum retry delay in seconds
            circuit_breaker_threshold: Circuit breaker failure threshold
            circuit_breaker_timeout: Circuit breaker timeout in seconds
            logger: Logger instance
        """
        self.client = Client(auth=auth)
        self.rate_limiter = AdaptiveRateLimiter(rate_limit_config)
        self.circuit_breaker = CircuitBreaker(
            failure_threshold=circuit_breaker_threshold,
            timeout=circuit_breaker_timeout
        )
        
        self.max_retries = max_retries
        self.retry_backoff_factor = retry_backoff_factor
        self.retry_max_delay = retry_max_delay
        
        self.api_logger = APICallLogger(logger)
        self._request_count = 0
        self._error_count = 0
    
    def safe_api_call(self, func: Callable[[], T], operation: str = "") -> T:
        """
        Execute API call with comprehensive error handling and retries.
        
        Args:
            func: Function that makes the API call
            operation: Description of the operation for logging
            
        Returns:
            API call result
            
        Raises:
            Various exceptions after all retry attempts are exhausted
        """
        last_exception = None
        
        for attempt in range(self.max_retries + 1):
            try:
                # Rate limiting
                wait_time = self.rate_limiter.wait_if_needed()
                if wait_time > 0:
                    self.api_logger.log_rate_limit(
                        wait_time=wait_time,
                        current_rate=self.rate_limiter.get_current_rate(),
                        limit=self.rate_limiter.config.requests_per_second,
                        operation=operation
                    )
                
                # Circuit breaker protection
                def protected_call():
                    start_time = time.time()
                    try:
                        result = func()
                        response_time = time.time() - start_time
                        
                        self._request_count += 1
                        self.rate_limiter.handle_success_response()
                        
                        self.api_logger.log_response(
                            method="API",
                            endpoint=operation,
                            status_code=200,
                            response_time=response_time
                        )
                        
                        return result
                    
                    except APIResponseError as e:
                        response_time = time.time() - start_time
                        self._error_count += 1
                        
                        self.api_logger.log_response(
                            method="API",
                            endpoint=operation,
                            status_code=e.status,
                            response_time=response_time,
                            error=str(e)
                        )
                        
                        # Handle rate limiting
                        if e.status == 429:
                            retry_after = self._extract_retry_after(e)
                            self.rate_limiter.handle_429_response(retry_after)
                        
                        raise e
                
                return self.circuit_breaker.call(protected_call)
            
            except (APIResponseError, RequestTimeoutError, CircuitBreakerError) as e:
                last_exception = e
                
                # Don't retry on certain errors
                if isinstance(e, APIResponseError):
                    if e.status in [400, 401, 403, 404]:  # Client errors
                        self.api_logger.log_error(e, f"Non-retryable error in {operation}")
                        raise e
                
                if attempt < self.max_retries:
                    # Calculate delay with exponential backoff and jitter
                    delay = min(
                        self.retry_max_delay,
                        (self.retry_backoff_factor ** attempt) + random.uniform(0, 1)
                    )
                    
                    self.api_logger.log_retry(
                        attempt=attempt + 1,
                        max_attempts=self.max_retries + 1,
                        delay=delay,
                        error=str(e),
                        operation=operation
                    )
                    
                    time.sleep(delay)
                else:
                    self.api_logger.log_error(e, f"Max retries exceeded for {operation}")
            
            except Exception as e:
                # Unexpected error
                self._error_count += 1
                self.api_logger.log_error(e, f"Unexpected error in {operation}")
                raise e
        
        # If we get here, all retries were exhausted
        if last_exception:
            raise last_exception
        else:
            raise RuntimeError(f"All retry attempts failed for {operation}")
    
    def _extract_retry_after(self, error: APIResponseError) -> Optional[int]:
        """
        Extract Retry-After header from API error.
        
        Args:
            error: API response error
            
        Returns:
            Retry-after value in seconds, or None if not present
        """
        # The notion-client doesn't expose headers directly,
        # so we'll use a conservative default
        return 30  # Conservative 30-second delay for 429 errors
    
    # Notion API method wrappers
    
    def search(self, **kwargs) -> Dict[str, Any]:
        """Search for pages and databases."""
        return self.safe_api_call(
            lambda: self.client.search(**kwargs),
            f"search({kwargs.get('query', 'all')})"
        )
    
    def get_database(self, database_id: str) -> Dict[str, Any]:
        """Retrieve a database."""
        return self.safe_api_call(
            lambda: self.client.databases.retrieve(database_id),
            f"get_database({database_id})"
        )
    
    def query_database(self, database_id: str, **kwargs) -> Dict[str, Any]:
        """Query database pages."""
        # Notion API renamed databases.query() to data_sources.query()
        try:
            return self.safe_api_call(
                lambda: self.client.data_sources.query(database_id, **kwargs),
                f"query_database({database_id})"
            )
        except AttributeError:
            # Fallback for older notion-client versions
            return self.safe_api_call(
                lambda: self.client.databases.query(database_id, **kwargs),
                f"query_database({database_id})"
            )
    
    def create_database(self, **kwargs) -> Dict[str, Any]:
        """Create a new database."""
        return self.safe_api_call(
            lambda: self.client.databases.create(**kwargs),
            "create_database"
        )
    
    def update_database(self, database_id: str, **kwargs) -> Dict[str, Any]:
        """Update a database."""
        return self.safe_api_call(
            lambda: self.client.databases.update(database_id, **kwargs),
            f"update_database({database_id})"
        )
    
    def get_page(self, page_id: str) -> Dict[str, Any]:
        """Retrieve a page."""
        return self.safe_api_call(
            lambda: self.client.pages.retrieve(page_id),
            f"get_page({page_id})"
        )
    
    def create_page(self, **kwargs) -> Dict[str, Any]:
        """Create a new page."""
        return self.safe_api_call(
            lambda: self.client.pages.create(**kwargs),
            "create_page"
        )
    
    def update_page(self, page_id: str, **kwargs) -> Dict[str, Any]:
        """Update a page."""
        return self.safe_api_call(
            lambda: self.client.pages.update(page_id, **kwargs),
            f"update_page({page_id})"
        )
    
    def get_block_children(self, block_id: str, **kwargs) -> Dict[str, Any]:
        """Get children of a block."""
        return self.safe_api_call(
            lambda: self.client.blocks.children.list(block_id, **kwargs),
            f"get_block_children({block_id})"
        )
    
    def append_block_children(self, block_id: str, **kwargs) -> Dict[str, Any]:
        """Append children to a block."""
        return self.safe_api_call(
            lambda: self.client.blocks.children.append(block_id, **kwargs),
            f"append_block_children({block_id})"
        )
    
    def get_user(self, user_id: str) -> Dict[str, Any]:
        """Retrieve a user."""
        return self.safe_api_call(
            lambda: self.client.users.retrieve(user_id),
            f"get_user({user_id})"
        )
    
    def list_users(self, **kwargs) -> Dict[str, Any]:
        """List all users."""
        return self.safe_api_call(
            lambda: self.client.users.list(**kwargs),
            "list_users"
        )
    
    # Utility methods
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get client statistics.
        
        Returns:
            Dictionary with client statistics
        """
        rate_limiter_stats = self.rate_limiter.get_stats()
        
        return {
            "total_requests": self._request_count,
            "total_errors": self._error_count,
            "error_rate": self._error_count / max(1, self._request_count),
            "circuit_breaker_state": self.circuit_breaker.state,
            "circuit_breaker_failures": self.circuit_breaker.failure_count,
            "rate_limiter": rate_limiter_stats,
        }
    
    def reset_stats(self) -> None:
        """Reset client statistics."""
        self._request_count = 0
        self._error_count = 0
        self.circuit_breaker.failure_count = 0
        self.circuit_breaker.state = "closed"
        self.rate_limiter.reset()


def create_notion_client(
    auth: str,
    requests_per_second: float = 2.5,
    max_retries: int = 3,
    logger: Optional[logging.Logger] = None
) -> NotionAPIClient:
    """
    Create a configured Notion API client.
    
    Args:
        auth: Notion integration token
        requests_per_second: Rate limit (requests per second)
        max_retries: Maximum retry attempts
        logger: Logger instance
        
    Returns:
        Configured NotionAPIClient instance
    """
    rate_config = RateLimitConfig(requests_per_second=requests_per_second)
    
    return NotionAPIClient(
        auth=auth,
        rate_limit_config=rate_config,
        max_retries=max_retries,
        logger=logger
    )
