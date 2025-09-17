"""
Rate limiting implementation for Notion API requests.

This module provides sophisticated rate limiting with sliding window algorithm
to respect Notion's 3 requests per second average limit while allowing bursts.
"""

import time
import threading
from collections import deque
from typing import Optional
from dataclasses import dataclass


@dataclass
class RateLimitConfig:
    """Configuration for rate limiting."""
    requests_per_second: float = 2.5  # Conservative limit
    burst_size: int = 5  # Allow short bursts
    window_size: int = 10  # Time window in seconds


class RateLimiter:
    """
    Thread-safe rate limiter using sliding window algorithm.
    
    This implementation tracks request timestamps and ensures the average
    rate doesn't exceed the configured limit while allowing short bursts.
    """
    
    def __init__(self, config: Optional[RateLimitConfig] = None):
        """
        Initialize rate limiter.
        
        Args:
            config: Rate limiting configuration
        """
        self.config = config or RateLimitConfig()
        self._lock = threading.Lock()
        self._requests: deque = deque()
        self._last_request_time: Optional[float] = None
    
    def wait_if_needed(self) -> float:
        """
        Wait if necessary to respect rate limits.
        
        Returns:
            Time waited in seconds
        """
        with self._lock:
            current_time = time.time()
            
            # Clean old requests outside the window
            self._clean_old_requests(current_time)
            
            # Calculate wait time
            wait_time = self._calculate_wait_time(current_time)
            
            if wait_time > 0:
                # Release lock during sleep to allow other threads
                pass
            
        # Sleep outside the lock to avoid blocking other threads
        if wait_time > 0:
            time.sleep(wait_time)
            
        # Record the request
        with self._lock:
            actual_time = time.time()
            self._requests.append(actual_time)
            self._last_request_time = actual_time
            
        return wait_time
    
    def _clean_old_requests(self, current_time: float) -> None:
        """Remove requests outside the sliding window."""
        cutoff_time = current_time - self.config.window_size
        
        while self._requests and self._requests[0] < cutoff_time:
            self._requests.popleft()
    
    def _calculate_wait_time(self, current_time: float) -> float:
        """
        Calculate how long to wait before making the next request.
        
        Args:
            current_time: Current timestamp
            
        Returns:
            Wait time in seconds
        """
        # Check burst limit
        if len(self._requests) >= self.config.burst_size:
            # If we're at burst limit, ensure minimum spacing
            if self._last_request_time:
                min_interval = 1.0 / self.config.requests_per_second
                time_since_last = current_time - self._last_request_time
                if time_since_last < min_interval:
                    return min_interval - time_since_last
        
        # Check average rate over the window
        if len(self._requests) > 0:
            window_start = current_time - self.config.window_size
            requests_in_window = len(self._requests)
            
            # Calculate current rate
            actual_window_size = min(self.config.window_size, 
                                   current_time - self._requests[0])
            if actual_window_size > 0:
                current_rate = requests_in_window / actual_window_size
                
                # If we're exceeding the rate, calculate wait time
                if current_rate >= self.config.requests_per_second:
                    # Wait until the oldest request falls outside the window
                    oldest_request = self._requests[0]
                    wait_until = oldest_request + self.config.window_size
                    return max(0, wait_until - current_time)
        
        return 0.0
    
    def get_current_rate(self) -> float:
        """
        Get current request rate.
        
        Returns:
            Current requests per second
        """
        with self._lock:
            current_time = time.time()
            self._clean_old_requests(current_time)
            
            if len(self._requests) < 2:
                return 0.0
            
            window_start = current_time - self.config.window_size
            requests_in_window = len(self._requests)
            actual_window_size = min(self.config.window_size,
                                   current_time - self._requests[0])
            
            if actual_window_size > 0:
                return requests_in_window / actual_window_size
            
            return 0.0
    
    def get_stats(self) -> dict:
        """
        Get rate limiter statistics.
        
        Returns:
            Dictionary with current statistics
        """
        with self._lock:
            current_time = time.time()
            self._clean_old_requests(current_time)
            
            return {
                "current_rate": self.get_current_rate(),
                "requests_in_window": len(self._requests),
                "configured_rate": self.config.requests_per_second,
                "burst_size": self.config.burst_size,
                "window_size": self.config.window_size,
                "last_request_time": self._last_request_time,
            }
    
    def reset(self) -> None:
        """Reset rate limiter state."""
        with self._lock:
            self._requests.clear()
            self._last_request_time = None


class AdaptiveRateLimiter(RateLimiter):
    """
    Adaptive rate limiter that adjusts based on API responses.
    
    This extends the basic rate limiter to handle Retry-After headers
    and automatically adjust rates based on 429 responses.
    """
    
    def __init__(self, config: Optional[RateLimitConfig] = None):
        super().__init__(config)
        self._adaptive_delay: float = 0.0
        self._consecutive_429s: int = 0
        self._last_429_time: Optional[float] = None
    
    def handle_429_response(self, retry_after: Optional[int] = None) -> None:
        """
        Handle a 429 (rate limited) response.
        
        Args:
            retry_after: Retry-After header value in seconds
        """
        with self._lock:
            current_time = time.time()
            self._consecutive_429s += 1
            self._last_429_time = current_time
            
            if retry_after:
                # Use the server's suggested delay
                self._adaptive_delay = max(self._adaptive_delay, retry_after)
            else:
                # Exponential backoff based on consecutive 429s
                self._adaptive_delay = min(60.0, 2.0 ** self._consecutive_429s)
    
    def handle_success_response(self) -> None:
        """Handle a successful response (reset adaptive delay)."""
        with self._lock:
            self._consecutive_429s = 0
            # Gradually reduce adaptive delay
            self._adaptive_delay = max(0.0, self._adaptive_delay * 0.8)
    
    def _calculate_wait_time(self, current_time: float) -> float:
        """Calculate wait time including adaptive delay."""
        base_wait = super()._calculate_wait_time(current_time)
        
        # Add adaptive delay if we've had recent 429s
        if self._adaptive_delay > 0:
            if self._last_429_time and (current_time - self._last_429_time) < 60:
                return max(base_wait, self._adaptive_delay)
        
        return base_wait
    
    def get_stats(self) -> dict:
        """Get enhanced statistics including adaptive information."""
        stats = super().get_stats()
        stats.update({
            "adaptive_delay": self._adaptive_delay,
            "consecutive_429s": self._consecutive_429s,
            "last_429_time": self._last_429_time,
        })
        return stats
