"""
Centralized logging configuration for Notion backup and restore operations.

This module provides structured logging with file and console handlers,
log rotation, and different log levels for development and production.
"""

import logging
import logging.handlers
import sys
from pathlib import Path
from typing import Optional
from datetime import datetime


def setup_logger(
    name: str = "notion_backup_restore",
    log_level: str = "INFO",
    log_file: Optional[str] = None,
    log_max_size: int = 10485760,  # 10MB
    log_backup_count: int = 5,
    verbose: bool = False,
    debug: bool = False
) -> logging.Logger:
    """
    Set up centralized logging configuration.
    
    Args:
        name: Logger name
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Log file path (optional)
        log_max_size: Maximum log file size in bytes
        log_backup_count: Number of backup log files to keep
        verbose: Enable verbose console output
        debug: Enable debug mode (overrides log_level)
        
    Returns:
        Configured logger instance
    """
    # Create logger
    logger = logging.getLogger(name)
    
    # Clear any existing handlers
    logger.handlers.clear()
    
    # Set log level
    if debug:
        level = logging.DEBUG
    else:
        level = getattr(logging, log_level.upper(), logging.INFO)
    
    logger.setLevel(level)
    
    # Create formatters
    detailed_formatter = logging.Formatter(
        fmt='%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    simple_formatter = logging.Formatter(
        fmt='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%H:%M:%S'
    )
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    if verbose or debug:
        console_handler.setFormatter(detailed_formatter)
        console_handler.setLevel(logging.DEBUG if debug else logging.INFO)
    else:
        console_handler.setFormatter(simple_formatter)
        console_handler.setLevel(logging.INFO)
    
    logger.addHandler(console_handler)
    
    # File handler (if specified)
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        
        file_handler = logging.handlers.RotatingFileHandler(
            filename=log_file,
            maxBytes=log_max_size,
            backupCount=log_backup_count,
            encoding='utf-8'
        )
        file_handler.setFormatter(detailed_formatter)
        file_handler.setLevel(logging.DEBUG)
        
        logger.addHandler(file_handler)
    
    # Prevent duplicate logs from parent loggers
    logger.propagate = False
    
    return logger


def get_logger(name: str = "notion_backup_restore") -> logging.Logger:
    """
    Get an existing logger or create a basic one.
    
    Args:
        name: Logger name
        
    Returns:
        Logger instance
    """
    logger = logging.getLogger(name)
    
    # If logger has no handlers, set up basic configuration
    if not logger.handlers:
        logger = setup_logger(name)
    
    return logger


class APICallLogger:
    """
    Specialized logger for API calls with structured logging.
    
    This class provides methods for logging API requests, responses,
    rate limiting events, and errors in a structured format.
    """
    
    def __init__(self, logger: Optional[logging.Logger] = None):
        """
        Initialize API call logger.
        
        Args:
            logger: Logger instance (creates default if None)
        """
        self.logger = logger or get_logger("notion_api")
    
    def log_request(self, method: str, endpoint: str, **kwargs) -> None:
        """
        Log an API request.
        
        Args:
            method: HTTP method
            endpoint: API endpoint
            **kwargs: Additional request parameters
        """
        self.logger.debug(
            f"API Request: {method} {endpoint}",
            extra={
                "event_type": "api_request",
                "method": method,
                "endpoint": endpoint,
                "timestamp": datetime.utcnow().isoformat(),
                **kwargs
            }
        )
    
    def log_response(self, method: str, endpoint: str, status_code: int, 
                    response_time: float, **kwargs) -> None:
        """
        Log an API response.
        
        Args:
            method: HTTP method
            endpoint: API endpoint
            status_code: HTTP status code
            response_time: Response time in seconds
            **kwargs: Additional response data
        """
        level = logging.INFO if status_code < 400 else logging.WARNING
        
        self.logger.log(
            level,
            f"API Response: {method} {endpoint} - {status_code} ({response_time:.2f}s)",
            extra={
                "event_type": "api_response",
                "method": method,
                "endpoint": endpoint,
                "status_code": status_code,
                "response_time": response_time,
                "timestamp": datetime.utcnow().isoformat(),
                **kwargs
            }
        )
    
    def log_rate_limit(self, wait_time: float, current_rate: float, 
                      limit: float, **kwargs) -> None:
        """
        Log rate limiting event.
        
        Args:
            wait_time: Time waited in seconds
            current_rate: Current request rate
            limit: Rate limit threshold
            **kwargs: Additional rate limiting data
        """
        if wait_time > 0:
            self.logger.info(
                f"Rate limit: waiting {wait_time:.2f}s (rate: {current_rate:.2f}/{limit})",
                extra={
                    "event_type": "rate_limit",
                    "wait_time": wait_time,
                    "current_rate": current_rate,
                    "limit": limit,
                    "timestamp": datetime.utcnow().isoformat(),
                    **kwargs
                }
            )
    
    def log_retry(self, attempt: int, max_attempts: int, delay: float, 
                 error: str, **kwargs) -> None:
        """
        Log retry attempt.
        
        Args:
            attempt: Current attempt number
            max_attempts: Maximum number of attempts
            delay: Delay before retry in seconds
            error: Error that triggered retry
            **kwargs: Additional retry data
        """
        self.logger.warning(
            f"Retry {attempt}/{max_attempts} after {delay:.2f}s: {error}",
            extra={
                "event_type": "retry",
                "attempt": attempt,
                "max_attempts": max_attempts,
                "delay": delay,
                "error": error,
                "timestamp": datetime.utcnow().isoformat(),
                **kwargs
            }
        )
    
    def log_error(self, error: Exception, context: str = "", **kwargs) -> None:
        """
        Log an error with context.
        
        Args:
            error: Exception that occurred
            context: Additional context about the error
            **kwargs: Additional error data
        """
        self.logger.error(
            f"Error {context}: {error}",
            extra={
                "event_type": "error",
                "error_type": type(error).__name__,
                "error_message": str(error),
                "context": context,
                "timestamp": datetime.utcnow().isoformat(),
                **kwargs
            },
            exc_info=True
        )


class ProgressLogger:
    """
    Logger for tracking operation progress.
    
    This class provides methods for logging progress updates,
    completion status, and performance metrics.
    """
    
    def __init__(self, logger: Optional[logging.Logger] = None):
        """
        Initialize progress logger.
        
        Args:
            logger: Logger instance (creates default if None)
        """
        self.logger = logger or get_logger("notion_progress")
        self.start_time: Optional[datetime] = None
    
    def start_operation(self, operation: str, total_items: Optional[int] = None) -> None:
        """
        Log the start of an operation.
        
        Args:
            operation: Operation name
            total_items: Total number of items to process (optional)
        """
        self.start_time = datetime.utcnow()
        
        message = f"Starting {operation}"
        if total_items:
            message += f" ({total_items} items)"
        
        self.logger.info(
            message,
            extra={
                "event_type": "operation_start",
                "operation": operation,
                "total_items": total_items,
                "start_time": self.start_time.isoformat(),
            }
        )
    
    def log_progress(self, operation: str, completed: int, total: int, 
                    current_item: str = "") -> None:
        """
        Log progress update.
        
        Args:
            operation: Operation name
            completed: Number of completed items
            total: Total number of items
            current_item: Current item being processed
        """
        percentage = (completed / total) * 100 if total > 0 else 0
        
        message = f"{operation}: {completed}/{total} ({percentage:.1f}%)"
        if current_item:
            message += f" - {current_item}"
        
        self.logger.info(
            message,
            extra={
                "event_type": "progress",
                "operation": operation,
                "completed": completed,
                "total": total,
                "percentage": percentage,
                "current_item": current_item,
                "timestamp": datetime.utcnow().isoformat(),
            }
        )
    
    def complete_operation(self, operation: str, total_items: int, 
                          success_count: int, error_count: int = 0) -> None:
        """
        Log operation completion.
        
        Args:
            operation: Operation name
            total_items: Total number of items processed
            success_count: Number of successful items
            error_count: Number of failed items
        """
        duration = None
        if self.start_time:
            duration = (datetime.utcnow() - self.start_time).total_seconds()
        
        message = f"Completed {operation}: {success_count}/{total_items} successful"
        if error_count > 0:
            message += f", {error_count} errors"
        if duration:
            message += f" (took {duration:.2f}s)"
        
        level = logging.INFO if error_count == 0 else logging.WARNING
        
        self.logger.log(
            level,
            message,
            extra={
                "event_type": "operation_complete",
                "operation": operation,
                "total_items": total_items,
                "success_count": success_count,
                "error_count": error_count,
                "duration": duration,
                "timestamp": datetime.utcnow().isoformat(),
            }
        )
