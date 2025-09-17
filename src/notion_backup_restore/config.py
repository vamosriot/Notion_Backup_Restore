"""
Configuration management for Notion backup and restore operations.

This module provides dataclasses for managing configuration settings,
environment variable loading, and validation of required parameters.
"""

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, Dict, Any
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


@dataclass
class BackupConfig:
    """Configuration for backup operations."""
    
    # Notion API Configuration
    notion_token: str = field(default_factory=lambda: os.getenv("NOTION_TOKEN", ""))
    
    # Backup Settings
    output_dir: Path = field(default_factory=lambda: Path(os.getenv("BACKUP_OUTPUT_DIR", "./backups")))
    include_blocks: bool = field(default_factory=lambda: os.getenv("BACKUP_INCLUDE_BLOCKS", "false").lower() == "true")
    validate_integrity: bool = field(default_factory=lambda: os.getenv("BACKUP_VALIDATE_INTEGRITY", "true").lower() == "true")
    process_for_compatibility: bool = field(default_factory=lambda: os.getenv("BACKUP_PROCESS_FOR_COMPATIBILITY", "true").lower() == "true")
    
    # Rate Limiting
    requests_per_second: float = field(default_factory=lambda: float(os.getenv("RATE_LIMIT_REQUESTS_PER_SECOND", "2.5")))
    burst_size: int = field(default_factory=lambda: int(os.getenv("RATE_LIMIT_BURST_SIZE", "5")))
    window_size: int = field(default_factory=lambda: int(os.getenv("RATE_LIMIT_WINDOW_SIZE", "10")))
    
    # Retry Configuration
    max_retries: int = field(default_factory=lambda: int(os.getenv("MAX_RETRIES", "3")))
    retry_backoff_factor: float = field(default_factory=lambda: float(os.getenv("RETRY_BACKOFF_FACTOR", "2")))
    retry_max_delay: int = field(default_factory=lambda: int(os.getenv("RETRY_MAX_DELAY", "60")))
    
    # Logging
    log_level: str = field(default_factory=lambda: os.getenv("LOG_LEVEL", "INFO"))
    log_file: Optional[str] = field(default_factory=lambda: os.getenv("LOG_FILE"))
    log_max_size: int = field(default_factory=lambda: int(os.getenv("LOG_MAX_SIZE", "10485760")))  # 10MB
    log_backup_count: int = field(default_factory=lambda: int(os.getenv("LOG_BACKUP_COUNT", "5")))
    
    # Validation
    validation_timeout: int = field(default_factory=lambda: int(os.getenv("VALIDATION_TIMEOUT", "300")))
    validation_sample_size: int = field(default_factory=lambda: int(os.getenv("VALIDATION_SAMPLE_SIZE", "100")))
    
    # Development
    debug: bool = field(default_factory=lambda: os.getenv("DEBUG", "false").lower() == "true")
    verbose: bool = field(default_factory=lambda: os.getenv("VERBOSE", "false").lower() == "true")
    
    def __post_init__(self):
        """Validate configuration after initialization."""
        self.validate()
        
        # Ensure output directory exists
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def validate(self) -> None:
        """Validate configuration settings."""
        if not self.notion_token:
            raise ValueError("NOTION_TOKEN is required. Please set it in your .env file.")
        
        if not (self.notion_token.startswith("secret_") or self.notion_token.startswith("ntn_")):
            raise ValueError("NOTION_TOKEN must be a valid Notion integration token starting with 'secret_' or 'ntn_'")
        
        if self.requests_per_second <= 0:
            raise ValueError("RATE_LIMIT_REQUESTS_PER_SECOND must be positive")
        
        if self.requests_per_second > 3:
            raise ValueError("RATE_LIMIT_REQUESTS_PER_SECOND should not exceed 3 (Notion API limit)")
        
        if self.max_retries < 0:
            raise ValueError("MAX_RETRIES must be non-negative")
        
        if self.retry_backoff_factor <= 0:
            raise ValueError("RETRY_BACKOFF_FACTOR must be positive")
        
        if self.validation_timeout <= 0:
            raise ValueError("VALIDATION_TIMEOUT must be positive")


@dataclass
class RestoreConfig:
    """Configuration for restore operations."""
    
    # Notion API Configuration
    notion_token: str = field(default_factory=lambda: os.getenv("NOTION_TOKEN", ""))
    
    # Restore Settings
    backup_dir: Optional[Path] = None
    parent_page_id: Optional[str] = field(default_factory=lambda: os.getenv("RESTORE_PARENT_PAGE_ID"))
    validate_after: bool = field(default_factory=lambda: os.getenv("RESTORE_VALIDATE_AFTER", "true").lower() == "true")
    dry_run: bool = field(default_factory=lambda: os.getenv("RESTORE_DRY_RUN", "false").lower() == "true")
    
    # Rate Limiting (same as backup)
    requests_per_second: float = field(default_factory=lambda: float(os.getenv("RATE_LIMIT_REQUESTS_PER_SECOND", "2.5")))
    burst_size: int = field(default_factory=lambda: int(os.getenv("RATE_LIMIT_BURST_SIZE", "5")))
    window_size: int = field(default_factory=lambda: int(os.getenv("RATE_LIMIT_WINDOW_SIZE", "10")))
    
    # Retry Configuration (same as backup)
    max_retries: int = field(default_factory=lambda: int(os.getenv("MAX_RETRIES", "3")))
    retry_backoff_factor: float = field(default_factory=lambda: float(os.getenv("RETRY_BACKOFF_FACTOR", "2")))
    retry_max_delay: int = field(default_factory=lambda: int(os.getenv("RETRY_MAX_DELAY", "60")))
    
    # Logging (same as backup)
    log_level: str = field(default_factory=lambda: os.getenv("LOG_LEVEL", "INFO"))
    log_file: Optional[str] = field(default_factory=lambda: os.getenv("LOG_FILE"))
    log_max_size: int = field(default_factory=lambda: int(os.getenv("LOG_MAX_SIZE", "10485760")))
    log_backup_count: int = field(default_factory=lambda: int(os.getenv("LOG_BACKUP_COUNT", "5")))
    
    # Validation (same as backup)
    validation_timeout: int = field(default_factory=lambda: int(os.getenv("VALIDATION_TIMEOUT", "300")))
    validation_sample_size: int = field(default_factory=lambda: int(os.getenv("VALIDATION_SAMPLE_SIZE", "100")))
    
    # Development (same as backup)
    debug: bool = field(default_factory=lambda: os.getenv("DEBUG", "false").lower() == "true")
    verbose: bool = field(default_factory=lambda: os.getenv("VERBOSE", "false").lower() == "true")
    
    def __post_init__(self):
        """Validate configuration after initialization."""
        self.validate()
    
    def validate(self) -> None:
        """Validate configuration settings."""
        if not self.notion_token:
            raise ValueError("NOTION_TOKEN is required. Please set it in your .env file.")
        
        if not (self.notion_token.startswith("secret_") or self.notion_token.startswith("ntn_")):
            raise ValueError("NOTION_TOKEN must be a valid Notion integration token starting with 'secret_' or 'ntn_'")
        
        if self.backup_dir and not self.backup_dir.exists():
            raise ValueError(f"Backup directory does not exist: {self.backup_dir}")
        
        if self.backup_dir and not self.backup_dir.is_dir():
            raise ValueError(f"Backup path is not a directory: {self.backup_dir}")
        
        if self.requests_per_second <= 0:
            raise ValueError("RATE_LIMIT_REQUESTS_PER_SECOND must be positive")
        
        if self.requests_per_second > 3:
            raise ValueError("RATE_LIMIT_REQUESTS_PER_SECOND should not exceed 3 (Notion API limit)")
        
        if self.max_retries < 0:
            raise ValueError("MAX_RETRIES must be non-negative")
        
        if self.retry_backoff_factor <= 0:
            raise ValueError("RETRY_BACKOFF_FACTOR must be positive")
        
        if self.validation_timeout <= 0:
            raise ValueError("VALIDATION_TIMEOUT must be positive")


def get_backup_config(**overrides) -> BackupConfig:
    """
    Get backup configuration with optional overrides.
    
    Args:
        **overrides: Configuration values to override
        
    Returns:
        BackupConfig instance
    """
    config = BackupConfig()
    
    # Apply overrides
    for key, value in overrides.items():
        if hasattr(config, key):
            setattr(config, key, value)
        else:
            raise ValueError(f"Unknown configuration key: {key}")
    
    return config


def get_restore_config(**overrides) -> RestoreConfig:
    """
    Get restore configuration with optional overrides.
    
    Args:
        **overrides: Configuration values to override
        
    Returns:
        RestoreConfig instance
    """
    config = RestoreConfig()
    
    # Apply overrides
    for key, value in overrides.items():
        if hasattr(config, key):
            setattr(config, key, value)
        else:
            raise ValueError(f"Unknown configuration key: {key}")
    
    return config


# Workspace-specific configuration
# Note: Documentation is a wiki but still needs to be backed up
WORKSPACE_DATABASES = {
    "Documentation": {
        "type": "wiki",  # Special handling for wikis
        "properties": {
            "Page": {"type": "title"},
            "Type": {"type": "multi_select"},
            "Parent item": {"type": "relation"},
            "Sub item": {"type": "relation"},
            "Responsible for the page": {"type": "people"},
            "Responsible for the job": {"type": "people"},
            "✅ Tasks": {"type": "relation", "relation_database": "Tasks"},
            "Created time": {"type": "created_time"},
            "Last edited time": {"type": "last_edited_time"},
            "Last edited by": {"type": "last_edited_by"},
        }
    },
    "Tasks": {
        "type": "database",
        "properties": {
            "Summary": {"type": "title"},
            "Status": {"type": "status"},
            "Priority": {"type": "select"},
            "Assignee": {"type": "people"},
            "Due": {"type": "date"},
            "Effort": {"type": "number"},
            "Value": {"type": "number"},
            "ROI": {"type": "formula", "formula": "round(Value/(Effort*400)*10)/10"},
            "📗 Documentation": {"type": "relation", "relation_database": "Documentation"},
            "🗒️ Meeting notes": {"type": "relation", "relation_database": "Notes"},
            "Sprint": {"type": "relation", "relation_database": "Sprints"},
            "Tags": {"type": "multi_select"},
            "Watching": {"type": "people"},
            "Parent task": {"type": "relation"},
            "Sub-task": {"type": "relation"},
            "Blocking": {"type": "relation"},
            "Blocked by": {"type": "relation"},
            "GitHub PRs": {"type": "relation"},
            "ID": {"type": "unique_id"},
            "Merged at (github)": {"type": "rollup"},
            "Created time": {"type": "created_time"},
            "Last edited time": {"type": "last_edited_time"},
            "Created by": {"type": "created_by"},
            "Last edited by": {"type": "last_edited_by"},
        }
    },
    "Notes": {
        "type": "database",
        "properties": {
            "Name": {"type": "title"},
            "Date": {"type": "date"},
            "Owner": {"type": "people"},
            "Attendees": {"type": "people"},
            "📗 Documentation": {"type": "relation", "relation_database": "Documentation"},
            "✅ Tasks": {"type": "relation", "relation_database": "Tasks"},
            "Created time": {"type": "created_time"},
            "Last edited time": {"type": "last_edited_time"},
            "Last edited by": {"type": "last_edited_by"},
        }
    },
    "Sprints": {
        "type": "database",
        "properties": {
            "Name": {"type": "title"},
            "Start of the sprint": {"type": "date"},
            "End of the sprint": {"type": "date"},
            "Tasks": {"type": "relation", "relation_database": "Tasks"},
            "Effort delivered": {"type": "formula"},
            "Effort \"planned\"": {"type": "formula"},
        }
    }
}

# Dependency order for restoration (databases with no dependencies first)
RESTORATION_ORDER = ["Documentation", "Notes", "Sprints", "Tasks"]

# Workspace structure information
WORKSPACE_STRUCTURE = {
    "teamspace": "General",
    "databases_parent_page": "Databases",  # Page containing all databases and wikis
    "databases": ["Tasks", "Notes", "Sprints"],
    "wikis": ["Documentation"],  # Wikis are backed up but handled differently
}
