#!/usr/bin/env python3
"""
Main backup script entry point.

This provides a simple `python backup.py` interface for users.
"""

import sys
from pathlib import Path

# Add src to Python path to allow imports
src_path = Path(__file__).parent / "src"
sys.path.insert(0, str(src_path))

from src.notion_backup_restore.cli.backup_cli import backup_app

if __name__ == "__main__":
    backup_app()
