#!/usr/bin/env python3
"""
Process existing backup for compatibility.

This script takes an existing backup and processes it for compatibility
with current API requirements, creating a new processed backup.
"""

import sys
import json
from pathlib import Path
from typing import Dict, Any

# Add src to Python path to allow imports
src_path = Path(__file__).parent / "src"
sys.path.insert(0, str(src_path))

from src.notion_backup_restore.backup.backup_processor import BackupProcessor
from src.notion_backup_restore.backup.data_processor import DataProcessor
from src.notion_backup_restore.utils.logger import setup_logger


def load_backup_data(backup_dir: Path) -> tuple[Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]]]:
    """
    Load existing backup data from files.
    
    Args:
        backup_dir: Path to backup directory
        
    Returns:
        Tuple of (schemas, contents)
    """
    databases_dir = backup_dir / "databases"
    
    if not databases_dir.exists():
        raise ValueError(f"Databases directory not found: {databases_dir}")
    
    # Load manifest to get database list
    manifest_file = backup_dir / "manifest.json"
    if not manifest_file.exists():
        raise ValueError(f"Manifest file not found: {manifest_file}")
    
    with open(manifest_file, 'r', encoding='utf-8') as f:
        manifest = json.load(f)
    
    schemas = {}
    contents = {}
    
    # Load each database
    for db_name, db_info in manifest.get("databases", {}).items():
        # Load schema
        schema_file = databases_dir / db_info.get("schema_file", f"{db_name.lower()}_schema.json")
        if schema_file.exists():
            with open(schema_file, 'r', encoding='utf-8') as f:
                schemas[db_name] = json.load(f)
        
        # Load content
        content_file = databases_dir / db_info.get("data_file", f"{db_name.lower()}_data.json")
        if content_file.exists():
            with open(content_file, 'r', encoding='utf-8') as f:
                contents[db_name] = json.load(f)
    
    return schemas, contents


def process_existing_backup(source_dir: Path, target_dir: Path) -> None:
    """
    Process an existing backup for compatibility.
    
    Args:
        source_dir: Source backup directory
        target_dir: Target directory for processed backup
    """
    logger = setup_logger("backup_processor", verbose=True)
    
    logger.info(f"Processing backup from {source_dir} to {target_dir}")
    
    # Load existing backup data
    logger.info("Loading existing backup data...")
    schemas, contents = load_backup_data(source_dir)
    
    logger.info(f"Loaded {len(schemas)} schemas and {len(contents)} content files")
    
    # Initialize processor
    processor = BackupProcessor(logger)
    data_processor = DataProcessor(logger)
    
    # Process schemas
    logger.info("Processing schemas for compatibility...")
    processed_schemas = {}
    for db_name, schema_data in schemas.items():
        processed_schema = data_processor.process_database_schema(schema_data)
        processed_schemas[db_name] = processed_schema
    
    # Process contents
    logger.info("Processing contents for compatibility...")
    processed_contents = {}
    for db_name, content_data in contents.items():
        processed_content = data_processor.process_database_content(content_data)
        processed_contents[db_name] = processed_content
    
    # Save processed backup
    logger.info(f"Saving processed backup to {target_dir}")
    processor.save_processed_backup(
        processed_schemas=processed_schemas,
        processed_contents=processed_contents,
        backup_dir=target_dir
    )
    
    # Copy other files from source
    logger.info("Copying additional files...")
    for item in source_dir.iterdir():
        if item.name not in ["databases", "manifest.json"]:
            if item.is_file():
                target_file = target_dir / item.name
                target_file.write_bytes(item.read_bytes())
                logger.debug(f"Copied {item.name}")
    
    # Get processing stats
    stats = data_processor.get_processing_stats()
    
    logger.info("Processing completed successfully!")
    logger.info(f"Processing Summary:")
    logger.info(f"  - Users normalized: {stats['users_normalized']}")
    logger.info(f"  - Relations fixed: {stats['relations_fixed']}")
    logger.info(f"  - Blocks sanitized: {stats['blocks_sanitized']}")
    logger.info(f"  - Select options cleaned: {stats['select_options_cleaned']}")
    logger.info(f"  - Properties processed: {stats['properties_processed']}")
    logger.info(f"  - Pages processed: {stats['pages_processed']}")
    
    if stats['errors_found'] > 0:
        logger.warning(f"  - Errors found: {stats['errors_found']}")


def main():
    """Main processing script."""
    if len(sys.argv) != 3:
        print("Usage: python process_existing_backup.py <source_backup_dir> <target_backup_dir>")
        print("\nExample:")
        print("  python process_existing_backup.py backups/backup_20250916_095206 backups/backup_20250916_095206_processed")
        sys.exit(1)
    
    source_dir = Path(sys.argv[1])
    target_dir = Path(sys.argv[2])
    
    if not source_dir.exists():
        print(f"Error: Source backup directory does not exist: {source_dir}")
        sys.exit(1)
    
    if not source_dir.is_dir():
        print(f"Error: Source path is not a directory: {source_dir}")
        sys.exit(1)
    
    if target_dir.exists():
        print(f"Error: Target directory already exists: {target_dir}")
        print("Please choose a different target directory or remove the existing one.")
        sys.exit(1)
    
    try:
        # Process the backup
        process_existing_backup(source_dir, target_dir)
        
        print(f"\n✅ Backup processing completed successfully!")
        print(f"📁 Processed backup saved to: {target_dir}")
        print(f"\n🔍 To validate the processed backup, run:")
        print(f"   python validate_backup.py {target_dir}")
        
    except Exception as e:
        print(f"❌ Error during processing: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
