#!/usr/bin/env python3
"""
Test backup from original Documentation database with actual content.
"""

import sys
import os
from pathlib import Path
from datetime import datetime

# Add src to Python path
src_path = Path(__file__).parent / "src"
sys.path.insert(0, str(src_path))

from src.notion_backup_restore.backup.schema_extractor import SchemaExtractor
from src.notion_backup_restore.backup.content_extractor import ContentExtractor
from src.notion_backup_restore.backup.data_processor import DataProcessor
from src.notion_backup_restore.backup.backup_processor import BackupProcessor
from src.notion_backup_restore.utils.api_client import create_notion_client
from src.notion_backup_restore.utils.logger import setup_logger
from src.notion_backup_restore.config import BackupConfig

def create_test_backup_from_original(page_limit: int = 5):
    """Create a test backup from the original Documentation database."""
    
    # Original Documentation database ID (from your backup manifest)
    ORIGINAL_DB_ID = "2139ae88-3977-4af8-96f4-9ecb1c106708"
    
    logger = setup_logger("test_original_backup")
    
    try:
        # Initialize components
        notion_token = os.getenv("NOTION_TOKEN")
        if not notion_token:
            raise ValueError("NOTION_TOKEN environment variable not set")
        api_client = create_notion_client(notion_token)
        schema_extractor = SchemaExtractor(api_client, logger)
        content_extractor = ContentExtractor(api_client, logger)
        
        # Create backup directory
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_dir = Path(f"backups/test_original_documentation_{page_limit}pages_{timestamp}")
        backup_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"🧪 Creating test backup from ORIGINAL Documentation database...")
        logger.info(f"📊 Target: {page_limit} pages from database {ORIGINAL_DB_ID}")
        
        # Extract schema
        logger.info("📋 Extracting schema...")
        schema = schema_extractor.extract_schema(ORIGINAL_DB_ID)
        
        # Extract limited content
        logger.info(f"📄 Extracting content (max {page_limit} pages)...")
        content = content_extractor.extract_content(
            database_id=ORIGINAL_DB_ID,
            include_blocks=True,
            page_limit=page_limit
        )
        
        logger.info(f"✅ Extracted {content.total_pages} pages from original database")
        
        # Process for compatibility
        logger.info("🔧 Processing data for compatibility...")
        data_processor = DataProcessor(logger)
        
        # Convert to dict format for processing
        schema_dict = {
            "id": schema.id,
            "name": schema.name,
            "title": schema.title,
            "description": schema.description,
            "properties": {name: {
                "name": prop.name,
                "type": prop.type,
                "config": prop.config,
                "id": prop.id,
                "description": prop.description
            } for name, prop in schema.properties.items()},
            "parent": schema.parent,
            "url": schema.url,
            "archived": schema.archived,
            "is_inline": schema.is_inline,
            "created_time": schema.created_time,
            "last_edited_time": schema.last_edited_time,
            "created_by": schema.created_by,
            "last_edited_by": schema.last_edited_by,
            "cover": schema.cover,
            "icon": schema.icon
        }
        
        content_dict = {
            "database_id": content.database_id,
            "total_pages": content.total_pages,
            "pages": [
                {
                    "id": page.id,
                    "created_time": page.created_time,
                    "last_edited_time": page.last_edited_time,
                    "created_by": page.created_by,
                    "last_edited_by": page.last_edited_by,
                    "cover": page.cover,
                    "icon": page.icon,
                    "parent": page.parent,
                    "archived": page.archived,
                    "properties": page.properties,
                    "url": page.url,
                    "blocks": page.blocks
                } for page in content.pages
            ]
        }
        
        # Process the data
        processed_schema = data_processor.process_database_schema(schema_dict)
        processed_content = data_processor.process_database_content(content_dict)
        
        # Save processed data
        backup_processor = BackupProcessor(logger)
        backup_processor.save_processed_backup(
            processed_schemas={"Documentation": processed_schema},
            processed_contents={"Documentation": processed_content},
            backup_dir=backup_dir
        )
        
        logger.info(f"✅ Test backup completed: {backup_dir}")
        logger.info(f"📊 Backed up {content.total_pages} pages from original Documentation database")
        
        print(f"\n✅ Test backup completed successfully!")
        print(f"📁 Backup location: {backup_dir}")
        print(f"📊 Pages backed up: {content.total_pages}")
        print(f"\n🔍 To validate this backup, run:")
        print(f"   python validate_backup.py {backup_dir}")
        print(f"\n🚀 To restore this backup, run:")
        print(f"   python restore.py main --parent-id YOUR_PAGE_ID {backup_dir}")
        
        return str(backup_dir)
        
    except Exception as e:
        logger.error(f"Test backup failed: {e}")
        raise

if __name__ == "__main__":
    page_limit = int(sys.argv[1]) if len(sys.argv) > 1 else 5
    create_test_backup_from_original(page_limit)
