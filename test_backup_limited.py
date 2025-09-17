#!/usr/bin/env python3
"""
Limited backup script for testing.

This script creates a backup with a limited number of pages from specific databases,
perfect for testing the backup and restore functionality without processing huge datasets.
"""

import sys
import os
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Any

# Add src to Python path to allow imports
src_path = Path(__file__).parent / "src"
sys.path.insert(0, str(src_path))

from src.notion_backup_restore.backup.manager import NotionBackupManager
from src.notion_backup_restore.backup.database_finder import DatabaseFinder
from src.notion_backup_restore.backup.schema_extractor import SchemaExtractor
from src.notion_backup_restore.backup.content_extractor import ContentExtractor, DatabaseContent, PageContent
from src.notion_backup_restore.backup.backup_processor import BackupProcessor
from src.notion_backup_restore.utils.api_client import create_notion_client
from src.notion_backup_restore.utils.logger import setup_logger
from src.notion_backup_restore.config import BackupConfig


class LimitedContentExtractor(ContentExtractor):
    """Content extractor with page limits for testing."""
    
    def extract_content_limited(
        self,
        database_id: str,
        database_name: str = "",
        include_blocks: bool = False,
        page_limit: int = 100,
        page_size: int = 100,
        progress_callback: Optional[callable] = None
    ) -> DatabaseContent:
        """
        Extract limited content from a database.
        
        Args:
            database_id: ID of the database
            database_name: Name of the database (for logging)
            include_blocks: Whether to extract block content from pages
            page_limit: Maximum number of pages to extract
            page_size: Number of pages to fetch per request
            progress_callback: Optional callback for progress updates
            
        Returns:
            DatabaseContent object with limited pages
        """
        self.logger.info(
            f"Extracting LIMITED content from database '{database_name}' ({database_id}) - max {page_limit} pages"
        )
        
        pages = []
        total_pages = 0
        
        try:
            # Extract pages with pagination and limit
            for page_batch in self._paginate_pages_limited(database_id, page_size, page_limit):
                batch_pages = []
                
                for page_data in page_batch:
                    if total_pages >= page_limit:
                        break
                        
                    page_content = self._extract_page_content(
                        page_data, 
                        include_blocks=include_blocks
                    )
                    batch_pages.append(page_content)
                    total_pages += 1
                
                pages.extend(batch_pages)
                
                # Progress callback
                if progress_callback:
                    progress_callback(total_pages, min(page_limit, total_pages))
                
                self.logger.debug(f"Extracted {len(batch_pages)} pages (total: {total_pages}/{page_limit})")
                
                # Stop if we've reached the limit
                if total_pages >= page_limit:
                    break
            
            content = DatabaseContent(
                database_id=database_id,
                database_name=database_name,
                pages=pages,
                total_pages=total_pages,
                extraction_time=datetime.utcnow().isoformat()
            )
            
            self.logger.info(
                f"Extracted {total_pages} pages from database '{database_name}' (limited to {page_limit})"
            )
            
            return content
            
        except Exception as e:
            self.logger.error(
                f"Error extracting limited content from database '{database_name}' ({database_id}): {e}"
            )
            raise
    
    def _paginate_pages_limited(self, database_id: str, page_size: int, page_limit: int):
        """Paginate through pages with a limit."""
        start_cursor = None
        pages_extracted = 0
        
        while pages_extracted < page_limit:
            # Adjust page size for final batch
            current_page_size = min(page_size, page_limit - pages_extracted)
            
            query_params = {
                "page_size": current_page_size
            }
            
            if start_cursor:
                query_params["start_cursor"] = start_cursor
            
            try:
                response = self.api_client.query_database(database_id, **query_params)
                
                results = response.get("results", [])
                if not results:
                    break
                
                # Only yield up to the limit
                results_to_yield = results[:page_limit - pages_extracted]
                yield results_to_yield
                pages_extracted += len(results_to_yield)
                
                # Stop if we've reached the limit or no more pages
                if pages_extracted >= page_limit or not response.get("has_more", False):
                    break
                
                start_cursor = response.get("next_cursor")
                if not start_cursor:
                    break
                    
            except Exception as e:
                self.logger.error(f"Error querying database {database_id}: {e}")
                raise


class LimitedBackupManager:
    """Backup manager for limited/testing backups."""
    
    def __init__(self, config: BackupConfig):
        self.config = config
        self.logger = setup_logger(
            name="limited_backup",
            log_level=config.log_level,
            verbose=True,
            debug=config.debug
        )
        
        # Initialize API client
        self.api_client = create_notion_client(
            auth=config.notion_token,
            requests_per_second=config.requests_per_second,
            max_retries=config.max_retries,
            logger=self.logger
        )
        
        # Initialize components
        self.database_finder = DatabaseFinder(self.api_client, self.logger)
        self.schema_extractor = SchemaExtractor(self.api_client, self.logger)
        self.content_extractor = LimitedContentExtractor(self.api_client, self.logger)
        self.backup_processor = BackupProcessor(self.logger)
    
    def create_limited_backup(
        self,
        database_name: str = "Documentation",
        page_limit: int = 100,
        include_blocks: bool = True
    ) -> Path:
        """
        Create a limited backup for testing.
        
        Args:
            database_name: Name of the database to backup
            page_limit: Maximum number of pages to backup
            include_blocks: Whether to include block content
            
        Returns:
            Path to the backup directory
        """
        self.logger.info(f"Starting LIMITED backup: {database_name} ({page_limit} pages max)")
        
        # Create backup directory
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_dir = self.config.output_dir / f"test_backup_{database_name.lower()}_{page_limit}pages_{timestamp}"
        backup_dir.mkdir(parents=True, exist_ok=True)
        (backup_dir / "databases").mkdir(exist_ok=True)
        
        try:
            # Step 1: Find the target database
            self.logger.info(f"Finding database: {database_name}")
            databases = self.database_finder.find_target_databases([database_name])
            
            if database_name not in databases:
                raise ValueError(f"Database '{database_name}' not found")
            
            db_info = databases[database_name]
            self.logger.info(f"Found database: {db_info.name} ({db_info.id})")
            
            # Step 2: Extract schema
            self.logger.info("Extracting schema...")
            schema = self.schema_extractor.extract_schema(db_info.id)
            
            # Step 3: Extract limited content
            self.logger.info(f"Extracting content (max {page_limit} pages)...")
            
            def progress_callback(current: int, total: int):
                self.logger.info(f"Progress: {current}/{total} pages extracted")
            
            content = self.content_extractor.extract_content_limited(
                database_id=db_info.id,
                database_name=database_name,
                include_blocks=include_blocks,
                page_limit=page_limit,
                progress_callback=progress_callback
            )
            
            # Step 4: Process for compatibility (if enabled)
            if self.config.process_for_compatibility:
                self.logger.info("Processing data for compatibility...")
                
                # Convert to dict format for processing
                schema_dict = self._schema_to_dict(schema)
                content_dict = self._content_to_dict(content)
                
                # Process the data using DataProcessor directly
                from src.notion_backup_restore.backup.data_processor import DataProcessor
                data_processor = DataProcessor(self.logger)
                
                # Set available databases for limited backup (only current database)
                data_processor.set_available_databases({db_info.id})
                
                processed_schema = data_processor.process_database_schema(schema_dict)
                processed_content = data_processor.process_database_content(content_dict)
                
                # Save processed data
                self.backup_processor.save_processed_backup(
                    processed_schemas={database_name: processed_schema},
                    processed_contents={database_name: processed_content},
                    backup_dir=backup_dir
                )
                # Note: save_processed_backup creates its own manifest with compatibility_layer: true
            else:
                # Save unprocessed data
                self._save_unprocessed_backup(schema, content, backup_dir, database_name)
                # Step 5: Create manifest for unprocessed backup
                self._create_manifest(backup_dir, database_name, db_info, schema, content)
            
            self.logger.info(f"✅ Limited backup completed: {backup_dir}")
            self.logger.info(f"📊 Backed up {content.total_pages} pages from {database_name}")
            
            return backup_dir
            
        except Exception as e:
            self.logger.error(f"Limited backup failed: {e}")
            raise
    
    def _schema_to_dict(self, schema):
        """Convert schema to dict format."""
        return {
            "id": schema.id,
            "name": schema.name,
            "title": schema.title,
            "description": schema.description,
            "properties": {
                prop_name: {
                    "name": prop_schema.name,
                    "type": prop_schema.type,
                    "config": prop_schema.config,
                    "id": prop_schema.id,
                    "description": prop_schema.description,
                }
                for prop_name, prop_schema in schema.properties.items()
            },
            "parent": schema.parent,
            "url": schema.url,
            "archived": schema.archived,
            "is_inline": schema.is_inline,
            "created_time": schema.created_time,
            "last_edited_time": schema.last_edited_time,
            "created_by": schema.created_by,
            "last_edited_by": schema.last_edited_by,
            "cover": schema.cover,
            "icon": schema.icon,
        }
    
    def _content_to_dict(self, content):
        """Convert content to dict format."""
        return {
            "database_id": content.database_id,
            "database_name": content.database_name,
            "total_pages": content.total_pages,
            "extraction_time": content.extraction_time,
            "pages": [
                {
                    "id": page.id,
                    "url": page.url,
                    "properties": page.properties,
                    "parent": page.parent,
                    "archived": page.archived,
                    "created_time": page.created_time,
                    "last_edited_time": page.last_edited_time,
                    "created_by": page.created_by,
                    "last_edited_by": page.last_edited_by,
                    "cover": page.cover,
                    "icon": page.icon,
                    "blocks": page.blocks,
                }
                for page in content.pages
            ]
        }
    
    def _save_unprocessed_backup(self, schema, content, backup_dir, database_name):
        """Save unprocessed backup data."""
        import json
        
        # Save schema
        schema_file = backup_dir / "databases" / f"{database_name.lower()}_schema.json"
        schema_dict = self._schema_to_dict(schema)
        with open(schema_file, 'w', encoding='utf-8') as f:
            json.dump(schema_dict, f, indent=2, ensure_ascii=False, default=str)
        
        # Save content
        content_file = backup_dir / "databases" / f"{database_name.lower()}_data.json"
        content_dict = self._content_to_dict(content)
        with open(content_file, 'w', encoding='utf-8') as f:
            json.dump(content_dict, f, indent=2, ensure_ascii=False, default=str)
    
    def _create_manifest(self, backup_dir, database_name, db_info, schema, content):
        """Create backup manifest."""
        import json
        
        manifest = {
            "version": "1.0",
            "backup_type": "limited_test",
            "created_at": datetime.utcnow().isoformat(),
            "config": {
                "include_blocks": True,
                "process_for_compatibility": self.config.process_for_compatibility,
                "page_limit": content.total_pages,
                "database_name": database_name
            },
            "databases": {
                database_name: {
                    "id": db_info.id,
                    "name": db_info.name,
                    "url": db_info.url,
                    "schema_file": f"{database_name.lower()}_schema.json",
                    "data_file": f"{database_name.lower()}_data.json",
                    "properties_count": len(schema.properties),
                    "pages_count": content.total_pages,
                    "created_time": db_info.created_time,
                    "last_edited_time": db_info.last_edited_time,
                }
            },
            "statistics": {
                "total_databases": 1,
                "total_pages": content.total_pages,
                "limited_backup": True
            }
        }
        
        manifest_file = backup_dir / "manifest.json"
        with open(manifest_file, 'w', encoding='utf-8') as f:
            json.dump(manifest, f, indent=2, ensure_ascii=False, default=str)


def main():
    """Main function for limited backup."""
    if len(sys.argv) > 1:
        try:
            page_limit = int(sys.argv[1])
        except ValueError:
            print("Error: Page limit must be a number")
            sys.exit(1)
    else:
        page_limit = 100  # Default
    
    print(f"🧪 Creating test backup with {page_limit} pages from Documentation database...")
    
    try:
        # Load configuration
        config = BackupConfig()
        
        # Create limited backup manager
        backup_manager = LimitedBackupManager(config)
        
        # Create the limited backup
        backup_dir = backup_manager.create_limited_backup(
            database_name="Documentation",
            page_limit=page_limit,
            include_blocks=True
        )
        
        print(f"\n✅ Test backup completed successfully!")
        print(f"📁 Backup location: {backup_dir}")
        print(f"📊 Pages backed up: {page_limit} (or fewer if database has less)")
        print(f"\n🔍 To validate this backup, run:")
        print(f"   python validate_backup.py {backup_dir}")
        print(f"\n🚀 To restore this backup, run:")
        print(f"   python restore.py {backup_dir}")
        
    except Exception as e:
        print(f"❌ Test backup failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
