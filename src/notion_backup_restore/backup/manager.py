"""
Main backup orchestration manager.

This module coordinates database discovery, schema extraction, and content backup
to create complete backup artifacts with validation and progress tracking.
"""

import json
import shutil
from pathlib import Path
from typing import Dict, List, Optional, Any, Callable
from datetime import datetime
import logging

from .database_finder import DatabaseFinder, DatabaseInfo
from .schema_extractor import SchemaExtractor, DatabaseSchema
from .content_extractor import ContentExtractor, DatabaseContent
from .backup_processor import BackupProcessor
from ..utils.api_client import NotionAPIClient, create_notion_client
from ..utils.logger import setup_logger, ProgressLogger
from ..config import BackupConfig, WORKSPACE_DATABASES
from ..validation.integrity_checker import IntegrityChecker


class NotionBackupManager:
    """
    Main backup orchestration class.
    
    Coordinates the complete backup workflow including database discovery,
    schema extraction, content backup, validation, and artifact creation.
    """
    
    def __init__(self, config: BackupConfig):
        """
        Initialize backup manager.
        
        Args:
            config: Backup configuration
        """
        self.config = config
        self.logger = setup_logger(
            name="backup_manager",
            log_level=config.log_level,
            log_file=config.log_file,
            verbose=config.verbose,
            debug=config.debug
        )
        self.progress_logger = ProgressLogger(self.logger)
        
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
        self.content_extractor = ContentExtractor(self.api_client, self.logger)
        self.backup_processor = BackupProcessor(self.logger)
        
        if config.validate_integrity:
            self.integrity_checker = IntegrityChecker(self.api_client, self.logger)
        else:
            self.integrity_checker = None
        
        # Backup state
        self.backup_dir: Optional[Path] = None
        self.discovered_databases: Dict[str, DatabaseInfo] = {}
        self.extracted_schemas: Dict[str, DatabaseSchema] = {}
        self.extracted_content: Dict[str, DatabaseContent] = {}
    
    def start_backup(
        self,
        database_names: Optional[List[str]] = None,
        progress_callback: Optional[Callable[[str, int, int], None]] = None,
        resume_from_dir: Optional[Path] = None
    ) -> Path:
        """
        Start the complete backup process.
        
        Args:
            database_names: List of database names to backup (defaults to all workspace databases)
            progress_callback: Optional callback for progress updates
            resume_from_dir: Optional path to resume from partial backup
            
        Returns:
            Path to the backup directory
            
        Raises:
            Various exceptions if backup fails
        """
        if database_names is None:
            database_names = list(WORKSPACE_DATABASES.keys())
        
        # Check if resuming from existing backup
        if resume_from_dir:
            self.logger.info(f"Resuming backup from: {resume_from_dir}")
            self.backup_dir = resume_from_dir
            if not self.backup_dir.exists():
                raise ValueError(f"Resume directory does not exist: {resume_from_dir}")
        else:
            self.logger.info(f"Starting new backup for databases: {database_names}")
            # Create backup directory
            self.backup_dir = self._create_backup_directory()
            self.logger.info(f"Created backup directory: {self.backup_dir}")
        
        self.progress_logger.start_operation("Backup", len(database_names))
        
        try:
            # Step 1: Discover databases
            self._discover_databases(database_names, progress_callback)
            
            # Step 2: Extract schemas
            self._extract_schemas(progress_callback)
            
            # Step 3: Extract content
            self._extract_content(progress_callback)
            
            # Step 4: Process data for compatibility (if enabled)
            if self.config.process_for_compatibility:
                self._process_backup_data(progress_callback)
            
            # Step 5: Validate backup (if enabled)
            if self.config.validate_integrity:
                self._validate_backup(progress_callback)
            
            # Step 6: Create backup manifest
            self._create_backup_manifest()
            
            # Step 7: Generate backup report
            self._generate_backup_report()
            
            self.progress_logger.complete_operation(
                "Backup",
                len(database_names),
                len(self.extracted_content),
                len(database_names) - len(self.extracted_content)
            )
            
            self.logger.info(f"Backup completed successfully: {self.backup_dir}")
            return self.backup_dir
            
        except Exception as e:
            self.logger.error(f"Backup failed: {e}")
            
            # Keep partial backup for resume capability
            if self.backup_dir and self.backup_dir.exists():
                self.logger.warning(
                    f"Partial backup preserved at: {self.backup_dir}\n"
                    f"To resume, use: --resume-from {self.backup_dir.name}"
                )
            
            raise
    
    def _create_backup_directory(self) -> Path:
        """Create timestamped backup directory."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_dir = self.config.output_dir / f"backup_{timestamp}"
        
        # Create directory structure
        backup_dir.mkdir(parents=True, exist_ok=True)
        (backup_dir / "databases").mkdir(exist_ok=True)
        (backup_dir / "logs").mkdir(exist_ok=True)
        
        return backup_dir
    
    def _discover_databases(
        self,
        database_names: List[str],
        progress_callback: Optional[Callable[[str, int, int], None]] = None
    ) -> None:
        """Discover target databases."""
        self.logger.info("Discovering databases...")
        
        try:
            self.discovered_databases = self.database_finder.find_target_databases(database_names)
            
            # Validate database structures
            validation_results = self.database_finder.validate_all_databases()
            
            for db_name, errors in validation_results.items():
                if errors:
                    self.logger.warning(f"Validation issues for database '{db_name}': {errors}")
            
            if progress_callback:
                progress_callback("Database Discovery", len(self.discovered_databases), len(database_names))
            
        except Exception as e:
            self.logger.error(f"Database discovery failed: {e}")
            raise
    
    def _extract_schemas(
        self,
        progress_callback: Optional[Callable[[str, int, int], None]] = None
    ) -> None:
        """Extract schemas from discovered databases."""
        self.logger.info("Extracting database schemas...")
        
        total_databases = len(self.discovered_databases)
        
        for i, (db_name, db_info) in enumerate(self.discovered_databases.items(), 1):
            try:
                self.logger.info(f"Extracting schema {i}/{total_databases}: {db_name}")
                
                # Use raw_data from discovery if available (avoids API call that may fail)
                schema = self.schema_extractor.extract_schema(db_info.id, db_info.raw_data)
                self.extracted_schemas[db_name] = schema
                
                # Save schema to file
                schema_file = self.backup_dir / "databases" / f"{db_name.lower()}_schema.json"
                self._save_schema_to_file(schema, schema_file)
                
                if progress_callback:
                    progress_callback("Schema Extraction", i, total_databases)
                
            except Exception as e:
                self.logger.error(f"Failed to extract schema for database '{db_name}': {e}")
                raise
    
    def _extract_content(
        self,
        progress_callback: Optional[Callable[[str, int, int], None]] = None
    ) -> None:
        """Extract content from discovered databases."""
        self.logger.info("Extracting database content...")
        
        total_databases = len(self.discovered_databases)
        
        for i, (db_name, db_info) in enumerate(self.discovered_databases.items(), 1):
            try:
                self.logger.info(f"Extracting content {i}/{total_databases}: {db_name}")
                
                # Check if resuming - load already downloaded page IDs
                skip_page_ids = set()
                existing_pages = []
                content_file = self.backup_dir / "databases" / f"{db_name.lower()}_data.json"
                
                if content_file.exists():
                    self.logger.info(f"Found existing data file for {db_name}, loading to resume...")
                    try:
                        with open(content_file, 'r', encoding='utf-8') as f:
                            existing_data = json.load(f)
                            existing_pages = existing_data.get("pages", [])
                            skip_page_ids = {page["id"] for page in existing_pages}
                            self.logger.info(f"Loaded {len(skip_page_ids)} existing pages for {db_name}, will skip them")
                    except Exception as e:
                        self.logger.warning(f"Could not load existing data for {db_name}: {e}, starting fresh")
                
                # Progress callback for individual pages
                def page_progress(current_pages: int, total_pages: int):
                    self.progress_logger.log_progress(
                        f"Extracting {db_name}",
                        current_pages,
                        total_pages,
                        f"page {current_pages}"
                    )
                
                content = self.content_extractor.extract_content(
                    database_id=db_info.id,
                    database_name=db_name,
                    include_blocks=self.config.include_blocks,
                    progress_callback=page_progress,
                    skip_page_ids=skip_page_ids
                )
                
                # Merge with existing pages if resuming
                if existing_pages:
                    self.logger.info(f"Merging {len(content.pages)} new pages with {len(existing_pages)} existing pages")
                    all_pages = existing_pages + [
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
                    # Update content with merged pages
                    from src.notion_backup_restore.backup.content_extractor import DatabaseContent, PageContent
                    content = DatabaseContent(
                        database_id=content.database_id,
                        database_name=content.database_name,
                        total_pages=len(all_pages),
                        extraction_time=content.extraction_time,
                        pages=[PageContent(**page) if isinstance(page, dict) else page for page in all_pages]
                    )
                
                self.extracted_content[db_name] = content
                
                # Save content to file
                self._save_content_to_file(content, content_file)
                
                if progress_callback:
                    progress_callback("Content Extraction", i, total_databases)
                
            except Exception as e:
                self.logger.error(f"Failed to extract content for database '{db_name}': {e}")
                raise
    
    def _process_backup_data(
        self,
        progress_callback: Optional[Callable[[str, int, int], None]] = None
    ) -> None:
        """Process backup data for compatibility."""
        self.logger.info("Processing backup data for compatibility...")
        
        try:
            # Progress callback for processing
            def processing_progress(stage: str, current: int, total: int):
                if progress_callback:
                    progress_callback(f"Processing: {stage}", current, total)
            
            # Process the extracted data
            processed_schemas, processed_contents = self.backup_processor.process_backup_data(
                schemas=self.extracted_schemas,
                contents=self.extracted_content,
                progress_callback=processing_progress
            )
            
            # Save processed data (overwrites original files with processed versions)
            self.backup_processor.save_processed_backup(
                processed_schemas=processed_schemas,
                processed_contents=processed_contents,
                backup_dir=self.backup_dir
            )
            
            # Update manifest to indicate processing was applied
            self._update_manifest_for_processing()
            
            if progress_callback:
                progress_callback("Data Processing", 1, 1)
            
        except Exception as e:
            self.logger.error(f"Data processing failed: {e}")
            # Don't fail the entire backup for processing errors, but log them
            self.logger.warning("Backup will continue without processing - restoration may encounter errors")
    
    def _update_manifest_for_processing(self) -> None:
        """Update manifest to indicate data was processed for compatibility."""
        # This will be handled by the backup_processor.save_processed_backup method
        # which creates a new manifest with processing metadata
        pass
    
    def _validate_backup(
        self,
        progress_callback: Optional[Callable[[str, int, int], None]] = None
    ) -> None:
        """Validate backup integrity."""
        if not self.integrity_checker:
            return
        
        self.logger.info("Validating backup integrity...")
        
        try:
            validation_results = self.integrity_checker.validate_backup(
                schemas=self.extracted_schemas,
                contents=self.extracted_content
            )
            
            # Save validation results
            validation_file = self.backup_dir / "validation_report.json"
            with open(validation_file, 'w', encoding='utf-8') as f:
                json.dump(validation_results, f, indent=2, ensure_ascii=False, default=str)
            
            # Log validation summary
            total_errors = sum(result.total_errors for result in validation_results.values())
            if total_errors > 0:
                self.logger.warning(f"Validation found {total_errors} issues")
            else:
                self.logger.info("Backup validation passed")
            
            if progress_callback:
                progress_callback("Validation", 1, 1)
            
        except Exception as e:
            self.logger.error(f"Backup validation failed: {e}")
            # Don't fail the entire backup for validation errors
    
    def _create_backup_manifest(self) -> None:
        """Create backup manifest with metadata."""
        manifest = {
            "version": "1.0",
            "created_at": datetime.utcnow().isoformat(),
            "config": {
                "include_blocks": self.config.include_blocks,
                "validate_integrity": self.config.validate_integrity,
                "process_for_compatibility": self.config.process_for_compatibility,
            },
            "databases": {},
            "statistics": {
                "total_databases": len(self.discovered_databases),
                "total_pages": sum(content.total_pages for content in self.extracted_content.values()),
                "api_stats": self.api_client.get_stats(),
            }
        }
        
        # Add database information
        for db_name, db_info in self.discovered_databases.items():
            schema = self.extracted_schemas.get(db_name)
            content = self.extracted_content.get(db_name)
            
            manifest["databases"][db_name] = {
                "id": db_info.id,
                "name": db_info.name,
                "url": db_info.url,
                "schema_file": f"{db_name.lower()}_schema.json",
                "data_file": f"{db_name.lower()}_data.json",
                "properties_count": len(schema.properties) if schema else 0,
                "pages_count": content.total_pages if content else 0,
                "created_time": db_info.created_time,
                "last_edited_time": db_info.last_edited_time,
            }
        
        # Save manifest
        manifest_file = self.backup_dir / "manifest.json"
        with open(manifest_file, 'w', encoding='utf-8') as f:
            json.dump(manifest, f, indent=2, ensure_ascii=False, default=str)
        
        self.logger.info(f"Created backup manifest: {manifest_file}")
    
    def _generate_backup_report(self) -> None:
        """Generate human-readable backup report."""
        report_lines = [
            "# Notion Backup Report",
            f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            f"Backup Directory: {self.backup_dir}",
            "",
            "## Summary",
            f"- Databases backed up: {len(self.extracted_content)}",
            f"- Total pages: {sum(content.total_pages for content in self.extracted_content.values())}",
            f"- Include blocks: {self.config.include_blocks}",
            "",
            "## Databases",
        ]
        
        for db_name, content in self.extracted_content.items():
            schema = self.extracted_schemas.get(db_name)
            report_lines.extend([
                f"### {db_name}",
                f"- Pages: {content.total_pages}",
                f"- Properties: {len(schema.properties) if schema else 'Unknown'}",
                f"- Database ID: {content.database_id}",
                ""
            ])
        
        # API statistics
        api_stats = self.api_client.get_stats()
        report_lines.extend([
            "## API Statistics",
            f"- Total requests: {api_stats['total_requests']}",
            f"- Error rate: {api_stats['error_rate']:.2%}",
            f"- Rate limiter stats: {api_stats['rate_limiter']}",
            ""
        ])
        
        # Save report
        report_file = self.backup_dir / "backup_report.md"
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write('\n'.join(report_lines))
        
        self.logger.info(f"Generated backup report: {report_file}")
    
    def _save_schema_to_file(self, schema: DatabaseSchema, file_path: Path) -> None:
        """Save database schema to JSON file."""
        schema_data = {
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
        
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(schema_data, f, indent=2, ensure_ascii=False, default=str)
    
    def _save_content_to_file(self, content: DatabaseContent, file_path: Path) -> None:
        """Save database content to JSON file."""
        import sys
        
        content_data = {
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
        
        # For large databases (>5000 pages), increase recursion limit and save without indent
        # to avoid "maximum recursion depth exceeded" errors
        original_limit = sys.getrecursionlimit()
        try:
            if content.total_pages > 5000:
                sys.setrecursionlimit(50000)
                self.logger.info(f"Large database ({content.total_pages} pages), saving without indentation")
                with open(file_path, 'w', encoding='utf-8') as f:
                    json.dump(content_data, f, ensure_ascii=False, default=str)
            else:
                with open(file_path, 'w', encoding='utf-8') as f:
                    json.dump(content_data, f, indent=2, ensure_ascii=False, default=str)
        finally:
            sys.setrecursionlimit(original_limit)
    
    def get_backup_stats(self) -> Dict[str, Any]:
        """
        Get comprehensive backup statistics.
        
        Returns:
            Dictionary with backup statistics
        """
        return {
            "backup_directory": str(self.backup_dir) if self.backup_dir else None,
            "databases_discovered": len(self.discovered_databases),
            "schemas_extracted": len(self.extracted_schemas),
            "content_extracted": len(self.extracted_content),
            "total_pages": sum(content.total_pages for content in self.extracted_content.values()),
            "api_stats": self.api_client.get_stats(),
            "config": {
                "include_blocks": self.config.include_blocks,
                "validate_integrity": self.config.validate_integrity,
                "process_for_compatibility": self.config.process_for_compatibility,
            }
        }
