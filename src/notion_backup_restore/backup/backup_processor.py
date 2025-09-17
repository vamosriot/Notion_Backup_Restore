"""
Main backup processor that orchestrates data normalization and validation.

This module provides the main BackupProcessor class that coordinates
all data processing operations to create restoration-ready backups.
"""

import json
from pathlib import Path
from typing import Dict, List, Optional, Any, Callable
from datetime import datetime
import logging

from .data_processor import DataProcessor
from .schema_extractor import DatabaseSchema
from .content_extractor import DatabaseContent
from ..utils.logger import setup_logger


class BackupProcessor:
    """
    Main backup processor that orchestrates data normalization.
    
    This class coordinates all data processing operations to ensure
    backup data is compatible with current API requirements and
    prevents restoration errors.
    """
    
    def __init__(self, logger: Optional[logging.Logger] = None):
        """
        Initialize backup processor.
        
        Args:
            logger: Logger instance
        """
        self.logger = logger or logging.getLogger(__name__)
        self.data_processor = DataProcessor(logger)
        
        # Processing configuration
        self.config = {
            'normalize_users': True,
            'fix_relations': True,
            'sanitize_blocks': True,
            'validate_selects': True,
            'create_validation_report': True,
            'add_processing_metadata': True
        }
    
    def process_backup_data(
        self,
        schemas: Dict[str, DatabaseSchema],
        contents: Dict[str, DatabaseContent],
        progress_callback: Optional[Callable[[str, int, int], None]] = None
    ) -> tuple[Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]]]:
        """
        Process complete backup data for compatibility.
        
        Args:
            schemas: Dictionary of database schemas
            contents: Dictionary of database contents
            progress_callback: Optional progress callback
            
        Returns:
            Tuple of (processed_schemas, processed_contents)
        """
        self.logger.info("Starting backup data processing for compatibility")
        
        total_databases = len(schemas) + len(contents)
        current_progress = 0
        
        # Process schemas
        processed_schemas = {}
        for db_name, schema in schemas.items():
            self.logger.info(f"Processing schema for database: {db_name}")
            
            # Convert DatabaseSchema to dict for processing
            schema_dict = self._schema_to_dict(schema)
            processed_schema = self.data_processor.process_database_schema(schema_dict)
            processed_schemas[db_name] = processed_schema
            
            current_progress += 1
            if progress_callback:
                progress_callback("Processing Schemas", current_progress, total_databases)
        
        # Process contents
        processed_contents = {}
        for db_name, content in contents.items():
            self.logger.info(f"Processing content for database: {db_name}")
            
            # Convert DatabaseContent to dict for processing
            content_dict = self._content_to_dict(content)
            processed_content = self.data_processor.process_database_content(content_dict)
            processed_contents[db_name] = processed_content
            
            current_progress += 1
            if progress_callback:
                progress_callback("Processing Content", current_progress, total_databases)
        
        # Generate processing report
        if self.config['create_validation_report']:
            self._create_processing_report(processed_schemas, processed_contents)
        
        self.logger.info("Backup data processing completed successfully")
        return processed_schemas, processed_contents
    
    def _schema_to_dict(self, schema: DatabaseSchema) -> Dict[str, Any]:
        """Convert DatabaseSchema object to dictionary."""
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
    
    def _content_to_dict(self, content: DatabaseContent) -> Dict[str, Any]:
        """Convert DatabaseContent object to dictionary."""
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
    
    def save_processed_backup(
        self,
        processed_schemas: Dict[str, Dict[str, Any]],
        processed_contents: Dict[str, Dict[str, Any]],
        backup_dir: Path
    ) -> None:
        """
        Save processed backup data to files.
        
        Args:
            processed_schemas: Processed schema data
            processed_contents: Processed content data
            backup_dir: Backup directory path
        """
        self.logger.info(f"Saving processed backup to: {backup_dir}")
        
        # Ensure backup directory exists
        backup_dir.mkdir(parents=True, exist_ok=True)
        databases_dir = backup_dir / "databases"
        databases_dir.mkdir(exist_ok=True)
        
        # Save processed schemas
        for db_name, schema_data in processed_schemas.items():
            schema_file = databases_dir / f"{db_name.lower()}_schema.json"
            self._save_json_file(schema_data, schema_file)
            self.logger.debug(f"Saved processed schema: {schema_file}")
        
        # Save processed contents
        for db_name, content_data in processed_contents.items():
            content_file = databases_dir / f"{db_name.lower()}_data.json"
            self._save_json_file(content_data, content_file)
            self.logger.debug(f"Saved processed content: {content_file}")
        
        # Save processing manifest
        self._save_processing_manifest(processed_schemas, processed_contents, backup_dir)
    
    def _save_json_file(self, data: Dict[str, Any], file_path: Path) -> None:
        """Save data to JSON file with proper formatting."""
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False, default=str)
    
    def _save_processing_manifest(
        self,
        processed_schemas: Dict[str, Dict[str, Any]],
        processed_contents: Dict[str, Dict[str, Any]],
        backup_dir: Path
    ) -> None:
        """Save processing manifest with metadata and statistics."""
        manifest = {
            "version": "2.0",
            "processing_version": self.data_processor.processing_version,
            "api_version": self.data_processor.api_version,
            "created_at": datetime.utcnow().isoformat(),
            "compatibility_layer": True,
            "config": self.config,
            "databases": {},
            "processing_stats": self.data_processor.get_processing_stats(),
        }
        
        # Add database information
        for db_name in processed_schemas.keys():
            schema_data = processed_schemas.get(db_name, {})
            content_data = processed_contents.get(db_name, {})
            
            manifest["databases"][db_name] = {
                "id": schema_data.get("id"),
                "name": schema_data.get("name"),
                "schema_file": f"{db_name.lower()}_schema.json",
                "data_file": f"{db_name.lower()}_data.json",
                "properties_count": len(schema_data.get("properties", {})),
                "pages_count": content_data.get("total_pages", 0),
                "processed": True,
                "processing_metadata": {
                    "schema_processed": "_processing" in schema_data,
                    "content_processed": "_processing" in content_data,
                }
            }
        
        # Save manifest
        manifest_file = backup_dir / "manifest.json"
        self._save_json_file(manifest, manifest_file)
        self.logger.info(f"Saved processing manifest: {manifest_file}")
    
    def _create_processing_report(
        self,
        processed_schemas: Dict[str, Dict[str, Any]],
        processed_contents: Dict[str, Dict[str, Any]]
    ) -> None:
        """Create detailed processing report."""
        stats = self.data_processor.get_processing_stats()
        
        report_lines = [
            "# Backup Processing Report",
            f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            f"Processing Version: {self.data_processor.processing_version}",
            f"API Version: {self.data_processor.api_version}",
            "",
            "## Processing Statistics",
            f"- Users normalized: {stats['users_normalized']}",
            f"- Relations fixed: {stats['relations_fixed']}",
            f"- Blocks sanitized: {stats['blocks_sanitized']}",
            f"- Select options cleaned: {stats['select_options_cleaned']}",
            f"- Properties processed: {stats['properties_processed']}",
            f"- Pages processed: {stats['pages_processed']}",
            f"- Errors found: {stats['errors_found']}",
            f"- Warnings issued: {stats['warnings_issued']}",
            "",
            "## Databases Processed",
        ]
        
        for db_name in processed_schemas.keys():
            schema_data = processed_schemas.get(db_name, {})
            content_data = processed_contents.get(db_name, {})
            
            report_lines.extend([
                f"### {db_name}",
                f"- Database ID: {schema_data.get('id', 'Unknown')}",
                f"- Properties: {len(schema_data.get('properties', {}))}",
                f"- Pages: {content_data.get('total_pages', 0)}",
                f"- Schema processed: {'Yes' if '_processing' in schema_data else 'No'}",
                f"- Content processed: {'Yes' if '_processing' in content_data else 'No'}",
                ""
            ])
        
        # Validation results
        report_lines.extend([
            "## Validation Results",
            ""
        ])
        
        for db_name, schema_data in processed_schemas.items():
            issues = self.data_processor.validate_processed_data(schema_data)
            if issues:
                report_lines.extend([
                    f"### {db_name} Schema Issues",
                    *[f"- {issue}" for issue in issues],
                    ""
                ])
        
        for db_name, content_data in processed_contents.items():
            issues = self.data_processor.validate_processed_data(content_data)
            if issues:
                report_lines.extend([
                    f"### {db_name} Content Issues",
                    *[f"- {issue}" for issue in issues],
                    ""
                ])
        
        self.logger.info("Processing report generated")
        
        # Log summary to console
        self.logger.info(f"Processing Summary: {stats['users_normalized']} users normalized, "
                        f"{stats['relations_fixed']} relations fixed, "
                        f"{stats['blocks_sanitized']} blocks sanitized")
    
    def validate_backup_compatibility(
        self,
        backup_dir: Path
    ) -> Dict[str, Any]:
        """
        Validate backup compatibility for restoration.
        
        Args:
            backup_dir: Path to backup directory
            
        Returns:
            Validation results dictionary
        """
        self.logger.info(f"Validating backup compatibility: {backup_dir}")
        
        validation_results = {
            "backup_dir": str(backup_dir),
            "validated_at": datetime.utcnow().isoformat(),
            "is_compatible": True,
            "issues": [],
            "warnings": [],
            "databases": {}
        }
        
        try:
            # Check for manifest
            manifest_file = backup_dir / "manifest.json"
            if not manifest_file.exists():
                validation_results["issues"].append("Missing manifest.json file")
                validation_results["is_compatible"] = False
                return validation_results
            
            # Load and validate manifest
            with open(manifest_file, 'r', encoding='utf-8') as f:
                manifest = json.load(f)
            
            # Check processing version
            if not manifest.get("compatibility_layer", False):
                validation_results["warnings"].append("Backup was not processed for compatibility")
            
            # Validate each database
            databases_dir = backup_dir / "databases"
            for db_name, db_info in manifest.get("databases", {}).items():
                db_validation = self._validate_database_files(databases_dir, db_name, db_info)
                validation_results["databases"][db_name] = db_validation
                
                if not db_validation["is_valid"]:
                    validation_results["is_compatible"] = False
                
                validation_results["issues"].extend(db_validation.get("issues", []))
                validation_results["warnings"].extend(db_validation.get("warnings", []))
        
        except Exception as e:
            validation_results["issues"].append(f"Validation error: {str(e)}")
            validation_results["is_compatible"] = False
        
        return validation_results
    
    def _validate_database_files(
        self,
        databases_dir: Path,
        db_name: str,
        db_info: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Validate individual database files."""
        validation = {
            "database_name": db_name,
            "is_valid": True,
            "issues": [],
            "warnings": []
        }
        
        # Check schema file
        schema_file = databases_dir / db_info.get("schema_file", f"{db_name.lower()}_schema.json")
        if not schema_file.exists():
            validation["issues"].append(f"Missing schema file: {schema_file}")
            validation["is_valid"] = False
        else:
            try:
                with open(schema_file, 'r', encoding='utf-8') as f:
                    schema_data = json.load(f)
                
                # Validate schema processing
                if "_processing" not in schema_data:
                    validation["warnings"].append("Schema was not processed for compatibility")
                
                # Validate schema data
                schema_issues = self.data_processor.validate_processed_data(schema_data)
                validation["issues"].extend(schema_issues)
                
            except Exception as e:
                validation["issues"].append(f"Error reading schema file: {str(e)}")
                validation["is_valid"] = False
        
        # Check content file
        content_file = databases_dir / db_info.get("data_file", f"{db_name.lower()}_data.json")
        if not content_file.exists():
            validation["issues"].append(f"Missing content file: {content_file}")
            validation["is_valid"] = False
        else:
            try:
                with open(content_file, 'r', encoding='utf-8') as f:
                    content_data = json.load(f)
                
                # Validate content processing
                if "_processing" not in content_data:
                    validation["warnings"].append("Content was not processed for compatibility")
                
                # Validate content data
                content_issues = self.data_processor.validate_processed_data(content_data)
                validation["issues"].extend(content_issues)
                
            except Exception as e:
                validation["issues"].append(f"Error reading content file: {str(e)}")
                validation["is_valid"] = False
        
        return validation
    
    def get_processor_config(self) -> Dict[str, Any]:
        """Get current processor configuration."""
        return self.config.copy()
    
    def update_processor_config(self, config_updates: Dict[str, Any]) -> None:
        """Update processor configuration."""
        self.config.update(config_updates)
        self.logger.info(f"Updated processor configuration: {config_updates}")
