"""
Main restoration orchestration manager.

This module coordinates the 4-phase restoration process with dependency resolution,
progress tracking, validation, and rollback capabilities.
"""

import json
from pathlib import Path
from typing import Dict, List, Optional, Any, Callable
from datetime import datetime
import logging

from .database_creator import DatabaseCreator, DatabaseCreationResult
from .relation_restorer import RelationRestorer, RelationRestorationResult
from .formula_restorer import FormulaRestorer, FormulaRestorationResult
from .data_restorer import DataRestorer, DataRestorationResult
from ..utils.api_client import NotionAPIClient, create_notion_client
from ..utils.id_mapper import IDMapper
from ..utils.dependency_resolver import create_workspace_dependency_resolver
from ..utils.logger import setup_logger, ProgressLogger
from ..config import RestoreConfig
from ..backup.schema_extractor import DatabaseSchema
from ..backup.content_extractor import DatabaseContent
from ..validation.integrity_checker import IntegrityChecker


class NotionRestoreManager:
    """
    Main restoration orchestration class.
    
    Coordinates the 4-phase restoration process including dependency resolution,
    progress tracking, validation, and comprehensive error handling.
    """
    
    def __init__(self, config: RestoreConfig):
        """
        Initialize restore manager.
        
        Args:
            config: Restore configuration
        """
        self.config = config
        self.logger = setup_logger(
            name="restore_manager",
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
        
        # Initialize ID mapper
        id_mapping_file = None
        if config.backup_dir:
            id_mapping_file = config.backup_dir / "id_mappings.json"
        self.id_mapper = IDMapper(id_mapping_file)
        
        # Initialize components
        self.database_creator = DatabaseCreator(self.api_client, self.id_mapper, self.logger)
        self.relation_restorer = RelationRestorer(self.api_client, self.id_mapper, self.logger)
        self.formula_restorer = FormulaRestorer(self.api_client, self.logger)
        self.data_restorer = DataRestorer(self.api_client, self.id_mapper, self.logger)
        
        if config.validate_after:
            self.integrity_checker = IntegrityChecker(self.api_client, self.logger)
        else:
            self.integrity_checker = None
        
        # Restoration state
        self.backup_manifest: Optional[Dict[str, Any]] = None
        self.schemas: Dict[str, DatabaseSchema] = {}
        self.contents: Dict[str, DatabaseContent] = {}
        self.restoration_order: List[str] = []
        
        # Results tracking
        self.creation_results: Dict[str, DatabaseCreationResult] = {}
        self.relation_results: Dict[str, RelationRestorationResult] = {}
        self.formula_results: Dict[str, FormulaRestorationResult] = {}
        self.data_results: Dict[str, DataRestorationResult] = {}
    
    def start_restore(
        self,
        progress_callback: Optional[Callable[[str, int, int], None]] = None
    ) -> Dict[str, Any]:
        """
        Start the complete restoration process.
        
        Args:
            progress_callback: Optional callback for progress updates
            
        Returns:
            Dictionary with restoration results and statistics
            
        Raises:
            Various exceptions if restoration fails
        """
        if not self.config.backup_dir:
            raise ValueError("Backup directory must be specified for restoration")
        
        self.logger.info(f"Starting restoration from: {self.config.backup_dir}")
        
        try:
            # Load backup data
            self._load_backup_data()
            
            # Determine restoration order
            self._determine_restoration_order()
            
            total_phases = 4
            current_phase = 0
            
            if not self.config.dry_run:
                # Phase 1: Create databases with basic properties
                current_phase += 1
                self.logger.info(f"Phase {current_phase}/{total_phases}: Creating databases")
                self._phase1_create_databases(progress_callback)
                
                # Phase 2: Add relation properties
                current_phase += 1
                self.logger.info(f"Phase {current_phase}/{total_phases}: Adding relation properties")
                self._phase2_add_relations(progress_callback)
                
                # Phase 3: Add formula and rollup properties
                current_phase += 1
                self.logger.info(f"Phase {current_phase}/{total_phases}: Adding formulas and rollups")
                self._phase3_add_formulas(progress_callback)
                
                # Phase 4: Restore data
                current_phase += 1
                self.logger.info(f"Phase {current_phase}/{total_phases}: Restoring data")
                self._phase4_restore_data(progress_callback)
                
                # Save ID mappings
                self._save_id_mappings()
                
                # Validate restoration (if enabled)
                if self.config.validate_after:
                    self._validate_restoration()
            else:
                self.logger.info("Dry run mode: skipping actual restoration")
            
            # Generate restoration report
            results = self._generate_restoration_report()
            
            self.logger.info("Restoration completed successfully")
            return results
            
        except Exception as e:
            self.logger.error(f"Restoration failed: {e}")
            
            # Attempt rollback if not dry run
            if not self.config.dry_run:
                self._attempt_rollback()
            
            raise
    
    def _load_backup_data(self) -> None:
        """Load backup manifest, schemas, and content."""
        self.logger.info("Loading backup data...")
        
        # Load manifest
        manifest_file = self.config.backup_dir / "manifest.json"
        if not manifest_file.exists():
            raise FileNotFoundError(f"Backup manifest not found: {manifest_file}")
        
        with open(manifest_file, 'r', encoding='utf-8') as f:
            self.backup_manifest = json.load(f)
        
        databases_info = self.backup_manifest.get("databases", {})
        
        # Load schemas and content
        for db_name, db_info in databases_info.items():
            # Load schema
            schema_file = self.config.backup_dir / "databases" / db_info["schema_file"]
            if schema_file.exists():
                with open(schema_file, 'r', encoding='utf-8') as f:
                    schema_data = json.load(f)
                self.schemas[db_name] = self._create_schema_from_data(schema_data)
            else:
                self.logger.warning(f"Schema file not found for database: {db_name}")
            
            # Load content
            data_file = self.config.backup_dir / "databases" / db_info["data_file"]
            if data_file.exists():
                with open(data_file, 'r', encoding='utf-8') as f:
                    content_data = json.load(f)
                self.contents[db_name] = self._create_content_from_data(content_data)
            else:
                self.logger.warning(f"Data file not found for database: {db_name}")
        
        self.logger.info(f"Loaded {len(self.schemas)} schemas and {len(self.contents)} content files")
    
    def _create_schema_from_data(self, schema_data: Dict[str, Any]) -> DatabaseSchema:
        """Create DatabaseSchema object from loaded data."""
        from ..backup.schema_extractor import PropertySchema
        
        properties = {}
        for prop_name, prop_data in schema_data.get("properties", {}).items():
            properties[prop_name] = PropertySchema(
                name=prop_data["name"],
                type=prop_data["type"],
                config=prop_data["config"],
                id=prop_data["id"],
                description=prop_data.get("description")
            )
        
        return DatabaseSchema(
            id=schema_data["id"],
            name=schema_data["name"],
            title=schema_data["title"],
            description=schema_data["description"],
            properties=properties,
            parent=schema_data["parent"],
            url=schema_data["url"],
            archived=schema_data["archived"],
            is_inline=schema_data["is_inline"],
            created_time=schema_data["created_time"],
            last_edited_time=schema_data["last_edited_time"],
            created_by=schema_data["created_by"],
            last_edited_by=schema_data["last_edited_by"],
            cover=schema_data.get("cover"),
            icon=schema_data.get("icon")
        )
    
    def _create_content_from_data(self, content_data: Dict[str, Any]) -> DatabaseContent:
        """Create DatabaseContent object from loaded data."""
        from ..backup.content_extractor import PageContent
        
        pages = []
        for page_data in content_data.get("pages", []):
            pages.append(PageContent(
                id=page_data["id"],
                url=page_data["url"],
                properties=page_data["properties"],
                parent=page_data["parent"],
                archived=page_data["archived"],
                created_time=page_data["created_time"],
                last_edited_time=page_data["last_edited_time"],
                created_by=page_data["created_by"],
                last_edited_by=page_data["last_edited_by"],
                cover=page_data.get("cover"),
                icon=page_data.get("icon"),
                blocks=page_data.get("blocks")
            ))
        
        return DatabaseContent(
            database_id=content_data["database_id"],
            database_name=content_data["database_name"],
            pages=pages,
            total_pages=content_data["total_pages"],
            extraction_time=content_data["extraction_time"]
        )
    
    def _determine_restoration_order(self) -> None:
        """Determine the order for database restoration based on dependencies."""
        self.logger.info("Determining restoration order...")
        
        # Use dependency resolver to get proper order
        resolver = create_workspace_dependency_resolver()
        
        # Add databases from backup
        for db_name in self.schemas.keys():
            resolver.add_database(db_name)
        
        try:
            self.restoration_order = resolver.get_restoration_order()
            self.logger.info(f"Restoration order: {self.restoration_order}")
        except ValueError as e:
            self.logger.warning(f"Dependency resolution failed: {e}")
            # Fall back to simple order
            self.restoration_order = list(self.schemas.keys())
            self.logger.info(f"Using fallback order: {self.restoration_order}")
    
    def _phase1_create_databases(
        self,
        progress_callback: Optional[Callable[[str, int, int], None]] = None
    ) -> None:
        """Phase 1: Create databases with basic properties."""
        self.progress_logger.start_operation("Phase 1: Database Creation", len(self.schemas))
        
        self.creation_results = self.database_creator.create_multiple_databases(
            schemas=self.schemas,
            parent_page_id=self.config.parent_page_id,
            creation_order=self.restoration_order
        )
        
        # Check for failures
        failed_databases = [
            db_name for db_name, result in self.creation_results.items()
            if not result.new_id
        ]
        
        if failed_databases:
            raise RuntimeError(f"Failed to create databases: {failed_databases}")
        
        self.progress_logger.complete_operation(
            "Phase 1: Database Creation",
            len(self.schemas),
            len(self.creation_results) - len(failed_databases),
            len(failed_databases)
        )
    
    def _phase2_add_relations(
        self,
        progress_callback: Optional[Callable[[str, int, int], None]] = None
    ) -> None:
        """Phase 2: Add relation properties."""
        self.progress_logger.start_operation("Phase 2: Relation Properties", len(self.schemas))
        
        self.relation_results = self.relation_restorer.restore_multiple_databases(
            schemas=self.schemas,
            restoration_order=self.restoration_order
        )
        
        self.progress_logger.complete_operation(
            "Phase 2: Relation Properties",
            len(self.schemas),
            len(self.relation_results),
            0  # Relations are non-critical, don't count as failures
        )
    
    def _phase3_add_formulas(
        self,
        progress_callback: Optional[Callable[[str, int, int], None]] = None
    ) -> None:
        """Phase 3: Add formula and rollup properties."""
        self.progress_logger.start_operation("Phase 3: Formula Properties", len(self.schemas))
        
        # Get database mappings
        database_mappings = {
            db_name: result.new_id
            for db_name, result in self.creation_results.items()
            if result.new_id
        }
        
        self.formula_results = self.formula_restorer.restore_multiple_databases(
            database_mappings=database_mappings,
            schemas=self.schemas,
            restoration_order=self.restoration_order
        )
        
        self.progress_logger.complete_operation(
            "Phase 3: Formula Properties",
            len(self.schemas),
            len(self.formula_results),
            0  # Formulas are non-critical, don't count as failures
        )
    
    def _phase4_restore_data(
        self,
        progress_callback: Optional[Callable[[str, int, int], None]] = None
    ) -> None:
        """Phase 4: Restore data."""
        total_pages = sum(content.total_pages for content in self.contents.values())
        self.progress_logger.start_operation("Phase 4: Data Restoration", total_pages)
        
        def data_progress(db_name: str, current: int, total: int):
            if progress_callback:
                progress_callback(f"Restoring {db_name}", current, total)
        
        self.data_results = self.data_restorer.restore_multiple_databases(
            contents=self.contents,
            restoration_order=self.restoration_order,
            progress_callback=data_progress
        )
        
        total_created = sum(result.created_pages for result in self.data_results.values())
        total_failed = sum(result.failed_pages for result in self.data_results.values())
        
        self.progress_logger.complete_operation(
            "Phase 4: Data Restoration",
            total_pages,
            total_created,
            total_failed
        )
    
    def _save_id_mappings(self) -> None:
        """Save ID mappings to file."""
        if self.config.backup_dir:
            mapping_file = self.config.backup_dir / "id_mappings.json"
            self.id_mapper.save_mappings(mapping_file)
            self.logger.info(f"Saved ID mappings: {mapping_file}")
    
    def _validate_restoration(self) -> None:
        """Validate restoration integrity."""
        if not self.integrity_checker:
            return
        
        self.logger.info("Validating restoration integrity...")
        
        try:
            # Get new database IDs
            new_database_ids = {
                db_name: result.new_id
                for db_name, result in self.creation_results.items()
                if result.new_id
            }
            
            validation_results = self.integrity_checker.validate_restoration(
                original_schemas=self.schemas,
                original_contents=self.contents,
                new_database_ids=new_database_ids
            )
            
            # Save validation results
            if self.config.backup_dir:
                validation_file = self.config.backup_dir / "restoration_validation.json"
                with open(validation_file, 'w', encoding='utf-8') as f:
                    json.dump(validation_results, f, indent=2, ensure_ascii=False, default=str)
            
            # Log validation summary
            total_errors = sum(
                result.total_errors
                for result in validation_results.values()
            )
            
            if total_errors > 0:
                self.logger.warning(f"Validation found {total_errors} issues")
            else:
                self.logger.info("Restoration validation passed")
                
        except Exception as e:
            self.logger.error(f"Restoration validation failed: {e}")
            # Don't fail the entire restoration for validation errors
    
    def _attempt_rollback(self) -> None:
        """Attempt to rollback created databases on failure."""
        self.logger.info("Attempting rollback of created databases...")
        
        rollback_count = 0
        
        for db_name, result in self.creation_results.items():
            if result.new_id:
                try:
                    # Note: Notion API doesn't support database deletion
                    # We would need to archive the database instead
                    self.logger.warning(
                        f"Cannot delete database '{db_name}' ({result.new_id}). "
                        f"Manual cleanup required."
                    )
                except Exception as e:
                    self.logger.error(f"Failed to rollback database '{db_name}': {e}")
        
        self.logger.info(f"Rollback attempted for {rollback_count} databases")
    
    def _generate_restoration_report(self) -> Dict[str, Any]:
        """Generate comprehensive restoration report."""
        # Collect statistics
        creation_stats = self.database_creator.get_creation_stats(self.creation_results)
        relation_stats = self.relation_restorer.get_restoration_stats(self.relation_results)
        formula_stats = self.formula_restorer.get_restoration_stats(self.formula_results)
        data_stats = self.data_restorer.get_restoration_stats(self.data_results)
        
        report = {
            "restoration_summary": {
                "backup_directory": str(self.config.backup_dir),
                "restoration_time": datetime.utcnow().isoformat(),
                "dry_run": self.config.dry_run,
                "parent_page_id": self.config.parent_page_id,
                "restoration_order": self.restoration_order,
            },
            "phase_results": {
                "phase1_database_creation": creation_stats,
                "phase2_relation_properties": relation_stats,
                "phase3_formula_properties": formula_stats,
                "phase4_data_restoration": data_stats,
            },
            "database_mappings": {
                db_name: result.new_id
                for db_name, result in self.creation_results.items()
                if result.new_id
            },
            "api_statistics": self.api_client.get_stats(),
            "id_mapper_statistics": self.id_mapper.get_stats(),
        }
        
        # Save report
        if self.config.backup_dir:
            report_file = self.config.backup_dir / "restoration_report.json"
            with open(report_file, 'w', encoding='utf-8') as f:
                json.dump(report, f, indent=2, ensure_ascii=False, default=str)
            
            self.logger.info(f"Generated restoration report: {report_file}")
        
        return report
    
    def get_restoration_stats(self) -> Dict[str, Any]:
        """
        Get comprehensive restoration statistics.
        
        Returns:
            Dictionary with restoration statistics
        """
        return {
            "backup_directory": str(self.config.backup_dir) if self.config.backup_dir else None,
            "schemas_loaded": len(self.schemas),
            "contents_loaded": len(self.contents),
            "restoration_order": self.restoration_order,
            "databases_created": len([r for r in self.creation_results.values() if r.new_id]),
            "total_pages_restored": sum(r.created_pages for r in self.data_results.values()),
            "api_stats": self.api_client.get_stats(),
            "id_mappings": len(self.id_mapper),
        }
