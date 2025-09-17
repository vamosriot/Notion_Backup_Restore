"""
Database creation logic for Phase 1 of restoration.

This module creates databases with basic properties (text, number, select, etc.)
but excludes relation properties initially to avoid dependency issues.
"""

from typing import Dict, List, Optional, Any, Set
import logging
from dataclasses import dataclass

from ..utils.api_client import NotionAPIClient
from ..utils.id_mapper import IDMapper
from ..backup.schema_extractor import DatabaseSchema, PropertySchema


@dataclass
class DatabaseCreationResult:
    """Result of database creation operation."""
    original_id: str
    new_id: str
    name: str
    created_properties: List[str]
    skipped_properties: List[str]
    errors: List[str]


class DatabaseCreator:
    """
    Creates databases with basic properties during Phase 1 restoration.
    
    This class handles the first phase of database restoration where we create
    databases with non-relation properties to establish the basic structure.
    """
    
    def __init__(
        self,
        api_client: NotionAPIClient,
        id_mapper: IDMapper,
        logger: Optional[logging.Logger] = None
    ):
        """
        Initialize database creator.
        
        Args:
            api_client: Notion API client
            id_mapper: ID mapping system
            logger: Logger instance
        """
        self.api_client = api_client
        self.id_mapper = id_mapper
        self.logger = logger or logging.getLogger(__name__)
        
        # Property types to skip in Phase 1 (will be added in later phases)
        self.phase1_skip_types = {
            "relation",
            "rollup",
            "formula"
        }
    
    def create_database(
        self,
        schema: DatabaseSchema,
        parent_page_id: Optional[str] = None
    ) -> DatabaseCreationResult:
        """
        Create a database with basic properties.
        
        Args:
            schema: Database schema to create
            parent_page_id: Parent page ID (optional)
            
        Returns:
            DatabaseCreationResult with creation details
        """
        self.logger.info(f"Creating database: {schema.name}")
        
        created_properties = []
        skipped_properties = []
        errors = []
        
        try:
            # Prepare database creation payload
            create_payload = self._prepare_database_payload(
                schema, parent_page_id, created_properties, skipped_properties, errors
            )
            
            # Create the database
            response = self.api_client.create_database(**create_payload)
            new_database_id = response["id"]
            
            # Add ID mapping
            self.id_mapper.add_mapping(
                original_id=schema.id,
                new_id=new_database_id,
                object_type="database",
                name=schema.name
            )
            
            self.logger.info(
                f"Created database '{schema.name}': {new_database_id} "
                f"({len(created_properties)} properties, {len(skipped_properties)} skipped)"
            )
            
            return DatabaseCreationResult(
                original_id=schema.id,
                new_id=new_database_id,
                name=schema.name,
                created_properties=created_properties,
                skipped_properties=skipped_properties,
                errors=errors
            )
            
        except Exception as e:
            error_msg = f"Failed to create database '{schema.name}': {e}"
            self.logger.error(error_msg)
            errors.append(error_msg)
            
            return DatabaseCreationResult(
                original_id=schema.id,
                new_id="",
                name=schema.name,
                created_properties=created_properties,
                skipped_properties=skipped_properties,
                errors=errors
            )
    
    def _prepare_database_payload(
        self,
        schema: DatabaseSchema,
        parent_page_id: Optional[str],
        created_properties: List[str],
        skipped_properties: List[str],
        errors: List[str]
    ) -> Dict[str, Any]:
        """
        Prepare the payload for database creation.
        
        Args:
            schema: Database schema
            parent_page_id: Parent page ID
            created_properties: List to track created properties
            skipped_properties: List to track skipped properties
            errors: List to track errors
            
        Returns:
            Database creation payload
        """
        # Basic database structure
        payload = {
            "title": schema.title,
            "properties": {}
        }
        
        # Add parent if specified
        if parent_page_id:
            payload["parent"] = {"type": "page_id", "page_id": parent_page_id}
        else:
            # Use the original parent if no override specified
            payload["parent"] = schema.parent
        
        # Add description if present
        if schema.description:
            payload["description"] = schema.description
        
        # Add icon and cover if present
        if schema.icon:
            payload["icon"] = schema.icon
        if schema.cover:
            payload["cover"] = schema.cover
        
        # Process properties
        for prop_name, prop_schema in schema.properties.items():
            try:
                if prop_schema.type in self.phase1_skip_types:
                    skipped_properties.append(prop_name)
                    self.logger.debug(f"Skipping {prop_schema.type} property '{prop_name}' for Phase 1")
                    continue
                
                prop_config = self._create_property_config(prop_schema)
                if prop_config:
                    payload["properties"][prop_name] = prop_config
                    created_properties.append(prop_name)
                else:
                    skipped_properties.append(prop_name)
                    
            except Exception as e:
                error_msg = f"Error processing property '{prop_name}': {e}"
                errors.append(error_msg)
                self.logger.warning(error_msg)
                skipped_properties.append(prop_name)
        
        return payload
    
    def _create_property_config(self, prop_schema: PropertySchema) -> Optional[Dict[str, Any]]:
        """
        Create property configuration for API call.
        
        Args:
            prop_schema: Property schema
            
        Returns:
            Property configuration dict or None if unsupported
        """
        prop_type = prop_schema.type
        
        # Basic property structure
        config = {"type": prop_type}
        
        if prop_type == "title":
            config["title"] = {}
            
        elif prop_type == "rich_text":
            config["rich_text"] = {}
            
        elif prop_type == "number":
            number_config = {}
            if "format" in prop_schema.config:
                number_config["format"] = prop_schema.config["format"]
            config["number"] = number_config
            
        elif prop_type == "select":
            select_config = {"options": []}
            if "options" in prop_schema.config:
                select_config["options"] = prop_schema.config["options"]
            config["select"] = select_config
            
        elif prop_type == "multi_select":
            multi_select_config = {"options": []}
            if "options" in prop_schema.config:
                multi_select_config["options"] = prop_schema.config["options"]
            config["multi_select"] = multi_select_config
            
        elif prop_type == "date":
            config["date"] = {}
            
        elif prop_type == "people":
            config["people"] = {}
            
        elif prop_type == "files":
            config["files"] = {}
            
        elif prop_type == "checkbox":
            config["checkbox"] = {}
            
        elif prop_type == "url":
            config["url"] = {}
            
        elif prop_type == "email":
            config["email"] = {}
            
        elif prop_type == "phone_number":
            config["phone_number"] = {}
            
        elif prop_type == "created_time":
            config["created_time"] = {}
            
        elif prop_type == "created_by":
            config["created_by"] = {}
            
        elif prop_type == "last_edited_time":
            config["last_edited_time"] = {}
            
        elif prop_type == "last_edited_by":
            config["last_edited_by"] = {}
            
        else:
            # Unsupported property type for Phase 1
            self.logger.warning(f"Unsupported property type for Phase 1: {prop_type}")
            return None
        
        return config
    
    def create_multiple_databases(
        self,
        schemas: Dict[str, DatabaseSchema],
        parent_page_id: Optional[str] = None,
        creation_order: Optional[List[str]] = None
    ) -> Dict[str, DatabaseCreationResult]:
        """
        Create multiple databases in the specified order.
        
        Args:
            schemas: Dictionary mapping database names to schemas
            parent_page_id: Parent page ID for all databases
            creation_order: Order to create databases (defaults to schemas keys)
            
        Returns:
            Dictionary mapping database names to creation results
        """
        if creation_order is None:
            creation_order = list(schemas.keys())
        
        results = {}
        
        for db_name in creation_order:
            if db_name not in schemas:
                self.logger.warning(f"Database '{db_name}' not found in schemas")
                continue
            
            schema = schemas[db_name]
            result = self.create_database(schema, parent_page_id)
            results[db_name] = result
            
            if result.errors:
                self.logger.error(f"Database creation had errors for '{db_name}': {result.errors}")
            else:
                self.logger.info(f"Successfully created database '{db_name}': {result.new_id}")
        
        return results
    
    def validate_creation_results(self, results: Dict[str, DatabaseCreationResult]) -> List[str]:
        """
        Validate database creation results.
        
        Args:
            results: Dictionary of creation results
            
        Returns:
            List of validation errors
        """
        errors = []
        
        for db_name, result in results.items():
            if not result.new_id:
                errors.append(f"Database '{db_name}' was not created successfully")
            
            if result.errors:
                errors.extend([f"Database '{db_name}': {error}" for error in result.errors])
            
            if not result.created_properties:
                errors.append(f"Database '{db_name}' has no properties created")
        
        return errors
    
    def get_creation_stats(self, results: Dict[str, DatabaseCreationResult]) -> Dict[str, Any]:
        """
        Get statistics about database creation.
        
        Args:
            results: Dictionary of creation results
            
        Returns:
            Dictionary with creation statistics
        """
        successful_creations = sum(1 for result in results.values() if result.new_id)
        total_properties_created = sum(len(result.created_properties) for result in results.values())
        total_properties_skipped = sum(len(result.skipped_properties) for result in results.values())
        total_errors = sum(len(result.errors) for result in results.values())
        
        return {
            "total_databases": len(results),
            "successful_creations": successful_creations,
            "failed_creations": len(results) - successful_creations,
            "total_properties_created": total_properties_created,
            "total_properties_skipped": total_properties_skipped,
            "total_errors": total_errors,
            "success_rate": successful_creations / len(results) if results else 0,
        }
