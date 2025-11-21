"""
Schema extraction for comprehensive database schema backup.

This module handles extraction of all property types, configurations, and
relationships while preserving metadata critical for accurate restoration.
"""

from typing import Dict, List, Optional, Any, Set
import logging
from dataclasses import dataclass
from datetime import datetime

from ..utils.api_client import NotionAPIClient


@dataclass
class PropertySchema:
    """Schema information for a database property."""
    name: str
    type: str
    config: Dict[str, Any]
    id: str
    description: Optional[str] = None


@dataclass
class DatabaseSchema:
    """Complete schema information for a database."""
    id: str
    name: str
    title: List[Dict[str, Any]]
    description: List[Dict[str, Any]]
    properties: Dict[str, PropertySchema]
    parent: Dict[str, Any]
    url: str
    archived: bool
    is_inline: bool
    created_time: str
    last_edited_time: str
    created_by: Dict[str, Any]
    last_edited_by: Dict[str, Any]
    cover: Optional[Dict[str, Any]]
    icon: Optional[Dict[str, Any]]


class SchemaExtractor:
    """
    Extracts comprehensive schema information from Notion databases.
    
    This class handles all property types including complex configurations
    like multi-select options, formula expressions, and relation settings.
    """
    
    def __init__(self, api_client: NotionAPIClient, logger: Optional[logging.Logger] = None):
        """
        Initialize schema extractor.
        
        Args:
            api_client: Notion API client
            logger: Logger instance
        """
        self.api_client = api_client
        self.logger = logger or logging.getLogger(__name__)
    
    def extract_schema(self, database_id: str, database_data: Optional[Dict[str, Any]] = None) -> DatabaseSchema:
        """
        Extract complete schema for a database.
        
        Args:
            database_id: ID of the database
            database_data: Optional pre-fetched database data (from search results)
            
        Returns:
            DatabaseSchema object with complete schema information
        """
        self.logger.info(f"Extracting schema for database: {database_id}")
        
        try:
            # Get database information (use provided data or fetch from API)
            if database_data is None:
                database_data = self.api_client.get_database(database_id)
                self.logger.debug(f"Fetched database data from API for {database_id}")
            else:
                self.logger.debug(f"Using provided database data for {database_id}")
            
            # Extract properties
            properties = {}
            for prop_name, prop_data in database_data.get("properties", {}).items():
                property_schema = self._extract_property_schema(prop_name, prop_data)
                properties[prop_name] = property_schema
            
            # Create database schema
            schema = DatabaseSchema(
                id=database_data["id"],
                name=self._extract_database_name(database_data),
                title=database_data.get("title", []),
                description=database_data.get("description", []),
                properties=properties,
                parent=database_data.get("parent", {}),
                url=database_data.get("url", ""),
                archived=database_data.get("archived", False),
                is_inline=database_data.get("is_inline", False),
                created_time=database_data.get("created_time", ""),
                last_edited_time=database_data.get("last_edited_time", ""),
                created_by=database_data.get("created_by", {}),
                last_edited_by=database_data.get("last_edited_by", {}),
                cover=database_data.get("cover"),
                icon=database_data.get("icon")
            )
            
            self.logger.info(
                f"Extracted schema for '{schema.name}' with {len(properties)} properties"
            )
            
            return schema
            
        except Exception as e:
            self.logger.error(f"Error extracting schema for database {database_id}: {e}")
            raise
    
    def _extract_database_name(self, database_data: Dict[str, Any]) -> str:
        """Extract database name from title property."""
        title_property = database_data.get("title", [])
        return "".join([
            text.get("plain_text", "") 
            for text in title_property
        ]).strip()
    
    def _extract_property_schema(self, prop_name: str, prop_data: Dict[str, Any]) -> PropertySchema:
        """
        Extract schema for a single property.
        
        Args:
            prop_name: Name of the property
            prop_data: Property data from API
            
        Returns:
            PropertySchema object
        """
        prop_type = prop_data.get("type")
        prop_id = prop_data.get("id", "")
        
        # Extract type-specific configuration
        config = self._extract_property_config(prop_type, prop_data)
        
        return PropertySchema(
            name=prop_name,
            type=prop_type,
            config=config,
            id=prop_id,
            description=prop_data.get("description")
        )
    
    def _extract_property_config(self, prop_type: str, prop_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract type-specific configuration for a property.
        
        Args:
            prop_type: Type of the property
            prop_data: Property data from API
            
        Returns:
            Configuration dictionary
        """
        config = {
            "type": prop_type,
            "id": prop_data.get("id", ""),
        }
        
        # Add type-specific configuration
        if prop_type in prop_data:
            type_config = prop_data[prop_type]
            
            if prop_type == "select":
                config.update(self._extract_select_config(type_config))
            elif prop_type == "multi_select":
                config.update(self._extract_multi_select_config(type_config))
            elif prop_type == "number":
                config.update(self._extract_number_config(type_config))
            elif prop_type == "formula":
                config.update(self._extract_formula_config(type_config))
            elif prop_type == "relation":
                config.update(self._extract_relation_config(type_config))
            elif prop_type == "rollup":
                config.update(self._extract_rollup_config(type_config))
            elif prop_type == "people":
                config.update(self._extract_people_config(type_config))
            elif prop_type == "date":
                config.update(self._extract_date_config(type_config))
            elif prop_type == "checkbox":
                config.update(type_config)
            elif prop_type == "url":
                config.update(type_config)
            elif prop_type == "email":
                config.update(type_config)
            elif prop_type == "phone_number":
                config.update(type_config)
            elif prop_type == "files":
                config.update(type_config)
            elif prop_type in ["created_time", "last_edited_time"]:
                config.update(type_config)
            elif prop_type in ["created_by", "last_edited_by"]:
                config.update(type_config)
            elif prop_type == "title":
                config.update(type_config)
            elif prop_type == "rich_text":
                config.update(type_config)
            else:
                # Generic handling for unknown types
                config.update(type_config)
        
        return config
    
    def _extract_select_config(self, select_config: Dict[str, Any]) -> Dict[str, Any]:
        """Extract select property configuration."""
        return {
            "options": select_config.get("options", [])
        }
    
    def _extract_multi_select_config(self, multi_select_config: Dict[str, Any]) -> Dict[str, Any]:
        """Extract multi-select property configuration."""
        return {
            "options": multi_select_config.get("options", [])
        }
    
    def _extract_number_config(self, number_config: Dict[str, Any]) -> Dict[str, Any]:
        """Extract number property configuration."""
        return {
            "format": number_config.get("format", "number")
        }
    
    def _extract_formula_config(self, formula_config: Dict[str, Any]) -> Dict[str, Any]:
        """Extract formula property configuration."""
        return {
            "expression": formula_config.get("expression", "")
        }
    
    def _extract_relation_config(self, relation_config: Dict[str, Any]) -> Dict[str, Any]:
        """Extract relation property configuration."""
        config = {
            "database_id": relation_config.get("database_id", ""),
            "type": relation_config.get("type", "single_property")
        }
        
        # Add synced property information if available
        if "synced_property_name" in relation_config:
            config["synced_property_name"] = relation_config["synced_property_name"]
        if "synced_property_id" in relation_config:
            config["synced_property_id"] = relation_config["synced_property_id"]
        
        return config
    
    def _extract_rollup_config(self, rollup_config: Dict[str, Any]) -> Dict[str, Any]:
        """Extract rollup property configuration."""
        return {
            "relation_property_name": rollup_config.get("relation_property_name", ""),
            "relation_property_id": rollup_config.get("relation_property_id", ""),
            "rollup_property_name": rollup_config.get("rollup_property_name", ""),
            "rollup_property_id": rollup_config.get("rollup_property_id", ""),
            "function": rollup_config.get("function", "")
        }
    
    def _extract_people_config(self, people_config: Dict[str, Any]) -> Dict[str, Any]:
        """Extract people property configuration."""
        return people_config  # People properties don't have complex config
    
    def _extract_date_config(self, date_config: Dict[str, Any]) -> Dict[str, Any]:
        """Extract date property configuration."""
        return date_config  # Date properties don't have complex config
    
    def extract_multiple_schemas(self, database_ids: List[str]) -> Dict[str, DatabaseSchema]:
        """
        Extract schemas for multiple databases.
        
        Args:
            database_ids: List of database IDs
            
        Returns:
            Dictionary mapping database IDs to DatabaseSchema objects
        """
        schemas = {}
        
        for db_id in database_ids:
            try:
                schema = self.extract_schema(db_id)
                schemas[db_id] = schema
            except Exception as e:
                self.logger.error(f"Failed to extract schema for database {db_id}: {e}")
                # Continue with other databases
        
        return schemas
    
    def get_property_dependencies(self, schema: DatabaseSchema) -> Dict[str, List[str]]:
        """
        Get property dependencies within a database.
        
        Args:
            schema: Database schema
            
        Returns:
            Dictionary mapping property names to their dependencies
        """
        dependencies = {}
        
        for prop_name, prop_schema in schema.properties.items():
            prop_deps = []
            
            if prop_schema.type == "rollup":
                # Rollup depends on relation property
                relation_prop = prop_schema.config.get("relation_property_name")
                if relation_prop and relation_prop in schema.properties:
                    prop_deps.append(relation_prop)
            
            elif prop_schema.type == "formula":
                # Formula might reference other properties
                # This is complex to parse, so we'll note it for manual review
                expression = prop_schema.config.get("expression", "")
                if expression:
                    # Simple heuristic: look for property names in the expression
                    for other_prop in schema.properties:
                        if other_prop != prop_name and other_prop in expression:
                            prop_deps.append(other_prop)
            
            dependencies[prop_name] = prop_deps
        
        return dependencies
    
    def validate_schema_integrity(self, schema: DatabaseSchema) -> List[str]:
        """
        Validate schema integrity and return any issues found.
        
        Args:
            schema: Database schema to validate
            
        Returns:
            List of validation error messages
        """
        errors = []
        
        # Check for required title property
        title_properties = [
            prop for prop in schema.properties.values() 
            if prop.type == "title"
        ]
        if not title_properties:
            errors.append("Database must have at least one title property")
        elif len(title_properties) > 1:
            errors.append("Database cannot have more than one title property")
        
        # Check relation property references
        for prop_name, prop_schema in schema.properties.items():
            if prop_schema.type == "relation":
                db_id = prop_schema.config.get("database_id")
                if not db_id:
                    errors.append(f"Relation property '{prop_name}' missing database_id")
        
        # Check rollup property references
        for prop_name, prop_schema in schema.properties.items():
            if prop_schema.type == "rollup":
                relation_prop = prop_schema.config.get("relation_property_name")
                if not relation_prop:
                    errors.append(f"Rollup property '{prop_name}' missing relation_property_name")
                elif relation_prop not in schema.properties:
                    errors.append(
                        f"Rollup property '{prop_name}' references non-existent "
                        f"relation property '{relation_prop}'"
                    )
        
        return errors
    
    def get_schema_stats(self, schema: DatabaseSchema) -> Dict[str, Any]:
        """
        Get statistics about a database schema.
        
        Args:
            schema: Database schema
            
        Returns:
            Dictionary with schema statistics
        """
        property_types = {}
        for prop_schema in schema.properties.values():
            prop_type = prop_schema.type
            property_types[prop_type] = property_types.get(prop_type, 0) + 1
        
        relation_count = sum(1 for p in schema.properties.values() if p.type == "relation")
        formula_count = sum(1 for p in schema.properties.values() if p.type == "formula")
        rollup_count = sum(1 for p in schema.properties.values() if p.type == "rollup")
        
        return {
            "database_id": schema.id,
            "database_name": schema.name,
            "total_properties": len(schema.properties),
            "property_types": property_types,
            "relation_properties": relation_count,
            "formula_properties": formula_count,
            "rollup_properties": rollup_count,
            "is_archived": schema.archived,
            "is_inline": schema.is_inline,
            "created_time": schema.created_time,
            "last_edited_time": schema.last_edited_time,
        }
