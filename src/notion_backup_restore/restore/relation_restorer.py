"""
Relation property restoration for Phase 2 of restoration.

This module adds relation properties to databases using the ID mapping system
to update relation configurations with new database IDs.
"""

from typing import Dict, List, Optional, Any
import logging
from dataclasses import dataclass

from ..utils.api_client import NotionAPIClient
from ..utils.id_mapper import IDMapper
from ..backup.schema_extractor import DatabaseSchema, PropertySchema


@dataclass
class RelationRestorationResult:
    """Result of relation property restoration."""
    database_id: str
    database_name: str
    added_properties: List[str]
    failed_properties: List[str]
    errors: List[str]


class RelationRestorer:
    """
    Restores relation properties during Phase 2 of restoration.
    
    This class handles adding relation properties to databases after
    all databases have been created in Phase 1.
    """
    
    def __init__(
        self,
        api_client: NotionAPIClient,
        id_mapper: IDMapper,
        logger: Optional[logging.Logger] = None
    ):
        """
        Initialize relation restorer.
        
        Args:
            api_client: Notion API client
            id_mapper: ID mapping system
            logger: Logger instance
        """
        self.api_client = api_client
        self.id_mapper = id_mapper
        self.logger = logger or logging.getLogger(__name__)
    
    def restore_relations(
        self,
        schema: DatabaseSchema,
        restoration_order: Optional[List[str]] = None
    ) -> RelationRestorationResult:
        """
        Restore relation properties for a database.
        
        Args:
            schema: Database schema
            restoration_order: Order to restore properties (optional)
            
        Returns:
            RelationRestorationResult with restoration details
        """
        # Get the new database ID
        new_database_id = self.id_mapper.get_new_id(schema.id)
        if not new_database_id:
            error_msg = f"No ID mapping found for database {schema.id}"
            self.logger.error(error_msg)
            return RelationRestorationResult(
                database_id="",
                database_name=schema.name,
                added_properties=[],
                failed_properties=[],
                errors=[error_msg]
            )
        
        self.logger.info(f"Restoring relations for database: {schema.name}")
        
        added_properties = []
        failed_properties = []
        errors = []
        
        # Find relation properties
        relation_properties = {
            prop_name: prop_schema
            for prop_name, prop_schema in schema.properties.items()
            if prop_schema.type == "relation"
        }
        
        if not relation_properties:
            self.logger.info(f"No relation properties found for database: {schema.name}")
            return RelationRestorationResult(
                database_id=new_database_id,
                database_name=schema.name,
                added_properties=[],
                failed_properties=[],
                errors=[]
            )
        
        # Determine restoration order
        if restoration_order is None:
            restoration_order = list(relation_properties.keys())
        
        # Restore each relation property
        for prop_name in restoration_order:
            if prop_name not in relation_properties:
                continue
            
            prop_schema = relation_properties[prop_name]
            
            try:
                success = self._add_relation_property(
                    new_database_id, prop_name, prop_schema
                )
                
                if success:
                    added_properties.append(prop_name)
                    self.logger.debug(f"Added relation property: {prop_name}")
                else:
                    failed_properties.append(prop_name)
                    
            except Exception as e:
                error_msg = f"Failed to add relation property '{prop_name}': {e}"
                errors.append(error_msg)
                failed_properties.append(prop_name)
                self.logger.error(error_msg)
        
        self.logger.info(
            f"Restored relations for '{schema.name}': "
            f"{len(added_properties)} added, {len(failed_properties)} failed"
        )
        
        return RelationRestorationResult(
            database_id=new_database_id,
            database_name=schema.name,
            added_properties=added_properties,
            failed_properties=failed_properties,
            errors=errors
        )
    
    def _add_relation_property(
        self,
        database_id: str,
        prop_name: str,
        prop_schema: PropertySchema
    ) -> bool:
        """
        Add a single relation property to a database.
        
        Args:
            database_id: ID of the database to update
            prop_name: Name of the property to add
            prop_schema: Property schema
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Get the original relation configuration
            relation_config = prop_schema.config
            original_target_db_id = relation_config.get("database_id")
            
            if not original_target_db_id:
                self.logger.error(f"No target database ID found for relation property: {prop_name}")
                return False
            
            # Map to new database ID
            new_target_db_id = self.id_mapper.get_new_id(original_target_db_id)
            if not new_target_db_id:
                self.logger.error(
                    f"No ID mapping found for target database {original_target_db_id} "
                    f"in relation property: {prop_name}"
                )
                return False
            
            # Create the relation property configuration
            relation_property_config = self._create_relation_config(
                relation_config, new_target_db_id
            )
            
            # Update the database with the new property
            update_payload = {
                "properties": {
                    prop_name: relation_property_config
                }
            }
            
            self.api_client.update_database(database_id, **update_payload)
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error adding relation property '{prop_name}': {e}")
            return False
    
    def _create_relation_config(
        self,
        original_config: Dict[str, Any],
        new_target_db_id: str
    ) -> Dict[str, Any]:
        """
        Create relation property configuration with updated database ID.
        
        Args:
            original_config: Original relation configuration
            new_target_db_id: New target database ID
            
        Returns:
            Updated relation configuration
        """
        # Create base relation config
        relation_config = {
            "database_id": new_target_db_id
        }
        
        # Determine relation type from original config
        relation_type = original_config.get("type", "single_property")
        
        # Add proper relation configuration based on type
        if relation_type == "dual_property":
            relation_config["dual_property"] = original_config.get("dual_property", {})
        else:
            relation_config["single_property"] = original_config.get("single_property", {})
        
        config = {
            "type": "relation",
            "relation": relation_config
        }
        
        # Add synced property information if present
        if "synced_property_name" in original_config:
            config["relation"]["synced_property_name"] = original_config["synced_property_name"]
        
        if "synced_property_id" in original_config:
            # Note: synced_property_id will need to be updated after the target property is created
            # For now, we'll omit it and let Notion handle the sync automatically
            pass
        
        return config
    
    def restore_multiple_databases(
        self,
        schemas: Dict[str, DatabaseSchema],
        restoration_order: Optional[List[str]] = None
    ) -> Dict[str, RelationRestorationResult]:
        """
        Restore relation properties for multiple databases.
        
        Args:
            schemas: Dictionary mapping database names to schemas
            restoration_order: Order to restore databases (optional)
            
        Returns:
            Dictionary mapping database names to restoration results
        """
        if restoration_order is None:
            restoration_order = list(schemas.keys())
        
        results = {}
        
        for db_name in restoration_order:
            if db_name not in schemas:
                self.logger.warning(f"Database '{db_name}' not found in schemas")
                continue
            
            schema = schemas[db_name]
            result = self.restore_relations(schema)
            results[db_name] = result
            
            if result.errors:
                self.logger.error(f"Relation restoration had errors for '{db_name}': {result.errors}")
            else:
                self.logger.info(f"Successfully restored relations for '{db_name}'")
        
        return results
    
    def validate_relation_mappings(self, schemas: Dict[str, DatabaseSchema]) -> List[str]:
        """
        Validate that all relation targets have ID mappings.
        
        Args:
            schemas: Dictionary of database schemas
            
        Returns:
            List of validation errors
        """
        errors = []
        
        for db_name, schema in schemas.items():
            for prop_name, prop_schema in schema.properties.items():
                if prop_schema.type == "relation":
                    target_db_id = prop_schema.config.get("database_id")
                    
                    if not target_db_id:
                        errors.append(
                            f"Database '{db_name}', property '{prop_name}': "
                            f"missing target database ID"
                        )
                        continue
                    
                    if not self.id_mapper.has_mapping(target_db_id):
                        errors.append(
                            f"Database '{db_name}', property '{prop_name}': "
                            f"no ID mapping for target database {target_db_id}"
                        )
        
        return errors
    
    def get_relation_dependencies(self, schemas: Dict[str, DatabaseSchema]) -> Dict[str, List[str]]:
        """
        Get relation dependencies between databases.
        
        Args:
            schemas: Dictionary of database schemas
            
        Returns:
            Dictionary mapping database names to lists of databases they depend on
        """
        dependencies = {}
        
        for db_name, schema in schemas.items():
            deps = []
            
            for prop_schema in schema.properties.values():
                if prop_schema.type == "relation":
                    target_db_id = prop_schema.config.get("database_id")
                    
                    if target_db_id:
                        # Find the database name for this ID
                        for other_db_name, other_schema in schemas.items():
                            if other_schema.id == target_db_id:
                                deps.append(other_db_name)
                                break
            
            dependencies[db_name] = deps
        
        return dependencies
    
    def get_restoration_stats(self, results: Dict[str, RelationRestorationResult]) -> Dict[str, Any]:
        """
        Get statistics about relation restoration.
        
        Args:
            results: Dictionary of restoration results
            
        Returns:
            Dictionary with restoration statistics
        """
        total_properties_added = sum(len(result.added_properties) for result in results.values())
        total_properties_failed = sum(len(result.failed_properties) for result in results.values())
        total_errors = sum(len(result.errors) for result in results.values())
        
        databases_with_relations = sum(
            1 for result in results.values() 
            if result.added_properties or result.failed_properties
        )
        
        return {
            "total_databases": len(results),
            "databases_with_relations": databases_with_relations,
            "total_properties_added": total_properties_added,
            "total_properties_failed": total_properties_failed,
            "total_errors": total_errors,
            "success_rate": (
                total_properties_added / (total_properties_added + total_properties_failed)
                if (total_properties_added + total_properties_failed) > 0 else 1.0
            ),
        }
