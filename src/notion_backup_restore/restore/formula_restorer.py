"""
Formula and rollup property restoration for Phase 3 of restoration.

This module restores formula expressions and rollup properties after
all basic properties and relations have been established.
"""

from typing import Dict, List, Optional, Any
import logging
from dataclasses import dataclass

from ..utils.api_client import NotionAPIClient
from ..backup.schema_extractor import DatabaseSchema, PropertySchema


@dataclass
class FormulaRestorationResult:
    """Result of formula property restoration."""
    database_id: str
    database_name: str
    added_formulas: List[str]
    added_rollups: List[str]
    failed_properties: List[str]
    errors: List[str]


class FormulaRestorer:
    """
    Restores formula and rollup properties during Phase 3 of restoration.
    
    This class handles adding formula and rollup properties after all
    basic properties and relations have been established.
    """
    
    def __init__(
        self,
        api_client: NotionAPIClient,
        logger: Optional[logging.Logger] = None
    ):
        """
        Initialize formula restorer.
        
        Args:
            api_client: Notion API client
            logger: Logger instance
        """
        self.api_client = api_client
        self.logger = logger or logging.getLogger(__name__)
    
    def restore_formulas(
        self,
        database_id: str,
        schema: DatabaseSchema,
        restoration_order: Optional[List[str]] = None
    ) -> FormulaRestorationResult:
        """
        Restore formula and rollup properties for a database.
        
        Args:
            database_id: ID of the database to update
            schema: Database schema
            restoration_order: Order to restore properties (optional)
            
        Returns:
            FormulaRestorationResult with restoration details
        """
        self.logger.info(f"Restoring formulas for database: {schema.name}")
        
        added_formulas = []
        added_rollups = []
        failed_properties = []
        errors = []
        
        # Find formula and rollup properties
        formula_properties = {}
        rollup_properties = {}
        
        for prop_name, prop_schema in schema.properties.items():
            if prop_schema.type == "formula":
                formula_properties[prop_name] = prop_schema
            elif prop_schema.type == "rollup":
                rollup_properties[prop_name] = prop_schema
        
        if not formula_properties and not rollup_properties:
            self.logger.info(f"No formula or rollup properties found for database: {schema.name}")
            return FormulaRestorationResult(
                database_id=database_id,
                database_name=schema.name,
                added_formulas=[],
                added_rollups=[],
                failed_properties=[],
                errors=[]
            )
        
        # Determine restoration order (rollups first, then formulas)
        if restoration_order is None:
            restoration_order = list(rollup_properties.keys()) + list(formula_properties.keys())
        
        # Restore each property
        for prop_name in restoration_order:
            prop_schema = None
            
            if prop_name in rollup_properties:
                prop_schema = rollup_properties[prop_name]
                property_type = "rollup"
            elif prop_name in formula_properties:
                prop_schema = formula_properties[prop_name]
                property_type = "formula"
            else:
                continue
            
            try:
                success = self._add_formula_property(
                    database_id, prop_name, prop_schema
                )
                
                if success:
                    if property_type == "formula":
                        added_formulas.append(prop_name)
                    else:
                        added_rollups.append(prop_name)
                    self.logger.debug(f"Added {property_type} property: {prop_name}")
                else:
                    failed_properties.append(prop_name)
                    
            except Exception as e:
                error_msg = f"Failed to add {property_type} property '{prop_name}': {e}"
                errors.append(error_msg)
                failed_properties.append(prop_name)
                self.logger.error(error_msg)
        
        self.logger.info(
            f"Restored formulas for '{schema.name}': "
            f"{len(added_formulas)} formulas, {len(added_rollups)} rollups, "
            f"{len(failed_properties)} failed"
        )
        
        return FormulaRestorationResult(
            database_id=database_id,
            database_name=schema.name,
            added_formulas=added_formulas,
            added_rollups=added_rollups,
            failed_properties=failed_properties,
            errors=errors
        )
    
    def _add_formula_property(
        self,
        database_id: str,
        prop_name: str,
        prop_schema: PropertySchema
    ) -> bool:
        """
        Add a single formula or rollup property to a database.
        
        Args:
            database_id: ID of the database to update
            prop_name: Name of the property to add
            prop_schema: Property schema
            
        Returns:
            True if successful, False otherwise
        """
        try:
            if prop_schema.type == "formula":
                property_config = self._create_formula_config(prop_schema)
            elif prop_schema.type == "rollup":
                property_config = self._create_rollup_config(prop_schema)
            else:
                self.logger.error(f"Unsupported property type: {prop_schema.type}")
                return False
            
            if not property_config:
                return False
            
            # Update the database with the new property
            update_payload = {
                "properties": {
                    prop_name: property_config
                }
            }
            
            self.api_client.update_database(database_id, **update_payload)
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error adding {prop_schema.type} property '{prop_name}': {e}")
            return False
    
    def _create_formula_config(self, prop_schema: PropertySchema) -> Optional[Dict[str, Any]]:
        """
        Create formula property configuration.
        
        Args:
            prop_schema: Property schema
            
        Returns:
            Formula configuration dict or None if invalid
        """
        expression = prop_schema.config.get("expression")
        if not expression:
            self.logger.error(f"No expression found for formula property: {prop_schema.name}")
            return None
        
        return {
            "type": "formula",
            "formula": {
                "expression": expression
            }
        }
    
    def _create_rollup_config(self, prop_schema: PropertySchema) -> Optional[Dict[str, Any]]:
        """
        Create rollup property configuration.
        
        Args:
            prop_schema: Property schema
            
        Returns:
            Rollup configuration dict or None if invalid
        """
        config = prop_schema.config
        
        relation_property_name = config.get("relation_property_name")
        rollup_property_name = config.get("rollup_property_name")
        function = config.get("function")
        
        if not all([relation_property_name, rollup_property_name, function]):
            self.logger.error(
                f"Incomplete rollup configuration for property: {prop_schema.name}"
            )
            return None
        
        rollup_config = {
            "type": "rollup",
            "rollup": {
                "relation_property_name": relation_property_name,
                "rollup_property_name": rollup_property_name,
                "function": function
            }
        }
        
        # Add relation and rollup property IDs if available
        if "relation_property_id" in config:
            rollup_config["rollup"]["relation_property_id"] = config["relation_property_id"]
        
        if "rollup_property_id" in config:
            rollup_config["rollup"]["rollup_property_id"] = config["rollup_property_id"]
        
        return rollup_config
    
    def restore_multiple_databases(
        self,
        database_mappings: Dict[str, str],
        schemas: Dict[str, DatabaseSchema],
        restoration_order: Optional[List[str]] = None
    ) -> Dict[str, FormulaRestorationResult]:
        """
        Restore formula properties for multiple databases.
        
        Args:
            database_mappings: Dictionary mapping database names to new IDs
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
            
            if db_name not in database_mappings:
                self.logger.warning(f"No database mapping found for '{db_name}'")
                continue
            
            database_id = database_mappings[db_name]
            schema = schemas[db_name]
            
            result = self.restore_formulas(database_id, schema)
            results[db_name] = result
            
            if result.errors:
                self.logger.error(f"Formula restoration had errors for '{db_name}': {result.errors}")
            else:
                self.logger.info(f"Successfully restored formulas for '{db_name}'")
        
        return results
    
    def validate_formula_dependencies(self, schema: DatabaseSchema) -> List[str]:
        """
        Validate that formula dependencies exist.
        
        Args:
            schema: Database schema
            
        Returns:
            List of validation errors
        """
        errors = []
        property_names = set(schema.properties.keys())
        
        # Check rollup dependencies
        for prop_name, prop_schema in schema.properties.items():
            if prop_schema.type == "rollup":
                relation_prop = prop_schema.config.get("relation_property_name")
                
                if not relation_prop:
                    errors.append(f"Rollup property '{prop_name}' missing relation_property_name")
                elif relation_prop not in property_names:
                    errors.append(
                        f"Rollup property '{prop_name}' references non-existent "
                        f"relation property '{relation_prop}'"
                    )
                else:
                    # Check that the referenced property is actually a relation
                    ref_prop = schema.properties[relation_prop]
                    if ref_prop.type != "relation":
                        errors.append(
                            f"Rollup property '{prop_name}' references "
                            f"non-relation property '{relation_prop}'"
                        )
        
        # Check formula dependencies (basic check for property references)
        for prop_name, prop_schema in schema.properties.items():
            if prop_schema.type == "formula":
                expression = prop_schema.config.get("expression", "")
                
                # Simple heuristic: check if other property names appear in the expression
                for other_prop in property_names:
                    if other_prop != prop_name and other_prop in expression:
                        # This is a potential dependency - just log it for now
                        self.logger.debug(
                            f"Formula property '{prop_name}' may depend on property '{other_prop}'"
                        )
        
        return errors
    
    def get_restoration_stats(self, results: Dict[str, FormulaRestorationResult]) -> Dict[str, Any]:
        """
        Get statistics about formula restoration.
        
        Args:
            results: Dictionary of restoration results
            
        Returns:
            Dictionary with restoration statistics
        """
        total_formulas_added = sum(len(result.added_formulas) for result in results.values())
        total_rollups_added = sum(len(result.added_rollups) for result in results.values())
        total_properties_failed = sum(len(result.failed_properties) for result in results.values())
        total_errors = sum(len(result.errors) for result in results.values())
        
        databases_with_formulas = sum(
            1 for result in results.values() 
            if result.added_formulas or result.added_rollups or result.failed_properties
        )
        
        total_properties_added = total_formulas_added + total_rollups_added
        
        return {
            "total_databases": len(results),
            "databases_with_formulas": databases_with_formulas,
            "total_formulas_added": total_formulas_added,
            "total_rollups_added": total_rollups_added,
            "total_properties_added": total_properties_added,
            "total_properties_failed": total_properties_failed,
            "total_errors": total_errors,
            "success_rate": (
                total_properties_added / (total_properties_added + total_properties_failed)
                if (total_properties_added + total_properties_failed) > 0 else 1.0
            ),
        }
