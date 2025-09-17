"""
Comprehensive validation logic for backup and restoration integrity.

This module provides validation of schema, data, relationships, and formulas
to ensure backup and restore operations maintain data integrity.
"""

from typing import Dict, List, Optional, Any, Set
import logging
from dataclasses import dataclass
from datetime import datetime

from ..utils.api_client import NotionAPIClient
from ..backup.schema_extractor import DatabaseSchema
from ..backup.content_extractor import DatabaseContent


@dataclass
class ValidationResult:
    """Result of a validation check."""
    check_name: str
    passed: bool
    errors: List[str]
    warnings: List[str]
    details: Dict[str, Any]


@dataclass
class DatabaseValidation:
    """Validation results for a single database."""
    database_name: str
    database_id: str
    schema_validation: ValidationResult
    data_validation: ValidationResult
    relationship_validation: ValidationResult
    formula_validation: ValidationResult
    overall_passed: bool
    total_errors: int
    total_warnings: int


class IntegrityChecker:
    """
    Comprehensive integrity checker for backup and restoration operations.
    
    This class provides validation of schema accuracy, data integrity,
    relationship consistency, and formula correctness.
    """
    
    def __init__(self, api_client: NotionAPIClient, logger: Optional[logging.Logger] = None):
        """
        Initialize integrity checker.
        
        Args:
            api_client: Notion API client
            logger: Logger instance
        """
        self.api_client = api_client
        self.logger = logger or logging.getLogger(__name__)
    
    def validate_backup(
        self,
        schemas: Dict[str, DatabaseSchema],
        contents: Dict[str, DatabaseContent]
    ) -> Dict[str, DatabaseValidation]:
        """
        Validate backup integrity.
        
        Args:
            schemas: Dictionary of database schemas
            contents: Dictionary of database contents
            
        Returns:
            Dictionary mapping database names to validation results
        """
        self.logger.info("Starting backup validation...")
        
        validation_results = {}
        
        for db_name in schemas.keys():
            schema = schemas.get(db_name)
            content = contents.get(db_name)
            
            if not schema:
                self.logger.error(f"No schema found for database: {db_name}")
                continue
            
            if not content:
                self.logger.error(f"No content found for database: {db_name}")
                continue
            
            validation = self._validate_database_backup(db_name, schema, content)
            validation_results[db_name] = validation
        
        self._log_validation_summary(validation_results, "backup")
        return validation_results
    
    def validate_restoration(
        self,
        original_schemas: Dict[str, DatabaseSchema],
        original_contents: Dict[str, DatabaseContent],
        new_database_ids: Dict[str, str]
    ) -> Dict[str, DatabaseValidation]:
        """
        Validate restoration integrity by comparing with original data.
        
        Args:
            original_schemas: Original database schemas
            original_contents: Original database contents
            new_database_ids: Mapping of database names to new IDs
            
        Returns:
            Dictionary mapping database names to validation results
        """
        self.logger.info("Starting restoration validation...")
        
        validation_results = {}
        
        for db_name, new_db_id in new_database_ids.items():
            original_schema = original_schemas.get(db_name)
            original_content = original_contents.get(db_name)
            
            if not original_schema or not original_content:
                self.logger.error(f"Missing original data for database: {db_name}")
                continue
            
            validation = self._validate_database_restoration(
                db_name, new_db_id, original_schema, original_content
            )
            validation_results[db_name] = validation
        
        self._log_validation_summary(validation_results, "restoration")
        return validation_results
    
    def _validate_database_backup(
        self,
        db_name: str,
        schema: DatabaseSchema,
        content: DatabaseContent
    ) -> DatabaseValidation:
        """
        Validate backup for a single database.
        
        Args:
            db_name: Database name
            schema: Database schema
            content: Database content
            
        Returns:
            DatabaseValidation result
        """
        # Schema validation
        schema_validation = self._validate_schema_integrity(schema)
        
        # Data validation
        data_validation = self._validate_content_integrity(content, schema)
        
        # Relationship validation
        relationship_validation = self._validate_relationships_backup(content, schema)
        
        # Formula validation
        formula_validation = self._validate_formulas_backup(schema)
        
        # Overall result
        overall_passed = all([
            schema_validation.passed,
            data_validation.passed,
            relationship_validation.passed,
            formula_validation.passed
        ])
        
        total_errors = sum([
            len(schema_validation.errors),
            len(data_validation.errors),
            len(relationship_validation.errors),
            len(formula_validation.errors)
        ])
        
        total_warnings = sum([
            len(schema_validation.warnings),
            len(data_validation.warnings),
            len(relationship_validation.warnings),
            len(formula_validation.warnings)
        ])
        
        return DatabaseValidation(
            database_name=db_name,
            database_id=schema.id,
            schema_validation=schema_validation,
            data_validation=data_validation,
            relationship_validation=relationship_validation,
            formula_validation=formula_validation,
            overall_passed=overall_passed,
            total_errors=total_errors,
            total_warnings=total_warnings
        )
    
    def _validate_database_restoration(
        self,
        db_name: str,
        new_db_id: str,
        original_schema: DatabaseSchema,
        original_content: DatabaseContent
    ) -> DatabaseValidation:
        """
        Validate restoration for a single database.
        
        Args:
            db_name: Database name
            new_db_id: New database ID
            original_schema: Original database schema
            original_content: Original database content
            
        Returns:
            DatabaseValidation result
        """
        try:
            # Get current database state
            current_db_data = self.api_client.get_database(new_db_id)
            current_pages_data = self.api_client.query_database(new_db_id)
            
            # Schema validation
            schema_validation = self._validate_schema_restoration(
                original_schema, current_db_data
            )
            
            # Data validation
            data_validation = self._validate_data_restoration(
                original_content, current_pages_data
            )
            
            # Relationship validation
            relationship_validation = self._validate_relationships_restoration(
                original_content, current_pages_data
            )
            
            # Formula validation
            formula_validation = self._validate_formulas_restoration(
                original_schema, current_db_data
            )
            
        except Exception as e:
            # If we can't access the database, create error validation
            error_msg = f"Failed to access restored database: {e}"
            self.logger.error(error_msg)
            
            error_validation = ValidationResult(
                check_name="database_access",
                passed=False,
                errors=[error_msg],
                warnings=[],
                details={}
            )
            
            return DatabaseValidation(
                database_name=db_name,
                database_id=new_db_id,
                schema_validation=error_validation,
                data_validation=error_validation,
                relationship_validation=error_validation,
                formula_validation=error_validation,
                overall_passed=False,
                total_errors=1,
                total_warnings=0
            )
        
        # Overall result
        overall_passed = all([
            schema_validation.passed,
            data_validation.passed,
            relationship_validation.passed,
            formula_validation.passed
        ])
        
        total_errors = sum([
            len(schema_validation.errors),
            len(data_validation.errors),
            len(relationship_validation.errors),
            len(formula_validation.errors)
        ])
        
        total_warnings = sum([
            len(schema_validation.warnings),
            len(data_validation.warnings),
            len(relationship_validation.warnings),
            len(formula_validation.warnings)
        ])
        
        return DatabaseValidation(
            database_name=db_name,
            database_id=new_db_id,
            schema_validation=schema_validation,
            data_validation=data_validation,
            relationship_validation=relationship_validation,
            formula_validation=formula_validation,
            overall_passed=overall_passed,
            total_errors=total_errors,
            total_warnings=total_warnings
        )
    
    def _validate_schema_integrity(self, schema: DatabaseSchema) -> ValidationResult:
        """Validate schema integrity."""
        errors = []
        warnings = []
        details = {}
        
        # Check for required title property
        title_properties = [
            prop for prop in schema.properties.values()
            if prop.type == "title"
        ]
        
        if not title_properties:
            errors.append("Database must have at least one title property")
        elif len(title_properties) > 1:
            errors.append("Database has multiple title properties")
        
        # Check property configurations
        for prop_name, prop_schema in schema.properties.items():
            if prop_schema.type == "relation":
                if not prop_schema.config.get("database_id"):
                    errors.append(f"Relation property '{prop_name}' missing database_id")
            
            elif prop_schema.type == "rollup":
                relation_prop = prop_schema.config.get("relation_property_name")
                if not relation_prop:
                    errors.append(f"Rollup property '{prop_name}' missing relation_property_name")
                elif relation_prop not in schema.properties:
                    errors.append(
                        f"Rollup property '{prop_name}' references non-existent property '{relation_prop}'"
                    )
            
            elif prop_schema.type == "formula":
                if not prop_schema.config.get("expression"):
                    errors.append(f"Formula property '{prop_name}' missing expression")
        
        details.update({
            "total_properties": len(schema.properties),
            "property_types": {
                prop_type: sum(1 for p in schema.properties.values() if p.type == prop_type)
                for prop_type in set(p.type for p in schema.properties.values())
            }
        })
        
        return ValidationResult(
            check_name="schema_integrity",
            passed=len(errors) == 0,
            errors=errors,
            warnings=warnings,
            details=details
        )
    
    def _validate_content_integrity(
        self,
        content: DatabaseContent,
        schema: DatabaseSchema
    ) -> ValidationResult:
        """Validate content integrity."""
        errors = []
        warnings = []
        details = {}
        
        # Check for duplicate page IDs
        page_ids = [page.id for page in content.pages]
        if len(page_ids) != len(set(page_ids)):
            errors.append("Duplicate page IDs found in content")
        
        # Check page properties
        pages_without_title = 0
        pages_with_missing_props = 0
        
        for page in content.pages:
            # Check for title property
            has_title = any(
                isinstance(prop, dict) and prop.get("type") == "title"
                for prop in page.properties.values()
            )
            if not has_title:
                pages_without_title += 1
            
            # Check for missing properties (compared to schema)
            missing_props = set(schema.properties.keys()) - set(page.properties.keys())
            if missing_props:
                pages_with_missing_props += 1
        
        if pages_without_title > 0:
            warnings.append(f"{pages_without_title} pages missing title property")
        
        if pages_with_missing_props > 0:
            warnings.append(f"{pages_with_missing_props} pages missing some properties")
        
        details.update({
            "total_pages": content.total_pages,
            "pages_without_title": pages_without_title,
            "pages_with_missing_props": pages_with_missing_props,
            "unique_page_ids": len(set(page_ids))
        })
        
        return ValidationResult(
            check_name="content_integrity",
            passed=len(errors) == 0,
            errors=errors,
            warnings=warnings,
            details=details
        )
    
    def _validate_relationships_backup(
        self,
        content: DatabaseContent,
        schema: DatabaseSchema
    ) -> ValidationResult:
        """Validate relationship integrity in backup."""
        errors = []
        warnings = []
        details = {}
        
        # Get relation properties
        relation_properties = {
            prop_name: prop_schema
            for prop_name, prop_schema in schema.properties.items()
            if prop_schema.type == "relation"
        }
        
        if not relation_properties:
            return ValidationResult(
                check_name="relationship_integrity",
                passed=True,
                errors=[],
                warnings=[],
                details={"relation_properties": 0}
            )
        
        # Collect all relation references
        relation_refs = {}
        invalid_relations = 0
        
        for page in content.pages:
            for prop_name, prop_value in page.properties.items():
                if prop_name in relation_properties and isinstance(prop_value, dict):
                    if prop_value.get("type") == "relation":
                        relations = prop_value.get("relation", [])
                        
                        for relation in relations:
                            if isinstance(relation, dict) and "id" in relation:
                                ref_id = relation["id"]
                                if prop_name not in relation_refs:
                                    relation_refs[prop_name] = set()
                                relation_refs[prop_name].add(ref_id)
                            else:
                                invalid_relations += 1
        
        if invalid_relations > 0:
            warnings.append(f"{invalid_relations} invalid relation references found")
        
        details.update({
            "relation_properties": len(relation_properties),
            "relation_refs_by_property": {
                prop: len(refs) for prop, refs in relation_refs.items()
            },
            "total_relation_refs": sum(len(refs) for refs in relation_refs.values()),
            "invalid_relations": invalid_relations
        })
        
        return ValidationResult(
            check_name="relationship_integrity",
            passed=len(errors) == 0,
            errors=errors,
            warnings=warnings,
            details=details
        )
    
    def _validate_formulas_backup(self, schema: DatabaseSchema) -> ValidationResult:
        """Validate formula integrity in backup."""
        errors = []
        warnings = []
        details = {}
        
        formula_properties = [
            prop for prop in schema.properties.values()
            if prop.type in ["formula", "rollup"]
        ]
        
        if not formula_properties:
            return ValidationResult(
                check_name="formula_integrity",
                passed=True,
                errors=[],
                warnings=[],
                details={"formula_properties": 0, "rollup_properties": 0}
            )
        
        formula_count = 0
        rollup_count = 0
        
        for prop in formula_properties:
            if prop.type == "formula":
                formula_count += 1
                expression = prop.config.get("expression", "")
                if not expression:
                    errors.append(f"Formula property '{prop.name}' has empty expression")
                elif "ROI" in prop.name and "round(Value/(Effort*400)*10)/10" not in expression:
                    warnings.append(f"ROI formula in '{prop.name}' may not match expected pattern")
            
            elif prop.type == "rollup":
                rollup_count += 1
                if not prop.config.get("relation_property_name"):
                    errors.append(f"Rollup property '{prop.name}' missing relation_property_name")
                if not prop.config.get("rollup_property_name"):
                    errors.append(f"Rollup property '{prop.name}' missing rollup_property_name")
                if not prop.config.get("function"):
                    errors.append(f"Rollup property '{prop.name}' missing function")
        
        details.update({
            "formula_properties": formula_count,
            "rollup_properties": rollup_count,
            "total_computed_properties": len(formula_properties)
        })
        
        return ValidationResult(
            check_name="formula_integrity",
            passed=len(errors) == 0,
            errors=errors,
            warnings=warnings,
            details=details
        )
    
    def _validate_schema_restoration(
        self,
        original_schema: DatabaseSchema,
        current_db_data: Dict[str, Any]
    ) -> ValidationResult:
        """Validate schema restoration."""
        errors = []
        warnings = []
        details = {}
        
        current_properties = current_db_data.get("properties", {})
        
        # Check property count
        original_prop_count = len(original_schema.properties)
        current_prop_count = len(current_properties)
        
        if current_prop_count < original_prop_count:
            errors.append(
                f"Property count mismatch: expected {original_prop_count}, "
                f"got {current_prop_count}"
            )
        
        # Check individual properties
        missing_properties = []
        type_mismatches = []
        
        for prop_name, original_prop in original_schema.properties.items():
            if prop_name not in current_properties:
                missing_properties.append(prop_name)
                continue
            
            current_prop = current_properties[prop_name]
            current_type = current_prop.get("type")
            
            if current_type != original_prop.type:
                type_mismatches.append(
                    f"Property '{prop_name}': expected {original_prop.type}, "
                    f"got {current_type}"
                )
        
        if missing_properties:
            errors.extend([f"Missing property: {prop}" for prop in missing_properties])
        
        if type_mismatches:
            errors.extend(type_mismatches)
        
        details.update({
            "original_property_count": original_prop_count,
            "current_property_count": current_prop_count,
            "missing_properties": missing_properties,
            "type_mismatches": len(type_mismatches)
        })
        
        return ValidationResult(
            check_name="schema_restoration",
            passed=len(errors) == 0,
            errors=errors,
            warnings=warnings,
            details=details
        )
    
    def _validate_data_restoration(
        self,
        original_content: DatabaseContent,
        current_pages_data: Dict[str, Any]
    ) -> ValidationResult:
        """Validate data restoration."""
        errors = []
        warnings = []
        details = {}
        
        current_pages = current_pages_data.get("results", [])
        
        # Check page count
        original_page_count = original_content.total_pages
        current_page_count = len(current_pages)
        
        if current_page_count != original_page_count:
            if current_page_count < original_page_count:
                errors.append(
                    f"Page count mismatch: expected {original_page_count}, "
                    f"got {current_page_count}"
                )
            else:
                warnings.append(
                    f"More pages than expected: expected {original_page_count}, "
                    f"got {current_page_count}"
                )
        
        details.update({
            "original_page_count": original_page_count,
            "current_page_count": current_page_count,
            "page_count_match": current_page_count == original_page_count
        })
        
        return ValidationResult(
            check_name="data_restoration",
            passed=len(errors) == 0,
            errors=errors,
            warnings=warnings,
            details=details
        )
    
    def _validate_relationships_restoration(
        self,
        original_content: DatabaseContent,
        current_pages_data: Dict[str, Any]
    ) -> ValidationResult:
        """Validate relationship restoration."""
        errors = []
        warnings = []
        details = {}
        
        # This is a simplified validation - in a full implementation,
        # we would need to check that relation references are properly updated
        
        current_pages = current_pages_data.get("results", [])
        
        # Count relation properties in current pages
        current_relation_count = 0
        for page in current_pages:
            for prop_value in page.get("properties", {}).values():
                if isinstance(prop_value, dict) and prop_value.get("type") == "relation":
                    relations = prop_value.get("relation", [])
                    current_relation_count += len(relations)
        
        # Count relation properties in original pages
        original_relation_count = 0
        for page in original_content.pages:
            for prop_value in page.properties.values():
                if isinstance(prop_value, dict) and prop_value.get("type") == "relation":
                    relations = prop_value.get("relation", [])
                    original_relation_count += len(relations)
        
        if current_relation_count != original_relation_count:
            warnings.append(
                f"Relation count difference: original {original_relation_count}, "
                f"current {current_relation_count}"
            )
        
        details.update({
            "original_relation_count": original_relation_count,
            "current_relation_count": current_relation_count,
            "relation_count_match": current_relation_count == original_relation_count
        })
        
        return ValidationResult(
            check_name="relationship_restoration",
            passed=len(errors) == 0,
            errors=errors,
            warnings=warnings,
            details=details
        )
    
    def _validate_formulas_restoration(
        self,
        original_schema: DatabaseSchema,
        current_db_data: Dict[str, Any]
    ) -> ValidationResult:
        """Validate formula restoration."""
        errors = []
        warnings = []
        details = {}
        
        current_properties = current_db_data.get("properties", {})
        
        # Check formula properties
        original_formulas = {
            prop_name: prop_schema
            for prop_name, prop_schema in original_schema.properties.items()
            if prop_schema.type in ["formula", "rollup"]
        }
        
        current_formulas = {
            prop_name: prop_data
            for prop_name, prop_data in current_properties.items()
            if prop_data.get("type") in ["formula", "rollup"]
        }
        
        # Check formula count
        if len(current_formulas) != len(original_formulas):
            errors.append(
                f"Formula property count mismatch: expected {len(original_formulas)}, "
                f"got {len(current_formulas)}"
            )
        
        # Check individual formulas
        for prop_name, original_prop in original_formulas.items():
            if prop_name not in current_formulas:
                errors.append(f"Missing formula property: {prop_name}")
                continue
            
            current_prop = current_formulas[prop_name]
            
            if original_prop.type == "formula":
                original_expr = original_prop.config.get("expression", "")
                current_expr = current_prop.get("formula", {}).get("expression", "")
                
                if original_expr != current_expr:
                    warnings.append(
                        f"Formula expression difference in '{prop_name}': "
                        f"may have been modified during restoration"
                    )
        
        details.update({
            "original_formula_count": len(original_formulas),
            "current_formula_count": len(current_formulas),
            "formula_count_match": len(current_formulas) == len(original_formulas)
        })
        
        return ValidationResult(
            check_name="formula_restoration",
            passed=len(errors) == 0,
            errors=errors,
            warnings=warnings,
            details=details
        )
    
    def _log_validation_summary(
        self,
        validation_results: Dict[str, DatabaseValidation],
        operation_type: str
    ) -> None:
        """Log validation summary."""
        total_databases = len(validation_results)
        passed_databases = sum(1 for v in validation_results.values() if v.overall_passed)
        total_errors = sum(v.total_errors for v in validation_results.values())
        total_warnings = sum(v.total_warnings for v in validation_results.values())
        
        self.logger.info(
            f"{operation_type.title()} validation summary: "
            f"{passed_databases}/{total_databases} databases passed, "
            f"{total_errors} errors, {total_warnings} warnings"
        )
        
        # Log details for failed databases
        for db_name, validation in validation_results.items():
            if not validation.overall_passed:
                self.logger.warning(
                    f"Database '{db_name}' validation failed: "
                    f"{validation.total_errors} errors, {validation.total_warnings} warnings"
                )
