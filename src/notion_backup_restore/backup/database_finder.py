"""
Database discovery logic for finding target databases in the workspace.

This module implements database discovery specifically for the user's workspace
structure, finding databases by name and validating their structure.
"""

from typing import List, Dict, Optional, Set, Any
import logging
from dataclasses import dataclass

from ..utils.api_client import NotionAPIClient
from ..config import WORKSPACE_DATABASES


@dataclass
class DatabaseInfo:
    """Information about a discovered database."""
    id: str
    name: str
    title: str
    url: str
    properties: Dict[str, Any]
    created_time: str
    last_edited_time: str
    parent: Dict[str, Any]


class DatabaseFinder:
    """
    Discovers databases in the Notion workspace.
    
    Since Notion API doesn't expose teamspaces directly, this class uses
    search functionality to find databases by name and validates they
    belong to the correct workspace structure.
    """
    
    def __init__(self, api_client: NotionAPIClient, logger: Optional[logging.Logger] = None):
        """
        Initialize database finder.
        
        Args:
            api_client: Notion API client
            logger: Logger instance
        """
        self.api_client = api_client
        self.logger = logger or logging.getLogger(__name__)
        self._discovered_databases: Dict[str, DatabaseInfo] = {}
    
    def find_target_databases(self, database_names: Optional[List[str]] = None) -> Dict[str, DatabaseInfo]:
        """
        Find target databases by name.
        
        Args:
            database_names: List of database names to find (defaults to workspace databases)
            
        Returns:
            Dictionary mapping database names to DatabaseInfo objects
            
        Raises:
            ValueError: If required databases are not found
        """
        if database_names is None:
            database_names = list(WORKSPACE_DATABASES.keys())
        
        self.logger.info(f"Searching for databases: {database_names}")
        
        found_databases = {}
        missing_databases = []
        
        for db_name in database_names:
            self.logger.debug(f"Searching for database: {db_name}")
            
            database_info = self._search_database_by_name(db_name)
            if database_info:
                found_databases[db_name] = database_info
                self._discovered_databases[db_name] = database_info
                self.logger.info(f"Found database '{db_name}': {database_info.id}")
            else:
                missing_databases.append(db_name)
                self.logger.warning(f"Database not found: {db_name}")
        
        if missing_databases:
            raise ValueError(
                f"Could not find the following databases: {missing_databases}. "
                f"Please ensure they exist and are shared with your integration."
            )
        
        self.logger.info(f"Successfully found {len(found_databases)} databases")
        return found_databases
    
    def _search_database_by_name(self, database_name: str) -> Optional[DatabaseInfo]:
        """
        Search for a database or wiki by name using Notion's search API.
        
        Args:
            database_name: Name of the database/wiki to search for
            
        Returns:
            DatabaseInfo if found, None otherwise
        """
        try:
            # Search with "data_source" filter - Notion API changed from "database" to "data_source"
            # According to Notion API docs, databases are now called "Data Sources"
            # Filter values are now "page" or "data_source" instead of "database"
            search_results = self.api_client.search(
                query=database_name,
                filter={
                    "value": "data_source",
                    "property": "object"
                }
            )
            
            # Look for exact name matches in databases
            self.logger.info(f"Search returned {len(search_results.get('results', []))} results for '{database_name}'")
            
            # Debug: show what types of objects we're getting
            obj_types = {}
            for result in search_results.get("results", []):
                obj_type = result.get("object")
                obj_types[obj_type] = obj_types.get(obj_type, 0) + 1
            self.logger.info(f"  Object types in results: {obj_types}")
            
            for result in search_results.get("results", []):
                obj_type = result.get("object")
                result_id = result.get("id", "N/A")
                
                # More detailed logging to see what we're getting
                self.logger.debug(f"  Result: type={obj_type}, id={result_id}")
                
                # Get title for logging (works for all object types)
                title_for_log = "UNKNOWN"
                title_array = result.get("title", [])
                if title_array:
                    title_for_log = "".join([t.get("plain_text", "") for t in title_array])
                
                # Log all results with their titles
                self.logger.info(f"    {obj_type} '{result_id}': '{title_for_log}'")
                
                if obj_type == "page":
                    # Check if this page is actually a database
                    props = result.get("properties", {})
                    self.logger.debug(f"    Page has {len(props)} properties")
                
                self.logger.debug(f"  Result details: {obj_type}, id={result_id}")
                
                # Notion API changed - databases now come as "database" objects OR as "page" objects
                # We need to check both
                if obj_type == "database":
                    title_property = result.get("title", [])
                    if title_property:
                        title = "".join([
                            text.get("plain_text", "") 
                            for text in title_property
                        ])
                        
                        self.logger.debug(f"    Database title: '{title}'")
                        
                        # Check for exact match (case-insensitive)
                        if title.strip().lower() == database_name.lower():
                            self.logger.info(f"Found exact match for '{database_name}': {result_id}")
                            return self._create_database_info(result)
                
                elif obj_type == "page":
                    # With data_source filter, we shouldn't get many pages, but handle them anyway
                    # Try regular page title matching for wikis
                    title_property = result.get("title", [])
                    if title_property:
                        title = "".join([
                            text.get("plain_text", "") 
                            for text in title_property
                        ])
                        
                        self.logger.debug(f"    Page title: '{title}'")
                        
                        # Check for exact match (case-insensitive)
                        if title.strip().lower() == database_name.lower():
                            # This might be a wiki appearing as a page
                            # Verify it has properties
                            if result.get("properties"):
                                self.logger.info(f"Found wiki/database as page for '{database_name}': {result_id}")
                                return self._create_database_info(result)
                            else:
                                self.logger.debug(f"    Page '{title}' matched name but has no properties, skipping")
            
            # If not found as database, search for pages (wikis appear as pages)
            search_results = self.api_client.search(
                query=database_name,
                filter={
                    "value": "page",
                    "property": "object"
                }
            )
            
            # Look for exact name matches in pages (for wikis)
            for result in search_results.get("results", []):
                if result.get("object") == "page":
                    # Check if this page has properties (indicating it might be a wiki)
                    page_properties = result.get("properties", {})
                    if page_properties:  # If it has properties, it might be a wiki
                        # Get the page title
                        title_property = result.get("properties", {}).get("title")
                        if not title_property:
                            # Try to get title from the page title field
                            title_property = result.get("title", [])
                        
                        if title_property:
                            if isinstance(title_property, list):
                                title = "".join([
                                    text.get("plain_text", "") 
                                    for text in title_property
                                ])
                            else:
                                # Handle property-based title
                                title_content = title_property.get("title", [])
                                title = "".join([
                                    text.get("plain_text", "") 
                                    for text in title_content
                                ])
                            
                            # Check for exact match (case-insensitive)
                            if title.strip().lower() == database_name.lower():
                                # Convert page to database-like structure for wikis
                                wiki_as_db = self._convert_wiki_to_database_info(result, database_name)
                                if wiki_as_db:
                                    return wiki_as_db
            
            # If no exact match found, try partial matches in databases
            # Search again without filter for partial matches
            search_results = self.api_client.search(
                query=database_name
            )
            
            for result in search_results.get("results", []):
                if result.get("object") == "database":
                    title_property = result.get("title", [])
                    if title_property:
                        title = "".join([
                            text.get("plain_text", "") 
                            for text in title_property
                        ])
                        
                        # Check for partial match
                        if database_name.lower() in title.strip().lower():
                            self.logger.warning(
                                f"Found partial match for '{database_name}': '{title}' "
                                f"(ID: {result['id']})"
                            )
                            return self._create_database_info(result)
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error searching for database '{database_name}': {e}")
            return None
    
    def _create_database_info(self, database_data: Dict[str, Any]) -> DatabaseInfo:
        """
        Create DatabaseInfo from API response data.
        
        Args:
            database_data: Database data from Notion API
            
        Returns:
            DatabaseInfo object
        """
        # Extract title
        title_property = database_data.get("title", [])
        title = "".join([
            text.get("plain_text", "") 
            for text in title_property
        ])
        
        return DatabaseInfo(
            id=database_data["id"],
            name=title.strip(),
            title=title.strip(),
            url=database_data.get("url", ""),
            properties=database_data.get("properties", {}),
            created_time=database_data.get("created_time", ""),
            last_edited_time=database_data.get("last_edited_time", ""),
            parent=database_data.get("parent", {})
        )
    
    def _convert_wiki_to_database_info(self, page_data: Dict[str, Any], expected_name: str) -> Optional[DatabaseInfo]:
        """
        Convert a wiki page to DatabaseInfo structure.
        
        Wikis in Notion appear as pages but can have database-like properties.
        This method attempts to treat them like databases for backup purposes.
        
        Args:
            page_data: Page data from Notion API
            expected_name: Expected name of the wiki
            
        Returns:
            DatabaseInfo object if the page looks like a wiki, None otherwise
        """
        try:
            # For wikis, we need to check if this page has child pages with properties
            # This is a heuristic - wikis often contain pages with structured data
            
            # Create a database-like structure from the wiki page
            # Note: We'll need to query the wiki's child pages to get the actual schema
            return DatabaseInfo(
                id=page_data["id"],
                name=expected_name,
                title=expected_name,
                url=page_data.get("url", ""),
                properties={},  # Will be populated when we query the wiki's structure
                created_time=page_data.get("created_time", ""),
                last_edited_time=page_data.get("last_edited_time", ""),
                parent=page_data.get("parent", {})
            )
            
        except Exception as e:
            self.logger.error(f"Error converting wiki page to database info: {e}")
            return None
    
    def validate_database_structure(self, database_name: str, database_info: DatabaseInfo) -> List[str]:
        """
        Validate that a database has the expected structure.
        
        Args:
            database_name: Name of the database
            database_info: Database information
            
        Returns:
            List of validation errors (empty if valid)
        """
        errors = []
        
        if database_name not in WORKSPACE_DATABASES:
            errors.append(f"Unknown database: {database_name}")
            return errors
        
        expected_properties = WORKSPACE_DATABASES[database_name]["properties"]
        actual_properties = database_info.properties
        
        # Check for missing properties
        for prop_name, prop_config in expected_properties.items():
            if prop_name not in actual_properties:
                errors.append(f"Missing property '{prop_name}' in database '{database_name}'")
                continue
            
            # Check property type
            actual_prop = actual_properties[prop_name]
            expected_type = prop_config["type"]
            actual_type = actual_prop.get("type")
            
            if actual_type != expected_type:
                errors.append(
                    f"Property '{prop_name}' in database '{database_name}' "
                    f"has type '{actual_type}', expected '{expected_type}'"
                )
        
        # Check for unexpected properties (warning, not error)
        for prop_name in actual_properties:
            if prop_name not in expected_properties:
                self.logger.warning(
                    f"Unexpected property '{prop_name}' in database '{database_name}'"
                )
        
        return errors
    
    def validate_all_databases(self) -> Dict[str, List[str]]:
        """
        Validate structure of all discovered databases.
        
        Returns:
            Dictionary mapping database names to validation errors
        """
        validation_results = {}
        
        for db_name, db_info in self._discovered_databases.items():
            errors = self.validate_database_structure(db_name, db_info)
            validation_results[db_name] = errors
            
            if errors:
                self.logger.error(f"Validation errors for database '{db_name}': {errors}")
            else:
                self.logger.info(f"Database '{db_name}' structure is valid")
        
        return validation_results
    
    def get_database_relationships(self) -> Dict[str, List[str]]:
        """
        Analyze relationships between discovered databases.
        
        Returns:
            Dictionary mapping database names to lists of related database names
        """
        relationships = {}
        
        for db_name, db_info in self._discovered_databases.items():
            related_databases = []
            
            for prop_name, prop_data in db_info.properties.items():
                if prop_data.get("type") == "relation":
                    relation_config = prop_data.get("relation", {})
                    related_db_id = relation_config.get("database_id")
                    
                    if related_db_id:
                        # Find the database name for this ID
                        for other_db_name, other_db_info in self._discovered_databases.items():
                            if other_db_info.id == related_db_id:
                                related_databases.append(other_db_name)
                                break
            
            relationships[db_name] = related_databases
        
        return relationships
    
    def get_discovery_stats(self) -> Dict[str, Any]:
        """
        Get statistics about database discovery.
        
        Returns:
            Dictionary with discovery statistics
        """
        total_properties = sum(
            len(db_info.properties) 
            for db_info in self._discovered_databases.values()
        )
        
        relation_properties = 0
        for db_info in self._discovered_databases.values():
            for prop_data in db_info.properties.values():
                if prop_data.get("type") == "relation":
                    relation_properties += 1
        
        return {
            "databases_found": len(self._discovered_databases),
            "total_properties": total_properties,
            "relation_properties": relation_properties,
            "database_names": list(self._discovered_databases.keys()),
            "database_ids": [db.id for db in self._discovered_databases.values()],
        }
    
    def clear_cache(self) -> None:
        """Clear discovered databases cache."""
        self._discovered_databases.clear()
    
    def get_database_by_name(self, database_name: str) -> Optional[DatabaseInfo]:
        """
        Get database info by name from cache.
        
        Args:
            database_name: Name of the database
            
        Returns:
            DatabaseInfo if found in cache, None otherwise
        """
        return self._discovered_databases.get(database_name)
    
    def get_database_by_id(self, database_id: str) -> Optional[DatabaseInfo]:
        """
        Get database info by ID from cache.
        
        Args:
            database_id: ID of the database
            
        Returns:
            DatabaseInfo if found in cache, None otherwise
        """
        for db_info in self._discovered_databases.values():
            if db_info.id == database_id:
                return db_info
        return None
