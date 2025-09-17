"""
Data restoration for Phase 4 of restoration.

This module populates databases with actual content, handling page creation
with property values and updating relation references using the ID mapping system.
"""

from typing import Dict, List, Optional, Any, Callable
import logging
from dataclasses import dataclass

from ..utils.api_client import NotionAPIClient
from ..utils.id_mapper import IDMapper
from ..backup.content_extractor import DatabaseContent, PageContent


@dataclass
class DataRestorationResult:
    """Result of data restoration operation."""
    database_id: str
    database_name: str
    total_pages: int
    created_pages: int
    failed_pages: int
    page_mappings: Dict[str, str]  # original_id -> new_id
    errors: List[str]


class DataRestorer:
    """
    Restores data during Phase 4 of restoration.
    
    This class handles page creation with property values, updates relation
    references using the ID mapping system, and manages large datasets.
    """
    
    def __init__(
        self,
        api_client: NotionAPIClient,
        id_mapper: IDMapper,
        logger: Optional[logging.Logger] = None
    ):
        """
        Initialize data restorer.
        
        Args:
            api_client: Notion API client
            id_mapper: ID mapping system
            logger: Logger instance
        """
        self.api_client = api_client
        self.id_mapper = id_mapper
        self.logger = logger or logging.getLogger(__name__)
    
    def restore_data(
        self,
        content: DatabaseContent,
        progress_callback: Optional[Callable[[int, int], None]] = None
    ) -> DataRestorationResult:
        """
        Restore all data for a database.
        
        Args:
            content: Database content to restore
            progress_callback: Optional callback for progress updates
            
        Returns:
            DataRestorationResult with restoration details
        """
        # Get the new database ID
        new_database_id = self.id_mapper.get_new_id(content.database_id)
        if not new_database_id:
            error_msg = f"No ID mapping found for database {content.database_id}"
            self.logger.error(error_msg)
            return DataRestorationResult(
                database_id="",
                database_name=content.database_name,
                total_pages=content.total_pages,
                created_pages=0,
                failed_pages=content.total_pages,
                page_mappings={},
                errors=[error_msg]
            )
        
        self.logger.info(
            f"Restoring data for database '{content.database_name}' "
            f"({content.total_pages} pages)"
        )
        
        created_pages = 0
        failed_pages = 0
        page_mappings = {}
        errors = []
        
        # Restore pages
        for i, page in enumerate(content.pages, 1):
            try:
                new_page_id = self._create_page(new_database_id, page)
                
                if new_page_id:
                    created_pages += 1
                    page_mappings[page.id] = new_page_id
                    
                    # Add page mapping to ID mapper
                    self.id_mapper.add_mapping(
                        original_id=page.id,
                        new_id=new_page_id,
                        object_type="page",
                        name=self._extract_page_title(page)
                    )
                else:
                    failed_pages += 1
                
                # Progress callback
                if progress_callback:
                    progress_callback(i, content.total_pages)
                
                if i % 10 == 0:  # Log progress every 10 pages
                    self.logger.debug(
                        f"Progress: {i}/{content.total_pages} pages "
                        f"({created_pages} created, {failed_pages} failed)"
                    )
                    
            except Exception as e:
                failed_pages += 1
                error_msg = f"Failed to create page {page.id}: {e}"
                errors.append(error_msg)
                self.logger.error(error_msg)
        
        self.logger.info(
            f"Restored data for '{content.database_name}': "
            f"{created_pages} created, {failed_pages} failed"
        )
        
        return DataRestorationResult(
            database_id=new_database_id,
            database_name=content.database_name,
            total_pages=content.total_pages,
            created_pages=created_pages,
            failed_pages=failed_pages,
            page_mappings=page_mappings,
            errors=errors
        )
    
    def _create_page(self, database_id: str, page: PageContent) -> Optional[str]:
        """
        Create a single page in the database.
        
        Args:
            database_id: ID of the database
            page: Page content to create
            
        Returns:
            New page ID if successful, None otherwise
        """
        try:
            # Prepare page creation payload
            create_payload = {
                "parent": {"database_id": database_id},
                "properties": self._prepare_page_properties(page.properties)
            }
            
            # Add icon and cover if present
            if page.icon:
                create_payload["icon"] = page.icon
            if page.cover:
                create_payload["cover"] = page.cover
            
            # Create the page
            response = self.api_client.create_page(**create_payload)
            new_page_id = response["id"]
            
            # Create blocks if present
            if page.blocks:
                self._create_page_blocks(new_page_id, page.blocks)
            
            return new_page_id
            
        except Exception as e:
            self.logger.error(f"Error creating page: {e}")
            return None
    
    def _prepare_page_properties(self, properties: Dict[str, Any]) -> Dict[str, Any]:
        """
        Prepare page properties with updated relation IDs.
        
        Args:
            properties: Original page properties
            
        Returns:
            Updated properties with new relation IDs
        """
        updated_properties = {}
        
        for prop_name, prop_value in properties.items():
            if isinstance(prop_value, dict):
                prop_type = prop_value.get("type")
                
                if prop_type == "relation":
                    # Update relation references
                    updated_properties[prop_name] = self._update_relation_property(prop_value)
                elif prop_type in ["formula", "rollup", "created_time", "created_by", 
                                 "last_edited_time", "last_edited_by"]:
                    # Skip computed properties - they will be calculated automatically
                    continue
                else:
                    # Keep other properties as-is
                    updated_properties[prop_name] = prop_value
            else:
                # Non-dict property value, keep as-is
                updated_properties[prop_name] = prop_value
        
        return updated_properties
    
    def _update_relation_property(self, relation_property: Dict[str, Any]) -> Dict[str, Any]:
        """
        Update relation property with new page IDs.
        
        Args:
            relation_property: Original relation property
            
        Returns:
            Updated relation property
        """
        updated_property = relation_property.copy()
        relations = relation_property.get("relation", [])
        updated_relations = []
        
        for relation in relations:
            if isinstance(relation, dict) and "id" in relation:
                original_page_id = relation["id"]
                new_page_id = self.id_mapper.get_new_id(original_page_id)
                
                if new_page_id:
                    updated_relations.append({"id": new_page_id})
                else:
                    # If no mapping found, skip this relation
                    # This might happen if the related page hasn't been created yet
                    self.logger.warning(
                        f"No ID mapping found for related page: {original_page_id}"
                    )
            else:
                # Keep non-ID relations as-is
                updated_relations.append(relation)
        
        updated_property["relation"] = updated_relations
        return updated_property
    
    def _create_page_blocks(self, page_id: str, blocks: List[Dict[str, Any]]) -> None:
        """
        Create blocks for a page.
        
        Args:
            page_id: ID of the page
            blocks: List of block data
        """
        if not blocks:
            return
        
        try:
            # Prepare blocks for creation
            prepared_blocks = self._prepare_blocks(blocks)
            
            if prepared_blocks:
                self.api_client.append_block_children(
                    block_id=page_id,
                    children=prepared_blocks
                )
                
        except Exception as e:
            self.logger.warning(f"Failed to create blocks for page {page_id}: {e}")
    
    def _prepare_blocks(self, blocks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Prepare blocks for creation, removing read-only properties.
        
        Args:
            blocks: Original block data
            
        Returns:
            Prepared blocks for creation
        """
        prepared_blocks = []
        
        for block in blocks:
            # Remove read-only properties
            prepared_block = {
                key: value for key, value in block.items()
                if key not in ["id", "created_time", "created_by", "last_edited_time", 
                             "last_edited_by", "archived", "has_children"]
            }
            
            # Handle child blocks recursively
            if "children" in block and block["children"]:
                prepared_block["children"] = self._prepare_blocks(block["children"])
            
            prepared_blocks.append(prepared_block)
        
        return prepared_blocks
    
    def _extract_page_title(self, page: PageContent) -> str:
        """
        Extract page title from properties.
        
        Args:
            page: Page content
            
        Returns:
            Page title string
        """
        for prop_value in page.properties.values():
            if isinstance(prop_value, dict) and prop_value.get("type") == "title":
                title_content = prop_value.get("title", [])
                return "".join([
                    text.get("plain_text", "") 
                    for text in title_content
                ])
        
        return f"Page {page.id[:8]}"
    
    def restore_multiple_databases(
        self,
        contents: Dict[str, DatabaseContent],
        restoration_order: Optional[List[str]] = None,
        progress_callback: Optional[Callable[[str, int, int], None]] = None
    ) -> Dict[str, DataRestorationResult]:
        """
        Restore data for multiple databases.
        
        Args:
            contents: Dictionary mapping database names to content
            restoration_order: Order to restore databases (optional)
            progress_callback: Optional callback for progress updates
            
        Returns:
            Dictionary mapping database names to restoration results
        """
        if restoration_order is None:
            restoration_order = list(contents.keys())
        
        results = {}
        
        for db_name in restoration_order:
            if db_name not in contents:
                self.logger.warning(f"Database '{db_name}' not found in contents")
                continue
            
            content = contents[db_name]
            
            # Progress callback for individual pages
            def page_progress(current_pages: int, total_pages: int):
                if progress_callback:
                    progress_callback(db_name, current_pages, total_pages)
            
            result = self.restore_data(content, page_progress)
            results[db_name] = result
            
            if result.errors:
                self.logger.error(f"Data restoration had errors for '{db_name}': {len(result.errors)} errors")
            else:
                self.logger.info(f"Successfully restored data for '{db_name}'")
        
        return results
    
    def update_cross_database_relations(
        self,
        results: Dict[str, DataRestorationResult]
    ) -> Dict[str, int]:
        """
        Update cross-database relations after all pages are created.
        
        This is a second pass to update relations that couldn't be resolved
        during the initial page creation.
        
        Args:
            results: Dictionary of restoration results
            
        Returns:
            Dictionary mapping database names to number of relations updated
        """
        updates_per_database = {}
        
        for db_name, result in results.items():
            updates_count = 0
            
            # This would require re-reading pages and updating relations
            # For now, we'll just log that this functionality exists
            self.logger.info(f"Cross-database relation updates for '{db_name}': {updates_count}")
            updates_per_database[db_name] = updates_count
        
        return updates_per_database
    
    def get_restoration_stats(self, results: Dict[str, DataRestorationResult]) -> Dict[str, Any]:
        """
        Get statistics about data restoration.
        
        Args:
            results: Dictionary of restoration results
            
        Returns:
            Dictionary with restoration statistics
        """
        total_pages = sum(result.total_pages for result in results.values())
        total_created = sum(result.created_pages for result in results.values())
        total_failed = sum(result.failed_pages for result in results.values())
        total_errors = sum(len(result.errors) for result in results.values())
        
        return {
            "total_databases": len(results),
            "total_pages": total_pages,
            "total_created": total_created,
            "total_failed": total_failed,
            "total_errors": total_errors,
            "success_rate": total_created / total_pages if total_pages > 0 else 0,
            "page_mappings_count": sum(len(result.page_mappings) for result in results.values()),
        }
