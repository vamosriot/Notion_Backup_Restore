"""
Content extraction for database pages and block content.

This module handles extraction of all pages within databases, including
property values, relations, and optionally block content with pagination support.
"""

from typing import Dict, List, Optional, Any, Iterator, Callable, Set
import logging
from dataclasses import dataclass
from datetime import datetime

from ..utils.api_client import NotionAPIClient


@dataclass
class PageContent:
    """Content information for a database page."""
    id: str
    url: str
    properties: Dict[str, Any]
    parent: Dict[str, Any]
    archived: bool
    created_time: str
    last_edited_time: str
    created_by: Dict[str, Any]
    last_edited_by: Dict[str, Any]
    cover: Optional[Dict[str, Any]]
    icon: Optional[Dict[str, Any]]
    blocks: Optional[List[Dict[str, Any]]] = None


@dataclass
class DatabaseContent:
    """Complete content information for a database."""
    database_id: str
    database_name: str
    pages: List[PageContent]
    total_pages: int
    extraction_time: str
    has_more: bool = False
    next_cursor: Optional[str] = None


class ContentExtractor:
    """
    Extracts content from Notion database pages.
    
    This class handles pagination for large datasets and includes
    progress tracking for long-running operations.
    """
    
    def __init__(self, api_client: NotionAPIClient, logger: Optional[logging.Logger] = None):
        """
        Initialize content extractor.
        
        Args:
            api_client: Notion API client
            logger: Logger instance
        """
        self.api_client = api_client
        self.logger = logger or logging.getLogger(__name__)
    
    def extract_content(
        self,
        database_id: str,
        database_name: str = "",
        include_blocks: bool = False,
        page_size: int = 100,
        progress_callback: Optional[Callable[[int, int], None]] = None,
        skip_page_ids: Optional[Set[str]] = None
    ) -> DatabaseContent:
        """
        Extract all content from a database.
        
        Args:
            database_id: ID of the database
            database_name: Name of the database (for logging)
            include_blocks: Whether to extract block content from pages
            page_size: Number of pages to fetch per request
            progress_callback: Optional callback for progress updates
            skip_page_ids: Optional set of page IDs to skip (for resume)
            
        Returns:
            DatabaseContent object with all pages
        """
        if skip_page_ids is None:
            skip_page_ids = set()
        
        self.logger.info(
            f"Extracting content from database '{database_name}' ({database_id})"
        )
        if skip_page_ids:
            self.logger.info(f"Skipping {len(skip_page_ids)} already downloaded pages")
        
        pages = []
        total_pages = 0
        skipped_pages = 0
        start_cursor = None
        
        try:
            # Extract pages with pagination
            for page_batch in self._paginate_pages(database_id, page_size):
                batch_pages = []
                
                for page_data in page_batch:
                    page_id = page_data.get("id")
                    
                    # Skip if already downloaded
                    if page_id in skip_page_ids:
                        skipped_pages += 1
                        continue
                    
                    page_content = self._extract_page_content(
                        page_data, 
                        include_blocks=include_blocks
                    )
                    batch_pages.append(page_content)
                
                pages.extend(batch_pages)
                total_pages += len(batch_pages)
                
                # Progress callback
                if progress_callback:
                    progress_callback(total_pages, total_pages)  # We don't know total upfront
                
                self.logger.debug(f"Extracted {len(batch_pages)} pages (total: {total_pages})")
            
            content = DatabaseContent(
                database_id=database_id,
                database_name=database_name,
                pages=pages,
                total_pages=total_pages,
                extraction_time=datetime.utcnow().isoformat()
            )
            
            if skipped_pages > 0:
                self.logger.info(
                    f"Extracted {total_pages} new pages from database '{database_name}' (skipped {skipped_pages} already downloaded)"
                )
            else:
                self.logger.info(
                    f"Extracted {total_pages} pages from database '{database_name}'"
                )
            
            return content
            
        except Exception as e:
            self.logger.error(
                f"Error extracting content from database '{database_name}' ({database_id}): {e}"
            )
            raise
    
    def _paginate_pages(
        self, 
        database_id: str, 
        page_size: int = 100
    ) -> Iterator[List[Dict[str, Any]]]:
        """
        Paginate through all pages in a database.
        
        Args:
            database_id: ID of the database
            page_size: Number of pages per request
            
        Yields:
            Lists of page data
        """
        start_cursor = None
        
        while True:
            query_params = {
                "page_size": page_size
            }
            
            if start_cursor:
                query_params["start_cursor"] = start_cursor
            
            try:
                response = self.api_client.query_database(database_id, **query_params)
                
                results = response.get("results", [])
                if not results:
                    break
                
                yield results
                
                # Check if there are more pages
                has_more = response.get("has_more", False)
                if not has_more:
                    break
                
                start_cursor = response.get("next_cursor")
                if not start_cursor:
                    break
                    
            except Exception as e:
                self.logger.error(f"Error querying database {database_id}: {e}")
                raise
    
    def _extract_page_content(
        self, 
        page_data: Dict[str, Any], 
        include_blocks: bool = False
    ) -> PageContent:
        """
        Extract content from a single page.
        
        Args:
            page_data: Page data from API
            include_blocks: Whether to extract block content
            
        Returns:
            PageContent object
        """
        page_id = page_data["id"]
        
        # Extract blocks if requested
        blocks = None
        if include_blocks:
            try:
                blocks = self._extract_page_blocks(page_id)
            except Exception as e:
                self.logger.warning(f"Failed to extract blocks for page {page_id}: {e}")
                blocks = []
        
        return PageContent(
            id=page_id,
            url=page_data.get("url", ""),
            properties=page_data.get("properties", {}),
            parent=page_data.get("parent", {}),
            archived=page_data.get("archived", False),
            created_time=page_data.get("created_time", ""),
            last_edited_time=page_data.get("last_edited_time", ""),
            created_by=page_data.get("created_by", {}),
            last_edited_by=page_data.get("last_edited_by", {}),
            cover=page_data.get("cover"),
            icon=page_data.get("icon"),
            blocks=blocks
        )
    
    def _extract_page_blocks(self, page_id: str) -> List[Dict[str, Any]]:
        """
        Extract all blocks from a page.
        
        Args:
            page_id: ID of the page
            
        Returns:
            List of block data
        """
        blocks = []
        
        try:
            for block_batch in self._paginate_blocks(page_id):
                blocks.extend(block_batch)
        
        except Exception as e:
            self.logger.error(f"Error extracting blocks from page {page_id}: {e}")
            raise
        
        return blocks
    
    def _paginate_blocks(self, block_id: str) -> Iterator[List[Dict[str, Any]]]:
        """
        Paginate through all blocks in a page or block.
        
        Args:
            block_id: ID of the parent block/page
            
        Yields:
            Lists of block data
        """
        start_cursor = None
        
        while True:
            query_params = {}
            
            if start_cursor:
                query_params["start_cursor"] = start_cursor
            
            try:
                response = self.api_client.get_block_children(block_id, **query_params)
                
                results = response.get("results", [])
                if not results:
                    break
                
                # Recursively extract child blocks
                for block in results:
                    if block.get("has_children", False):
                        try:
                            child_blocks = list(self._paginate_blocks(block["id"]))
                            block["children"] = [
                                child for batch in child_blocks for child in batch
                            ]
                        except Exception as e:
                            self.logger.warning(
                                f"Failed to extract children for block {block['id']}: {e}"
                            )
                            block["children"] = []
                
                yield results
                
                # Check if there are more blocks
                has_more = response.get("has_more", False)
                if not has_more:
                    break
                
                start_cursor = response.get("next_cursor")
                if not start_cursor:
                    break
                    
            except Exception as e:
                self.logger.error(f"Error getting block children for {block_id}: {e}")
                raise
    
    def extract_multiple_databases(
        self,
        database_configs: Dict[str, Dict[str, Any]],
        include_blocks: bool = False,
        progress_callback: Optional[Callable[[str, int, int], None]] = None
    ) -> Dict[str, DatabaseContent]:
        """
        Extract content from multiple databases.
        
        Args:
            database_configs: Dict mapping database names to config dicts with 'id' key
            include_blocks: Whether to extract block content
            progress_callback: Optional callback for progress updates (db_name, current, total)
            
        Returns:
            Dictionary mapping database names to DatabaseContent objects
        """
        contents = {}
        total_databases = len(database_configs)
        
        for i, (db_name, db_config) in enumerate(database_configs.items(), 1):
            db_id = db_config["id"]
            
            self.logger.info(f"Extracting database {i}/{total_databases}: {db_name}")
            
            try:
                # Progress callback for individual pages
                def page_progress(current_pages: int, total_pages: int):
                    if progress_callback:
                        progress_callback(db_name, current_pages, total_pages)
                
                content = self.extract_content(
                    database_id=db_id,
                    database_name=db_name,
                    include_blocks=include_blocks,
                    progress_callback=page_progress
                )
                
                contents[db_name] = content
                
            except Exception as e:
                self.logger.error(f"Failed to extract content for database '{db_name}': {e}")
                # Continue with other databases
        
        return contents
    
    def get_relation_references(self, content: DatabaseContent) -> Dict[str, List[str]]:
        """
        Extract all relation references from database content.
        
        Args:
            content: Database content
            
        Returns:
            Dictionary mapping property names to lists of referenced page IDs
        """
        relation_refs = {}
        
        for page in content.pages:
            for prop_name, prop_value in page.properties.items():
                if isinstance(prop_value, dict) and prop_value.get("type") == "relation":
                    relations = prop_value.get("relation", [])
                    
                    if prop_name not in relation_refs:
                        relation_refs[prop_name] = []
                    
                    for relation in relations:
                        if isinstance(relation, dict) and "id" in relation:
                            relation_refs[prop_name].append(relation["id"])
        
        return relation_refs
    
    def validate_content_integrity(self, content: DatabaseContent) -> List[str]:
        """
        Validate content integrity and return any issues found.
        
        Args:
            content: Database content to validate
            
        Returns:
            List of validation error messages
        """
        errors = []
        
        # Check for duplicate page IDs
        page_ids = [page.id for page in content.pages]
        if len(page_ids) != len(set(page_ids)):
            errors.append("Duplicate page IDs found in content")
        
        # Check for pages with missing required properties
        for page in content.pages:
            if not page.properties:
                errors.append(f"Page {page.id} has no properties")
            
            # Check for title property (every page should have one)
            title_props = [
                prop for prop in page.properties.values()
                if isinstance(prop, dict) and prop.get("type") == "title"
            ]
            if not title_props:
                errors.append(f"Page {page.id} has no title property")
        
        return errors
    
    def get_content_stats(self, content: DatabaseContent) -> Dict[str, Any]:
        """
        Get statistics about database content.
        
        Args:
            content: Database content
            
        Returns:
            Dictionary with content statistics
        """
        # Count property types
        property_usage = {}
        total_relations = 0
        
        for page in content.pages:
            for prop_name, prop_value in page.properties.items():
                if isinstance(prop_value, dict):
                    prop_type = prop_value.get("type", "unknown")
                    property_usage[prop_type] = property_usage.get(prop_type, 0) + 1
                    
                    if prop_type == "relation":
                        relations = prop_value.get("relation", [])
                        total_relations += len(relations)
        
        # Count archived pages
        archived_pages = sum(1 for page in content.pages if page.archived)
        
        # Count pages with blocks
        pages_with_blocks = sum(
            1 for page in content.pages 
            if page.blocks is not None and len(page.blocks) > 0
        )
        
        return {
            "database_id": content.database_id,
            "database_name": content.database_name,
            "total_pages": content.total_pages,
            "archived_pages": archived_pages,
            "property_usage": property_usage,
            "total_relations": total_relations,
            "pages_with_blocks": pages_with_blocks,
            "extraction_time": content.extraction_time,
        }
