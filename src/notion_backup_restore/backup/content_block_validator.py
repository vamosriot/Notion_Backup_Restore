"""
Enhanced content block validation and sanitization.

This module provides comprehensive validation and sanitization for all Notion
content block types, ensuring compatibility with current API requirements and
preventing restoration errors.
"""

import re
import json
from typing import Dict, List, Optional, Any, Set, Tuple
import logging
from dataclasses import dataclass
from urllib.parse import urlparse

from ..utils.logger import setup_logger


@dataclass
class BlockValidationStats:
    """Statistics from block validation operations."""
    blocks_processed: int = 0
    blocks_sanitized: int = 0
    blocks_removed: int = 0
    content_truncated: int = 0
    urls_validated: int = 0
    rich_text_cleaned: int = 0
    errors_found: int = 0
    warnings_issued: int = 0


class ContentBlockValidator:
    """
    Comprehensive content block validator and sanitizer.
    
    This class handles validation and sanitization of all Notion block types,
    ensuring they meet current API requirements and won't cause restoration errors.
    """
    
    # Block type definitions and limits
    SUPPORTED_BLOCK_TYPES = {
        # Text blocks
        'paragraph', 'heading_1', 'heading_2', 'heading_3',
        'bulleted_list_item', 'numbered_list_item', 'to_do', 'toggle',
        'quote', 'callout',
        
        # Media blocks
        'image', 'video', 'file', 'pdf', 'bookmark', 'embed',
        
        # Database blocks
        'child_database', 'child_page',
        
        # Advanced blocks
        'code', 'equation', 'divider', 'breadcrumb',
        'table_of_contents', 'link_preview',
        
        # Layout blocks
        'column_list', 'column', 'table', 'table_row',
        
        # Notion-specific blocks
        'synced_block', 'template', 'link_to_page'
    }
    
    # Block types that can be backed up but have restoration limitations
    LIMITED_RESTORATION_BLOCK_TYPES = {
        'database_view',  # Can be backed up but views cannot be fully restored
        'unsupported'     # Placeholder for future block types
    }
    
    # Content length limits (based on Notion API constraints)
    MAX_TEXT_LENGTH = 2000
    MAX_CODE_LENGTH = 2000
    MAX_EQUATION_LENGTH = 1000
    MAX_URL_LENGTH = 2000
    MAX_RICH_TEXT_OBJECTS = 100
    MAX_BLOCK_DEPTH = 10
    
    # URL validation pattern
    URL_PATTERN = re.compile(
        r'^https?://'  # http:// or https://
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+[A-Z]{2,6}\.?|'  # domain...
        r'localhost|'  # localhost...
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # ...or ip
        r'(?::\d+)?'  # optional port
        r'(?:/?|[/?]\S+)$', re.IGNORECASE)
    
    def __init__(self, logger: Optional[logging.Logger] = None):
        """
        Initialize content block validator.
        
        Args:
            logger: Logger instance
        """
        self.logger = logger or logging.getLogger(__name__)
        self.stats = BlockValidationStats()
        
        # Validation configuration
        self.config = {
            'strict_validation': True,
            'remove_invalid_blocks': True,
            'truncate_oversized_content': True,
            'validate_urls': True,
            'clean_rich_text': True,
            'max_block_depth': self.MAX_BLOCK_DEPTH
        }
    
    def validate_and_sanitize_blocks(self, blocks: List[Dict[str, Any]], depth: int = 0) -> List[Dict[str, Any]]:
        """
        Validate and sanitize a list of content blocks.
        
        Args:
            blocks: List of block data
            depth: Current nesting depth (for recursion limit)
            
        Returns:
            List of validated and sanitized blocks
        """
        if depth > self.config['max_block_depth']:
            self.logger.warning(f"Maximum block depth ({self.config['max_block_depth']}) exceeded, truncating nested blocks")
            return []
        
        validated_blocks = []
        
        for block in blocks:
            try:
                validated_block = self._validate_single_block(block, depth)
                if validated_block:
                    validated_blocks.append(validated_block)
                    self.stats.blocks_sanitized += 1
                else:
                    self.stats.blocks_removed += 1
                
                self.stats.blocks_processed += 1
                
            except Exception as e:
                self.logger.error(f"Error validating block {block.get('id', 'unknown')}: {e}")
                self.stats.errors_found += 1
                
                if not self.config['remove_invalid_blocks']:
                    # Keep the block but log the error
                    validated_blocks.append(block)
        
        return validated_blocks
    
    def _validate_single_block(self, block: Dict[str, Any], depth: int) -> Optional[Dict[str, Any]]:
        """
        Validate and sanitize a single block.
        
        Args:
            block: Block data
            depth: Current nesting depth
            
        Returns:
            Validated block or None if invalid
        """
        if not isinstance(block, dict):
            self.logger.warning("Block is not a dictionary")
            return None
        
        # Check required fields
        if 'type' not in block:
            self.logger.warning("Block missing required 'type' field")
            return None
        
        block_type = block['type']
        
        # Check if block type is supported or has limited restoration
        if block_type not in self.SUPPORTED_BLOCK_TYPES:
            if block_type in self.LIMITED_RESTORATION_BLOCK_TYPES:
                self.logger.warning(f"Block type '{block_type}' can be backed up but has restoration limitations")
            else:
                self.logger.warning(f"Unsupported block type: {block_type}")
                if self.config['remove_invalid_blocks']:
                    return None
        
        # Create a copy for processing
        validated_block = block.copy()
        
        # Validate and sanitize based on block type
        try:
            if block_type in ['paragraph', 'heading_1', 'heading_2', 'heading_3', 
                             'bulleted_list_item', 'numbered_list_item', 'to_do', 
                             'toggle', 'quote', 'callout']:
                validated_block = self._validate_text_block(validated_block)
            elif block_type == 'code':
                validated_block = self._validate_code_block(validated_block)
            elif block_type in ['image', 'video', 'file', 'pdf']:
                validated_block = self._validate_media_block(validated_block)
            elif block_type == 'bookmark':
                validated_block = self._validate_bookmark_block(validated_block)
            elif block_type == 'embed':
                validated_block = self._validate_embed_block(validated_block)
            elif block_type == 'equation':
                validated_block = self._validate_equation_block(validated_block)
            elif block_type in ['table', 'table_row']:
                validated_block = self._validate_table_block(validated_block)
            elif block_type in ['column_list', 'column']:
                validated_block = self._validate_layout_block(validated_block)
            elif block_type in ['child_database', 'child_page']:
                validated_block = self._validate_database_block(validated_block)
            elif block_type == 'database_view':
                validated_block = self._validate_database_view_block(validated_block)
            elif block_type == 'synced_block':
                validated_block = self._validate_synced_block(validated_block)
            elif block_type in ['divider', 'breadcrumb', 'table_of_contents', 'link_preview']:
                validated_block = self._validate_simple_block(validated_block)
            
            # Validate common block properties
            validated_block = self._validate_common_properties(validated_block)
            
            # Process child blocks recursively
            if 'children' in validated_block and validated_block['children']:
                validated_block['children'] = self.validate_and_sanitize_blocks(
                    validated_block['children'], depth + 1
                )
            
            return validated_block
            
        except Exception as e:
            self.logger.error(f"Error processing {block_type} block: {e}")
            self.stats.errors_found += 1
            return None if self.config['remove_invalid_blocks'] else block
    
    def _validate_text_block(self, block: Dict[str, Any]) -> Dict[str, Any]:
        """Validate text-based blocks (paragraph, headings, lists, etc.)."""
        block_type = block['type']
        
        if block_type in block:
            block_content = block[block_type]
            
            # Validate rich_text array
            if 'rich_text' in block_content:
                block_content['rich_text'] = self._validate_rich_text(block_content['rich_text'])
            
            # Validate specific properties for certain block types
            if block_type == 'to_do' and 'checked' in block_content:
                # Ensure checked is boolean
                block_content['checked'] = bool(block_content['checked'])
            
            if block_type == 'callout':
                # Validate callout icon
                if 'icon' in block_content:
                    block_content['icon'] = self._validate_icon(block_content['icon'])
        
        return block
    
    def _validate_code_block(self, block: Dict[str, Any]) -> Dict[str, Any]:
        """Validate code blocks with size limits."""
        if 'code' in block:
            code_content = block['code']
            
            # Validate rich_text with code-specific limits
            if 'rich_text' in code_content:
                code_content['rich_text'] = self._validate_rich_text(
                    code_content['rich_text'], 
                    max_length=self.MAX_CODE_LENGTH
                )
            
            # Validate language (should be string or null)
            if 'language' in code_content:
                language = code_content['language']
                if language is not None and not isinstance(language, str):
                    code_content['language'] = str(language) if language else None
        
        return block
    
    def _validate_media_block(self, block: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Validate media blocks (image, video, file, pdf)."""
        block_type = block['type']
        
        if block_type in block:
            media_content = block[block_type]
            
            # Check that media has a valid source
            has_external = 'external' in media_content and media_content['external']
            has_file = 'file' in media_content and media_content['file']
            
            if not (has_external or has_file):
                self.logger.warning(f"Media block ({block_type}) missing valid source")
                return None
            
            # Validate external URL if present
            if has_external:
                external_url = media_content['external'].get('url')
                if not self._is_valid_url(external_url):
                    self.logger.warning(f"Invalid external URL in {block_type} block: {external_url}")
                    return None
            
            # Validate caption if present
            if 'caption' in media_content:
                media_content['caption'] = self._validate_rich_text(media_content['caption'])
        
        return block
    
    def _validate_bookmark_block(self, block: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Validate bookmark blocks."""
        if 'bookmark' in block:
            bookmark_content = block['bookmark']
            
            # URL is required for bookmarks
            url = bookmark_content.get('url')
            if not url or not self._is_valid_url(url):
                self.logger.warning(f"Invalid or missing URL in bookmark block: {url}")
                return None
            
            # Validate caption if present
            if 'caption' in bookmark_content:
                bookmark_content['caption'] = self._validate_rich_text(bookmark_content['caption'])
        
        return block
    
    def _validate_embed_block(self, block: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Validate embed blocks."""
        if 'embed' in block:
            embed_content = block['embed']
            
            # URL is required for embeds
            url = embed_content.get('url')
            if not url or not self._is_valid_url(url):
                self.logger.warning(f"Invalid or missing URL in embed block: {url}")
                return None
            
            # Validate caption if present
            if 'caption' in embed_content:
                embed_content['caption'] = self._validate_rich_text(embed_content['caption'])
        
        return block
    
    def _validate_equation_block(self, block: Dict[str, Any]) -> Dict[str, Any]:
        """Validate equation blocks."""
        if 'equation' in block:
            equation_content = block['equation']
            
            # Validate expression length
            if 'expression' in equation_content:
                expression = equation_content['expression']
                if len(expression) > self.MAX_EQUATION_LENGTH:
                    equation_content['expression'] = expression[:self.MAX_EQUATION_LENGTH]
                    self.logger.warning(f"Truncated equation expression from {len(expression)} to {self.MAX_EQUATION_LENGTH} characters")
                    self.stats.content_truncated += 1
        
        return block
    
    def _validate_table_block(self, block: Dict[str, Any]) -> Dict[str, Any]:
        """Validate table and table_row blocks."""
        block_type = block['type']
        
        if block_type == 'table':
            # Ensure table has required properties
            if 'table' in block:
                table_content = block['table']
                
                # Validate table width
                if 'table_width' in table_content:
                    width = table_content['table_width']
                    if not isinstance(width, int) or width <= 0:
                        table_content['table_width'] = 1
                
                # Validate has_column_header and has_row_header
                for prop in ['has_column_header', 'has_row_header']:
                    if prop in table_content:
                        table_content[prop] = bool(table_content[prop])
        
        elif block_type == 'table_row':
            # Validate table row cells
            if 'table_row' in block:
                table_row_content = block['table_row']
                
                if 'cells' in table_row_content:
                    validated_cells = []
                    for cell in table_row_content['cells']:
                        if isinstance(cell, list):
                            validated_cell = self._validate_rich_text(cell)
                            validated_cells.append(validated_cell)
                        else:
                            validated_cells.append([])  # Empty cell
                    table_row_content['cells'] = validated_cells
        
        return block
    
    def _validate_layout_block(self, block: Dict[str, Any]) -> Dict[str, Any]:
        """Validate layout blocks (column_list, column)."""
        # Layout blocks typically don't have special content to validate
        # but we ensure they have the correct structure
        return block
    
    def _validate_database_block(self, block: Dict[str, Any]) -> Dict[str, Any]:
        """Validate database blocks (child_database, child_page)."""
        block_type = block['type']
        
        if block_type in block:
            db_content = block[block_type]
            
            # Validate title if present
            if 'title' in db_content:
                db_content['title'] = str(db_content['title'])[:200]  # Limit title length
        
        return block
    
    def _validate_database_view_block(self, block: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate database view blocks.
        
        Note: Database views can be backed up but cannot be fully restored
        due to Notion API limitations. The view configuration will be preserved
        in the backup for reference but will need to be manually recreated.
        """
        if 'database_view' in block:
            view_content = block['database_view']
            
            # Log warning about restoration limitations
            self.logger.warning(
                "Database view block detected - can be backed up but view configuration "
                "cannot be restored due to Notion API limitations. Manual recreation required."
            )
            
            # Validate database_id reference if present
            if 'database_id' in view_content:
                database_id = view_content['database_id']
                if not self._is_valid_uuid(database_id):
                    self.logger.warning(f"Invalid database_id in database_view: {database_id}")
            
            # Preserve view configuration for reference
            # This includes filters, sorts, grouping, etc. that will need manual recreation
            
        return block
    
    def _validate_synced_block(self, block: Dict[str, Any]) -> Dict[str, Any]:
        """Validate synced blocks."""
        if 'synced_block' in block:
            synced_content = block['synced_block']
            
            # Validate synced_from reference if present
            if 'synced_from' in synced_content:
                synced_from = synced_content['synced_from']
                if synced_from and 'block_id' in synced_from:
                    # Ensure block_id is a valid UUID format
                    block_id = synced_from['block_id']
                    if not self._is_valid_uuid(block_id):
                        self.logger.warning(f"Invalid block_id in synced_block: {block_id}")
                        synced_content['synced_from'] = None
        
        return block
    
    def _validate_simple_block(self, block: Dict[str, Any]) -> Dict[str, Any]:
        """Validate simple blocks that don't have complex content."""
        # These blocks (divider, breadcrumb, etc.) typically don't need special validation
        return block
    
    def _validate_common_properties(self, block: Dict[str, Any]) -> Dict[str, Any]:
        """Validate properties common to all blocks."""
        # Validate block ID format
        if 'id' in block:
            block_id = block['id']
            if not self._is_valid_uuid(block_id):
                self.logger.warning(f"Invalid block ID format: {block_id}")
        
        # Validate timestamps
        for timestamp_field in ['created_time', 'last_edited_time']:
            if timestamp_field in block:
                timestamp = block[timestamp_field]
                if not self._is_valid_timestamp(timestamp):
                    self.logger.warning(f"Invalid timestamp in {timestamp_field}: {timestamp}")
        
        # Validate user references
        for user_field in ['created_by', 'last_edited_by']:
            if user_field in block:
                user_obj = block[user_field]
                if isinstance(user_obj, dict):
                    # Normalize user object (remove problematic fields)
                    normalized_user = {
                        'object': user_obj.get('object', 'user'),
                        'id': user_obj.get('id')
                    }
                    block[user_field] = normalized_user
        
        # Validate archived status
        if 'archived' in block:
            block['archived'] = bool(block['archived'])
        
        return block
    
    def _validate_rich_text(self, rich_text: List[Dict[str, Any]], max_length: int = None) -> List[Dict[str, Any]]:
        """
        Validate and sanitize rich text objects.
        
        Args:
            rich_text: List of rich text objects
            max_length: Maximum total character length (None for default)
            
        Returns:
            Validated rich text objects
        """
        if not isinstance(rich_text, list):
            return []
        
        if max_length is None:
            max_length = self.MAX_TEXT_LENGTH
        
        validated_rich_text = []
        total_length = 0
        
        for i, text_obj in enumerate(rich_text):
            if not isinstance(text_obj, dict):
                continue
            
            # Limit number of rich text objects
            if i >= self.MAX_RICH_TEXT_OBJECTS:
                self.logger.warning(f"Truncated rich text array at {self.MAX_RICH_TEXT_OBJECTS} objects")
                break
            
            validated_obj = text_obj.copy()
            
            # Validate text content
            if 'text' in validated_obj:
                text_content = validated_obj['text']
                if isinstance(text_content, dict) and 'content' in text_content:
                    content = text_content['content']
                    content_length = len(content)
                    
                    # Check total length limit
                    if total_length + content_length > max_length:
                        # Truncate this text object to fit within limit
                        remaining_length = max_length - total_length
                        if remaining_length > 0:
                            text_content['content'] = content[:remaining_length]
                            self.stats.content_truncated += 1
                        else:
                            # Skip this object entirely
                            continue
                    
                    total_length += len(text_content['content'])
                    
                    # Validate link URL if present
                    if 'link' in text_content and text_content['link']:
                        url = text_content['link'].get('url')
                        if url and not self._is_valid_url(url):
                            self.logger.warning(f"Invalid URL in rich text link: {url}")
                            text_content['link'] = None
            
            # Validate annotations
            if 'annotations' in validated_obj:
                annotations = validated_obj['annotations']
                if isinstance(annotations, dict):
                    # Ensure all annotation values are boolean
                    for key in ['bold', 'italic', 'strikethrough', 'underline', 'code']:
                        if key in annotations:
                            annotations[key] = bool(annotations[key])
                    
                    # Validate color
                    if 'color' in annotations:
                        color = annotations['color']
                        if not isinstance(color, str):
                            annotations['color'] = 'default'
            
            validated_rich_text.append(validated_obj)
            self.stats.rich_text_cleaned += 1
        
        return validated_rich_text
    
    def _validate_icon(self, icon: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Validate icon objects."""
        if not isinstance(icon, dict):
            return None
        
        icon_type = icon.get('type')
        if icon_type == 'emoji':
            # Validate emoji
            emoji = icon.get('emoji')
            if not emoji or not isinstance(emoji, str) or len(emoji) > 10:
                return None
        elif icon_type == 'external':
            # Validate external icon URL
            url = icon.get('external', {}).get('url')
            if not url or not self._is_valid_url(url):
                return None
        elif icon_type == 'file':
            # File icons should have URL
            url = icon.get('file', {}).get('url')
            if not url:
                return None
        else:
            return None
        
        return icon
    
    def _is_valid_url(self, url: str) -> bool:
        """Validate URL format."""
        if not url or not isinstance(url, str):
            return False
        
        if len(url) > self.MAX_URL_LENGTH:
            return False
        
        try:
            result = urlparse(url)
            return all([result.scheme, result.netloc])
        except Exception:
            return False
    
    def _is_valid_uuid(self, uuid_str: str) -> bool:
        """Validate UUID format (Notion block/page IDs)."""
        if not uuid_str or not isinstance(uuid_str, str):
            return False
        
        # Remove hyphens for validation
        uuid_clean = uuid_str.replace('-', '')
        
        # Should be 32 hex characters
        return len(uuid_clean) == 32 and all(c in '0123456789abcdefABCDEF' for c in uuid_clean)
    
    def _is_valid_timestamp(self, timestamp: str) -> bool:
        """Validate ISO timestamp format."""
        if not timestamp or not isinstance(timestamp, str):
            return False
        
        try:
            # Basic ISO format validation
            return 'T' in timestamp and ('Z' in timestamp or '+' in timestamp or timestamp.endswith('00'))
        except Exception:
            return False
    
    def get_validation_stats(self) -> Dict[str, Any]:
        """
        Get comprehensive validation statistics.
        
        Returns:
            Dictionary with validation statistics
        """
        return {
            'blocks_processed': self.stats.blocks_processed,
            'blocks_sanitized': self.stats.blocks_sanitized,
            'blocks_removed': self.stats.blocks_removed,
            'content_truncated': self.stats.content_truncated,
            'urls_validated': self.stats.urls_validated,
            'rich_text_cleaned': self.stats.rich_text_cleaned,
            'errors_found': self.stats.errors_found,
            'warnings_issued': self.stats.warnings_issued,
        }
    
    def reset_stats(self):
        """Reset validation statistics."""
        self.stats = BlockValidationStats()
    
    def update_config(self, config_updates: Dict[str, Any]):
        """Update validator configuration."""
        self.config.update(config_updates)
        self.logger.info(f"Updated validator configuration: {config_updates}")
