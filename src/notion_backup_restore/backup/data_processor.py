"""
Data processing and normalization for backup compatibility.

This module provides comprehensive data processing to ensure backup data
is compatible with current Notion API validation requirements, preventing
restoration errors caused by API version changes.
"""

import json
import re
from typing import Dict, List, Optional, Any, Set
import logging
from dataclasses import dataclass
from datetime import datetime

from ..utils.logger import setup_logger
from .content_block_validator import ContentBlockValidator


@dataclass
class ProcessingStats:
    """Statistics from data processing operations."""
    users_normalized: int = 0
    relations_fixed: int = 0
    relations_removed: int = 0
    blocks_sanitized: int = 0
    select_options_cleaned: int = 0
    properties_processed: int = 0
    pages_processed: int = 0
    errors_found: int = 0
    warnings_issued: int = 0


class DataProcessor:
    """
    Processes and normalizes backup data for compatibility.
    
    This class handles normalization of user objects, relation configurations,
    content blocks, and other data structures to prevent restoration errors.
    """
    
    def __init__(self, logger: Optional[logging.Logger] = None):
        """
        Initialize data processor.
        
        Args:
            logger: Logger instance
        """
        self.logger = logger or logging.getLogger(__name__)
        self.stats = ProcessingStats()
        
        # API version for compatibility tracking
        self.api_version = "2022-06-28"
        self.processing_version = "2.0"
        
        # Validation patterns
        self.valid_select_pattern = re.compile(r'^[a-zA-Z0-9\s\-_.,!?()]+$')
        self.max_code_length = 2000
        self.max_text_length = 2000
        
        # ID mapping for select options (old_id -> new_id)
        self.select_id_mapping: Dict[str, str] = {}
        
        # Track available databases and removed properties for limited backups
        self.available_databases: Optional[Set[str]] = None
        self.removed_properties: Set[str] = set()
        
        # Initialize enhanced content block validator
        self.block_validator = ContentBlockValidator(logger)
    
    def set_available_databases(self, database_ids: Set[str]):
        """
        Set available database IDs for limited backup processing.
        
        Args:
            database_ids: Set of available database IDs
        """
        self.available_databases = database_ids
        self.logger.info(f"Set available databases for limited backup: {len(database_ids)} databases")
    
    def process_database_schema(self, schema_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process database schema for compatibility.
        
        Args:
            schema_data: Raw database schema data
            
        Returns:
            Processed schema data
        """
        self.logger.info(f"Processing schema for database: {schema_data.get('name', 'Unknown')}")
        
        processed_schema = schema_data.copy()
        
        # Process properties
        if 'properties' in processed_schema:
            processed_properties = {}
            
            for prop_name, prop_config in processed_schema['properties'].items():
                processed_prop = self._process_property_schema(prop_name, prop_config)
                # Only add property if processing didn't return None (removed property)
                if processed_prop is not None:
                    processed_properties[prop_name] = processed_prop
                    self.stats.properties_processed += 1
                else:
                    self.logger.info(f"Removed property '{prop_name}' due to cross-database dependency")
                    self.removed_properties.add(prop_name)
            
            processed_schema['properties'] = processed_properties
        
        # Add processing metadata
        processed_schema['_processing'] = {
            'version': self.processing_version,
            'api_version': self.api_version,
            'processed_at': datetime.utcnow().isoformat(),
            'compatibility_layer': True
        }
        
        return processed_schema
    
    def _process_property_schema(self, prop_name: str, prop_config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process individual property schema.
        
        Args:
            prop_name: Property name
            prop_config: Property configuration
            
        Returns:
            Processed property configuration
        """
        processed_config = prop_config.copy()
        prop_type = processed_config.get('type')
        
        if prop_type == 'people':
            processed_config = self._process_people_property_schema(processed_config)
        elif prop_type == 'relation':
            processed_config = self._process_relation_property_schema(processed_config)
            # If relation processing returns None, it means the property should be removed
            if processed_config is None:
                return None
        elif prop_type in ['select', 'multi_select']:
            processed_config = self._process_select_property_schema(processed_config)
        
        return processed_config
    
    def _process_people_property_schema(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Process people property schema configuration."""
        # People properties don't need special schema processing
        # The main processing happens at the data level
        return config
    
    def _process_relation_property_schema(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Process relation property schema configuration."""
        processed_config = config.copy()
        
        # Check if this is a cross-database relation that should be removed in limited backups
        if hasattr(self, 'available_databases') and self.available_databases is not None and 'config' in processed_config:
            relation_config = processed_config['config']
            target_db_id = relation_config.get('database_id')
            
            if target_db_id and target_db_id not in self.available_databases:
                self.logger.warning(f"Removing cross-database relation to unavailable database: {target_db_id}")
                self.stats.relations_removed += 1
                return None  # Signal to remove this property
        
        # Ensure relation has proper configuration
        if 'config' in processed_config:
            relation_config = processed_config['config']
            
            # Check if this is a dual_property type
            if relation_config.get('type') == 'dual_property':
                # For dual_property, ensure dual_property config exists and remove single_property
                if 'single_property' in relation_config:
                    del relation_config['single_property']
                    self.stats.relations_fixed += 1
                    self.logger.debug(f"Removed single_property from dual_property relation")
                
                if 'dual_property' not in relation_config:
                    relation_config['dual_property'] = {}
                    self.stats.relations_fixed += 1
                    self.logger.debug(f"Added dual_property configuration to relation")
            else:
                # For single relations, ensure single_property exists and remove dual_property
                if 'dual_property' in relation_config:
                    del relation_config['dual_property']
                    self.stats.relations_fixed += 1
                    self.logger.debug(f"Removed dual_property from single relation")
                
                if 'single_property' not in relation_config:
                    relation_config['single_property'] = {}
                    self.stats.relations_fixed += 1
                    self.logger.debug(f"Added single_property configuration to relation")
        
        return processed_config
    
    def _process_select_property_schema(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Process select/multi-select property schema configuration."""
        processed_config = config.copy()
        
        # Clean select options in schema
        if 'config' in processed_config and 'options' in processed_config['config']:
            options = processed_config['config']['options']
            cleaned_options = []
            
            for i, option in enumerate(options):
                if isinstance(option, dict) and 'name' in option:
                    option_name = option['name'].strip()
                    option_id = option.get('id', '')
                    
                    # Validate option name and ID
                    name_valid = (option_name and 
                                len(option_name) <= 100 and 
                                self.valid_select_pattern.match(option_name))
                    
                    id_valid = (isinstance(option_id, str) and 
                              len(option_id) <= 50 and
                              self.valid_select_pattern.match(option_id))
                    
                    if name_valid and id_valid:
                        cleaned_options.append(option)
                    else:
                        # Create a new option with clean ID if name is valid
                        if name_valid:
                            clean_option = option.copy()
                            new_id = f"opt_{i}_{hash(option_name) % 10000}"
                            clean_option['id'] = new_id
                            # Track the ID mapping for data processing
                            self.select_id_mapping[option_id] = new_id
                            cleaned_options.append(clean_option)
                            self.logger.warning(f"Fixed corrupted select option ID: {option_name} (was: {option_id})")
                            self.stats.select_options_cleaned += 1
                        else:
                            self.logger.warning(f"Removed invalid select option: {option_name} (ID: {option_id})")
                            self.stats.select_options_cleaned += 1
            
            processed_config['config']['options'] = cleaned_options
        
        return processed_config
    
    def process_database_content(self, content_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process database content for compatibility.
        
        Args:
            content_data: Raw database content data
            
        Returns:
            Processed content data
        """
        self.logger.info(f"Processing content for database: {content_data.get('database_name', 'Unknown')}")
        
        processed_content = content_data.copy()
        
        # Process pages
        if 'pages' in processed_content:
            processed_pages = []
            
            for page_data in processed_content['pages']:
                processed_page = self._process_page_data(page_data)
                processed_pages.append(processed_page)
                self.stats.pages_processed += 1
            
            processed_content['pages'] = processed_pages
        
            # Add processing metadata including block validation stats
            block_validation_stats = self.block_validator.get_validation_stats()
            processed_content['_processing'] = {
                'version': self.processing_version,
                'api_version': self.api_version,
                'processed_at': datetime.utcnow().isoformat(),
                'compatibility_layer': True,
                'stats': {
                    'pages_processed': len(processed_content.get('pages', [])),
                    'users_normalized': self.stats.users_normalized,
                    'blocks_sanitized': self.stats.blocks_sanitized,
                    'block_validation': block_validation_stats
                }
            }
        
        return processed_content
    
    def _process_page_data(self, page_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process individual page data.
        
        Args:
            page_data: Raw page data
            
        Returns:
            Processed page data
        """
        processed_page = page_data.copy()
        
        # Process properties
        if 'properties' in processed_page:
            processed_properties = {}
            
            for prop_name, prop_value in processed_page['properties'].items():
                # Skip properties that were removed due to cross-database dependencies
                if prop_name in self.removed_properties:
                    self.logger.debug(f"Skipping removed property '{prop_name}' in page data")
                    continue
                    
                processed_prop = self._process_property_value(prop_name, prop_value)
                processed_properties[prop_name] = processed_prop
            
            processed_page['properties'] = processed_properties
        
        # Process user references in metadata
        for user_field in ['created_by', 'last_edited_by']:
            if user_field in processed_page:
                processed_page[user_field] = self._normalize_user_object(processed_page[user_field])
        
        # Process blocks if present using enhanced validator
        if 'blocks' in processed_page and processed_page['blocks']:
            processed_page['blocks'] = self.block_validator.validate_and_sanitize_blocks(processed_page['blocks'])
            
            # Update stats from block validator
            block_stats = self.block_validator.get_validation_stats()
            self.stats.blocks_sanitized += block_stats['blocks_sanitized']
        
        return processed_page
    
    def _process_property_value(self, prop_name: str, prop_value: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process individual property value.
        
        Args:
            prop_name: Property name
            prop_value: Property value
            
        Returns:
            Processed property value
        """
        if not isinstance(prop_value, dict):
            return prop_value
        
        processed_value = prop_value.copy()
        prop_type = processed_value.get('type')
        
        if prop_type == 'people':
            processed_value = self._process_people_property_value(processed_value)
        elif prop_type == 'relation':
            processed_value = self._process_relation_property_value(processed_value)
        elif prop_type in ['select', 'multi_select']:
            processed_value = self._process_select_property_value(processed_value)
        
        return processed_value
    
    def _process_people_property_value(self, prop_value: Dict[str, Any]) -> Dict[str, Any]:
        """Process people property value."""
        processed_value = prop_value.copy()
        
        if 'people' in processed_value:
            normalized_people = []
            
            for person in processed_value['people']:
                normalized_person = self._normalize_user_object(person)
                normalized_people.append(normalized_person)
            
            processed_value['people'] = normalized_people
        
        return processed_value
    
    def _normalize_user_object(self, user_obj: Dict[str, Any]) -> Dict[str, Any]:
        """
        Normalize user object to current API requirements.
        
        Args:
            user_obj: Raw user object
            
        Returns:
            Normalized user object
        """
        if not isinstance(user_obj, dict):
            return user_obj
        
        # Only keep essential fields that are accepted by current API
        normalized_user = {
            'object': user_obj.get('object', 'user'),
            'id': user_obj.get('id')
        }
        
        # Remove problematic fields that cause validation errors
        removed_fields = []
        for field in ['name', 'avatar_url', 'type', 'person']:
            if field in user_obj:
                removed_fields.append(field)
        
        if removed_fields:
            self.logger.debug(f"Normalized user {user_obj.get('id', 'unknown')}: removed {removed_fields}")
            self.stats.users_normalized += 1
        
        return normalized_user
    
    def _process_relation_property_value(self, prop_value: Dict[str, Any]) -> Dict[str, Any]:
        """Process relation property value."""
        # Relation values typically don't need processing at the data level
        # The main processing happens at the schema level
        return prop_value
    
    def _process_select_property_value(self, prop_value: Dict[str, Any]) -> Dict[str, Any]:
        """Process select property value."""
        processed_value = prop_value.copy()
        
        # Clean select values
        if 'select' in processed_value and processed_value['select']:
            select_obj = processed_value['select']
            if isinstance(select_obj, dict) and 'name' in select_obj:
                name = select_obj['name'].strip()
                option_id = select_obj.get('id', '')
                
                name_valid = name and self.valid_select_pattern.match(name)
                id_valid = isinstance(option_id, str) and self.valid_select_pattern.match(option_id)
                
                if not name_valid:
                    self.logger.warning(f"Removed invalid select value: {name}")
                    processed_value['select'] = None
                    self.stats.select_options_cleaned += 1
                elif not id_valid:
                    # Use mapped ID if available, otherwise generate new one
                    clean_select = select_obj.copy()
                    if option_id in self.select_id_mapping:
                        clean_select['id'] = self.select_id_mapping[option_id]
                    else:
                        clean_select['id'] = f"val_{hash(name) % 10000}"
                    processed_value['select'] = clean_select
                    self.logger.warning(f"Fixed corrupted select value ID: {name} (was: {option_id})")
                    self.stats.select_options_cleaned += 1
        
        # Clean multi-select values
        if 'multi_select' in processed_value:
            cleaned_options = []
            for option in processed_value['multi_select']:
                if isinstance(option, dict) and 'name' in option:
                    name = option['name'].strip()
                    option_id = option.get('id', '')
                    
                    name_valid = name and self.valid_select_pattern.match(name)
                    id_valid = isinstance(option_id, str) and self.valid_select_pattern.match(option_id)
                    
                    if name_valid and id_valid:
                        cleaned_options.append(option)
                    elif name_valid:
                        # Use mapped ID if available, otherwise generate new one
                        clean_option = option.copy()
                        if option_id in self.select_id_mapping:
                            clean_option['id'] = self.select_id_mapping[option_id]
                        else:
                            clean_option['id'] = f"val_{hash(name) % 10000}"
                        cleaned_options.append(clean_option)
                        self.logger.warning(f"Fixed corrupted multi-select value ID: {name} (was: {option_id})")
                        self.stats.select_options_cleaned += 1
                    else:
                        self.logger.warning(f"Removed invalid multi-select value: {name}")
                        self.stats.select_options_cleaned += 1
            processed_value['multi_select'] = cleaned_options
        
        return processed_value
    
    def _process_content_blocks(self, blocks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Process and sanitize content blocks.
        
        Args:
            blocks: List of block data
            
        Returns:
            List of processed blocks
        """
        processed_blocks = []
        
        for block in blocks:
            try:
                processed_block = self._process_single_block(block)
                if processed_block:  # Only add if block is valid
                    processed_blocks.append(processed_block)
            except Exception as e:
                self.logger.warning(f"Skipped invalid block {block.get('id', 'unknown')}: {e}")
                self.stats.errors_found += 1
        
        return processed_blocks
    
    def _process_single_block(self, block: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Process a single content block.
        
        Args:
            block: Block data
            
        Returns:
            Processed block or None if invalid
        """
        if not isinstance(block, dict):
            return None
        
        processed_block = block.copy()
        block_type = processed_block.get('type')
        
        if not block_type:
            return None
        
        # Process specific block types with custom logic
        if block_type == 'code':
            processed_block = self._process_code_block(processed_block)
        elif block_type == 'image':
            processed_block = self._process_image_block(processed_block)
        elif block_type in ['table', 'table_row']:
            processed_block = self._process_table_block(processed_block)
        elif block_type in ['paragraph', 'heading_1', 'heading_2', 'heading_3', 'bulleted_list_item', 'numbered_list_item']:
            processed_block = self._process_text_block(processed_block)
        
        # Always validate through ContentBlockValidator for comprehensive sanitization
        validated_blocks = self.block_validator.validate_and_sanitize_blocks([processed_block])
        if not validated_blocks:
            return None
        processed_block = validated_blocks[0]
        
        # Process child blocks recursively
        if 'children' in processed_block and processed_block['children']:
            processed_block['children'] = self._process_content_blocks(processed_block['children'])
        
        # Clean user references in block metadata
        for user_field in ['created_by', 'last_edited_by']:
            if user_field in processed_block:
                processed_block[user_field] = self._normalize_user_object(processed_block[user_field])
        
        self.stats.blocks_sanitized += 1
        return processed_block
    
    def _process_code_block(self, block: Dict[str, Any]) -> Dict[str, Any]:
        """Process code block to handle size limits."""
        processed_block = block.copy()
        
        if 'code' in processed_block and 'rich_text' in processed_block['code']:
            rich_text = processed_block['code']['rich_text']
            
            for text_obj in rich_text:
                if isinstance(text_obj, dict) and 'text' in text_obj:
                    text_content = text_obj['text']
                    if isinstance(text_content, dict) and 'content' in text_content:
                        content = text_content['content']
                        if len(content) > self.max_code_length:
                            # Truncate content
                            text_content['content'] = content[:self.max_code_length]
                            self.logger.warning(f"Truncated code block content from {len(content)} to {self.max_code_length} characters")
        
        return processed_block
    
    def _process_image_block(self, block: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Process image block to ensure required fields."""
        processed_block = block.copy()
        
        if 'image' in processed_block:
            image_config = processed_block['image']
            
            # Ensure image has either external or file reference
            if ('external' not in image_config and 
                'file' not in image_config and 
                'file_upload' not in image_config):
                self.logger.warning(f"Skipped image block without valid source")
                return None
        
        return processed_block
    
    def _process_table_block(self, block: Dict[str, Any]) -> Dict[str, Any]:
        """Process table block to ensure required properties."""
        processed_block = block.copy()
        
        # Ensure table blocks have required children property
        if block.get('type') == 'table' and 'children' not in processed_block:
            processed_block['children'] = []
        
        return processed_block
    
    def _process_text_block(self, block: Dict[str, Any]) -> Dict[str, Any]:
        """Process text-based blocks to handle size limits."""
        processed_block = block.copy()
        block_type = processed_block.get('type')
        
        if block_type in processed_block:
            block_content = processed_block[block_type]
            
            if 'rich_text' in block_content:
                rich_text = block_content['rich_text']
                
                for text_obj in rich_text:
                    if isinstance(text_obj, dict) and 'text' in text_obj:
                        text_content = text_obj['text']
                        if isinstance(text_content, dict) and 'content' in text_content:
                            content = text_content['content']
                            if len(content) > self.max_text_length:
                                # Truncate content
                                text_content['content'] = content[:self.max_text_length]
                                self.logger.warning(f"Truncated {block_type} content from {len(content)} to {self.max_text_length} characters")
        
        return processed_block
    
    def validate_processed_data(self, data: Dict[str, Any]) -> List[str]:
        """
        Validate processed data for common issues.
        
        Args:
            data: Processed data to validate
            
        Returns:
            List of validation warnings/errors
        """
        issues = []
        
        # Check for processing metadata
        if '_processing' not in data:
            issues.append("Data lacks processing metadata - may not be properly normalized")
        
        # Validate based on data type
        if 'pages' in data:  # Content data
            issues.extend(self._validate_content_data(data))
        elif 'properties' in data:  # Schema data
            issues.extend(self._validate_schema_data(data))
        
        return issues
    
    def _validate_content_data(self, content_data: Dict[str, Any]) -> List[str]:
        """Validate processed content data."""
        issues = []
        
        for page in content_data.get('pages', []):
            # Check for user objects in properties
            for prop_name, prop_value in page.get('properties', {}).items():
                if isinstance(prop_value, dict) and prop_value.get('type') == 'people':
                    for person in prop_value.get('people', []):
                        if isinstance(person, dict):
                            # Check for problematic fields
                            problematic_fields = ['name', 'avatar_url', 'type', 'person']
                            found_fields = [f for f in problematic_fields if f in person]
                            if found_fields:
                                issues.append(f"User object in {prop_name} still contains problematic fields: {found_fields}")
        
        return issues
    
    def _validate_schema_data(self, schema_data: Dict[str, Any]) -> List[str]:
        """Validate processed schema data."""
        issues = []
        
        for prop_name, prop_config in schema_data.get('properties', {}).items():
            if isinstance(prop_config, dict):
                prop_type = prop_config.get('type')
                
                # Check relation configurations
                if prop_type == 'relation' and 'config' in prop_config:
                    config = prop_config['config']
                    if ('single_property' not in config and 
                        'dual_property' not in config):
                        issues.append(f"Relation property {prop_name} missing single_property/dual_property configuration")
        
        return issues
    
    def get_processing_stats(self) -> Dict[str, Any]:
        """
        Get comprehensive processing statistics.
        
        Returns:
            Dictionary with processing statistics
        """
        return {
            'processing_version': self.processing_version,
            'api_version': self.api_version,
            'users_normalized': self.stats.users_normalized,
            'relations_fixed': self.stats.relations_fixed,
            'blocks_sanitized': self.stats.blocks_sanitized,
            'select_options_cleaned': self.stats.select_options_cleaned,
            'properties_processed': self.stats.properties_processed,
            'pages_processed': self.stats.pages_processed,
            'errors_found': self.stats.errors_found,
            'warnings_issued': self.stats.warnings_issued,
        }
    
    def reset_stats(self):
        """Reset processing statistics."""
        self.stats = ProcessingStats()
