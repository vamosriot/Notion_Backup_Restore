"""
ID mapping system for tracking original to new ID mappings during restoration.

This module provides functionality to track database and page ID mappings
which is critical for updating relation properties during the restoration process.
"""

import json
from pathlib import Path
from typing import Dict, Optional, Any, Set
from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class IDMapping:
    """Represents a single ID mapping."""
    original_id: str
    new_id: str
    object_type: str  # 'database', 'page', 'property'
    name: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.utcnow)
    metadata: Dict[str, Any] = field(default_factory=dict)


class IDMapper:
    """
    Manages ID mappings between original and new Notion objects.
    
    This class tracks mappings for databases, pages, and properties
    to enable proper relationship restoration.
    """
    
    def __init__(self, mapping_file: Optional[Path] = None):
        """
        Initialize ID mapper.
        
        Args:
            mapping_file: Path to save/load mappings (optional)
        """
        self.mapping_file = mapping_file
        self._mappings: Dict[str, IDMapping] = {}
        self._reverse_mappings: Dict[str, str] = {}  # new_id -> original_id
        
        # Load existing mappings if file exists
        if mapping_file and mapping_file.exists():
            self.load_mappings()
    
    def add_mapping(self, original_id: str, new_id: str, object_type: str,
                   name: Optional[str] = None, **metadata) -> None:
        """
        Add a new ID mapping.
        
        Args:
            original_id: Original Notion object ID
            new_id: New Notion object ID
            object_type: Type of object ('database', 'page', 'property')
            name: Human-readable name (optional)
            **metadata: Additional metadata
        """
        if original_id in self._mappings:
            existing = self._mappings[original_id]
            if existing.new_id != new_id:
                raise ValueError(
                    f"ID {original_id} already mapped to {existing.new_id}, "
                    f"cannot remap to {new_id}"
                )
            return
        
        mapping = IDMapping(
            original_id=original_id,
            new_id=new_id,
            object_type=object_type,
            name=name,
            metadata=metadata
        )
        
        self._mappings[original_id] = mapping
        self._reverse_mappings[new_id] = original_id
    
    def get_new_id(self, original_id: str) -> Optional[str]:
        """
        Get new ID for an original ID.
        
        Args:
            original_id: Original Notion object ID
            
        Returns:
            New ID if mapping exists, None otherwise
        """
        mapping = self._mappings.get(original_id)
        return mapping.new_id if mapping else None
    
    def get_original_id(self, new_id: str) -> Optional[str]:
        """
        Get original ID for a new ID.
        
        Args:
            new_id: New Notion object ID
            
        Returns:
            Original ID if mapping exists, None otherwise
        """
        return self._reverse_mappings.get(new_id)
    
    def get_mapping(self, original_id: str) -> Optional[IDMapping]:
        """
        Get complete mapping information.
        
        Args:
            original_id: Original Notion object ID
            
        Returns:
            IDMapping object if exists, None otherwise
        """
        return self._mappings.get(original_id)
    
    def has_mapping(self, original_id: str) -> bool:
        """
        Check if mapping exists for an original ID.
        
        Args:
            original_id: Original Notion object ID
            
        Returns:
            True if mapping exists, False otherwise
        """
        return original_id in self._mappings
    
    def get_mappings_by_type(self, object_type: str) -> Dict[str, IDMapping]:
        """
        Get all mappings of a specific type.
        
        Args:
            object_type: Type of object ('database', 'page', 'property')
            
        Returns:
            Dictionary of original_id -> IDMapping for the specified type
        """
        return {
            original_id: mapping
            for original_id, mapping in self._mappings.items()
            if mapping.object_type == object_type
        }
    
    def get_database_mappings(self) -> Dict[str, str]:
        """
        Get database ID mappings.
        
        Returns:
            Dictionary of original_database_id -> new_database_id
        """
        return {
            original_id: mapping.new_id
            for original_id, mapping in self._mappings.items()
            if mapping.object_type == "database"
        }
    
    def get_page_mappings(self) -> Dict[str, str]:
        """
        Get page ID mappings.
        
        Returns:
            Dictionary of original_page_id -> new_page_id
        """
        return {
            original_id: mapping.new_id
            for original_id, mapping in self._mappings.items()
            if mapping.object_type == "page"
        }
    
    def update_relation_ids(self, relation_data: Any) -> Any:
        """
        Update relation IDs in data structure.
        
        This recursively traverses data structures and updates any
        Notion IDs found in relation properties.
        
        Args:
            relation_data: Data structure containing relation IDs
            
        Returns:
            Updated data structure with new IDs
        """
        if isinstance(relation_data, dict):
            if "id" in relation_data:
                # This looks like a Notion object reference
                original_id = relation_data["id"]
                new_id = self.get_new_id(original_id)
                if new_id:
                    relation_data = relation_data.copy()
                    relation_data["id"] = new_id
            
            # Recursively update nested dictionaries
            return {
                key: self.update_relation_ids(value)
                for key, value in relation_data.items()
            }
        
        elif isinstance(relation_data, list):
            # Recursively update list items
            return [self.update_relation_ids(item) for item in relation_data]
        
        else:
            # Primitive value, return as-is
            return relation_data
    
    def update_property_relations(self, properties: Dict[str, Any]) -> Dict[str, Any]:
        """
        Update relation IDs in property values.
        
        Args:
            properties: Dictionary of property values
            
        Returns:
            Updated properties with new relation IDs
        """
        updated_properties = {}
        
        for prop_name, prop_value in properties.items():
            if isinstance(prop_value, dict):
                prop_type = prop_value.get("type")
                
                if prop_type == "relation" and "relation" in prop_value:
                    # Update relation property
                    relations = prop_value["relation"]
                    updated_relations = []
                    
                    for relation in relations:
                        if isinstance(relation, dict) and "id" in relation:
                            original_id = relation["id"]
                            new_id = self.get_new_id(original_id)
                            if new_id:
                                updated_relations.append({"id": new_id})
                            else:
                                # Keep original if no mapping found
                                updated_relations.append(relation)
                        else:
                            updated_relations.append(relation)
                    
                    updated_properties[prop_name] = {
                        **prop_value,
                        "relation": updated_relations
                    }
                else:
                    # Non-relation property, keep as-is
                    updated_properties[prop_name] = prop_value
            else:
                # Non-dict property value, keep as-is
                updated_properties[prop_name] = prop_value
        
        return updated_properties
    
    def get_unmapped_ids(self, ids: Set[str]) -> Set[str]:
        """
        Get IDs that don't have mappings.
        
        Args:
            ids: Set of original IDs to check
            
        Returns:
            Set of IDs without mappings
        """
        return {id for id in ids if not self.has_mapping(id)}
    
    def save_mappings(self, file_path: Optional[Path] = None) -> None:
        """
        Save mappings to JSON file.
        
        Args:
            file_path: Path to save file (uses instance file_path if None)
        """
        save_path = file_path or self.mapping_file
        if not save_path:
            raise ValueError("No file path specified for saving mappings")
        
        # Ensure directory exists
        save_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Convert mappings to serializable format
        serializable_mappings = {}
        for original_id, mapping in self._mappings.items():
            serializable_mappings[original_id] = {
                "new_id": mapping.new_id,
                "object_type": mapping.object_type,
                "name": mapping.name,
                "created_at": mapping.created_at.isoformat(),
                "metadata": mapping.metadata
            }
        
        # Save to file
        with open(save_path, 'w', encoding='utf-8') as f:
            json.dump({
                "version": "1.0",
                "created_at": datetime.utcnow().isoformat(),
                "mappings": serializable_mappings
            }, f, indent=2, ensure_ascii=False)
    
    def load_mappings(self, file_path: Optional[Path] = None) -> None:
        """
        Load mappings from JSON file.
        
        Args:
            file_path: Path to load file (uses instance file_path if None)
        """
        load_path = file_path or self.mapping_file
        if not load_path or not load_path.exists():
            return
        
        with open(load_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        mappings_data = data.get("mappings", {})
        
        for original_id, mapping_data in mappings_data.items():
            created_at = datetime.fromisoformat(mapping_data.get("created_at", datetime.utcnow().isoformat()))
            
            mapping = IDMapping(
                original_id=original_id,
                new_id=mapping_data["new_id"],
                object_type=mapping_data["object_type"],
                name=mapping_data.get("name"),
                created_at=created_at,
                metadata=mapping_data.get("metadata", {})
            )
            
            self._mappings[original_id] = mapping
            self._reverse_mappings[mapping.new_id] = original_id
    
    def clear(self) -> None:
        """Clear all mappings."""
        self._mappings.clear()
        self._reverse_mappings.clear()
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get mapping statistics.
        
        Returns:
            Dictionary with mapping statistics
        """
        type_counts = {}
        for mapping in self._mappings.values():
            type_counts[mapping.object_type] = type_counts.get(mapping.object_type, 0) + 1
        
        return {
            "total_mappings": len(self._mappings),
            "type_counts": type_counts,
            "mapping_file": str(self.mapping_file) if self.mapping_file else None,
        }
    
    def __len__(self) -> int:
        """Return number of mappings."""
        return len(self._mappings)
    
    def __contains__(self, original_id: str) -> bool:
        """Check if original ID has a mapping."""
        return original_id in self._mappings
    
    def __repr__(self) -> str:
        """String representation of ID mapper."""
        stats = self.get_stats()
        return f"IDMapper(mappings={stats['total_mappings']}, types={stats['type_counts']})"
