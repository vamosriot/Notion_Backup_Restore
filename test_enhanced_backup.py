#!/usr/bin/env python3
"""
Test script for the enhanced backup system.

This script demonstrates the new backup processing capabilities
and validates that the system works correctly.
"""

import sys
from pathlib import Path
from datetime import datetime

# Add src to Python path to allow imports
src_path = Path(__file__).parent / "src"
sys.path.insert(0, str(src_path))

from src.notion_backup_restore.backup.data_processor import DataProcessor
from src.notion_backup_restore.backup.backup_processor import BackupProcessor
from src.notion_backup_restore.utils.logger import setup_logger


def test_user_normalization():
    """Test user object normalization."""
    print("🧪 Testing user normalization...")
    
    logger = setup_logger("test", verbose=False)
    processor = DataProcessor(logger)
    
    # Test user object with problematic fields
    test_user = {
        "object": "user",
        "id": "test-user-id",
        "name": "Test User",
        "avatar_url": "https://example.com/avatar.jpg",
        "type": "person",
        "person": {"email": "test@example.com"}
    }
    
    normalized_user = processor._normalize_user_object(test_user)
    
    # Check that problematic fields are removed
    assert "name" not in normalized_user
    assert "avatar_url" not in normalized_user
    assert "type" not in normalized_user
    assert "person" not in normalized_user
    
    # Check that essential fields remain
    assert normalized_user["object"] == "user"
    assert normalized_user["id"] == "test-user-id"
    
    print("✅ User normalization test passed!")


def test_people_property_processing():
    """Test people property processing."""
    print("🧪 Testing people property processing...")
    
    logger = setup_logger("test", verbose=False)
    processor = DataProcessor(logger)
    
    # Test people property with problematic user objects
    test_property = {
        "type": "people",
        "people": [
            {
                "object": "user",
                "id": "user-1",
                "name": "User One",
                "avatar_url": "https://example.com/avatar1.jpg"
            },
            {
                "object": "user", 
                "id": "user-2",
                "name": "User Two",
                "type": "person"
            }
        ]
    }
    
    processed_property = processor._process_people_property_value(test_property)
    
    # Check that users are normalized
    for person in processed_property["people"]:
        assert "name" not in person
        assert "avatar_url" not in person
        assert "type" not in person
        assert person["object"] == "user"
        assert "id" in person
    
    print("✅ People property processing test passed!")


def test_relation_schema_processing():
    """Test relation schema processing."""
    print("🧪 Testing relation schema processing...")
    
    logger = setup_logger("test", verbose=False)
    processor = DataProcessor(logger)
    
    # Test relation property without single_property/dual_property
    test_config = {
        "type": "relation",
        "config": {
            "type": "relation",
            "database_id": "test-db-id"
        }
    }
    
    processed_config = processor._process_relation_property_schema(test_config)
    
    # Check that single_property was added
    assert "single_property" in processed_config["config"]
    assert processed_config["config"]["single_property"] == {}
    
    print("✅ Relation schema processing test passed!")


def test_select_option_validation():
    """Test select option validation."""
    print("🧪 Testing select option validation...")
    
    logger = setup_logger("test", verbose=False)
    processor = DataProcessor(logger)
    
    # Test select property with invalid options
    test_config = {
        "type": "select",
        "config": {
            "options": [
                {"name": "Valid Option", "id": "1", "color": "blue"},
                {"name": "]v{A", "id": "2", "color": "red"},  # Invalid characters
                {"name": "", "id": "3", "color": "green"},  # Empty name
                {"name": "Another Valid", "id": "4", "color": "yellow"}
            ]
        }
    }
    
    processed_config = processor._process_select_property_schema(test_config)
    
    # Check that only valid options remain
    valid_options = processed_config["config"]["options"]
    print(f"Valid options found: {[opt['name'] for opt in valid_options]}")
    
    # Should have filtered out invalid options
    valid_names = [opt["name"] for opt in valid_options]
    assert "Valid Option" in valid_names
    assert "Another Valid" in valid_names
    assert "]v{A" not in valid_names  # Should be filtered out
    assert "" not in valid_names  # Should be filtered out
    
    print("✅ Select option validation test passed!")


def test_code_block_truncation():
    """Test code block content truncation."""
    print("🧪 Testing code block truncation...")
    
    logger = setup_logger("test", verbose=False)
    processor = DataProcessor(logger)
    
    # Create a code block with oversized content
    long_content = "x" * 3000  # Exceeds 2000 character limit
    test_block = {
        "type": "code",
        "code": {
            "rich_text": [
                {
                    "text": {
                        "content": long_content
                    }
                }
            ]
        }
    }
    
    processed_block = processor._process_code_block(test_block)
    
    # Check that content was truncated
    processed_content = processed_block["code"]["rich_text"][0]["text"]["content"]
    assert len(processed_content) == 2000
    assert processed_content == "x" * 2000
    
    print("✅ Code block truncation test passed!")


def test_complete_processing_workflow():
    """Test complete processing workflow."""
    print("🧪 Testing complete processing workflow...")
    
    logger = setup_logger("test", verbose=False)
    processor = BackupProcessor(logger)
    
    # Create mock schema and content data
    mock_schema = {
        "id": "test-db-id",
        "name": "Test Database",
        "properties": {
            "Assignee": {
                "name": "Assignee",
                "type": "people",
                "config": {"type": "people"}
            },
            "Related": {
                "name": "Related",
                "type": "relation",
                "config": {
                    "type": "relation",
                    "database_id": "other-db-id"
                }
            }
        }
    }
    
    mock_content = {
        "database_id": "test-db-id",
        "database_name": "Test Database",
        "total_pages": 1,
        "pages": [
            {
                "id": "page-1",
                "properties": {
                    "Assignee": {
                        "type": "people",
                        "people": [
                            {
                                "object": "user",
                                "id": "user-1",
                                "name": "Test User",
                                "avatar_url": "https://example.com/avatar.jpg"
                            }
                        ]
                    }
                },
                "created_by": {
                    "object": "user",
                    "id": "creator-1",
                    "name": "Creator"
                }
            }
        ]
    }
    
    # Process the data directly with DataProcessor since we have dict objects
    data_processor = processor.data_processor
    
    # Process schema and content directly
    processed_schema = data_processor.process_database_schema(mock_schema)
    processed_content = data_processor.process_database_content(mock_content)
    
    processed_schemas = {"Test": processed_schema}
    processed_contents = {"Test": processed_content}
    
    # Validate processing results
    processed_schema = processed_schemas["Test"]
    processed_content = processed_contents["Test"]
    
    # Check processing metadata was added
    assert "_processing" in processed_schema
    assert "_processing" in processed_content
    
    # Check relation was fixed
    relation_config = processed_schema["properties"]["Related"]["config"]
    assert "single_property" in relation_config
    
    # Check user was normalized
    user_in_content = processed_content["pages"][0]["properties"]["Assignee"]["people"][0]
    assert "name" not in user_in_content
    assert "avatar_url" not in user_in_content
    assert user_in_content["id"] == "user-1"
    
    # Check created_by was normalized
    created_by = processed_content["pages"][0]["created_by"]
    assert "name" not in created_by
    assert created_by["id"] == "creator-1"
    
    print("✅ Complete processing workflow test passed!")


def run_all_tests():
    """Run all tests."""
    print("🚀 Starting enhanced backup system tests...\n")
    
    try:
        test_user_normalization()
        test_people_property_processing()
        test_relation_schema_processing()
        test_select_option_validation()
        test_code_block_truncation()
        test_complete_processing_workflow()
        
        print(f"\n🎉 All tests passed! Enhanced backup system is working correctly.")
        print(f"📅 Test completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        return True
        
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Main test function."""
    success = run_all_tests()
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
