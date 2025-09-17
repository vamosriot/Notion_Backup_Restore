#!/usr/bin/env python3
"""
Test script for enhanced content block validation and sanitization.

This script tests all the content block validation features to ensure
they work correctly and handle edge cases properly.
"""

import sys
from pathlib import Path
from datetime import datetime

# Add src to Python path to allow imports
src_path = Path(__file__).parent / "src"
sys.path.insert(0, str(src_path))

from src.notion_backup_restore.backup.content_block_validator import ContentBlockValidator
from src.notion_backup_restore.utils.logger import setup_logger


def test_text_block_validation():
    """Test validation of text-based blocks."""
    print("🧪 Testing text block validation...")
    
    logger = setup_logger("test", verbose=False)
    validator = ContentBlockValidator(logger)
    
    # Test paragraph with oversized content
    test_blocks = [
        {
            "type": "paragraph",
            "id": "test-paragraph-1",
            "paragraph": {
                "rich_text": [
                    {
                        "type": "text",
                        "text": {
                            "content": "x" * 3000  # Exceeds limit
                        },
                        "annotations": {
                            "bold": True,
                            "italic": False
                        }
                    }
                ]
            }
        }
    ]
    
    validated_blocks = validator.validate_and_sanitize_blocks(test_blocks)
    
    # Check that content was truncated
    content = validated_blocks[0]["paragraph"]["rich_text"][0]["text"]["content"]
    assert len(content) <= validator.MAX_TEXT_LENGTH
    
    print("✅ Text block validation test passed!")


def test_code_block_validation():
    """Test validation of code blocks."""
    print("🧪 Testing code block validation...")
    
    logger = setup_logger("test", verbose=False)
    validator = ContentBlockValidator(logger)
    
    # Test code block with oversized content
    test_blocks = [
        {
            "type": "code",
            "id": "test-code-1",
            "code": {
                "rich_text": [
                    {
                        "type": "text",
                        "text": {
                            "content": "print('hello')\n" * 500  # Very long code
                        }
                    }
                ],
                "language": "python"
            }
        }
    ]
    
    validated_blocks = validator.validate_and_sanitize_blocks(test_blocks)
    
    # Check that content was truncated
    content = validated_blocks[0]["code"]["rich_text"][0]["text"]["content"]
    assert len(content) <= validator.MAX_CODE_LENGTH
    
    print("✅ Code block validation test passed!")


def test_media_block_validation():
    """Test validation of media blocks."""
    print("🧪 Testing media block validation...")
    
    logger = setup_logger("test", verbose=False)
    validator = ContentBlockValidator(logger)
    
    # Test valid image block
    valid_image = {
        "type": "image",
        "id": "test-image-1",
        "image": {
            "external": {
                "url": "https://example.com/image.jpg"
            },
            "caption": [
                {
                    "type": "text",
                    "text": {
                        "content": "Test image"
                    }
                }
            ]
        }
    }
    
    # Test invalid image block (no source)
    invalid_image = {
        "type": "image",
        "id": "test-image-2",
        "image": {
            "caption": []
        }
    }
    
    test_blocks = [valid_image, invalid_image]
    validated_blocks = validator.validate_and_sanitize_blocks(test_blocks)
    
    # Should only have the valid image
    assert len(validated_blocks) == 1
    assert validated_blocks[0]["id"] == "test-image-1"
    
    print("✅ Media block validation test passed!")


def test_url_validation():
    """Test URL validation in various block types."""
    print("🧪 Testing URL validation...")
    
    logger = setup_logger("test", verbose=False)
    validator = ContentBlockValidator(logger)
    
    # Test bookmark with invalid URL
    test_blocks = [
        {
            "type": "bookmark",
            "id": "test-bookmark-1",
            "bookmark": {
                "url": "not-a-valid-url"
            }
        },
        {
            "type": "bookmark",
            "id": "test-bookmark-2",
            "bookmark": {
                "url": "https://valid-url.com"
            }
        }
    ]
    
    validated_blocks = validator.validate_and_sanitize_blocks(test_blocks)
    
    # Should only have the valid bookmark
    assert len(validated_blocks) == 1
    assert validated_blocks[0]["id"] == "test-bookmark-2"
    
    print("✅ URL validation test passed!")


def test_rich_text_validation():
    """Test rich text validation and sanitization."""
    print("🧪 Testing rich text validation...")
    
    logger = setup_logger("test", verbose=False)
    validator = ContentBlockValidator(logger)
    
    # Test rich text with invalid link
    test_blocks = [
        {
            "type": "paragraph",
            "id": "test-paragraph-1",
            "paragraph": {
                "rich_text": [
                    {
                        "type": "text",
                        "text": {
                            "content": "Valid text",
                            "link": {
                                "url": "invalid-url"
                            }
                        }
                    },
                    {
                        "type": "text",
                        "text": {
                            "content": "Text with valid link",
                            "link": {
                                "url": "https://example.com"
                            }
                        }
                    }
                ]
            }
        }
    ]
    
    validated_blocks = validator.validate_and_sanitize_blocks(test_blocks)
    
    # Check that invalid link was removed
    rich_text = validated_blocks[0]["paragraph"]["rich_text"]
    assert rich_text[0]["text"]["link"] is None  # Invalid link removed
    assert rich_text[1]["text"]["link"]["url"] == "https://example.com"  # Valid link kept
    
    print("✅ Rich text validation test passed!")


def test_table_validation():
    """Test table block validation."""
    print("🧪 Testing table validation...")
    
    logger = setup_logger("test", verbose=False)
    validator = ContentBlockValidator(logger)
    
    # Test table with table_row
    test_blocks = [
        {
            "type": "table",
            "id": "test-table-1",
            "table": {
                "table_width": 3,
                "has_column_header": True,
                "has_row_header": False
            },
            "children": [
                {
                    "type": "table_row",
                    "id": "test-row-1",
                    "table_row": {
                        "cells": [
                            [
                                {
                                    "type": "text",
                                    "text": {
                                        "content": "Cell 1"
                                    }
                                }
                            ],
                            [
                                {
                                    "type": "text",
                                    "text": {
                                        "content": "Cell 2"
                                    }
                                }
                            ],
                            []  # Empty cell
                        ]
                    }
                }
            ]
        }
    ]
    
    validated_blocks = validator.validate_and_sanitize_blocks(test_blocks)
    
    # Check table structure is preserved
    table = validated_blocks[0]
    assert table["table"]["table_width"] == 3
    assert len(table["children"]) == 1
    assert len(table["children"][0]["table_row"]["cells"]) == 3
    
    print("✅ Table validation test passed!")


def test_nested_blocks_depth_limit():
    """Test nested block depth limiting."""
    print("🧪 Testing nested block depth limiting...")
    
    logger = setup_logger("test", verbose=False)
    validator = ContentBlockValidator(logger)
    
    # Create deeply nested structure
    def create_nested_block(depth):
        if depth <= 0:
            return {
                "type": "paragraph",
                "id": f"deep-block-{depth}",
                "paragraph": {
                    "rich_text": [
                        {
                            "type": "text",
                            "text": {
                                "content": f"Deep block at level {depth}"
                            }
                        }
                    ]
                }
            }
        
        return {
            "type": "toggle",
            "id": f"toggle-{depth}",
            "toggle": {
                "rich_text": [
                    {
                        "type": "text",
                        "text": {
                            "content": f"Toggle at level {depth}"
                        }
                    }
                ]
            },
            "children": [create_nested_block(depth - 1)]
        }
    
    # Create a block nested 15 levels deep (exceeds limit of 10)
    deeply_nested = create_nested_block(15)
    test_blocks = [deeply_nested]
    
    validated_blocks = validator.validate_and_sanitize_blocks(test_blocks)
    
    # Should have the block but with limited nesting
    assert len(validated_blocks) == 1
    
    # Count actual depth
    def count_depth(block):
        if 'children' not in block or not block['children']:
            return 1
        return 1 + max(count_depth(child) for child in block['children'])
    
    actual_depth = count_depth(validated_blocks[0])
    assert actual_depth <= validator.config['max_block_depth'] + 1  # +1 for root level
    
    print("✅ Nested block depth limiting test passed!")


def test_user_reference_normalization():
    """Test user reference normalization in blocks."""
    print("🧪 Testing user reference normalization in blocks...")
    
    logger = setup_logger("test", verbose=False)
    validator = ContentBlockValidator(logger)
    
    # Test block with user references
    test_blocks = [
        {
            "type": "paragraph",
            "id": "test-paragraph-1",
            "paragraph": {
                "rich_text": [
                    {
                        "type": "text",
                        "text": {
                            "content": "Test content"
                        }
                    }
                ]
            },
            "created_by": {
                "object": "user",
                "id": "user-123",
                "name": "Test User",
                "avatar_url": "https://example.com/avatar.jpg"
            },
            "last_edited_by": {
                "object": "user",
                "id": "user-456",
                "type": "person"
            }
        }
    ]
    
    validated_blocks = validator.validate_and_sanitize_blocks(test_blocks)
    
    # Check that user references were normalized
    block = validated_blocks[0]
    created_by = block["created_by"]
    last_edited_by = block["last_edited_by"]
    
    # Should only have object and id
    assert set(created_by.keys()) == {"object", "id"}
    assert set(last_edited_by.keys()) == {"object", "id"}
    assert created_by["id"] == "user-123"
    assert last_edited_by["id"] == "user-456"
    
    print("✅ User reference normalization test passed!")


def test_unsupported_block_handling():
    """Test handling of unsupported block types."""
    print("🧪 Testing unsupported block handling...")
    
    logger = setup_logger("test", verbose=False)
    validator = ContentBlockValidator(logger)
    
    # Test with unsupported block type
    test_blocks = [
        {
            "type": "unsupported_block_type",
            "id": "test-unsupported-1",
            "unsupported_block_type": {
                "some_property": "some_value"
            }
        },
        {
            "type": "paragraph",
            "id": "test-paragraph-1",
            "paragraph": {
                "rich_text": [
                    {
                        "type": "text",
                        "text": {
                            "content": "Valid paragraph"
                        }
                    }
                ]
            }
        }
    ]
    
    validated_blocks = validator.validate_and_sanitize_blocks(test_blocks)
    
    # Should only have the valid paragraph (unsupported block removed)
    assert len(validated_blocks) == 1
    assert validated_blocks[0]["type"] == "paragraph"
    
    print("✅ Unsupported block handling test passed!")


def test_database_view_block_handling():
    """Test database view block handling with restoration limitations."""
    print("🧪 Testing database view block handling...")
    
    logger = setup_logger("test", verbose=False)
    validator = ContentBlockValidator(logger)
    
    # Test database view block
    test_blocks = [
        {
            "type": "database_view",
            "id": "test-db-view-1",
            "database_view": {
                "database_id": "12345678-1234-1234-1234-123456789abc",
                "view_type": "table",
                "filters": [
                    {
                        "property": "Status",
                        "condition": "equals",
                        "value": "Done"
                    }
                ],
                "sorts": [
                    {
                        "property": "Created",
                        "direction": "descending"
                    }
                ]
            }
        }
    ]
    
    validated_blocks = validator.validate_and_sanitize_blocks(test_blocks)
    
    # Should preserve the block but with warnings
    assert len(validated_blocks) == 1
    assert validated_blocks[0]["type"] == "database_view"
    
    # Check that view configuration is preserved
    view_content = validated_blocks[0]["database_view"]
    assert "filters" in view_content
    assert "sorts" in view_content
    assert view_content["database_id"] == "12345678-1234-1234-1234-123456789abc"
    
    print("✅ Database view block handling test passed!")


def test_comprehensive_validation_workflow():
    """Test complete validation workflow with mixed content."""
    print("🧪 Testing comprehensive validation workflow...")
    
    logger = setup_logger("test", verbose=False)
    validator = ContentBlockValidator(logger)
    
    # Complex test case with various issues
    test_blocks = [
        # Valid paragraph
        {
            "type": "paragraph",
            "id": "valid-paragraph",
            "paragraph": {
                "rich_text": [
                    {
                        "type": "text",
                        "text": {
                            "content": "This is valid content"
                        }
                    }
                ]
            }
        },
        # Code block with oversized content
        {
            "type": "code",
            "id": "oversized-code",
            "code": {
                "rich_text": [
                    {
                        "type": "text",
                        "text": {
                            "content": "print('hello')\n" * 200  # Too long
                        }
                    }
                ],
                "language": "python"
            }
        },
        # Invalid image block
        {
            "type": "image",
            "id": "invalid-image",
            "image": {
                "caption": []  # No source
            }
        },
        # Valid bookmark
        {
            "type": "bookmark",
            "id": "valid-bookmark",
            "bookmark": {
                "url": "https://example.com"
            }
        }
    ]
    
    validated_blocks = validator.validate_and_sanitize_blocks(test_blocks)
    
    # Should have 3 blocks (invalid image removed)
    assert len(validated_blocks) == 3
    
    # Check that code content was truncated
    code_block = next(b for b in validated_blocks if b["type"] == "code")
    code_content = code_block["code"]["rich_text"][0]["text"]["content"]
    assert len(code_content) <= validator.MAX_CODE_LENGTH
    
    # Get validation stats
    stats = validator.get_validation_stats()
    assert stats['blocks_processed'] == 4
    assert stats['blocks_removed'] == 1  # Invalid image
    assert stats['content_truncated'] >= 1  # Code block
    
    print("✅ Comprehensive validation workflow test passed!")


def run_all_tests():
    """Run all content block validation tests."""
    print("🚀 Starting enhanced content block validation tests...\n")
    
    try:
        test_text_block_validation()
        test_code_block_validation()
        test_media_block_validation()
        test_url_validation()
        test_rich_text_validation()
        test_table_validation()
        test_nested_blocks_depth_limit()
        test_user_reference_normalization()
        test_unsupported_block_handling()
        test_database_view_block_handling()
        test_comprehensive_validation_workflow()
        
        print(f"\n🎉 All content block validation tests passed!")
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
