"""
Comprehensive test suite for backup functionality.

Tests database discovery, schema extraction, content backup, and validation
with mock API responses and test data for the specific workspace structure.
"""

import pytest
import json
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime

# Add src to path for imports
import sys
src_path = Path(__file__).parent.parent / "src"
sys.path.insert(0, str(src_path))

from src.notion_backup_restore.backup.manager import NotionBackupManager
from src.notion_backup_restore.backup.database_finder import DatabaseFinder, DatabaseInfo
from src.notion_backup_restore.backup.schema_extractor import SchemaExtractor, DatabaseSchema
from src.notion_backup_restore.backup.content_extractor import ContentExtractor, DatabaseContent
from src.notion_backup_restore.config import BackupConfig
from src.notion_backup_restore.utils.api_client import NotionAPIClient


class TestDatabaseFinder:
    """Test database discovery functionality."""
    
    @pytest.fixture
    def mock_api_client(self):
        """Create mock API client."""
        client = Mock(spec=NotionAPIClient)
        return client
    
    @pytest.fixture
    def database_finder(self, mock_api_client):
        """Create database finder with mock client."""
        return DatabaseFinder(mock_api_client)
    
    def test_find_target_databases_success(self, database_finder, mock_api_client):
        """Test successful database discovery."""
        # Mock search response
        mock_api_client.search.return_value = {
            "results": [
                {
                    "object": "database",
                    "id": "doc-db-123",
                    "title": [{"plain_text": "Documentation"}],
                    "url": "https://notion.so/doc-db-123",
                    "properties": {},
                    "created_time": "2023-01-01T00:00:00.000Z",
                    "last_edited_time": "2023-01-01T00:00:00.000Z",
                    "parent": {"type": "page_id", "page_id": "parent-123"}
                }
            ]
        }
        
        # Test finding databases
        result = database_finder.find_target_databases(["Documentation"])
        
        assert len(result) == 1
        assert "Documentation" in result
        assert result["Documentation"].id == "doc-db-123"
        assert result["Documentation"].name == "Documentation"
    
    def test_find_target_databases_not_found(self, database_finder, mock_api_client):
        """Test database not found scenario."""
        # Mock empty search response
        mock_api_client.search.return_value = {"results": []}
        
        # Test should raise ValueError
        with pytest.raises(ValueError, match="Could not find the following databases"):
            database_finder.find_target_databases(["NonExistent"])
    
    def test_validate_database_structure(self, database_finder):
        """Test database structure validation."""
        # Create test database info
        db_info = DatabaseInfo(
            id="test-db",
            name="Documentation",
            title="Documentation",
            url="https://notion.so/test",
            properties={
                "Title": {"type": "title"},
                "Category": {"type": "select"},
                "Status": {"type": "select"},
                "Priority": {"type": "select"},
                "Tags": {"type": "multi_select"},
                "Related Tasks": {"type": "relation"},
                "Created": {"type": "created_time"},
                "Last Edited": {"type": "last_edited_time"},
                "Assignee": {"type": "people"}
            },
            created_time="2023-01-01T00:00:00.000Z",
            last_edited_time="2023-01-01T00:00:00.000Z",
            parent={}
        )
        
        # Test validation (should pass for Documentation database)
        errors = database_finder.validate_database_structure("Documentation", db_info)
        assert len(errors) == 0
    
    def test_validate_database_structure_missing_properties(self, database_finder):
        """Test validation with missing properties."""
        # Create test database info with missing properties
        db_info = DatabaseInfo(
            id="test-db",
            name="Documentation",
            title="Documentation", 
            url="https://notion.so/test",
            properties={
                "Title": {"type": "title"}
                # Missing other required properties
            },
            created_time="2023-01-01T00:00:00.000Z",
            last_edited_time="2023-01-01T00:00:00.000Z",
            parent={}
        )
        
        # Test validation (should find missing properties)
        errors = database_finder.validate_database_structure("Documentation", db_info)
        assert len(errors) > 0
        assert any("Missing property" in error for error in errors)


class TestSchemaExtractor:
    """Test schema extraction functionality."""
    
    @pytest.fixture
    def mock_api_client(self):
        """Create mock API client."""
        client = Mock(spec=NotionAPIClient)
        return client
    
    @pytest.fixture
    def schema_extractor(self, mock_api_client):
        """Create schema extractor with mock client."""
        return SchemaExtractor(mock_api_client)
    
    def test_extract_schema_success(self, schema_extractor, mock_api_client):
        """Test successful schema extraction."""
        # Mock database response
        mock_api_client.get_database.return_value = {
            "id": "test-db-123",
            "title": [{"plain_text": "Test Database"}],
            "description": [],
            "properties": {
                "Title": {
                    "id": "title-prop",
                    "type": "title",
                    "title": {}
                },
                "Status": {
                    "id": "status-prop", 
                    "type": "select",
                    "select": {
                        "options": [
                            {"id": "opt1", "name": "Draft", "color": "gray"}
                        ]
                    }
                },
                "ROI": {
                    "id": "roi-prop",
                    "type": "formula",
                    "formula": {
                        "expression": "round(Value/(Effort*400)*10)/10"
                    }
                }
            },
            "parent": {"type": "page_id", "page_id": "parent-123"},
            "url": "https://notion.so/test-db-123",
            "archived": False,
            "is_inline": False,
            "created_time": "2023-01-01T00:00:00.000Z",
            "last_edited_time": "2023-01-01T00:00:00.000Z",
            "created_by": {"object": "user", "id": "user-1"},
            "last_edited_by": {"object": "user", "id": "user-1"},
            "cover": None,
            "icon": None
        }
        
        # Test schema extraction
        schema = schema_extractor.extract_schema("test-db-123")
        
        assert schema.id == "test-db-123"
        assert schema.name == "Test Database"
        assert len(schema.properties) == 3
        assert "Title" in schema.properties
        assert "Status" in schema.properties
        assert "ROI" in schema.properties
        
        # Test specific property configurations
        title_prop = schema.properties["Title"]
        assert title_prop.type == "title"
        
        status_prop = schema.properties["Status"]
        assert status_prop.type == "select"
        assert "options" in status_prop.config
        
        roi_prop = schema.properties["ROI"]
        assert roi_prop.type == "formula"
        assert roi_prop.config["expression"] == "round(Value/(Effort*400)*10)/10"
    
    def test_validate_schema_integrity(self, schema_extractor):
        """Test schema integrity validation."""
        # Create test schema with issues
        from src.notion_backup_restore.backup.schema_extractor import PropertySchema
        
        properties = {
            "Title": PropertySchema(
                name="Title",
                type="title", 
                config={"type": "title"},
                id="title-prop"
            ),
            "BadRollup": PropertySchema(
                name="BadRollup",
                type="rollup",
                config={
                    "type": "rollup",
                    # Missing required rollup configuration
                },
                id="rollup-prop"
            )
        }
        
        schema = DatabaseSchema(
            id="test-db",
            name="Test",
            title=[],
            description=[],
            properties=properties,
            parent={},
            url="",
            archived=False,
            is_inline=False,
            created_time="",
            last_edited_time="",
            created_by={},
            last_edited_by={},
            cover=None,
            icon=None
        )
        
        # Test validation
        errors = schema_extractor.validate_schema_integrity(schema)
        assert len(errors) > 0
        assert any("rollup" in error.lower() for error in errors)


class TestContentExtractor:
    """Test content extraction functionality."""
    
    @pytest.fixture
    def mock_api_client(self):
        """Create mock API client."""
        client = Mock(spec=NotionAPIClient)
        return client
    
    @pytest.fixture
    def content_extractor(self, mock_api_client):
        """Create content extractor with mock client."""
        return ContentExtractor(mock_api_client)
    
    def test_extract_content_success(self, content_extractor, mock_api_client):
        """Test successful content extraction."""
        # Mock database query response
        mock_api_client.query_database.return_value = {
            "results": [
                {
                    "id": "page-1",
                    "url": "https://notion.so/page-1",
                    "properties": {
                        "Title": {
                            "type": "title",
                            "title": [{"plain_text": "Test Page"}]
                        }
                    },
                    "parent": {"type": "database_id", "database_id": "test-db"},
                    "archived": False,
                    "created_time": "2023-01-01T00:00:00.000Z",
                    "last_edited_time": "2023-01-01T00:00:00.000Z",
                    "created_by": {"object": "user", "id": "user-1"},
                    "last_edited_by": {"object": "user", "id": "user-1"},
                    "cover": None,
                    "icon": None
                }
            ],
            "has_more": False,
            "next_cursor": None
        }
        
        # Test content extraction
        content = content_extractor.extract_content(
            database_id="test-db",
            database_name="Test Database"
        )
        
        assert content.database_id == "test-db"
        assert content.database_name == "Test Database"
        assert content.total_pages == 1
        assert len(content.pages) == 1
        
        page = content.pages[0]
        assert page.id == "page-1"
        assert "Title" in page.properties
    
    def test_extract_content_with_pagination(self, content_extractor, mock_api_client):
        """Test content extraction with pagination."""
        # Mock paginated responses
        responses = [
            {
                "results": [{"id": "page-1", "properties": {}, "parent": {}, 
                           "archived": False, "created_time": "", "last_edited_time": "",
                           "created_by": {}, "last_edited_by": {}, "url": ""}],
                "has_more": True,
                "next_cursor": "cursor-1"
            },
            {
                "results": [{"id": "page-2", "properties": {}, "parent": {},
                           "archived": False, "created_time": "", "last_edited_time": "",
                           "created_by": {}, "last_edited_by": {}, "url": ""}],
                "has_more": False,
                "next_cursor": None
            }
        ]
        
        mock_api_client.query_database.side_effect = responses
        
        # Test content extraction
        content = content_extractor.extract_content(
            database_id="test-db",
            database_name="Test Database"
        )
        
        assert content.total_pages == 2
        assert len(content.pages) == 2
        assert mock_api_client.query_database.call_count == 2


class TestBackupManager:
    """Test backup manager functionality."""
    
    @pytest.fixture
    def temp_backup_dir(self, tmp_path):
        """Create temporary backup directory."""
        return tmp_path / "test_backups"
    
    @pytest.fixture
    def backup_config(self, temp_backup_dir):
        """Create test backup configuration."""
        return BackupConfig(
            notion_token="secret_test_token_123",
            output_dir=temp_backup_dir,
            include_blocks=False,
            validate_integrity=False,  # Disable for testing
            debug=True
        )
    
    @patch('src.notion_backup_restore.backup.manager.create_notion_client')
    @patch('src.notion_backup_restore.backup.manager.DatabaseFinder')
    @patch('src.notion_backup_restore.backup.manager.SchemaExtractor')
    @patch('src.notion_backup_restore.backup.manager.ContentExtractor')
    def test_backup_manager_success(
        self,
        mock_content_extractor_class,
        mock_schema_extractor_class,
        mock_database_finder_class,
        mock_create_client,
        backup_config
    ):
        """Test successful backup operation."""
        # Setup mocks
        mock_api_client = Mock()
        mock_create_client.return_value = mock_api_client
        
        # Mock database finder
        mock_db_finder = Mock()
        mock_database_finder_class.return_value = mock_db_finder
        mock_db_finder.find_target_databases.return_value = {
            "Documentation": DatabaseInfo(
                id="doc-db-123",
                name="Documentation",
                title="Documentation",
                url="https://notion.so/doc",
                properties={},
                created_time="2023-01-01T00:00:00.000Z",
                last_edited_time="2023-01-01T00:00:00.000Z",
                parent={}
            )
        }
        mock_db_finder.validate_all_databases.return_value = {"Documentation": []}
        
        # Mock schema extractor
        mock_schema_extractor = Mock()
        mock_schema_extractor_class.return_value = mock_schema_extractor
        
        from src.notion_backup_restore.backup.schema_extractor import PropertySchema
        test_schema = DatabaseSchema(
            id="doc-db-123",
            name="Documentation",
            title=[],
            description=[],
            properties={
                "Title": PropertySchema("Title", "title", {"type": "title"}, "title-prop")
            },
            parent={},
            url="",
            archived=False,
            is_inline=False,
            created_time="",
            last_edited_time="",
            created_by={},
            last_edited_by={},
            cover=None,
            icon=None
        )
        mock_schema_extractor.extract_schema.return_value = test_schema
        
        # Mock content extractor
        mock_content_extractor = Mock()
        mock_content_extractor_class.return_value = mock_content_extractor
        
        from src.notion_backup_restore.backup.content_extractor import PageContent
        test_content = DatabaseContent(
            database_id="doc-db-123",
            database_name="Documentation",
            pages=[
                PageContent(
                    id="page-1",
                    url="",
                    properties={},
                    parent={},
                    archived=False,
                    created_time="",
                    last_edited_time="",
                    created_by={},
                    last_edited_by={},
                    cover=None,
                    icon=None
                )
            ],
            total_pages=1,
            extraction_time="2023-01-01T00:00:00.000Z"
        )
        mock_content_extractor.extract_content.return_value = test_content
        
        # Mock API client stats
        mock_api_client.get_stats.return_value = {
            "total_requests": 10,
            "total_errors": 0,
            "error_rate": 0.0
        }
        
        # Test backup
        backup_manager = NotionBackupManager(backup_config)
        backup_dir = backup_manager.start_backup(database_names=["Documentation"])
        
        # Verify results
        assert backup_dir.exists()
        assert (backup_dir / "manifest.json").exists()
        assert (backup_dir / "databases").exists()
        
        # Check manifest content
        with open(backup_dir / "manifest.json", 'r') as f:
            manifest = json.load(f)
        
        assert manifest["version"] == "1.0"
        assert "Documentation" in manifest["databases"]
        assert manifest["statistics"]["total_databases"] == 1
    
    def test_backup_stats(self, backup_config):
        """Test backup statistics collection."""
        with patch('src.notion_backup_restore.backup.manager.create_notion_client'):
            backup_manager = NotionBackupManager(backup_config)
            
            # Mock some state
            backup_manager.discovered_databases = {"Test": Mock()}
            backup_manager.extracted_schemas = {"Test": Mock()}
            backup_manager.extracted_content = {"Test": Mock(total_pages=10)}
            
            # Mock API client stats
            backup_manager.api_client.get_stats.return_value = {
                "total_requests": 5,
                "total_errors": 0
            }
            
            stats = backup_manager.get_backup_stats()
            
            assert stats["databases_discovered"] == 1
            assert stats["schemas_extracted"] == 1
            assert stats["content_extracted"] == 1
            assert stats["total_pages"] == 10


if __name__ == "__main__":
    pytest.main([__file__])
