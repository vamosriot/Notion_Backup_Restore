"""
Comprehensive test suite for restore functionality.

Tests the 4-phase restoration process, dependency resolution, ID mapping,
and validation with mock data and comprehensive error scenarios.
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

from src.notion_backup_restore.restore.manager import NotionRestoreManager
from src.notion_backup_restore.restore.database_creator import DatabaseCreator, DatabaseCreationResult
from src.notion_backup_restore.restore.relation_restorer import RelationRestorer, RelationRestorationResult
from src.notion_backup_restore.restore.formula_restorer import FormulaRestorer, FormulaRestorationResult
from src.notion_backup_restore.restore.data_restorer import DataRestorer, DataRestorationResult
from src.notion_backup_restore.config import RestoreConfig
from src.notion_backup_restore.utils.id_mapper import IDMapper
from src.notion_backup_restore.utils.api_client import NotionAPIClient


class TestDatabaseCreator:
    """Test database creation functionality."""
    
    @pytest.fixture
    def mock_api_client(self):
        """Create mock API client."""
        client = Mock(spec=NotionAPIClient)
        return client
    
    @pytest.fixture
    def mock_id_mapper(self):
        """Create mock ID mapper."""
        mapper = Mock(spec=IDMapper)
        return mapper
    
    @pytest.fixture
    def database_creator(self, mock_api_client, mock_id_mapper):
        """Create database creator with mocks."""
        return DatabaseCreator(mock_api_client, mock_id_mapper)
    
    def test_create_database_success(self, database_creator, mock_api_client, mock_id_mapper):
        """Test successful database creation."""
        # Mock API response
        mock_api_client.create_database.return_value = {
            "id": "new-db-123",
            "title": [{"plain_text": "Test Database"}]
        }
        
        # Create test schema
        from src.notion_backup_restore.backup.schema_extractor import DatabaseSchema, PropertySchema
        
        properties = {
            "Title": PropertySchema(
                name="Title",
                type="title",
                config={"type": "title"},
                id="title-prop"
            ),
            "Status": PropertySchema(
                name="Status", 
                type="select",
                config={
                    "type": "select",
                    "options": [{"id": "opt1", "name": "Draft", "color": "gray"}]
                },
                id="status-prop"
            )
        }
        
        schema = DatabaseSchema(
            id="original-db-123",
            name="Test Database",
            title=[{"type": "text", "text": {"content": "Test Database"}}],
            description=[],
            properties=properties,
            parent={"type": "page_id", "page_id": "parent-123"},
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
        
        # Test database creation
        result = database_creator.create_database(schema)
        
        assert result.new_id == "new-db-123"
        assert result.name == "Test Database"
        assert len(result.created_properties) == 2
        assert "Title" in result.created_properties
        assert "Status" in result.created_properties
        assert len(result.errors) == 0
        
        # Verify API calls
        mock_api_client.create_database.assert_called_once()
        mock_id_mapper.add_mapping.assert_called_once_with(
            original_id="original-db-123",
            new_id="new-db-123",
            object_type="database",
            name="Test Database"
        )
    
    def test_create_database_skip_relation_properties(self, database_creator, mock_api_client, mock_id_mapper):
        """Test that relation properties are skipped in Phase 1."""
        # Mock API response
        mock_api_client.create_database.return_value = {"id": "new-db-123"}
        
        # Create schema with relation property
        from src.notion_backup_restore.backup.schema_extractor import DatabaseSchema, PropertySchema
        
        properties = {
            "Title": PropertySchema(
                name="Title",
                type="title", 
                config={"type": "title"},
                id="title-prop"
            ),
            "Related": PropertySchema(
                name="Related",
                type="relation",
                config={"type": "relation", "database_id": "other-db"},
                id="relation-prop"
            ),
            "Formula": PropertySchema(
                name="Formula",
                type="formula",
                config={"type": "formula", "expression": "1+1"},
                id="formula-prop"
            )
        }
        
        schema = DatabaseSchema(
            id="original-db",
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
        
        # Test creation
        result = database_creator.create_database(schema)
        
        # Should only create Title property, skip relation and formula
        assert len(result.created_properties) == 1
        assert "Title" in result.created_properties
        assert len(result.skipped_properties) == 2
        assert "Related" in result.skipped_properties
        assert "Formula" in result.skipped_properties


class TestRelationRestorer:
    """Test relation property restoration."""
    
    @pytest.fixture
    def mock_api_client(self):
        """Create mock API client."""
        return Mock(spec=NotionAPIClient)
    
    @pytest.fixture
    def mock_id_mapper(self):
        """Create mock ID mapper."""
        mapper = Mock(spec=IDMapper)
        # Mock ID mappings
        mapper.get_new_id.side_effect = lambda old_id: {
            "original-db": "new-db-123",
            "target-db": "new-target-456"
        }.get(old_id)
        return mapper
    
    @pytest.fixture
    def relation_restorer(self, mock_api_client, mock_id_mapper):
        """Create relation restorer with mocks."""
        return RelationRestorer(mock_api_client, mock_id_mapper)
    
    def test_restore_relations_success(self, relation_restorer, mock_api_client, mock_id_mapper):
        """Test successful relation restoration."""
        # Create schema with relation property
        from src.notion_backup_restore.backup.schema_extractor import DatabaseSchema, PropertySchema
        
        properties = {
            "Title": PropertySchema(
                name="Title",
                type="title",
                config={"type": "title"},
                id="title-prop"
            ),
            "Related": PropertySchema(
                name="Related",
                type="relation",
                config={
                    "type": "relation",
                    "database_id": "target-db"
                },
                id="relation-prop"
            )
        }
        
        schema = DatabaseSchema(
            id="original-db",
            name="Test Database",
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
        
        # Test relation restoration
        result = relation_restorer.restore_relations(schema)
        
        assert result.database_id == "new-db-123"
        assert len(result.added_properties) == 1
        assert "Related" in result.added_properties
        assert len(result.errors) == 0
        
        # Verify API call
        mock_api_client.update_database.assert_called_once()
        call_args = mock_api_client.update_database.call_args
        assert call_args[0][0] == "new-db-123"  # database_id
        
        # Check the update payload
        update_payload = call_args[1]
        assert "properties" in update_payload
        assert "Related" in update_payload["properties"]
        
        relation_config = update_payload["properties"]["Related"]
        assert relation_config["type"] == "relation"
        assert relation_config["relation"]["database_id"] == "new-target-456"


class TestFormulaRestorer:
    """Test formula and rollup property restoration."""
    
    @pytest.fixture
    def mock_api_client(self):
        """Create mock API client."""
        return Mock(spec=NotionAPIClient)
    
    @pytest.fixture
    def formula_restorer(self, mock_api_client):
        """Create formula restorer with mock client."""
        return FormulaRestorer(mock_api_client)
    
    def test_restore_formulas_success(self, formula_restorer, mock_api_client):
        """Test successful formula restoration."""
        # Create schema with formula property
        from src.notion_backup_restore.backup.schema_extractor import DatabaseSchema, PropertySchema
        
        properties = {
            "Title": PropertySchema(
                name="Title",
                type="title",
                config={"type": "title"},
                id="title-prop"
            ),
            "ROI": PropertySchema(
                name="ROI",
                type="formula",
                config={
                    "type": "formula",
                    "expression": "round(Value/(Effort*400)*10)/10"
                },
                id="roi-prop"
            ),
            "Summary": PropertySchema(
                name="Summary",
                type="rollup",
                config={
                    "type": "rollup",
                    "relation_property_name": "Related",
                    "rollup_property_name": "Status",
                    "function": "count"
                },
                id="rollup-prop"
            )
        }
        
        schema = DatabaseSchema(
            id="test-db",
            name="Test Database",
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
        
        # Test formula restoration
        result = formula_restorer.restore_formulas("new-db-123", schema)
        
        assert result.database_id == "new-db-123"
        assert len(result.added_formulas) == 1
        assert "ROI" in result.added_formulas
        assert len(result.added_rollups) == 1
        assert "Summary" in result.added_rollups
        assert len(result.errors) == 0
        
        # Verify API calls (should be called twice - once for rollup, once for formula)
        assert mock_api_client.update_database.call_count == 2


class TestDataRestorer:
    """Test data restoration functionality."""
    
    @pytest.fixture
    def mock_api_client(self):
        """Create mock API client."""
        return Mock(spec=NotionAPIClient)
    
    @pytest.fixture
    def mock_id_mapper(self):
        """Create mock ID mapper."""
        mapper = Mock(spec=IDMapper)
        # Mock ID mappings
        mapper.get_new_id.side_effect = lambda old_id: {
            "original-db": "new-db-123",
            "original-page-1": "new-page-456",
            "related-page-1": "new-related-789"
        }.get(old_id)
        return mapper
    
    @pytest.fixture
    def data_restorer(self, mock_api_client, mock_id_mapper):
        """Create data restorer with mocks."""
        return DataRestorer(mock_api_client, mock_id_mapper)
    
    def test_restore_data_success(self, data_restorer, mock_api_client, mock_id_mapper):
        """Test successful data restoration."""
        # Mock page creation response
        mock_api_client.create_page.return_value = {"id": "new-page-456"}
        
        # Create test content
        from src.notion_backup_restore.backup.content_extractor import DatabaseContent, PageContent
        
        pages = [
            PageContent(
                id="original-page-1",
                url="https://notion.so/page-1",
                properties={
                    "Title": {
                        "type": "title",
                        "title": [{"plain_text": "Test Page"}]
                    },
                    "Status": {
                        "type": "select",
                        "select": {"id": "status1", "name": "Draft"}
                    },
                    "Related": {
                        "type": "relation",
                        "relation": [{"id": "related-page-1"}]
                    }
                },
                parent={"type": "database_id", "database_id": "original-db"},
                archived=False,
                created_time="2023-01-01T00:00:00.000Z",
                last_edited_time="2023-01-01T00:00:00.000Z",
                created_by={},
                last_edited_by={},
                cover=None,
                icon=None
            )
        ]
        
        content = DatabaseContent(
            database_id="original-db",
            database_name="Test Database",
            pages=pages,
            total_pages=1,
            extraction_time="2023-01-01T00:00:00.000Z"
        )
        
        # Test data restoration
        result = data_restorer.restore_data(content)
        
        assert result.database_id == "new-db-123"
        assert result.created_pages == 1
        assert result.failed_pages == 0
        assert len(result.page_mappings) == 1
        assert "original-page-1" in result.page_mappings
        assert result.page_mappings["original-page-1"] == "new-page-456"
        
        # Verify API call
        mock_api_client.create_page.assert_called_once()
        call_args = mock_api_client.create_page.call_args[1]
        
        # Check that relation IDs were updated
        properties = call_args["properties"]
        assert "Related" in properties
        relation_prop = properties["Related"]
        assert relation_prop["relation"][0]["id"] == "new-related-789"


class TestRestoreManager:
    """Test restore manager functionality."""
    
    @pytest.fixture
    def temp_backup_dir(self, tmp_path):
        """Create temporary backup directory with test data."""
        backup_dir = tmp_path / "test_backup"
        backup_dir.mkdir()
        
        # Create manifest
        manifest = {
            "version": "1.0",
            "created_at": "2023-01-01T00:00:00.000Z",
            "databases": {
                "Documentation": {
                    "id": "doc-db-123",
                    "schema_file": "documentation_schema.json",
                    "data_file": "documentation_data.json"
                }
            }
        }
        
        with open(backup_dir / "manifest.json", 'w') as f:
            json.dump(manifest, f)
        
        # Create databases directory
        (backup_dir / "databases").mkdir()
        
        # Create schema file
        schema_data = {
            "id": "doc-db-123",
            "name": "Documentation",
            "title": [{"type": "text", "text": {"content": "Documentation"}}],
            "description": [],
            "properties": {
                "Title": {
                    "name": "Title",
                    "type": "title",
                    "config": {"type": "title"},
                    "id": "title-prop"
                }
            },
            "parent": {},
            "url": "",
            "archived": False,
            "is_inline": False,
            "created_time": "",
            "last_edited_time": "",
            "created_by": {},
            "last_edited_by": {},
            "cover": None,
            "icon": None
        }
        
        with open(backup_dir / "databases" / "documentation_schema.json", 'w') as f:
            json.dump(schema_data, f)
        
        # Create data file
        data_data = {
            "database_id": "doc-db-123",
            "database_name": "Documentation",
            "total_pages": 1,
            "extraction_time": "2023-01-01T00:00:00.000Z",
            "pages": [
                {
                    "id": "page-1",
                    "url": "",
                    "properties": {
                        "Title": {
                            "type": "title",
                            "title": [{"plain_text": "Test Page"}]
                        }
                    },
                    "parent": {},
                    "archived": False,
                    "created_time": "",
                    "last_edited_time": "",
                    "created_by": {},
                    "last_edited_by": {},
                    "cover": None,
                    "icon": None,
                    "blocks": None
                }
            ]
        }
        
        with open(backup_dir / "databases" / "documentation_data.json", 'w') as f:
            json.dump(data_data, f)
        
        return backup_dir
    
    @pytest.fixture
    def restore_config(self, temp_backup_dir):
        """Create test restore configuration."""
        return RestoreConfig(
            notion_token="secret_test_token_123",
            backup_dir=temp_backup_dir,
            dry_run=True,  # Use dry run for testing
            validate_after=False,
            debug=True
        )
    
    @patch('src.notion_backup_restore.restore.manager.create_notion_client')
    def test_restore_manager_dry_run(self, mock_create_client, restore_config):
        """Test restore manager in dry run mode."""
        # Setup mocks
        mock_api_client = Mock()
        mock_create_client.return_value = mock_api_client
        mock_api_client.get_stats.return_value = {
            "total_requests": 0,
            "total_errors": 0,
            "error_rate": 0.0
        }
        
        # Test restore
        restore_manager = NotionRestoreManager(restore_config)
        results = restore_manager.start_restore()
        
        # Verify results
        assert "restoration_summary" in results
        assert results["restoration_summary"]["dry_run"] is True
        assert "phase_results" in results
        
        # In dry run mode, no API calls should be made
        mock_api_client.create_database.assert_not_called()
    
    def test_load_backup_data(self, restore_config):
        """Test loading backup data from files."""
        with patch('src.notion_backup_restore.restore.manager.create_notion_client'):
            restore_manager = NotionRestoreManager(restore_config)
            restore_manager._load_backup_data()
            
            # Verify data was loaded
            assert len(restore_manager.schemas) == 1
            assert "Documentation" in restore_manager.schemas
            assert len(restore_manager.contents) == 1
            assert "Documentation" in restore_manager.contents
            
            # Verify schema details
            schema = restore_manager.schemas["Documentation"]
            assert schema.id == "doc-db-123"
            assert schema.name == "Documentation"
            assert "Title" in schema.properties
            
            # Verify content details
            content = restore_manager.contents["Documentation"]
            assert content.database_id == "doc-db-123"
            assert content.total_pages == 1
            assert len(content.pages) == 1


if __name__ == "__main__":
    pytest.main([__file__])
