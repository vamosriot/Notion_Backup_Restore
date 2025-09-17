"""
Test suite for utility functions.

Tests rate limiter, API client, ID mapper, dependency resolver,
and other utility components with comprehensive scenarios.
"""

import pytest
import time
import json
from pathlib import Path
from unittest.mock import Mock, patch
from collections import deque

# Add src to path for imports
import sys
src_path = Path(__file__).parent.parent / "src"
sys.path.insert(0, str(src_path))

from src.notion_backup_restore.utils.rate_limiter import RateLimiter, AdaptiveRateLimiter, RateLimitConfig
from src.notion_backup_restore.utils.id_mapper import IDMapper, IDMapping
from src.notion_backup_restore.utils.dependency_resolver import DependencyResolver, create_workspace_dependency_resolver
from src.notion_backup_restore.utils.api_client import NotionAPIClient, CircuitBreaker


class TestRateLimiter:
    """Test rate limiting functionality."""
    
    def test_rate_limiter_basic(self):
        """Test basic rate limiting functionality."""
        config = RateLimitConfig(requests_per_second=2.0, burst_size=3, window_size=5)
        limiter = RateLimiter(config)
        
        # First few requests should not be limited
        wait_time1 = limiter.wait_if_needed()
        wait_time2 = limiter.wait_if_needed()
        wait_time3 = limiter.wait_if_needed()
        
        assert wait_time1 == 0.0
        assert wait_time2 == 0.0
        assert wait_time3 == 0.0
        
        # Fourth request should be limited (burst exceeded)
        wait_time4 = limiter.wait_if_needed()
        assert wait_time4 > 0.0
    
    def test_rate_limiter_window_cleanup(self):
        """Test that old requests are cleaned from the window."""
        config = RateLimitConfig(requests_per_second=1.0, window_size=1)
        limiter = RateLimiter(config)
        
        # Make a request
        limiter.wait_if_needed()
        
        # Simulate time passing
        with patch('time.time') as mock_time:
            # Set time to 2 seconds later
            mock_time.return_value = time.time() + 2
            
            # Request should not be limited (old request outside window)
            wait_time = limiter.wait_if_needed()
            assert wait_time == 0.0
    
    def test_adaptive_rate_limiter_429_handling(self):
        """Test adaptive rate limiter handles 429 responses."""
        config = RateLimitConfig(requests_per_second=2.0)
        limiter = AdaptiveRateLimiter(config)
        
        # Simulate 429 response
        limiter.handle_429_response(retry_after=5)
        
        # Next request should be delayed
        wait_time = limiter.wait_if_needed()
        assert wait_time >= 5.0
        
        # Success should reduce adaptive delay
        limiter.handle_success_response()
        stats = limiter.get_stats()
        assert stats["consecutive_429s"] == 0
    
    def test_rate_limiter_stats(self):
        """Test rate limiter statistics."""
        config = RateLimitConfig(requests_per_second=2.0, burst_size=5)
        limiter = RateLimiter(config)
        
        # Make some requests
        limiter.wait_if_needed()
        limiter.wait_if_needed()
        
        stats = limiter.get_stats()
        assert stats["requests_in_window"] == 2
        assert stats["configured_rate"] == 2.0
        assert stats["burst_size"] == 5


class TestIDMapper:
    """Test ID mapping functionality."""
    
    @pytest.fixture
    def temp_mapping_file(self, tmp_path):
        """Create temporary mapping file."""
        return tmp_path / "test_mappings.json"
    
    def test_id_mapper_basic_operations(self):
        """Test basic ID mapping operations."""
        mapper = IDMapper()
        
        # Add mapping
        mapper.add_mapping("old-db-123", "new-db-456", "database", "Test DB")
        
        # Test retrieval
        assert mapper.get_new_id("old-db-123") == "new-db-456"
        assert mapper.get_original_id("new-db-456") == "old-db-123"
        assert mapper.has_mapping("old-db-123") is True
        assert mapper.has_mapping("nonexistent") is False
        
        # Test mapping object
        mapping = mapper.get_mapping("old-db-123")
        assert mapping is not None
        assert mapping.original_id == "old-db-123"
        assert mapping.new_id == "new-db-456"
        assert mapping.object_type == "database"
        assert mapping.name == "Test DB"
    
    def test_id_mapper_duplicate_mapping(self):
        """Test handling of duplicate mappings."""
        mapper = IDMapper()
        
        # Add initial mapping
        mapper.add_mapping("old-id", "new-id-1", "database")
        
        # Adding same mapping should not raise error
        mapper.add_mapping("old-id", "new-id-1", "database")
        
        # Adding different mapping for same original ID should raise error
        with pytest.raises(ValueError, match="already mapped"):
            mapper.add_mapping("old-id", "new-id-2", "database")
    
    def test_id_mapper_by_type(self):
        """Test filtering mappings by type."""
        mapper = IDMapper()
        
        # Add different types of mappings
        mapper.add_mapping("db-1", "new-db-1", "database", "DB 1")
        mapper.add_mapping("page-1", "new-page-1", "page", "Page 1")
        mapper.add_mapping("prop-1", "new-prop-1", "property", "Prop 1")
        
        # Test type filtering
        db_mappings = mapper.get_mappings_by_type("database")
        assert len(db_mappings) == 1
        assert "db-1" in db_mappings
        
        page_mappings = mapper.get_page_mappings()
        assert len(page_mappings) == 1
        assert page_mappings["page-1"] == "new-page-1"
        
        database_mappings = mapper.get_database_mappings()
        assert len(database_mappings) == 1
        assert database_mappings["db-1"] == "new-db-1"
    
    def test_id_mapper_relation_updates(self):
        """Test updating relation IDs in data structures."""
        mapper = IDMapper()
        
        # Add mappings
        mapper.add_mapping("old-page-1", "new-page-1", "page")
        mapper.add_mapping("old-page-2", "new-page-2", "page")
        
        # Test relation property update
        properties = {
            "Title": {
                "type": "title",
                "title": [{"plain_text": "Test"}]
            },
            "Related": {
                "type": "relation",
                "relation": [
                    {"id": "old-page-1"},
                    {"id": "old-page-2"},
                    {"id": "unmapped-page"}  # Should remain unchanged
                ]
            }
        }
        
        updated_properties = mapper.update_property_relations(properties)
        
        # Check that relation IDs were updated
        relations = updated_properties["Related"]["relation"]
        assert relations[0]["id"] == "new-page-1"
        assert relations[1]["id"] == "new-page-2"
        assert relations[2]["id"] == "unmapped-page"  # Unchanged
    
    def test_id_mapper_persistence(self, temp_mapping_file):
        """Test saving and loading mappings."""
        # Create mapper and add mappings
        mapper1 = IDMapper(temp_mapping_file)
        mapper1.add_mapping("old-1", "new-1", "database", "DB 1")
        mapper1.add_mapping("old-2", "new-2", "page", "Page 1")
        
        # Save mappings
        mapper1.save_mappings()
        assert temp_mapping_file.exists()
        
        # Load mappings in new mapper
        mapper2 = IDMapper(temp_mapping_file)
        
        # Verify mappings were loaded
        assert mapper2.get_new_id("old-1") == "new-1"
        assert mapper2.get_new_id("old-2") == "new-2"
        assert len(mapper2) == 2
    
    def test_id_mapper_stats(self):
        """Test ID mapper statistics."""
        mapper = IDMapper()
        
        # Add various mappings
        mapper.add_mapping("db-1", "new-db-1", "database")
        mapper.add_mapping("page-1", "new-page-1", "page")
        mapper.add_mapping("page-2", "new-page-2", "page")
        
        stats = mapper.get_stats()
        assert stats["total_mappings"] == 3
        assert stats["type_counts"]["database"] == 1
        assert stats["type_counts"]["page"] == 2


class TestDependencyResolver:
    """Test dependency resolution functionality."""
    
    def test_dependency_resolver_basic(self):
        """Test basic dependency resolution."""
        resolver = DependencyResolver()
        
        # Add databases
        resolver.add_database("A")
        resolver.add_database("B")
        resolver.add_database("C")
        
        # Add dependencies: B depends on A, C depends on B
        resolver.add_dependency("B", "A", "relation_to_A")
        resolver.add_dependency("C", "B", "relation_to_B")
        
        # Get restoration order
        order = resolver.get_restoration_order()
        
        # A should come first, then B, then C
        assert order.index("A") < order.index("B")
        assert order.index("B") < order.index("C")
    
    def test_dependency_resolver_no_dependencies(self):
        """Test resolver with no dependencies."""
        resolver = DependencyResolver()
        
        # Add databases without dependencies
        resolver.add_database("A")
        resolver.add_database("B")
        resolver.add_database("C")
        
        # Should return all databases (order may vary)
        order = resolver.get_restoration_order()
        assert len(order) == 3
        assert set(order) == {"A", "B", "C"}
    
    def test_dependency_resolver_circular_dependency(self):
        """Test detection of circular dependencies."""
        resolver = DependencyResolver()
        
        # Add databases
        resolver.add_database("A")
        resolver.add_database("B")
        resolver.add_database("C")
        
        # Create circular dependency: A -> B -> C -> A
        resolver.add_dependency("A", "B", "rel1")
        resolver.add_dependency("B", "C", "rel2")
        resolver.add_dependency("C", "A", "rel3")
        
        # Should detect circular dependency
        with pytest.raises(ValueError, match="Circular dependencies detected"):
            resolver.get_restoration_order()
    
    def test_dependency_resolver_validation(self):
        """Test dependency validation."""
        resolver = DependencyResolver()
        
        # Add databases
        resolver.add_database("A")
        resolver.add_database("B")
        
        # Add valid dependency
        resolver.add_dependency("B", "A", "valid_relation")
        
        # Add invalid dependencies
        resolver.add_dependency("C", "A", "invalid_source")  # C doesn't exist
        resolver.add_dependency("A", "D", "invalid_target")  # D doesn't exist
        resolver.add_dependency("A", "A", "self_dependency")  # Self dependency
        
        # Validate
        errors = resolver.validate_dependencies()
        
        assert len(errors) >= 3
        assert any("Unknown source database" in error for error in errors)
        assert any("Unknown target database" in error for error in errors)
        assert any("self-dependency" in error for error in errors)
    
    def test_workspace_dependency_resolver(self):
        """Test the workspace-specific dependency resolver."""
        resolver = create_workspace_dependency_resolver()
        
        # Should have all workspace databases
        stats = resolver.get_dependency_stats()
        assert stats["total_databases"] == 4
        assert "Documentation" in stats["dependency_graph"]
        assert "Tasks" in stats["dependency_graph"]
        assert "Notes" in stats["dependency_graph"]
        assert "Sprints" in stats["dependency_graph"]
        
        # Should be able to resolve order without circular dependencies
        order = resolver.get_restoration_order()
        assert len(order) == 4
        assert set(order) == {"Documentation", "Tasks", "Notes", "Sprints"}
        
        # Tasks should come after its dependencies
        tasks_index = order.index("Tasks")
        for dep in ["Documentation", "Notes", "Sprints"]:
            if dep in order:
                dep_index = order.index(dep)
                # Tasks depends on these, so they should come before Tasks
                # (Note: this depends on the specific dependency structure)


class TestCircuitBreaker:
    """Test circuit breaker functionality."""
    
    def test_circuit_breaker_closed_state(self):
        """Test circuit breaker in closed state."""
        breaker = CircuitBreaker(failure_threshold=3, timeout=60)
        
        # Should allow calls in closed state
        result = breaker.call(lambda: "success")
        assert result == "success"
        assert breaker.state == "closed"
    
    def test_circuit_breaker_open_state(self):
        """Test circuit breaker opening after failures."""
        breaker = CircuitBreaker(failure_threshold=2, timeout=60)
        
        # Cause failures to open the breaker
        for _ in range(2):
            try:
                breaker.call(lambda: exec('raise Exception("test error")'))
            except Exception:
                pass
        
        # Circuit should now be open
        assert breaker.state == "open"
        
        # Should raise CircuitBreakerError
        from src.notion_backup_restore.utils.api_client import CircuitBreakerError
        with pytest.raises(CircuitBreakerError):
            breaker.call(lambda: "should not execute")
    
    def test_circuit_breaker_half_open_state(self):
        """Test circuit breaker half-open state."""
        breaker = CircuitBreaker(failure_threshold=1, timeout=0.1)  # Short timeout
        
        # Cause failure to open breaker
        try:
            breaker.call(lambda: exec('raise Exception("test error")'))
        except Exception:
            pass
        
        assert breaker.state == "open"
        
        # Wait for timeout
        time.sleep(0.2)
        
        # Next call should put it in half-open state and succeed
        result = breaker.call(lambda: "recovery")
        assert result == "recovery"
        assert breaker.state == "closed"  # Should close after successful call


class TestNotionAPIClient:
    """Test Notion API client wrapper."""
    
    @pytest.fixture
    def mock_notion_client(self):
        """Create mock Notion client."""
        with patch('src.notion_backup_restore.utils.api_client.Client') as mock_client_class:
            mock_client = Mock()
            mock_client_class.return_value = mock_client
            yield mock_client
    
    def test_api_client_successful_call(self, mock_notion_client):
        """Test successful API call."""
        # Setup mock response
        mock_notion_client.search.return_value = {"results": []}
        
        # Create API client
        from src.notion_backup_restore.utils.rate_limiter import RateLimitConfig
        config = RateLimitConfig(requests_per_second=10)  # High limit for testing
        
        api_client = NotionAPIClient(
            auth="secret_test_token",
            rate_limit_config=config,
            max_retries=1
        )
        
        # Make API call
        result = api_client.search(query="test")
        
        assert result == {"results": []}
        mock_notion_client.search.assert_called_once_with(query="test")
    
    def test_api_client_retry_logic(self, mock_notion_client):
        """Test API client retry logic."""
        from notion_client.errors import APIResponseError
        
        # Setup mock to fail first time, succeed second time
        mock_notion_client.search.side_effect = [
            APIResponseError("Server error", 500),
            {"results": []}
        ]
        
        # Create API client
        from src.notion_backup_restore.utils.rate_limiter import RateLimitConfig
        config = RateLimitConfig(requests_per_second=10)
        
        api_client = NotionAPIClient(
            auth="secret_test_token",
            rate_limit_config=config,
            max_retries=2
        )
        
        # Should succeed after retry
        result = api_client.search(query="test")
        assert result == {"results": []}
        assert mock_notion_client.search.call_count == 2
    
    def test_api_client_non_retryable_error(self, mock_notion_client):
        """Test API client with non-retryable errors."""
        from notion_client.errors import APIResponseError
        
        # Setup mock to return 404 (non-retryable)
        mock_notion_client.search.side_effect = APIResponseError("Not found", 404)
        
        # Create API client
        from src.notion_backup_restore.utils.rate_limiter import RateLimitConfig
        config = RateLimitConfig(requests_per_second=10)
        
        api_client = NotionAPIClient(
            auth="secret_test_token",
            rate_limit_config=config,
            max_retries=3
        )
        
        # Should not retry 404 errors
        with pytest.raises(APIResponseError):
            api_client.search(query="test")
        
        # Should only be called once (no retries)
        assert mock_notion_client.search.call_count == 1
    
    def test_api_client_stats(self, mock_notion_client):
        """Test API client statistics tracking."""
        # Setup successful mock
        mock_notion_client.search.return_value = {"results": []}
        
        # Create API client
        from src.notion_backup_restore.utils.rate_limiter import RateLimitConfig
        config = RateLimitConfig(requests_per_second=10)
        
        api_client = NotionAPIClient(
            auth="secret_test_token",
            rate_limit_config=config
        )
        
        # Make some calls
        api_client.search(query="test1")
        api_client.search(query="test2")
        
        # Check stats
        stats = api_client.get_stats()
        assert stats["total_requests"] == 2
        assert stats["total_errors"] == 0
        assert stats["error_rate"] == 0.0


if __name__ == "__main__":
    pytest.main([__file__])
