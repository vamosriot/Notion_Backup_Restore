"""
Dependency resolution system for determining database restoration order.

This module implements topological sorting to handle relationship dependencies
between databases and ensure proper restoration order.
"""

from typing import Dict, List, Set, Optional, Tuple
from dataclasses import dataclass
from collections import defaultdict, deque


@dataclass
class DatabaseDependency:
    """Represents a dependency between databases."""
    source_database: str  # Database that has the relation property
    target_database: str  # Database that is referenced
    property_name: str    # Name of the relation property
    bidirectional: bool = False  # Whether the relation is bidirectional


class DependencyResolver:
    """
    Resolves database dependencies and determines restoration order.
    
    This class analyzes database relationships and uses topological sorting
    to determine the correct order for database restoration.
    """
    
    def __init__(self):
        """Initialize dependency resolver."""
        self.dependencies: List[DatabaseDependency] = []
        self.databases: Set[str] = set()
    
    def add_database(self, database_name: str) -> None:
        """
        Add a database to the dependency graph.
        
        Args:
            database_name: Name of the database
        """
        self.databases.add(database_name)
    
    def add_dependency(self, source_database: str, target_database: str,
                      property_name: str, bidirectional: bool = False) -> None:
        """
        Add a dependency between databases.
        
        Args:
            source_database: Database that has the relation property
            target_database: Database that is referenced
            property_name: Name of the relation property
            bidirectional: Whether the relation is bidirectional
        """
        dependency = DatabaseDependency(
            source_database=source_database,
            target_database=target_database,
            property_name=property_name,
            bidirectional=bidirectional
        )
        
        self.dependencies.append(dependency)
        self.databases.add(source_database)
        self.databases.add(target_database)
    
    def build_dependency_graph(self) -> Dict[str, Set[str]]:
        """
        Build dependency graph from relationships.
        
        Returns:
            Dictionary mapping each database to its dependencies
        """
        graph = defaultdict(set)
        
        # Initialize all databases in the graph
        for database in self.databases:
            graph[database] = set()
        
        # Add dependencies
        for dep in self.dependencies:
            # Source database depends on target database existing first
            graph[dep.source_database].add(dep.target_database)
        
        return dict(graph)
    
    def get_restoration_order(self) -> List[str]:
        """
        Get the order in which databases should be restored.
        
        Uses topological sorting to ensure dependencies are satisfied.
        
        Returns:
            List of database names in restoration order
            
        Raises:
            ValueError: If circular dependencies are detected
        """
        graph = self.build_dependency_graph()
        
        # Calculate in-degree for each node
        in_degree = {db: 0 for db in self.databases}
        for db, deps in graph.items():
            for dep in deps:
                in_degree[db] += 1
        
        # Initialize queue with nodes that have no dependencies
        queue = deque([db for db, degree in in_degree.items() if degree == 0])
        result = []
        
        while queue:
            # Remove a node with no dependencies
            current = queue.popleft()
            result.append(current)
            
            # For each database that depends on the current one
            for db, deps in graph.items():
                if current in deps:
                    # Remove the dependency
                    deps.remove(current)
                    in_degree[db] -= 1
                    
                    # If no more dependencies, add to queue
                    if in_degree[db] == 0:
                        queue.append(db)
        
        # Check for circular dependencies
        if len(result) != len(self.databases):
            remaining = self.databases - set(result)
            raise ValueError(f"Circular dependencies detected among databases: {remaining}")
        
        return result
    
    def get_dependencies_for_database(self, database_name: str) -> List[str]:
        """
        Get direct dependencies for a specific database.
        
        Args:
            database_name: Name of the database
            
        Returns:
            List of database names that this database depends on
        """
        dependencies = []
        for dep in self.dependencies:
            if dep.source_database == database_name:
                dependencies.append(dep.target_database)
        
        return dependencies
    
    def get_dependents_of_database(self, database_name: str) -> List[str]:
        """
        Get databases that depend on the specified database.
        
        Args:
            database_name: Name of the database
            
        Returns:
            List of database names that depend on this database
        """
        dependents = []
        for dep in self.dependencies:
            if dep.target_database == database_name:
                dependents.append(dep.source_database)
        
        return dependents
    
    def has_circular_dependencies(self) -> Tuple[bool, Optional[List[str]]]:
        """
        Check if there are circular dependencies.
        
        Returns:
            Tuple of (has_cycles, cycle_path)
        """
        try:
            self.get_restoration_order()
            return False, None
        except ValueError as e:
            # Extract database names from error message
            error_msg = str(e)
            if "Circular dependencies detected" in error_msg:
                # Try to find the actual cycle
                cycle = self._find_cycle()
                return True, cycle
            return True, None
    
    def _find_cycle(self) -> Optional[List[str]]:
        """
        Find a cycle in the dependency graph using DFS.
        
        Returns:
            List of databases forming a cycle, or None if no cycle found
        """
        graph = self.build_dependency_graph()
        visited = set()
        rec_stack = set()
        path = []
        
        def dfs(node: str) -> Optional[List[str]]:
            visited.add(node)
            rec_stack.add(node)
            path.append(node)
            
            for neighbor in graph.get(node, []):
                if neighbor not in visited:
                    cycle = dfs(neighbor)
                    if cycle:
                        return cycle
                elif neighbor in rec_stack:
                    # Found a cycle
                    cycle_start = path.index(neighbor)
                    return path[cycle_start:] + [neighbor]
            
            rec_stack.remove(node)
            path.pop()
            return None
        
        for database in self.databases:
            if database not in visited:
                cycle = dfs(database)
                if cycle:
                    return cycle
        
        return None
    
    def validate_dependencies(self) -> List[str]:
        """
        Validate all dependencies and return any issues found.
        
        Returns:
            List of validation error messages
        """
        errors = []
        
        # Check for self-dependencies
        for dep in self.dependencies:
            if dep.source_database == dep.target_database:
                errors.append(
                    f"Database '{dep.source_database}' has a self-dependency "
                    f"via property '{dep.property_name}'"
                )
        
        # Check for unknown databases
        for dep in self.dependencies:
            if dep.source_database not in self.databases:
                errors.append(f"Unknown source database: '{dep.source_database}'")
            if dep.target_database not in self.databases:
                errors.append(f"Unknown target database: '{dep.target_database}'")
        
        # Check for circular dependencies
        has_cycles, cycle = self.has_circular_dependencies()
        if has_cycles:
            if cycle:
                cycle_str = " -> ".join(cycle)
                errors.append(f"Circular dependency detected: {cycle_str}")
            else:
                errors.append("Circular dependencies detected (unable to determine cycle)")
        
        return errors
    
    def get_dependency_stats(self) -> Dict[str, any]:
        """
        Get statistics about the dependency graph.
        
        Returns:
            Dictionary with dependency statistics
        """
        graph = self.build_dependency_graph()
        
        # Calculate statistics
        total_dependencies = sum(len(deps) for deps in graph.values())
        databases_with_deps = sum(1 for deps in graph.values() if deps)
        databases_without_deps = len(self.databases) - databases_with_deps
        
        # Find databases with most dependencies
        max_deps = max(len(deps) for deps in graph.values()) if graph else 0
        most_dependent = [db for db, deps in graph.items() if len(deps) == max_deps]
        
        return {
            "total_databases": len(self.databases),
            "total_dependencies": total_dependencies,
            "databases_with_dependencies": databases_with_deps,
            "databases_without_dependencies": databases_without_deps,
            "max_dependencies_per_database": max_deps,
            "most_dependent_databases": most_dependent,
            "dependency_graph": {db: list(deps) for db, deps in graph.items()},
        }
    
    def clear(self) -> None:
        """Clear all dependencies and databases."""
        self.dependencies.clear()
        self.databases.clear()


def create_workspace_dependency_resolver() -> DependencyResolver:
    """
    Create a dependency resolver for the specific workspace structure.
    
    Note: Documentation is a wiki but is still backed up and restored like a database.
    
    Returns:
        Configured DependencyResolver for the workspace
    """
    resolver = DependencyResolver()
    
    # Add all databases and wikis (both need to be restored)
    resolver.add_database("Documentation")
    resolver.add_database("Tasks")
    resolver.add_database("Notes")
    resolver.add_database("Sprints")
    
    # Add dependencies based on the workspace structure
    # Documentation -> Tasks (Related Tasks property)
    resolver.add_dependency("Documentation", "Tasks", "Related Tasks")
    
    # Tasks -> Documentation (Documentation property)
    resolver.add_dependency("Tasks", "Documentation", "Documentation")
    
    # Tasks -> Notes (Notes property)
    resolver.add_dependency("Tasks", "Notes", "Notes")
    
    # Tasks -> Sprints (Sprint property)
    resolver.add_dependency("Tasks", "Sprints", "Sprint")
    
    # Notes -> Tasks (Related Task property)
    resolver.add_dependency("Notes", "Tasks", "Related Task")
    
    # Sprints -> Tasks (Tasks property)
    resolver.add_dependency("Sprints", "Tasks", "Tasks")
    
    return resolver
