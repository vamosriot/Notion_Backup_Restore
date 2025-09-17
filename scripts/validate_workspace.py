#!/usr/bin/env python3
"""
Validation script to verify workspace structure before backup operations.

This script checks for the presence of required databases and their
property configurations to ensure compatibility with the backup system.
"""

import sys
from pathlib import Path

# Add src to Python path
src_path = Path(__file__).parent.parent / "src"
sys.path.insert(0, str(src_path))

from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn

try:
    from notion_backup_restore.config import get_backup_config, WORKSPACE_DATABASES
    from notion_backup_restore.utils.api_client import create_notion_client
    from notion_backup_restore.backup.database_finder import DatabaseFinder
except ImportError:
    # Fallback for development
    from src.notion_backup_restore.config import get_backup_config, WORKSPACE_DATABASES
    from src.notion_backup_restore.utils.api_client import create_notion_client
    from src.notion_backup_restore.backup.database_finder import DatabaseFinder

console = Console()


def main():
    """Main validation function."""
    console.print(Panel.fit(
        "[bold blue]Workspace Validation[/bold blue]\n"
        "[dim]Validating workspace structure for backup compatibility[/dim]",
        border_style="blue"
    ))
    
    try:
        # Load configuration
        console.print("Loading configuration...")
        config = get_backup_config()
        
        if not config.notion_token:
            console.print("[red]Error:[/red] NOTION_TOKEN not found. Run setup_integration.py first.")
            sys.exit(1)
        
        console.print("[green]✓[/green] Configuration loaded")
        
        # Create API client
        console.print("Connecting to Notion API...")
        api_client = create_notion_client(
            auth=config.notion_token,
            requests_per_second=config.requests_per_second,
            max_retries=2
        )
        console.print("[green]✓[/green] API connection established")
        
        # Find databases
        console.print("\nDiscovering databases...")
        database_finder = DatabaseFinder(api_client)
        
        found_databases = {}
        validation_results = {}
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console
        ) as progress:
            
            for db_name in WORKSPACE_DATABASES.keys():
                task = progress.add_task(f"Searching for {db_name}...", total=None)
                
                try:
                    # Search for database
                    search_result = api_client.search(
                        query=db_name,
                        filter={"value": "database", "property": "object"}
                    )
                    
                    # Find exact match
                    db_found = None
                    for result in search_result.get("results", []):
                        if result.get("object") == "database":
                            title_property = result.get("title", [])
                            title = "".join([
                                text.get("plain_text", "") 
                                for text in title_property
                            ])
                            
                            if title.strip().lower() == db_name.lower():
                                db_found = result
                                break
                    
                    if db_found:
                        found_databases[db_name] = db_found
                        progress.update(task, description=f"[green]✓[/green] Found {db_name}")
                    else:
                        progress.update(task, description=f"[red]✗[/red] {db_name} not found")
                        
                except Exception as e:
                    progress.update(task, description=f"[red]✗[/red] Error searching {db_name}")
                    console.print(f"[red]Error searching for {db_name}:[/red] {e}")
        
        # Validate database structures
        console.print(f"\nValidating database structures...")
        
        for db_name, db_data in found_databases.items():
            console.print(f"Validating {db_name}...")
            
            validation_result = validate_database_structure(db_name, db_data)
            validation_results[db_name] = validation_result
            
            if validation_result["valid"]:
                console.print(f"[green]✓[/green] {db_name} structure is valid")
            else:
                console.print(f"[yellow]⚠[/yellow] {db_name} has issues")
        
        # Show detailed results
        show_validation_results(found_databases, validation_results)
        
        # Show summary
        show_summary(found_databases, validation_results)
        
    except KeyboardInterrupt:
        console.print("\n[yellow]Validation cancelled by user[/yellow]")
        sys.exit(1)
    except Exception as e:
        console.print(f"\n[red]Validation failed:[/red] {e}")
        sys.exit(1)


def validate_database_structure(db_name, db_data):
    """Validate a single database structure."""
    # Find database config with case-insensitive matching
    config_key = None
    for key in WORKSPACE_DATABASES.keys():
        if key.lower() == db_name.lower():
            config_key = key
            break
    
    if config_key is None:
        return {
            "valid": False,
            "errors": [f"Unknown database: {db_name}"],
            "warnings": [],
            "missing_properties": [],
            "type_mismatches": [],
            "extra_properties": []
        }
    
    expected_properties = WORKSPACE_DATABASES[config_key]["properties"]
    actual_properties = db_data.get("properties", {})
    
    errors = []
    warnings = []
    missing_properties = []
    type_mismatches = []
    extra_properties = []
    
    # Check for missing properties
    for prop_name, prop_config in expected_properties.items():
        if prop_name not in actual_properties:
            missing_properties.append(prop_name)
            errors.append(f"Missing property: {prop_name}")
        else:
            # Check property type
            expected_type = prop_config["type"]
            actual_prop = actual_properties[prop_name]
            actual_type = actual_prop.get("type")
            
            if actual_type != expected_type:
                type_mismatches.append({
                    "property": prop_name,
                    "expected": expected_type,
                    "actual": actual_type
                })
                errors.append(f"Property '{prop_name}': expected {expected_type}, got {actual_type}")
    
    # Check for extra properties
    for prop_name in actual_properties:
        if prop_name not in expected_properties:
            extra_properties.append(prop_name)
            warnings.append(f"Extra property: {prop_name}")
    
    # Validate specific property configurations
    for prop_name, prop_config in expected_properties.items():
        if prop_name in actual_properties:
            actual_prop = actual_properties[prop_name]
            
            # Validate select/multi-select options
            if prop_config["type"] in ["select", "multi_select"]:
                validate_select_options(prop_name, actual_prop, warnings)
            
            # Validate relation properties
            elif prop_config["type"] == "relation":
                validate_relation_property(prop_name, actual_prop, prop_config, warnings)
            
            # Validate formula properties
            elif prop_config["type"] == "formula":
                validate_formula_property(prop_name, actual_prop, prop_config, warnings)
    
    return {
        "valid": len(errors) == 0,
        "errors": errors,
        "warnings": warnings,
        "missing_properties": missing_properties,
        "type_mismatches": type_mismatches,
        "extra_properties": extra_properties
    }


def validate_select_options(prop_name, actual_prop, warnings):
    """Validate select/multi-select property options."""
    prop_type = actual_prop.get("type")
    options = actual_prop.get(prop_type, {}).get("options", [])
    
    if len(options) == 0:
        warnings.append(f"Property '{prop_name}' has no options defined")
    
    # Check for duplicate option names
    option_names = [opt.get("name", "") for opt in options]
    if len(option_names) != len(set(option_names)):
        warnings.append(f"Property '{prop_name}' has duplicate option names")


def validate_relation_property(prop_name, actual_prop, expected_config, warnings):
    """Validate relation property configuration."""
    relation_config = actual_prop.get("relation", {})
    
    if not relation_config.get("database_id"):
        warnings.append(f"Relation property '{prop_name}' missing database_id")
    
    # Check if it's a two-way relation
    relation_type = relation_config.get("type", "single_property")
    if relation_type == "dual_property":
        if not relation_config.get("synced_property_name"):
            warnings.append(f"Dual relation property '{prop_name}' missing synced_property_name")


def validate_formula_property(prop_name, actual_prop, expected_config, warnings):
    """Validate formula property configuration."""
    formula_config = actual_prop.get("formula", {})
    expression = formula_config.get("expression", "")
    
    if not expression:
        warnings.append(f"Formula property '{prop_name}' has empty expression")
    
    # Check for specific formulas we expect
    if "ROI" in prop_name and "round(Value/(Effort*400)*10)/10" not in expression:
        warnings.append(f"ROI formula in '{prop_name}' doesn't match expected pattern")


def show_validation_results(found_databases, validation_results):
    """Show detailed validation results."""
    console.print("\n[bold]Detailed Validation Results[/bold]")
    
    for db_name in WORKSPACE_DATABASES.keys():
        console.print(f"\n[bold cyan]{db_name}[/bold cyan]")
        
        if db_name not in found_databases:
            console.print("  [red]✗ Database not found[/red]")
            console.print("  [dim]Make sure the database is shared with your integration[/dim]")
            continue
        
        if db_name not in validation_results:
            console.print("  [yellow]⚠ Validation skipped[/yellow]")
            continue
        
        result = validation_results[db_name]
        
        if result["valid"]:
            console.print("  [green]✓ Structure is valid[/green]")
        else:
            console.print("  [red]✗ Structure has issues[/red]")
        
        # Show errors
        if result["errors"]:
            console.print("  [red]Errors:[/red]")
            for error in result["errors"]:
                console.print(f"    • {error}")
        
        # Show warnings
        if result["warnings"]:
            console.print("  [yellow]Warnings:[/yellow]")
            for warning in result["warnings"]:
                console.print(f"    • {warning}")
        
        # Show property summary
        db_data = found_databases[db_name]
        actual_props = len(db_data.get("properties", {}))
        expected_props = len(WORKSPACE_DATABASES[db_name]["properties"])
        
        console.print(f"  [dim]Properties: {actual_props}/{expected_props}[/dim]")


def show_summary(found_databases, validation_results):
    """Show validation summary."""
    console.print("\n[bold]Validation Summary[/bold]")
    
    # Summary table
    table = Table(show_header=True, header_style="bold magenta")
    table.add_column("Database", style="cyan")
    table.add_column("Found", style="green")
    table.add_column("Valid", style="yellow")
    table.add_column("Errors", style="red")
    table.add_column("Warnings", style="yellow")
    
    total_databases = len(WORKSPACE_DATABASES)
    found_count = len(found_databases)
    valid_count = 0
    total_errors = 0
    total_warnings = 0
    
    for db_name in WORKSPACE_DATABASES.keys():
        found = "✓" if db_name in found_databases else "✗"
        
        if db_name in validation_results:
            result = validation_results[db_name]
            valid = "✓" if result["valid"] else "✗"
            errors = len(result["errors"])
            warnings = len(result["warnings"])
            
            if result["valid"]:
                valid_count += 1
            
            total_errors += errors
            total_warnings += warnings
        else:
            valid = "N/A"
            errors = 0
            warnings = 0
        
        table.add_row(
            db_name,
            found,
            valid,
            str(errors) if errors > 0 else "",
            str(warnings) if warnings > 0 else ""
        )
    
    console.print(table)
    
    # Overall status
    console.print(f"\n[bold]Overall Status:[/bold]")
    console.print(f"• Databases found: {found_count}/{total_databases}")
    console.print(f"• Valid structures: {valid_count}/{found_count}")
    console.print(f"• Total errors: {total_errors}")
    console.print(f"• Total warnings: {total_warnings}")
    
    if found_count == total_databases and valid_count == found_count and total_errors == 0:
        console.print("\n[green]✓ Workspace is ready for backup![/green]")
    elif total_errors > 0:
        console.print("\n[red]✗ Workspace has structural issues that need to be fixed[/red]")
        console.print("[dim]Fix the errors above before running backup operations[/dim]")
    elif found_count < total_databases:
        console.print("\n[yellow]⚠ Some databases are missing[/yellow]")
        console.print("[dim]Share missing databases with your integration to include them[/dim]")
    else:
        console.print("\n[yellow]⚠ Workspace has warnings but should work[/yellow]")
        console.print("[dim]Review warnings above and consider fixing them[/dim]")


if __name__ == "__main__":
    main()
