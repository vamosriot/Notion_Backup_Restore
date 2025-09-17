#!/usr/bin/env python3
"""
Setup script to help users configure their Notion integration.

This script validates API access, tests connectivity to the workspace,
and provides guidance for sharing databases with the integration.
"""

import sys
from pathlib import Path

# Add src to Python path
src_path = Path(__file__).parent.parent / "src"
sys.path.insert(0, str(src_path))

from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.prompt import Prompt, Confirm
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
    """Main setup function."""
    console.print(Panel.fit(
        "[bold blue]Notion Integration Setup[/bold blue]\n"
        "[dim]This script will help you configure your Notion integration[/dim]",
        border_style="blue"
    ))
    
    try:
        # Step 1: Check configuration
        console.print("\n[bold]Step 1: Checking Configuration[/bold]")
        config = check_configuration()
        
        # Step 2: Test API access
        console.print("\n[bold]Step 2: Testing API Access[/bold]")
        api_client = test_api_access(config)
        
        # Step 3: Find databases
        console.print("\n[bold]Step 3: Finding Workspace Databases[/bold]")
        found_databases = find_databases(api_client)
        
        # Step 4: Validate database structure
        console.print("\n[bold]Step 4: Validating Database Structure[/bold]")
        validate_databases(found_databases)
        
        # Step 5: Summary and next steps
        console.print("\n[bold]Step 5: Setup Summary[/bold]")
        show_summary(found_databases)
        
    except KeyboardInterrupt:
        console.print("\n[yellow]Setup cancelled by user[/yellow]")
        sys.exit(1)
    except Exception as e:
        console.print(f"\n[red]Setup failed:[/red] {e}")
        sys.exit(1)


def check_configuration():
    """Check and validate configuration."""
    try:
        config = get_backup_config()
        
        if not config.notion_token:
            console.print("[red]✗[/red] NOTION_TOKEN not found")
            show_token_setup_instructions()
            sys.exit(1)
        
        if not (config.notion_token.startswith("secret_") or config.notion_token.startswith("ntn_")):
            console.print("[red]✗[/red] Invalid NOTION_TOKEN format")
            console.print("Token should start with 'secret_' or 'ntn_'")
            sys.exit(1)
        
        console.print("[green]✓[/green] Configuration loaded successfully")
        console.print(f"[dim]Output directory:[/dim] {config.output_dir}")
        console.print(f"[dim]Rate limit:[/dim] {config.requests_per_second} req/s")
        
        return config
        
    except Exception as e:
        console.print(f"[red]✗[/red] Configuration error: {e}")
        raise


def show_token_setup_instructions():
    """Show instructions for setting up Notion token."""
    console.print("\n[yellow]Notion Integration Setup Instructions:[/yellow]")
    console.print("1. Go to https://www.notion.so/my-integrations")
    console.print("2. Click 'Create new integration'")
    console.print("3. Fill in the integration details:")
    console.print("   - Name: 'Backup & Restore'")
    console.print("   - Associated workspace: Select your workspace")
    console.print("   - Capabilities: Read content, Update content, Insert content")
    console.print("4. Click 'Submit'")
    console.print("5. Copy the 'Internal Integration Token'")
    console.print("6. Create a .env file in this directory with:")
    console.print("   NOTION_TOKEN=secret_your_token_here  # or ntn_your_token_here")
    console.print("\n[bold]After setting up the token, run this script again.[/bold]")


def test_api_access(config):
    """Test API access and connectivity."""
    try:
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
            transient=True
        ) as progress:
            task = progress.add_task("Testing API connection...", total=None)
            
            api_client = create_notion_client(
                auth=config.notion_token,
                requests_per_second=config.requests_per_second,
                max_retries=1
            )
            
            # Test with a simple search
            search_result = api_client.search(query="", page_size=1)
            
        console.print("[green]✓[/green] API connection successful")
        
        results_count = len(search_result.get("results", []))
        console.print(f"[dim]Found {results_count} accessible items in workspace[/dim]")
        
        if results_count == 0:
            console.print("[yellow]⚠[/yellow] No accessible items found")
            console.print("Make sure you've shared your databases with the integration")
        
        return api_client
        
    except Exception as e:
        console.print(f"[red]✗[/red] API connection failed: {e}")
        
        if "Unauthorized" in str(e) or "401" in str(e):
            console.print("\n[yellow]This usually means:[/yellow]")
            console.print("• Invalid integration token")
            console.print("• Token not properly set in .env file")
            console.print("• Integration doesn't have access to the workspace")
        
        raise


def find_databases(api_client):
    """Find and validate workspace databases."""
    database_finder = DatabaseFinder(api_client)
    
    console.print("Searching for workspace databases...")
    
    found_databases = {}
    missing_databases = []
    
    for db_name in WORKSPACE_DATABASES.keys():
        try:
            with Progress(
                SpinnerColumn(),
                TextColumn(f"Searching for {db_name}..."),
                console=console,
                transient=True
            ) as progress:
                task = progress.add_task("", total=None)
                
                # Search for the database
                search_result = api_client.search(
                    query=db_name,
                    filter={"value": "database", "property": "object"}
                )
                
                # Look for exact match
                db_found = False
                for result in search_result.get("results", []):
                    if result.get("object") == "database":
                        title_property = result.get("title", [])
                        title = "".join([
                            text.get("plain_text", "") 
                            for text in title_property
                        ])
                        
                        if title.strip().lower() == db_name.lower():
                            found_databases[db_name] = result
                            db_found = True
                            console.print(f"[green]✓[/green] Found {db_name}")
                            break
                
                if not db_found:
                    missing_databases.append(db_name)
                    console.print(f"[red]✗[/red] {db_name} not found")
                    
        except Exception as e:
            console.print(f"[red]✗[/red] Error searching for {db_name}: {e}")
            missing_databases.append(db_name)
    
    if missing_databases:
        console.print(f"\n[yellow]Missing databases:[/yellow] {', '.join(missing_databases)}")
        show_database_sharing_instructions(missing_databases)
        
        if not Confirm.ask("Continue with found databases only?"):
            sys.exit(1)
    
    return found_databases


def show_database_sharing_instructions(missing_databases):
    """Show instructions for sharing databases with integration."""
    console.print("\n[yellow]Database Sharing Instructions:[/yellow]")
    console.print("To make databases accessible to your integration:")
    console.print("1. Open each database in Notion")
    console.print("2. Click the 'Share' button (top right)")
    console.print("3. Click 'Invite'")
    console.print("4. Search for your integration name")
    console.print("5. Select your integration and set permissions to 'Edit'")
    console.print("6. Click 'Invite'")
    
    console.print(f"\n[bold]Missing databases to share:[/bold]")
    for db_name in missing_databases:
        console.print(f"  • {db_name}")


def validate_databases(found_databases):
    """Validate database structure against expected schema."""
    if not found_databases:
        console.print("[yellow]⚠[/yellow] No databases found to validate")
        return
    
    validation_results = {}
    
    for db_name, db_data in found_databases.items():
        console.print(f"Validating {db_name}...")
        
        # Find database config with case-insensitive matching
        config_key = None
        for key in WORKSPACE_DATABASES.keys():
            if key.lower() == db_name.lower():
                config_key = key
                break
        
        if config_key is None:
            console.print(f"  [red]✗[/red] Unknown database: {db_name}")
            continue
            
        expected_properties = WORKSPACE_DATABASES[config_key]["properties"]
        actual_properties = db_data.get("properties", {})
        
        missing_props = []
        type_mismatches = []
        
        for prop_name, prop_config in expected_properties.items():
            if prop_name not in actual_properties:
                missing_props.append(prop_name)
            else:
                expected_type = prop_config["type"]
                actual_type = actual_properties[prop_name].get("type")
                
                if actual_type != expected_type:
                    type_mismatches.append(f"{prop_name}: expected {expected_type}, got {actual_type}")
        
        validation_results[db_name] = {
            "missing_props": missing_props,
            "type_mismatches": type_mismatches
        }
        
        if not missing_props and not type_mismatches:
            console.print(f"[green]✓[/green] {db_name} structure is valid")
        else:
            console.print(f"[yellow]⚠[/yellow] {db_name} has structure issues")
            
            if missing_props:
                console.print(f"  Missing properties: {', '.join(missing_props)}")
            
            if type_mismatches:
                console.print(f"  Type mismatches: {', '.join(type_mismatches)}")
    
    return validation_results


def show_summary(found_databases):
    """Show setup summary and next steps."""
    console.print(Panel.fit(
        "[bold green]Setup Complete![/bold green]",
        border_style="green"
    ))
    
    # Summary table
    table = Table(title="Database Status", show_header=True, header_style="bold magenta")
    table.add_column("Database", style="cyan")
    table.add_column("Status", style="green")
    table.add_column("Properties", style="yellow")
    
    for db_name in WORKSPACE_DATABASES.keys():
        if db_name in found_databases:
            db_data = found_databases[db_name]
            prop_count = len(db_data.get("properties", {}))
            table.add_row(db_name, "✓ Found", str(prop_count))
        else:
            table.add_row(db_name, "✗ Missing", "N/A")
    
    console.print(table)
    
    # Next steps
    console.print(f"\n[bold]Next Steps:[/bold]")
    console.print("1. Run backup: [cyan]python backup.py[/cyan]")
    console.print("2. Test restore: [cyan]python restore.py --backup-dir <backup_folder> --dry-run[/cyan]")
    
    if len(found_databases) < len(WORKSPACE_DATABASES):
        console.print("\n[yellow]Note:[/yellow] Some databases are missing. Share them with your integration to include them in backups.")
    
    console.print(f"\n[dim]Integration is ready for use![/dim]")


if __name__ == "__main__":
    main()
