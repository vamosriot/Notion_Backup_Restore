"""
Command-line interface for backup operations.

This module provides a user-friendly CLI for backup operations using Typer
with options for output directory, validation settings, and progress display.
"""

from pathlib import Path
from typing import Optional, List
import sys

import typer
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
from rich.table import Table
from rich.panel import Panel

from ..backup.manager import NotionBackupManager
from ..config import get_backup_config, WORKSPACE_DATABASES

# Create Typer app
backup_app = typer.Typer(
    name="backup",
    help="Backup Notion workspace databases with complete schema and data preservation.",
    add_completion=False
)

# Rich console for pretty output
console = Console()


@backup_app.command()
def main(
    output_dir: Optional[Path] = typer.Option(
        None,
        "--output-dir", "-o",
        help="Directory to store backup files (default: ./backups)"
    ),
    include_blocks: Optional[bool] = typer.Option(
        None,
        "--include-blocks/--no-include-blocks", "-b",
        help="Include page block content in backup (default: from .env BACKUP_INCLUDE_BLOCKS)"
    ),
    validate: bool = typer.Option(
        True,
        "--validate/--no-validate",
        help="Run integrity validation after backup"
    ),
    databases: Optional[List[str]] = typer.Option(
        None,
        "--database", "-d",
        help="Specific databases to backup (can be used multiple times)"
    ),
    verbose: bool = typer.Option(
        False,
        "--verbose", "-v",
        help="Enable verbose logging"
    ),
    debug: bool = typer.Option(
        False,
        "--debug",
        help="Enable debug logging"
    ),
    log_file: Optional[Path] = typer.Option(
        None,
        "--log-file", "-l",
        help="Log file path (default: no file logging)"
    )
):
    """
    Backup Notion workspace databases.
    
    This command creates a complete backup of your Notion workspace databases
    including schemas, properties, relationships, formulas, and data.
    """
    
    # Display startup banner
    console.print(Panel.fit(
        "[bold blue]Notion Backup & Restore System[/bold blue]\n"
        "[dim]Creating backup of your Notion workspace...[/dim]",
        border_style="blue"
    ))
    
    try:
        # Create configuration
        config_overrides = {
            "verbose": verbose,
            "debug": debug,
            "validate_integrity": validate,
        }
        
        # Only override include_blocks if explicitly provided
        if include_blocks is not None:
            config_overrides["include_blocks"] = include_blocks
        
        if output_dir:
            config_overrides["output_dir"] = output_dir
        
        if log_file:
            config_overrides["log_file"] = str(log_file)
        
        config = get_backup_config(**config_overrides)
        
        # Validate configuration
        if not config.notion_token:
            console.print("[red]Error:[/red] NOTION_TOKEN not found. Please set it in your .env file.")
            console.print("Get your token from: https://www.notion.so/my-integrations")
            raise typer.Exit(1)
        
        # Determine databases to backup
        target_databases = databases if databases else list(WORKSPACE_DATABASES.keys())
        
        console.print(f"[dim]Target databases:[/dim] {', '.join(target_databases)}")
        console.print(f"[dim]Output directory:[/dim] {config.output_dir}")
        console.print(f"[dim]Include blocks:[/dim] {config.include_blocks}")
        console.print(f"[dim]Validation:[/dim] {config.validate_integrity}")
        console.print()
        
        # Initialize backup manager
        backup_manager = NotionBackupManager(config)
        
        # Progress tracking
        progress_data = {
            "current_phase": "",
            "current_item": "",
            "completed": 0,
            "total": len(target_databases)
        }
        
        def progress_callback(phase: str, completed: int, total: int):
            progress_data["current_phase"] = phase
            progress_data["completed"] = completed
            progress_data["total"] = total
        
        # Start backup with progress display
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            console=console,
            transient=True
        ) as progress:
            
            task = progress.add_task("Starting backup...", total=len(target_databases))
            
            def update_progress(phase: str, completed: int, total: int):
                progress.update(
                    task,
                    description=f"{phase}: {completed}/{total}",
                    completed=completed,
                    total=total
                )
            
            # Run backup
            backup_dir = backup_manager.start_backup(
                database_names=target_databases,
                progress_callback=update_progress
            )
        
        # Display success message
        console.print()
        console.print("[green]✓[/green] Backup completed successfully!")
        console.print(f"[dim]Backup location:[/dim] {backup_dir}")
        
        # Display backup statistics
        stats = backup_manager.get_backup_stats()
        _display_backup_stats(stats)
        
        # Display validation results if enabled
        if config.validate_integrity:
            validation_file = backup_dir / "validation_report.json"
            if validation_file.exists():
                console.print(f"\n[dim]Validation report:[/dim] {validation_file}")
        
        console.print(f"\n[green]Backup saved to:[/green] [bold]{backup_dir}[/bold]")
        
    except KeyboardInterrupt:
        console.print("\n[yellow]Backup cancelled by user[/yellow]")
        raise typer.Exit(1)
        
    except Exception as e:
        console.print(f"\n[red]Error:[/red] {e}")
        if debug:
            console.print_exception()
        raise typer.Exit(1)


def _display_backup_stats(stats: dict):
    """Display backup statistics in a formatted table."""
    
    table = Table(title="Backup Statistics", show_header=True, header_style="bold magenta")
    table.add_column("Metric", style="cyan")
    table.add_column("Value", style="green")
    
    table.add_row("Databases Found", str(stats.get("databases_discovered", 0)))
    table.add_row("Schemas Extracted", str(stats.get("schemas_extracted", 0)))
    table.add_row("Content Extracted", str(stats.get("content_extracted", 0)))
    table.add_row("Total Pages", str(stats.get("total_pages", 0)))
    
    # API statistics
    api_stats = stats.get("api_stats", {})
    table.add_row("API Requests", str(api_stats.get("total_requests", 0)))
    table.add_row("API Errors", str(api_stats.get("total_errors", 0)))
    table.add_row("Error Rate", f"{api_stats.get('error_rate', 0):.2%}")
    
    console.print()
    console.print(table)


@backup_app.command("list-databases")
def list_databases():
    """List the expected workspace databases."""
    
    console.print(Panel.fit(
        "[bold blue]Expected Workspace Databases[/bold blue]",
        border_style="blue"
    ))
    
    table = Table(show_header=True, header_style="bold magenta")
    table.add_column("Database Name", style="cyan")
    table.add_column("Properties", style="green")
    table.add_column("Relations", style="yellow")
    
    for db_name, db_config in WORKSPACE_DATABASES.items():
        properties = db_config.get("properties", {})
        property_count = len(properties)
        
        relation_count = sum(
            1 for prop in properties.values()
            if prop.get("type") == "relation"
        )
        
        table.add_row(
            db_name,
            str(property_count),
            str(relation_count)
        )
    
    console.print(table)
    console.print(f"\n[dim]Total databases:[/dim] {len(WORKSPACE_DATABASES)}")


@backup_app.command("validate-config")
def validate_config():
    """Validate backup configuration and Notion API access."""
    
    console.print(Panel.fit(
        "[bold blue]Configuration Validation[/bold blue]",
        border_style="blue"
    ))
    
    try:
        # Test configuration
        config = get_backup_config()
        
        console.print("[green]✓[/green] Configuration loaded successfully")
        console.print(f"[dim]Output directory:[/dim] {config.output_dir}")
        console.print(f"[dim]Rate limit:[/dim] {config.requests_per_second} req/s")
        console.print(f"[dim]Max retries:[/dim] {config.max_retries}")
        
        # Test API access
        from ..utils.api_client import create_notion_client
        
        console.print("\n[dim]Testing Notion API access...[/dim]")
        
        api_client = create_notion_client(
            auth=config.notion_token,
            requests_per_second=config.requests_per_second,
            max_retries=1
        )
        
        # Test with a simple search
        search_result = api_client.search(query="", page_size=1)
        
        console.print("[green]✓[/green] Notion API access successful")
        console.print(f"[dim]Found {len(search_result.get('results', []))} accessible items[/dim]")
        
        console.print("\n[green]All checks passed![/green] Ready to backup.")
        
    except Exception as e:
        console.print(f"\n[red]Configuration error:[/red] {e}")
        
        if "NOTION_TOKEN" in str(e):
            console.print("\n[yellow]Setup instructions:[/yellow]")
            console.print("1. Go to https://www.notion.so/my-integrations")
            console.print("2. Create a new integration")
            console.print("3. Copy the 'Internal Integration Token'")
            console.print("4. Add it to your .env file as NOTION_TOKEN=secret_...")
            console.print("5. Share your databases with the integration")
        
        raise typer.Exit(1)


if __name__ == "__main__":
    backup_app()
