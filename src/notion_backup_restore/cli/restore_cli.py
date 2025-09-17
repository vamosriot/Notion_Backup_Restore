"""
Command-line interface for restore operations.

This module provides a user-friendly CLI for restore operations using Typer
with options for backup directory selection, target parent ID, and dry-run mode.
"""

import json
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
from rich.table import Table
from rich.panel import Panel
from rich.prompt import Confirm

from ..restore.manager import NotionRestoreManager
from ..config import get_restore_config

# Create Typer app
restore_app = typer.Typer(
    name="restore",
    help="Restore Notion workspace databases from backup with relationship preservation.",
    add_completion=False
)

# Rich console for pretty output
console = Console()


@restore_app.command()
def main(
    backup_dir: Path = typer.Argument(
        ...,
        help="Path to backup directory containing manifest.json"
    ),
    parent_id: Optional[str] = typer.Option(
        None,
        "--parent-id", "-p",
        help="Parent page ID for restored databases"
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run", "-n",
        help="Preview changes without executing restoration"
    ),
    validate: bool = typer.Option(
        True,
        "--validate/--no-validate",
        help="Run integrity validation after restoration"
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
    ),
    force: bool = typer.Option(
        False,
        "--force", "-f",
        help="Skip confirmation prompts"
    )
):
    """
    Restore Notion workspace databases from backup.
    
    This command restores databases from a backup directory using a 4-phase
    approach to ensure proper dependency handling and relationship integrity.
    """
    
    # Display startup banner
    console.print(Panel.fit(
        "[bold blue]Notion Backup & Restore System[/bold blue]\n"
        "[dim]Restoring from backup...[/dim]",
        border_style="blue"
    ))
    
    try:
        # Validate backup directory
        if not backup_dir.exists():
            console.print(f"[red]Error:[/red] Backup directory not found: {backup_dir}")
            raise typer.Exit(1)
        
        manifest_file = backup_dir / "manifest.json"
        if not manifest_file.exists():
            console.print(f"[red]Error:[/red] Backup manifest not found: {manifest_file}")
            console.print("Please ensure you're pointing to a valid backup directory.")
            raise typer.Exit(1)
        
        # Load and display backup information
        with open(manifest_file, 'r', encoding='utf-8') as f:
            manifest = json.load(f)
        
        _display_backup_info(manifest, backup_dir)
        
        # Confirmation prompt (unless forced or dry run)
        if not force and not dry_run:
            if not Confirm.ask("\nProceed with restoration?"):
                console.print("[yellow]Restoration cancelled by user[/yellow]")
                raise typer.Exit(0)
        
        # Create configuration
        config_overrides = {
            "backup_dir": backup_dir,
            "parent_page_id": parent_id,
            "dry_run": dry_run,
            "validate_after": validate,
            "verbose": verbose,
            "debug": debug,
        }
        
        if log_file:
            config_overrides["log_file"] = str(log_file)
        
        config = get_restore_config(**config_overrides)
        
        # Validate configuration
        if not config.notion_token:
            console.print("[red]Error:[/red] NOTION_TOKEN not found. Please set it in your .env file.")
            console.print("Get your token from: https://www.notion.so/my-integrations")
            raise typer.Exit(1)
        
        console.print(f"\n[dim]Backup directory:[/dim] {backup_dir}")
        console.print(f"[dim]Parent page ID:[/dim] {parent_id or 'None (use original parent)'}")
        console.print(f"[dim]Dry run:[/dim] {dry_run}")
        console.print(f"[dim]Validation:[/dim] {validate}")
        console.print()
        
        # Initialize restore manager
        restore_manager = NotionRestoreManager(config)
        
        # Progress tracking
        def progress_callback(phase: str, completed: int, total: int):
            pass  # Progress will be handled by the progress bar
        
        # Start restoration with progress display
        if dry_run:
            console.print("[yellow]DRY RUN MODE:[/yellow] No changes will be made")
            console.print()
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            console=console,
            transient=True
        ) as progress:
            
            task = progress.add_task("Starting restoration...", total=100)
            
            def update_progress(phase: str, completed: int, total: int):
                if total > 0:
                    percentage = (completed / total) * 100
                    progress.update(
                        task,
                        description=f"{phase}: {completed}/{total}",
                        completed=percentage
                    )
            
            # Run restoration
            results = restore_manager.start_restore(progress_callback=update_progress)
        
        # Display results
        console.print()
        if dry_run:
            console.print("[yellow]✓[/yellow] Dry run completed successfully!")
            console.print("[dim]No changes were made to your Notion workspace.[/dim]")
        else:
            console.print("[green]✓[/green] Restoration completed successfully!")
        
        # Display restoration statistics
        _display_restoration_stats(results)
        
        # Display validation results if enabled
        if validate and not dry_run:
            validation_file = backup_dir / "restoration_validation.json"
            if validation_file.exists():
                console.print(f"\n[dim]Validation report:[/dim] {validation_file}")
        
        # Display database mappings
        if not dry_run and "database_mappings" in results:
            _display_database_mappings(results["database_mappings"])
        
    except KeyboardInterrupt:
        console.print("\n[yellow]Restoration cancelled by user[/yellow]")
        raise typer.Exit(1)
        
    except Exception as e:
        console.print(f"\n[red]Error:[/red] {e}")
        if debug:
            console.print_exception()
        raise typer.Exit(1)


def _display_backup_info(manifest: dict, backup_dir: Path):
    """Display backup information from manifest."""
    
    console.print(Panel.fit(
        "[bold blue]Backup Information[/bold blue]",
        border_style="blue"
    ))
    
    # Basic info
    created_at = manifest.get("created_at", "Unknown")
    version = manifest.get("version", "Unknown")
    
    console.print(f"[dim]Backup created:[/dim] {created_at}")
    console.print(f"[dim]Backup version:[/dim] {version}")
    console.print(f"[dim]Backup location:[/dim] {backup_dir}")
    
    # Database info
    databases = manifest.get("databases", {})
    if databases:
        console.print(f"\n[bold]Databases in backup:[/bold]")
        
        table = Table(show_header=True, header_style="bold magenta")
        table.add_column("Database", style="cyan")
        table.add_column("Pages", style="green")
        table.add_column("Properties", style="yellow")
        table.add_column("ID", style="dim")
        
        for db_name, db_info in databases.items():
            table.add_row(
                db_name,
                str(db_info.get("pages_count", 0)),
                str(db_info.get("properties_count", 0)),
                db_info.get("id", "")[:8] + "..."
            )
        
        console.print(table)
    
    # Statistics
    stats = manifest.get("statistics", {})
    if stats:
        console.print(f"\n[dim]Total databases:[/dim] {stats.get('total_databases', 0)}")
        console.print(f"[dim]Total pages:[/dim] {stats.get('total_pages', 0)}")


def _display_restoration_stats(results: dict):
    """Display restoration statistics."""
    
    phase_results = results.get("phase_results", {})
    
    table = Table(title="Restoration Statistics", show_header=True, header_style="bold magenta")
    table.add_column("Phase", style="cyan")
    table.add_column("Success", style="green")
    table.add_column("Failed", style="red")
    table.add_column("Total", style="yellow")
    
    # Phase 1: Database Creation
    phase1 = phase_results.get("phase1_database_creation", {})
    table.add_row(
        "1. Database Creation",
        str(phase1.get("successful_creations", 0)),
        str(phase1.get("failed_creations", 0)),
        str(phase1.get("total_databases", 0))
    )
    
    # Phase 2: Relations
    phase2 = phase_results.get("phase2_relation_properties", {})
    table.add_row(
        "2. Relation Properties",
        str(phase2.get("total_properties_added", 0)),
        str(phase2.get("total_properties_failed", 0)),
        str(phase2.get("total_properties_added", 0) + phase2.get("total_properties_failed", 0))
    )
    
    # Phase 3: Formulas
    phase3 = phase_results.get("phase3_formula_properties", {})
    table.add_row(
        "3. Formula Properties",
        str(phase3.get("total_properties_added", 0)),
        str(phase3.get("total_properties_failed", 0)),
        str(phase3.get("total_properties_added", 0) + phase3.get("total_properties_failed", 0))
    )
    
    # Phase 4: Data
    phase4 = phase_results.get("phase4_data_restoration", {})
    table.add_row(
        "4. Data Restoration",
        str(phase4.get("total_created", 0)),
        str(phase4.get("total_failed", 0)),
        str(phase4.get("total_pages", 0))
    )
    
    console.print()
    console.print(table)
    
    # API statistics
    api_stats = results.get("api_statistics", {})
    if api_stats:
        console.print(f"\n[dim]API requests:[/dim] {api_stats.get('total_requests', 0)}")
        console.print(f"[dim]API errors:[/dim] {api_stats.get('total_errors', 0)}")
        console.print(f"[dim]Error rate:[/dim] {api_stats.get('error_rate', 0):.2%}")


def _display_database_mappings(mappings: dict):
    """Display database ID mappings."""
    
    if not mappings:
        return
    
    console.print(f"\n[bold]Database Mappings:[/bold]")
    
    table = Table(show_header=True, header_style="bold magenta")
    table.add_column("Database Name", style="cyan")
    table.add_column("New Database ID", style="green")
    
    for db_name, new_id in mappings.items():
        table.add_row(db_name, new_id)
    
    console.print(table)
    console.print(f"\n[dim]Use these IDs to access your restored databases in Notion.[/dim]")


@restore_app.command("list-backups")
def list_backups(
    backups_dir: Path = typer.Option(
        Path("./backups"),
        "--backups-dir", "-d",
        help="Directory containing backup folders"
    )
):
    """List available backup directories."""
    
    console.print(Panel.fit(
        "[bold blue]Available Backups[/bold blue]",
        border_style="blue"
    ))
    
    if not backups_dir.exists():
        console.print(f"[yellow]Backups directory not found:[/yellow] {backups_dir}")
        console.print("Run a backup first to create backup files.")
        return
    
    # Find backup directories
    backup_dirs = []
    for item in backups_dir.iterdir():
        if item.is_dir() and (item / "manifest.json").exists():
            backup_dirs.append(item)
    
    if not backup_dirs:
        console.print(f"[yellow]No backup directories found in:[/yellow] {backups_dir}")
        return
    
    # Sort by modification time (newest first)
    backup_dirs.sort(key=lambda x: x.stat().st_mtime, reverse=True)
    
    table = Table(show_header=True, header_style="bold magenta")
    table.add_column("Backup Directory", style="cyan")
    table.add_column("Created", style="green")
    table.add_column("Databases", style="yellow")
    table.add_column("Pages", style="blue")
    
    for backup_dir in backup_dirs:
        try:
            manifest_file = backup_dir / "manifest.json"
            with open(manifest_file, 'r', encoding='utf-8') as f:
                manifest = json.load(f)
            
            created_at = manifest.get("created_at", "Unknown")
            if created_at != "Unknown":
                # Format the timestamp
                from datetime import datetime
                try:
                    dt = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                    created_at = dt.strftime("%Y-%m-%d %H:%M:%S")
                except:
                    pass
            
            stats = manifest.get("statistics", {})
            databases_count = stats.get("total_databases", 0)
            pages_count = stats.get("total_pages", 0)
            
            table.add_row(
                backup_dir.name,
                created_at,
                str(databases_count),
                str(pages_count)
            )
            
        except Exception as e:
            table.add_row(
                backup_dir.name,
                "[red]Error reading manifest[/red]",
                "?",
                "?"
            )
    
    console.print(table)
    console.print(f"\n[dim]Found {len(backup_dirs)} backup(s) in {backups_dir}[/dim]")


@restore_app.command("validate-backup")
def validate_backup(
    backup_dir: Path = typer.Argument(
        ...,
        help="Path to backup directory to validate"
    )
):
    """Validate backup integrity without restoring."""
    
    console.print(Panel.fit(
        "[bold blue]Backup Validation[/bold blue]",
        border_style="blue"
    ))
    
    try:
        # Check backup directory
        if not backup_dir.exists():
            console.print(f"[red]Error:[/red] Backup directory not found: {backup_dir}")
            raise typer.Exit(1)
        
        manifest_file = backup_dir / "manifest.json"
        if not manifest_file.exists():
            console.print(f"[red]Error:[/red] Backup manifest not found: {manifest_file}")
            raise typer.Exit(1)
        
        # Load manifest
        with open(manifest_file, 'r', encoding='utf-8') as f:
            manifest = json.load(f)
        
        console.print(f"[green]✓[/green] Backup manifest is valid")
        
        # Check database files
        databases = manifest.get("databases", {})
        missing_files = []
        
        for db_name, db_info in databases.items():
            schema_file = backup_dir / "databases" / db_info.get("schema_file", "")
            data_file = backup_dir / "databases" / db_info.get("data_file", "")
            
            if not schema_file.exists():
                missing_files.append(f"Schema file for {db_name}: {schema_file}")
            
            if not data_file.exists():
                missing_files.append(f"Data file for {db_name}: {data_file}")
        
        if missing_files:
            console.print(f"\n[red]Missing files:[/red]")
            for file in missing_files:
                console.print(f"  • {file}")
            raise typer.Exit(1)
        
        console.print(f"[green]✓[/green] All database files are present")
        
        # Display backup info
        _display_backup_info(manifest, backup_dir)
        
        console.print(f"\n[green]Backup validation passed![/green]")
        console.print(f"[dim]This backup can be used for restoration.[/dim]")
        
    except Exception as e:
        console.print(f"\n[red]Validation error:[/red] {e}")
        raise typer.Exit(1)


if __name__ == "__main__":
    restore_app()
