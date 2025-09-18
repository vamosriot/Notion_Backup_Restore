#!/usr/bin/env python3
"""
Schema-only restore script.

This script restores only the database schemas (structure, properties, relationships, formulas)
without restoring the actual page data. It performs phases 1-3 of the restoration process:
- Phase 1: Create databases with basic properties
- Phase 2: Add relation properties  
- Phase 3: Add formula and rollup properties
- Skips Phase 4: Data restoration

Usage:
    python restore_schema_only.py <backup_dir> [options]
"""

import sys
import json
from pathlib import Path
from typing import Optional, Dict, Any, Callable

# Add src to Python path
src_path = Path(__file__).parent / "src"
sys.path.insert(0, str(src_path))

import typer
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
from rich.panel import Panel
from rich.prompt import Confirm

from src.notion_backup_restore.config import get_restore_config
from src.notion_backup_restore.restore.manager import NotionRestoreManager
from src.notion_backup_restore.utils.logger import setup_logger

# Rich console for pretty output
console = Console()

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
    force: bool = typer.Option(
        False,
        "--force", "-f",
        help="Skip confirmation prompts"
    )
):
    """
    Restore only database schemas (structure, properties, relationships, formulas).
    
    This performs phases 1-3 of restoration without restoring page data.
    """
    
    # Display startup banner
    console.print(Panel.fit(
        "[bold blue]Schema-Only Restoration[/bold blue]\n"
        "[dim]Restoring database structures without data...[/dim]",
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
            raise typer.Exit(1)
        
        # Load and display backup information
        with open(manifest_file, 'r', encoding='utf-8') as f:
            manifest = json.load(f)
        
        _display_backup_info(manifest, backup_dir)
        
        # Confirmation prompt
        if not force and not dry_run:
            console.print("\n[yellow]⚠️  Schema-Only Mode:[/yellow]")
            console.print("• Will create database structures, properties, and relationships")
            console.print("• Will NOT restore any page data or content")
            console.print("• You can restore data later using the full restore command")
            
            if not Confirm.ask("\nProceed with schema-only restoration?"):
                console.print("[yellow]Restoration cancelled by user[/yellow]")
                raise typer.Exit(0)
        
        # Create configuration
        config_overrides = {
            "backup_dir": backup_dir,
            "parent_page_id": parent_id,
            "dry_run": dry_run,
            "validate_after": False,  # Skip validation for schema-only
            "verbose": verbose,
            "debug": debug,
        }
        
        config = get_restore_config(**config_overrides)
        
        # Validate configuration
        if not config.notion_token:
            console.print("[red]Error:[/red] NOTION_TOKEN not found. Please set it in your .env file.")
            raise typer.Exit(1)
        
        console.print(f"\n[dim]Backup directory:[/dim] {backup_dir}")
        console.print(f"[dim]Parent page ID:[/dim] {parent_id or 'None (use original parent)'}")
        console.print(f"[dim]Dry run:[/dim] {dry_run}")
        console.print(f"[dim]Schema-only mode:[/dim] [yellow]Enabled[/yellow]")
        console.print()
        
        # Initialize custom restore manager
        restore_manager = SchemaOnlyRestoreManager(config)
        
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
            
            task = progress.add_task("Starting schema restoration...", total=100)
            
            def update_progress(phase: str, completed: int, total: int):
                if total > 0:
                    percentage = (completed / total) * 100
                    progress.update(
                        task,
                        description=f"{phase}: {completed}/{total}",
                        completed=percentage
                    )
            
            # Run schema-only restoration
            results = restore_manager.start_schema_restore(progress_callback=update_progress)
        
        # Display results
        console.print()
        if dry_run:
            console.print("[yellow]✓[/yellow] Schema preview completed!")
        else:
            console.print("[green]✓[/green] Schema restoration completed!")
            console.print("[dim]Database structures have been created without data.[/dim]")
        
        # Display restoration statistics
        _display_schema_stats(results)
        
        # Display database mappings
        if not dry_run and "database_mappings" in results:
            _display_database_mappings(results["database_mappings"])
            
        # Show next steps
        if not dry_run:
            console.print(f"\n[bold]Next Steps:[/bold]")
            console.print("• Your database schemas have been restored")
            console.print("• To restore data later, run:")
            console.print(f"  [cyan]python restore.py main {backup_dir} --parent-id <parent_id>[/cyan]")
            console.print("• The data restore will populate your existing schemas")
        
    except KeyboardInterrupt:
        console.print("\n[yellow]Schema restoration cancelled by user[/yellow]")
        raise typer.Exit(1)
        
    except Exception as e:
        console.print(f"\n[red]Error:[/red] {e}")
        if debug:
            console.print_exception()
        raise typer.Exit(1)


class SchemaOnlyRestoreManager(NotionRestoreManager):
    """Custom restore manager that only performs schema restoration (phases 1-3)."""
    
    def start_schema_restore(
        self,
        progress_callback: Optional[Callable[[str, int, int], None]] = None
    ) -> Dict[str, Any]:
        """
        Start schema-only restoration (phases 1-3 only).
        
        Args:
            progress_callback: Optional callback for progress updates
            
        Returns:
            Dictionary containing restoration results
        """
        self.logger.info(f"Starting schema-only restoration from: {self.config.backup_dir}")
        
        try:
            # Load backup data
            self._load_backup_data()
            
            # Determine restoration order
            self._determine_restoration_order()
            
            total_phases = 3  # Only schema phases
            current_phase = 0
            
            if not self.config.dry_run:
                # Phase 1: Create databases with basic properties
                current_phase += 1
                self.logger.info(f"Phase {current_phase}/{total_phases}: Creating databases")
                if progress_callback:
                    progress_callback("Creating databases", 0, len(self.restoration_order))
                self._phase1_create_databases(progress_callback)
                
                # Phase 2: Add relation properties
                current_phase += 1
                self.logger.info(f"Phase {current_phase}/{total_phases}: Adding relation properties")
                if progress_callback:
                    progress_callback("Adding relations", 0, len(self.restoration_order))
                self._phase2_add_relations(progress_callback)
                
                # Phase 3: Add formula and rollup properties
                current_phase += 1
                self.logger.info(f"Phase {current_phase}/{total_phases}: Adding formulas and rollups")
                if progress_callback:
                    progress_callback("Adding formulas", 0, len(self.restoration_order))
                self._phase3_add_formulas(progress_callback)
                
                # Save ID mappings (important for future data restoration)
                self._save_id_mappings()
                
                self.logger.info("Schema restoration completed - data restoration skipped")
            else:
                self.logger.info("Dry run mode: previewing schema restoration")
            
            # Generate restoration report (schema phases only)
            results = self._generate_schema_restoration_report()
            
            self.logger.info("Schema-only restoration completed successfully")
            return results
            
        except Exception as e:
            self.logger.error(f"Schema restoration failed: {e}")
            
            # Attempt rollback if not dry run
            if not self.config.dry_run:
                self._attempt_rollback()
            
            raise
    
    def _generate_schema_restoration_report(self) -> Dict[str, Any]:
        """Generate restoration report for schema-only restoration."""
        results = {
            "restoration_type": "schema_only",
            "phase_results": self.phase_results,
            "database_mappings": self.database_creator.get_database_mappings(),
            "api_statistics": {
                "total_requests": getattr(self.api_client, '_total_requests', 0),
                "total_errors": getattr(self.api_client, '_total_errors', 0),
                "error_rate": getattr(self.api_client, '_total_errors', 0) / max(getattr(self.api_client, '_total_requests', 1), 1)
            },
            "completion_time": self._get_current_timestamp(),
            "schemas_restored": len(self.schemas),
            "data_restoration_skipped": True
        }
        
        return results


def _display_backup_info(manifest: dict, backup_dir: Path):
    """Display backup information."""
    console.print(Panel.fit(
        "[bold blue]Backup Information[/bold blue]",
        border_style="blue"
    ))
    
    created_at = manifest.get("created_at", "Unknown")
    version = manifest.get("version", "Unknown")
    
    console.print(f"[dim]Backup created:[/dim] {created_at}")
    console.print(f"[dim]Backup version:[/dim] {version}")
    console.print(f"[dim]Backup location:[/dim] {backup_dir}")
    
    databases = manifest.get("databases", {})
    if databases:
        console.print(f"\n[bold]Schemas to restore:[/bold]")
        for db_name in databases.keys():
            console.print(f"  • {db_name}")


def _display_schema_stats(results: dict):
    """Display schema restoration statistics."""
    from rich.table import Table
    
    phase_results = results.get("phase_results", {})
    
    table = Table(title="Schema Restoration Statistics", show_header=True, header_style="bold magenta")
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
    
    console.print()
    console.print(table)
    
    # API statistics
    api_stats = results.get("api_statistics", {})
    if api_stats:
        console.print(f"\n[dim]API requests:[/dim] {api_stats.get('total_requests', 0)}")
        console.print(f"[dim]API errors:[/dim] {api_stats.get('total_errors', 0)}")


def _display_database_mappings(mappings: dict):
    """Display database ID mappings."""
    from rich.table import Table
    
    if not mappings:
        return
    
    console.print(f"\n[bold]Created Database IDs:[/bold]")
    
    table = Table(show_header=True, header_style="bold magenta")
    table.add_column("Database Name", style="cyan")
    table.add_column("New Database ID", style="green")
    
    for db_name, new_id in mappings.items():
        table.add_row(db_name, new_id)
    
    console.print(table)


if __name__ == "__main__":
    typer.run(main)
