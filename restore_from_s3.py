#!/usr/bin/env python3
"""
Restore from S3 backup.

This script downloads a compressed backup from AWS S3,
decompresses it, and restores it to Notion.
"""

import os
import sys
import json
import zipfile
import tempfile
from pathlib import Path
from datetime import datetime
from typing import Optional, List

import boto3
from botocore.exceptions import ClientError, NoCredentialsError
import typer
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
from rich.panel import Panel
from rich.table import Table
from rich.prompt import Confirm

# Add src to Python path
src_path = Path(__file__).parent / "src"
sys.path.insert(0, str(src_path))

from src.notion_backup_restore.restore.manager import NotionRestoreManager
from src.notion_backup_restore.config import get_restore_config

console = Console()

def get_s3_config():
    """Get S3 configuration from environment variables."""
    return {
        'bucket_name': os.getenv('S3_BUCKET_NAME'),
        'aws_access_key_id': os.getenv('AWS_ACCESS_KEY_ID'),
        'aws_secret_access_key': os.getenv('AWS_SECRET_ACCESS_KEY'),
        'aws_region': os.getenv('AWS_REGION', 'us-east-1'),
        's3_prefix': os.getenv('S3_PREFIX', 'notion-backups/'),
        'use_iam_role': os.getenv('AWS_USE_IAM_ROLE', 'true').lower() == 'true',
    }

def validate_s3_config(config: dict) -> bool:
    """Validate S3 configuration."""
    # Bucket name is always required
    if not config.get('bucket_name'):
        console.print(f"[red]Error:[/red] S3_BUCKET_NAME not found in environment variables")
        return False
    
    # If using IAM role (OIDC/IRSA), credentials are not required
    if config.get('use_iam_role'):
        console.print("[dim]Using IAM role authentication (OIDC/IRSA)[/dim]")
        return True
    
    # If not using IAM role, require access keys
    required_fields = ['aws_access_key_id', 'aws_secret_access_key']
    for field in required_fields:
        if not config.get(field):
            console.print(f"[red]Error:[/red] {field.upper()} not found in environment variables")
            console.print("[yellow]Tip:[/yellow] Set AWS_USE_IAM_ROLE=true to use IAM role authentication")
            return False
    
    return True

def create_s3_client(config: dict):
    """Create S3 client with configuration.
    
    Supports both IAM role authentication (OIDC/IRSA) and access key authentication.
    """
    try:
        # If using IAM role, boto3 will automatically use the instance/pod credentials
        if config.get('use_iam_role'):
            console.print("[dim]Creating S3 client with IAM role credentials[/dim]")
            return boto3.client(
                's3',
                region_name=config['aws_region']
            )
        else:
            # Use explicit access keys
            console.print("[dim]Creating S3 client with access key credentials[/dim]")
            return boto3.client(
                's3',
                aws_access_key_id=config['aws_access_key_id'],
                aws_secret_access_key=config['aws_secret_access_key'],
                region_name=config['aws_region']
            )
    except Exception as e:
        console.print(f"[red]Error creating S3 client:[/red] {e}")
        raise

def list_s3_backups(s3_client, bucket_name: str, s3_prefix: str) -> List[dict]:
    """List available backups from S3 index."""
    try:
        index_key = f"{s3_prefix.rstrip('/')}/index.json"
        response = s3_client.get_object(Bucket=bucket_name, Key=index_key)
        backups = json.loads(response['Body'].read().decode('utf-8'))
        
        # Sort by creation date (newest first)
        backups.sort(key=lambda x: x.get('created_at', ''), reverse=True)
        return backups
        
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            console.print("[yellow]No backup index found in S3[/yellow]")
            return []
        else:
            raise

def display_backup_list(backups: List[dict]) -> Optional[dict]:
    """Display backup list and let user select one."""
    if not backups:
        console.print("[yellow]No backups available[/yellow]")
        return None
    
    table = Table(show_header=True, header_style="bold magenta")
    table.add_column("#", style="dim", width=3)
    table.add_column("Backup Name", style="cyan")
    table.add_column("Created", style="green")
    table.add_column("Size", style="yellow")
    table.add_column("Compression", style="blue")
    
    for i, backup in enumerate(backups[:20], 1):  # Show last 20 backups
        created_at = backup.get('created_at', 'Unknown')
        if created_at != 'Unknown':
            try:
                dt = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                created_at = dt.strftime("%Y-%m-%d %H:%M")
            except:
                pass
        
        size_mb = backup.get('compressed_size', 0) / 1024 / 1024
        compression = f"{backup.get('compression_ratio', 0):.1f}%"
        
        table.add_row(
            str(i),
            backup.get('backup_name', 'Unknown'),
            created_at,
            f"{size_mb:.1f} MB",
            compression
        )
    
    console.print(table)
    
    # Let user select backup
    while True:
        try:
            selection = typer.prompt(f"\nSelect backup (1-{min(len(backups), 20)}) or 'q' to quit")
            
            if selection.lower() == 'q':
                return None
            
            index = int(selection) - 1
            if 0 <= index < min(len(backups), 20):
                return backups[index]
            else:
                console.print(f"[red]Please enter a number between 1 and {min(len(backups), 20)}[/red]")
                
        except (ValueError, typer.Abort):
            console.print("[red]Invalid selection[/red]")

def download_from_s3(s3_client, bucket_name: str, s3_key: str, local_file: Path) -> bool:
    """Download file from S3 with progress tracking."""
    try:
        # Get file size
        response = s3_client.head_object(Bucket=bucket_name, Key=s3_key)
        file_size = response['ContentLength']
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            console=console,
            transient=True
        ) as progress:
            
            task = progress.add_task(f"Downloading from S3...", total=file_size)
            
            def download_callback(bytes_transferred):
                progress.update(task, completed=bytes_transferred)
            
            s3_client.download_file(
                bucket_name,
                s3_key,
                str(local_file),
                Callback=download_callback
            )
        
        return True
        
    except ClientError as e:
        console.print(f"[red]S3 download error:[/red] {e}")
        return False
    except Exception as e:
        console.print(f"[red]Download error:[/red] {e}")
        return False

def extract_backup(zip_file: Path, extract_dir: Path) -> Path:
    """Extract backup zip file."""
    console.print(f"[dim]Extracting backup:[/dim] {zip_file.name}")
    
    with zipfile.ZipFile(zip_file, 'r') as zipf:
        zipf.extractall(extract_dir)
    
    # Find the backup directory (should be the only directory in extract_dir)
    backup_dirs = [d for d in extract_dir.iterdir() if d.is_dir()]
    
    if len(backup_dirs) != 1:
        raise ValueError(f"Expected exactly one backup directory, found {len(backup_dirs)}")
    
    return backup_dirs[0]

app = typer.Typer()

@app.command()
def main(
    backup_name: Optional[str] = typer.Argument(
        None,
        help="Specific backup name to restore (if not provided, will show selection menu)"
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
    force: bool = typer.Option(
        False,
        "--force", "-f",
        help="Skip confirmation prompts"
    ),
    keep_download: bool = typer.Option(
        False,
        "--keep-download",
        help="Keep downloaded backup files after restore"
    )
):
    """
    Download backup from S3 and restore to Notion.
    
    This command performs a complete restore workflow:
    1. Lists available backups in S3
    2. Downloads selected backup
    3. Extracts the backup
    4. Restores to Notion
    5. Optionally cleans up downloaded files
    """
    
    console.print(Panel.fit(
        "[bold blue]Notion Restore from S3[/bold blue]\n"
        "[dim]Downloading backup and restoring to Notion...[/dim]",
        border_style="blue"
    ))
    
    try:
        # Validate S3 configuration
        s3_config = get_s3_config()
        if not validate_s3_config(s3_config):
            console.print("\n[yellow]S3 Configuration Help:[/yellow]")
            console.print("\n[bold]Option 1: IAM Role (OIDC/IRSA) - Recommended[/bold]")
            console.print("Add to your .env file:")
            console.print("S3_BUCKET_NAME=your-bucket-name")
            console.print("AWS_REGION=us-east-1")
            console.print("AWS_USE_IAM_ROLE=true")
            console.print("S3_PREFIX=notion-backups/")
            console.print("\n[bold]Option 2: Access Keys[/bold]")
            console.print("Add to your .env file:")
            console.print("S3_BUCKET_NAME=your-bucket-name")
            console.print("AWS_ACCESS_KEY_ID=your-access-key")
            console.print("AWS_SECRET_ACCESS_KEY=your-secret-key")
            console.print("AWS_REGION=us-east-1")
            console.print("AWS_USE_IAM_ROLE=false")
            console.print("S3_PREFIX=notion-backups/")
            raise typer.Exit(1)
        
        # Create S3 client
        s3_client = create_s3_client(s3_config)
        
        console.print(f"[dim]S3 Bucket:[/dim] {s3_config['bucket_name']}")
        console.print(f"[dim]S3 Region:[/dim] {s3_config['aws_region']}")
        console.print(f"[dim]S3 Prefix:[/dim] {s3_config['s3_prefix']}")
        console.print()
        
        # List available backups
        console.print("[bold]Step 1:[/bold] Finding available backups...")
        backups = list_s3_backups(s3_client, s3_config['bucket_name'], s3_config['s3_prefix'])
        
        if not backups:
            console.print("[red]No backups found in S3[/red]")
            raise typer.Exit(1)
        
        # Select backup
        selected_backup = None
        
        if backup_name:
            # Find backup by name
            for backup in backups:
                if backup.get('backup_name') == backup_name:
                    selected_backup = backup
                    break
            
            if not selected_backup:
                console.print(f"[red]Backup '{backup_name}' not found[/red]")
                console.print("Available backups:")
                for backup in backups[:10]:
                    console.print(f"  - {backup.get('backup_name')}")
                raise typer.Exit(1)
        else:
            # Interactive selection
            console.print("\n[bold]Available backups:[/bold]")
            selected_backup = display_backup_list(backups)
            
            if not selected_backup:
                console.print("[yellow]No backup selected[/yellow]")
                raise typer.Exit(0)
        
        # Display selected backup info
        console.print(f"\n[bold]Selected backup:[/bold] {selected_backup.get('backup_name')}")
        console.print(f"[dim]Created:[/dim] {selected_backup.get('created_at')}")
        console.print(f"[dim]Size:[/dim] {selected_backup.get('compressed_size', 0) / 1024 / 1024:.1f} MB")
        console.print(f"[dim]S3 Key:[/dim] {selected_backup.get('s3_key')}")
        
        # Confirmation
        if not force and not dry_run:
            if not Confirm.ask(f"\nProceed with restore from '{selected_backup.get('backup_name')}'?"):
                console.print("[yellow]Restore cancelled by user[/yellow]")
                raise typer.Exit(0)
        
        # Download backup
        console.print(f"\n[bold]Step 2:[/bold] Downloading backup from S3...")
        
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            zip_file = temp_path / f"{selected_backup.get('backup_name')}.zip"
            
            success = download_from_s3(
                s3_client,
                s3_config['bucket_name'],
                selected_backup.get('s3_key'),
                zip_file
            )
            
            if not success:
                console.print("[red]❌ Download failed![/red]")
                raise typer.Exit(1)
            
            console.print(f"[green]✓[/green] Download complete: {zip_file.stat().st_size / 1024 / 1024:.1f} MB")
            
            # Extract backup
            console.print(f"\n[bold]Step 3:[/bold] Extracting backup...")
            
            extract_dir = temp_path / "extracted"
            extract_dir.mkdir()
            
            backup_dir = extract_backup(zip_file, extract_dir)
            console.print(f"[green]✓[/green] Extraction complete: {backup_dir}")
            
            # Validate backup structure
            manifest_file = backup_dir / "manifest.json"
            if not manifest_file.exists():
                console.print("[red]Error: Invalid backup - manifest.json not found[/red]")
                raise typer.Exit(1)
            
            # Load manifest
            with open(manifest_file, 'r', encoding='utf-8') as f:
                manifest = json.load(f)
            
            console.print(f"[dim]Backup version:[/dim] {manifest.get('version', 'Unknown')}")
            console.print(f"[dim]Databases:[/dim] {len(manifest.get('databases', {}))}")
            
            if dry_run:
                console.print(f"\n[yellow]DRY RUN:[/yellow] Would restore from {backup_dir}")
                console.print("[dim]No changes will be made to your Notion workspace.[/dim]")
                return
            
            # Restore backup
            console.print(f"\n[bold]Step 4:[/bold] Restoring to Notion...")
            
            # Create restore configuration
            config_overrides = {
                "backup_dir": backup_dir,
                "parent_page_id": parent_id,
                "dry_run": False,
                "validate_after": validate,
                "verbose": verbose,
                "debug": debug,
            }
            
            config = get_restore_config(**config_overrides)
            
            # Initialize restore manager
            restore_manager = NotionRestoreManager(config)
            
            # Start restoration with progress display
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
            
            console.print(f"\n[green]✓[/green] Restoration completed successfully!")
            
            # Display results
            from src.notion_backup_restore.cli.restore_cli import _display_restoration_stats
            _display_restoration_stats(results)
            
            # Display validation results if enabled
            if validate:
                validation_file = backup_dir / "restoration_validation.json"
                if validation_file.exists():
                    console.print(f"\n[dim]Validation report:[/dim] {validation_file}")
            
            console.print(f"\n[green]🎉 Successfully restored from S3 backup![/green]")
    
    except KeyboardInterrupt:
        console.print("\n[yellow]Restore cancelled by user[/yellow]")
        raise typer.Exit(1)
    except Exception as e:
        console.print(f"\n[red]Error:[/red] {e}")
        if debug:
            console.print_exception()
        raise typer.Exit(1)

@app.command("list")
def list_backups():
    """List available backups in S3."""
    
    console.print(Panel.fit(
        "[bold blue]S3 Backup List[/bold blue]",
        border_style="blue"
    ))
    
    try:
        s3_config = get_s3_config()
        if not validate_s3_config(s3_config):
            raise typer.Exit(1)
        
        s3_client = create_s3_client(s3_config)
        backups = list_s3_backups(s3_client, s3_config['bucket_name'], s3_config['s3_prefix'])
        
        if not backups:
            console.print("[yellow]No backups found in S3[/yellow]")
            return
        
        table = Table(show_header=True, header_style="bold magenta")
        table.add_column("Backup Name", style="cyan")
        table.add_column("Created", style="green")
        table.add_column("Size", style="yellow")
        table.add_column("Compression", style="blue")
        table.add_column("Databases", style="magenta")
        
        for backup in backups[:20]:  # Show last 20 backups
            created_at = backup.get('created_at', 'Unknown')
            if created_at != 'Unknown':
                try:
                    dt = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                    created_at = dt.strftime("%Y-%m-%d %H:%M")
                except:
                    pass
            
            size_mb = backup.get('compressed_size', 0) / 1024 / 1024
            compression = f"{backup.get('compression_ratio', 0):.1f}%"
            
            # Get database count from manifest
            manifest = backup.get('manifest', {})
            db_count = len(manifest.get('databases', {}))
            
            table.add_row(
                backup.get('backup_name', 'Unknown'),
                created_at,
                f"{size_mb:.1f} MB",
                compression,
                str(db_count)
            )
        
        console.print(table)
        console.print(f"\n[dim]Showing last 20 of {len(backups)} backups[/dim]")
        
    except Exception as e:
        console.print(f"[red]Error:[/red] {e}")
        raise typer.Exit(1)

@app.command("info")
def backup_info(
    backup_name: str = typer.Argument(..., help="Backup name to get info for")
):
    """Get detailed information about a specific backup."""
    
    try:
        s3_config = get_s3_config()
        if not validate_s3_config(s3_config):
            raise typer.Exit(1)
        
        s3_client = create_s3_client(s3_config)
        backups = list_s3_backups(s3_client, s3_config['bucket_name'], s3_config['s3_prefix'])
        
        # Find backup
        selected_backup = None
        for backup in backups:
            if backup.get('backup_name') == backup_name:
                selected_backup = backup
                break
        
        if not selected_backup:
            console.print(f"[red]Backup '{backup_name}' not found[/red]")
            raise typer.Exit(1)
        
        # Display detailed info
        console.print(Panel.fit(
            f"[bold blue]Backup Information: {backup_name}[/bold blue]",
            border_style="blue"
        ))
        
        console.print(f"[bold]Basic Information:[/bold]")
        console.print(f"[dim]Name:[/dim] {selected_backup.get('backup_name')}")
        console.print(f"[dim]Created:[/dim] {selected_backup.get('created_at')}")
        console.print(f"[dim]S3 Key:[/dim] {selected_backup.get('s3_key')}")
        
        console.print(f"\n[bold]Size Information:[/bold]")
        console.print(f"[dim]Original Size:[/dim] {selected_backup.get('original_size', 0) / 1024 / 1024:.1f} MB")
        console.print(f"[dim]Compressed Size:[/dim] {selected_backup.get('compressed_size', 0) / 1024 / 1024:.1f} MB")
        console.print(f"[dim]Compression Ratio:[/dim] {selected_backup.get('compression_ratio', 0):.1f}%")
        console.print(f"[dim]Files Count:[/dim] {selected_backup.get('files_count', 0)}")
        
        # Display manifest info if available
        manifest = selected_backup.get('manifest', {})
        if manifest:
            console.print(f"\n[bold]Backup Content:[/bold]")
            console.print(f"[dim]Backup Version:[/dim] {manifest.get('version', 'Unknown')}")
            
            databases = manifest.get('databases', {})
            if databases:
                console.print(f"\n[bold]Databases ({len(databases)}):[/bold]")
                
                table = Table(show_header=True, header_style="bold magenta")
                table.add_column("Database", style="cyan")
                table.add_column("Pages", style="green")
                table.add_column("Properties", style="yellow")
                
                for db_name, db_info in databases.items():
                    table.add_row(
                        db_name,
                        str(db_info.get('pages_count', 0)),
                        str(db_info.get('properties_count', 0))
                    )
                
                console.print(table)
            
            stats = manifest.get('statistics', {})
            if stats:
                console.print(f"\n[bold]Statistics:[/bold]")
                console.print(f"[dim]Total Databases:[/dim] {stats.get('total_databases', 0)}")
                console.print(f"[dim]Total Pages:[/dim] {stats.get('total_pages', 0)}")
        
    except Exception as e:
        console.print(f"[red]Error:[/red] {e}")
        raise typer.Exit(1)

if __name__ == "__main__":
    app()
