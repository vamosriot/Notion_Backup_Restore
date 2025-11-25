#!/usr/bin/env python3
"""
Backup to S3 with compression.

This script creates a Notion backup, compresses it to a zip file,
and uploads it to AWS S3 with optional cleanup.
"""

import os
import sys
import json
import zipfile
import tempfile
from pathlib import Path
from datetime import datetime
from typing import Optional

import boto3
from botocore.exceptions import ClientError, NoCredentialsError
import typer
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
from rich.panel import Panel

# Add src to Python path
src_path = Path(__file__).parent / "src"
sys.path.insert(0, str(src_path))

from src.notion_backup_restore.cli.backup_cli import backup_app
from src.notion_backup_restore.backup.manager import NotionBackupManager
from src.notion_backup_restore.config import get_backup_config

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

def compress_backup(backup_dir: Path, output_file: Path) -> dict:
    """Compress backup directory to zip file."""
    console.print(f"[dim]Compressing backup:[/dim] {backup_dir.name}")
    
    stats = {
        'original_size': 0,
        'compressed_size': 0,
        'files_count': 0
    }
    
    with zipfile.ZipFile(output_file, 'w', zipfile.ZIP_DEFLATED, compresslevel=6) as zipf:
        for file_path in backup_dir.rglob('*'):
            if file_path.is_file():
                # Calculate original size
                stats['original_size'] += file_path.stat().st_size
                stats['files_count'] += 1
                
                # Add to zip with relative path
                arcname = file_path.relative_to(backup_dir.parent)
                zipf.write(file_path, arcname)
    
    stats['compressed_size'] = output_file.stat().st_size
    
    return stats

def upload_to_s3(file_path: Path, s3_client, bucket_name: str, s3_key: str) -> bool:
    """Upload file to S3 with progress tracking."""
    try:
        file_size = file_path.stat().st_size
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            console=console,
            transient=True
        ) as progress:
            
            task = progress.add_task(f"Uploading to S3...", total=file_size)
            
            def upload_callback(bytes_transferred):
                progress.update(task, completed=bytes_transferred)
            
            s3_client.upload_file(
                str(file_path),
                bucket_name,
                s3_key,
                Callback=upload_callback
            )
        
        return True
        
    except ClientError as e:
        console.print(f"[red]S3 upload error:[/red] {e}")
        return False
    except Exception as e:
        console.print(f"[red]Upload error:[/red] {e}")
        return False

def generate_s3_key(s3_prefix: str, backup_name: str) -> str:
    """Generate S3 key for backup file."""
    timestamp = datetime.now().strftime("%Y/%m/%d")
    return f"{s3_prefix.rstrip('/')}/{timestamp}/{backup_name}.zip"

def create_backup_metadata(backup_dir: Path, s3_key: str, stats: dict) -> dict:
    """Create metadata for the backup."""
    manifest_file = backup_dir / "manifest.json"
    manifest = {}
    
    if manifest_file.exists():
        with open(manifest_file, 'r', encoding='utf-8') as f:
            manifest = json.load(f)
    
    return {
        'backup_name': backup_dir.name,
        'created_at': datetime.now().isoformat(),
        's3_key': s3_key,
        'original_size': stats['original_size'],
        'compressed_size': stats['compressed_size'],
        'files_count': stats['files_count'],
        'compression_ratio': round((1 - stats['compressed_size'] / stats['original_size']) * 100, 2) if stats['original_size'] > 0 else 0,
        'manifest': manifest
    }

def save_backup_index(s3_client, bucket_name: str, s3_prefix: str, metadata: dict):
    """Save backup metadata to S3 index."""
    try:
        index_key = f"{s3_prefix.rstrip('/')}/index.json"
        
        # Try to get existing index
        existing_index = []
        try:
            response = s3_client.get_object(Bucket=bucket_name, Key=index_key)
            existing_index = json.loads(response['Body'].read().decode('utf-8'))
        except ClientError:
            pass  # Index doesn't exist yet
        
        # Add new backup to index
        existing_index.append(metadata)
        
        # Keep only last 50 backups in index
        existing_index = existing_index[-50:]
        
        # Upload updated index
        s3_client.put_object(
            Bucket=bucket_name,
            Key=index_key,
            Body=json.dumps(existing_index, indent=2, default=str),
            ContentType='application/json'
        )
        
        console.print(f"[dim]Updated backup index:[/dim] s3://{bucket_name}/{index_key}")
        
    except Exception as e:
        console.print(f"[yellow]Warning: Could not update backup index:[/yellow] {e}")

app = typer.Typer(add_completion=False)

@app.callback(invoke_without_command=True)
def main(
    ctx: typer.Context,
    include_blocks: bool = typer.Option(
        True,
        "--include-blocks/--no-include-blocks",
        help="Include page block content in backup"
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
    keep_local: bool = typer.Option(
        False,
        "--keep-local",
        help="Keep local backup files after upload"
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Create backup but don't upload to S3"
    ),
    resume_from: Optional[str] = typer.Option(
        None,
        "--resume-from",
        help="Resume from specific partial backup directory (e.g., backup_20251124_174738)"
    ),
    resume: bool = typer.Option(
        False,
        "--resume",
        help="Automatically resume from the latest partial backup"
    )
):
    """
    Create Notion backup, compress it, and upload to S3.
    
    This command performs a complete backup workflow:
    1. Creates a Notion backup
    2. Compresses it to a zip file
    3. Uploads to S3
    4. Optionally cleans up local files
    """
    # If a subcommand is invoked (like 'list'), don't run the default backup
    if ctx.invoked_subcommand is not None:
        return
    
    console.print(Panel.fit(
        "[bold blue]Notion Backup to S3[/bold blue]\n"
        "[dim]Creating backup and uploading to cloud storage...[/dim]",
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
        
        # Check if resuming from partial backup
        resume_backup_dir = None
        if resume or resume_from:
            from pathlib import Path
            
            if resume:
                # Find latest backup directory automatically
                backups_dir = Path("backups")
                if not backups_dir.exists():
                    console.print(f"[red]Error:[/red] No backups directory found")
                    raise typer.Exit(1)
                
                backup_dirs = sorted(
                    [d for d in backups_dir.iterdir() if d.is_dir() and d.name.startswith("backup_")],
                    key=lambda x: x.stat().st_mtime,
                    reverse=True
                )
                
                if not backup_dirs:
                    console.print(f"[red]Error:[/red] No backup directories found to resume from")
                    raise typer.Exit(1)
                
                resume_backup_dir = backup_dirs[0]
                console.print(f"[yellow]Auto-detected latest backup:[/yellow] {resume_backup_dir.name}")
            else:
                # Use specific backup directory
                resume_backup_dir = Path("backups") / resume_from
                if not resume_backup_dir.exists():
                    console.print(f"[red]Error:[/red] Resume backup directory not found: {resume_backup_dir}")
                    raise typer.Exit(1)
            
            console.print(f"[yellow]Resuming from:[/yellow] {resume_backup_dir}")
        
        # Create backup configuration
        config_overrides = {
            "include_blocks": include_blocks,
            "verbose": verbose,
            "debug": debug,
        }
        
        config = get_backup_config(**config_overrides)
        
        # Initialize backup manager
        backup_manager = NotionBackupManager(config)
        
        # Create backup
        if resume_from:
            console.print("[bold]Step 1:[/bold] Resuming Notion backup...")
        else:
            console.print("[bold]Step 1:[/bold] Creating Notion backup...")
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            console=console,
            transient=True
        ) as progress:
            
            task = progress.add_task("Creating backup...", total=100)
            
            def update_progress(phase: str, completed: int, total: int):
                if total > 0:
                    percentage = (completed / total) * 100
                    progress.update(
                        task,
                        description=f"{phase}: {completed}/{total}",
                        completed=percentage
                    )
            
            backup_dir = backup_manager.start_backup(
                progress_callback=update_progress,
                resume_from_dir=resume_backup_dir
            )
        
        console.print(f"[green]✓[/green] Backup created: {backup_dir}")
        
        # Compress backup
        console.print("\n[bold]Step 2:[/bold] Compressing backup...")
        
        with tempfile.TemporaryDirectory() as temp_dir:
            zip_file = Path(temp_dir) / f"{backup_dir.name}.zip"
            stats = compress_backup(backup_dir, zip_file)
            
            console.print(f"[green]✓[/green] Compression complete!")
            console.print(f"[dim]Original size:[/dim] {stats['original_size'] / 1024 / 1024:.1f} MB")
            console.print(f"[dim]Compressed size:[/dim] {stats['compressed_size'] / 1024 / 1024:.1f} MB")
            console.print(f"[dim]Compression ratio:[/dim] {(1 - stats['compressed_size'] / stats['original_size']) * 100:.1f}%")
            console.print(f"[dim]Files compressed:[/dim] {stats['files_count']}")
            
            if dry_run:
                console.print("\n[yellow]DRY RUN:[/yellow] Skipping S3 upload")
                console.print(f"[dim]Would upload:[/dim] {zip_file.name}")
                return
            
            # Upload to S3
            console.print("\n[bold]Step 3:[/bold] Uploading to S3...")
            
            s3_key = generate_s3_key(s3_config['s3_prefix'], backup_dir.name)
            
            success = upload_to_s3(
                zip_file, 
                s3_client, 
                s3_config['bucket_name'], 
                s3_key
            )
            
            if success:
                console.print(f"[green]✓[/green] Upload successful!")
                console.print(f"[dim]S3 Location:[/dim] s3://{s3_config['bucket_name']}/{s3_key}")
                
                # Create and save metadata
                metadata = create_backup_metadata(backup_dir, s3_key, stats)
                save_backup_index(s3_client, s3_config['bucket_name'], s3_config['s3_prefix'], metadata)
                
                # Cleanup local files if requested
                if not keep_local:
                    console.print("\n[bold]Step 4:[/bold] Cleaning up local files...")
                    
                    import shutil
                    shutil.rmtree(backup_dir)
                    console.print(f"[green]✓[/green] Removed local backup: {backup_dir}")
                else:
                    console.print(f"\n[dim]Local backup preserved:[/dim] {backup_dir}")
                
                console.print(f"\n[green]🎉 Backup successfully uploaded to S3![/green]")
                
            else:
                console.print("[red]❌ Upload failed![/red]")
                raise typer.Exit(1)
    
    except KeyboardInterrupt:
        console.print("\n[yellow]Backup cancelled by user[/yellow]")
        raise typer.Exit(1)
    except Exception as e:
        console.print(f"\n[red]Error:[/red] {e}")
        if debug:
            console.print_exception()
        raise typer.Exit(1)

@app.command("list")
def list_s3_backups():
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
        
        # Get backup index
        index_key = f"{s3_config['s3_prefix'].rstrip('/')}/index.json"
        
        try:
            response = s3_client.get_object(Bucket=s3_config['bucket_name'], Key=index_key)
            backups = json.loads(response['Body'].read().decode('utf-8'))
            
            if not backups:
                console.print("[yellow]No backups found in S3[/yellow]")
                return
            
            from rich.table import Table
            
            table = Table(show_header=True, header_style="bold magenta")
            table.add_column("Backup Name", style="cyan")
            table.add_column("Created", style="green")
            table.add_column("Size", style="yellow")
            table.add_column("Compression", style="blue")
            table.add_column("S3 Key", style="dim")
            
            # Sort by creation date (newest first)
            backups.sort(key=lambda x: x.get('created_at', ''), reverse=True)
            
            for backup in backups[-10:]:  # Show last 10 backups
                created_at = backup.get('created_at', 'Unknown')
                if created_at != 'Unknown':
                    try:
                        from datetime import datetime
                        dt = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                        created_at = dt.strftime("%Y-%m-%d %H:%M")
                    except:
                        pass
                
                size_mb = backup.get('compressed_size', 0) / 1024 / 1024
                compression = f"{backup.get('compression_ratio', 0):.1f}%"
                s3_key_short = backup.get('s3_key', '')[-50:] if len(backup.get('s3_key', '')) > 50 else backup.get('s3_key', '')
                
                table.add_row(
                    backup.get('backup_name', 'Unknown'),
                    created_at,
                    f"{size_mb:.1f} MB",
                    compression,
                    s3_key_short
                )
            
            console.print(table)
            console.print(f"\n[dim]Showing last 10 of {len(backups)} backups[/dim]")
            
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                console.print("[yellow]No backup index found in S3[/yellow]")
                console.print("Run a backup first to create the index.")
            else:
                console.print(f"[red]Error accessing S3:[/red] {e}")
                raise typer.Exit(1)
    
    except Exception as e:
        console.print(f"[red]Error:[/red] {e}")
        raise typer.Exit(1)

if __name__ == "__main__":
    app()
