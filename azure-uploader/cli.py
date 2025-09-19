#!/usr/bin/env python3
"""
Command-line interface for Azure Uploader.
"""

import sys
import logging
from pathlib import Path
from typing import Optional, List
import click

from azure_uploader import AzureUploader
from config import Config


def setup_logging(log_level: str, log_file: Optional[str] = None) -> None:
    """Setup logging configuration."""
    # Configure logging format
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    
    # Setup handlers
    handlers = [logging.StreamHandler(sys.stdout)]
    if log_file:
        handlers.append(logging.FileHandler(log_file))
    
    # Configure logging
    logging.basicConfig(
        level=getattr(logging, log_level),
        format=log_format,
        handlers=handlers
    )


@click.group()
@click.option('--config', '-c', help='Path to configuration file (.env)')
@click.option('--log-level', default='INFO', help='Logging level')
@click.option('--log-file', help='Log file path')
@click.pass_context
def cli(ctx, config: Optional[str], log_level: str, log_file: Optional[str]):
    """Azure Blob Storage Uploader CLI."""
    # Ensure context object exists
    ctx.ensure_object(dict)
    
    # Load configuration
    try:
        ctx.obj['config'] = Config(config)
        ctx.obj['config_error'] = None
        # Don't validate here - let individual commands decide if they need valid config
    except ValueError as e:
        ctx.obj['config'] = None
        ctx.obj['config_error'] = str(e)

    # Setup logging with defaults if config failed
    if ctx.obj['config']:
        setup_logging(log_level or ctx.obj['config'].log_level,
                      log_file or ctx.obj['config'].log_file)
    else:
        setup_logging(log_level or 'INFO', log_file)


@cli.command()
@click.argument('file_path', type=click.Path(exists=True, path_type=Path))
@click.option('--blob-name', help='Custom blob name (defaults to filename)')
@click.option('--overwrite', is_flag=True, help='Overwrite existing blobs')
@click.option('--metadata', multiple=True, help='Metadata key=value pairs')
@click.pass_context
def upload_file(ctx, file_path: Path, blob_name: Optional[str], overwrite: bool, metadata: tuple):
    """Upload a single file to Azure Blob Storage."""
    config = ctx.obj['config']

    # Check for configuration errors
    if ctx.obj.get('config_error'):
        click.echo(f"Configuration error: {ctx.obj['config_error']}", err=True)
        raise click.Abort()

    if not config:
        click.echo("Configuration error: Unable to load configuration", err=True)
        raise click.Abort()

    # Validate configuration for upload operations
    try:
        config.validate()
    except ValueError as e:
        click.echo(f"Configuration error: {e}", err=True)
        raise click.Abort()
    
    # Parse metadata
    metadata_dict = {}
    for item in metadata:
        if '=' in item:
            key, value = item.split('=', 1)
            metadata_dict[key] = value
        else:
            click.echo(f"Invalid metadata format: {item}. Use key=value format.", err=True)
            raise click.Abort()
    
    try:
        # Create uploader
        uploader = AzureUploader(
            connection_string=config.azure_connection_string,
            container_name=config.azure_container_name,
            max_workers=config.max_workers,
            chunk_size=config.chunk_size,
            max_retries=config.max_retries
        )
        
        # Upload file
        success = uploader.upload_file(
            file_path=file_path,
            blob_name=blob_name,
            overwrite=overwrite,
            metadata=metadata_dict if metadata_dict else None
        )
        
        if success:
            click.echo(f"Successfully uploaded: {file_path}")
        else:
            click.echo(f"Failed to upload: {file_path}", err=True)
            raise click.Abort()

    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        raise click.Abort()


@cli.command()
@click.argument('directory_path', type=click.Path(exists=True, file_okay=False, path_type=Path))
@click.option('--blob-prefix', default='', help='Prefix for blob names')
@click.option('--preserve-structure/--flatten', default=True, 
              help='Preserve directory structure in blob names')
@click.option('--overwrite', is_flag=True, help='Overwrite existing blobs')
@click.option('--include', multiple=True, help='File patterns to include (e.g., *.jpg)')
@click.option('--exclude', multiple=True, help='File patterns to exclude (e.g., *.tmp)')
@click.option('--max-size', type=int, help='Maximum file size in bytes')
@click.option('--no-progress', is_flag=True, help='Disable progress bar')
@click.pass_context
def upload_directory(ctx, directory_path: Path, blob_prefix: str, preserve_structure: bool,
                    overwrite: bool, include: tuple, exclude: tuple, max_size: Optional[int],
                    no_progress: bool):
    """Upload a directory and its contents to Azure Blob Storage."""
    config = ctx.obj['config']

    # Check for configuration errors
    if ctx.obj.get('config_error'):
        click.echo(f"Configuration error: {ctx.obj['config_error']}", err=True)
        raise click.Abort()

    if not config:
        click.echo("Configuration error: Unable to load configuration", err=True)
        raise click.Abort()

    # Validate configuration for upload operations
    try:
        config.validate()
    except ValueError as e:
        click.echo(f"Configuration error: {e}", err=True)
        raise click.Abort()
    
    # Prepare patterns
    include_patterns = list(include) if include else config.default_file_patterns
    exclude_patterns = list(exclude) if exclude else config.default_exclude_patterns
    
    try:
        # Create uploader
        uploader = AzureUploader(
            connection_string=config.azure_connection_string,
            container_name=config.azure_container_name,
            max_workers=config.max_workers,
            chunk_size=config.chunk_size,
            max_retries=config.max_retries
        )
        
        # Upload directory
        stats = uploader.upload_directory(
            directory_path=directory_path,
            blob_prefix=blob_prefix,
            preserve_structure=preserve_structure,
            overwrite=overwrite,
            file_patterns=include_patterns,
            exclude_patterns=exclude_patterns,
            max_file_size=max_size or config.max_file_size,
            show_progress=not no_progress
        )
        
        # Display results
        click.echo(f"\nUpload Summary:")
        click.echo(f"  Total files: {stats['total']}")
        click.echo(f"  Successful: {stats['successful']}")
        click.echo(f"  Failed: {stats['failed']}")
        
        if stats['failed'] > 0:
            click.echo(f"Some uploads failed. Check logs for details.", err=True)
            raise click.Abort()
        else:
            click.echo("All uploads completed successfully!")

    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        raise click.Abort()


@cli.command()
@click.pass_context
def config_info(ctx):
    """Display current configuration."""
    config = ctx.obj['config']

    # Check if there was a configuration error
    if ctx.obj.get('config_error'):
        click.echo(f"Configuration error: {ctx.obj['config_error']}", err=True)
        raise click.Abort()

    if not config:
        click.echo("Configuration error: Unable to load configuration", err=True)
        raise click.Abort()

    try:
        click.echo("Current Configuration:")
        click.echo(f"  Container: {config.azure_container_name}")
        click.echo(f"  Max Workers: {config.max_workers}")
        click.echo(f"  Chunk Size: {config.chunk_size:,} bytes")
        click.echo(f"  Max Retries: {config.max_retries}")
        click.echo(f"  Log Level: {config.log_level}")
        if config.default_file_patterns:
            click.echo(f"  Default Include Patterns: {', '.join(config.default_file_patterns)}")
        if config.default_exclude_patterns:
            click.echo(f"  Default Exclude Patterns: {', '.join(config.default_exclude_patterns)}")
        if config.max_file_size:
            click.echo(f"  Max File Size: {config.max_file_size:,} bytes")
    except ValueError as e:
        click.echo(f"Configuration error: {e}", err=True)
        raise click.Abort()


if __name__ == '__main__':
    cli()
