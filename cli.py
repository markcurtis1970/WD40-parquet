#!/usr/bin/env python3
"""
Command Line Interface for Parquet File Generator

Usage:
    python cli.py generate --config config.yaml
    python cli.py validate --config config.yaml
    python cli.py info --file output/data_001.parquet
"""

import argparse
import sys
import os
import logging
from pathlib import Path
import yaml
import pandas as pd
import pyarrow.parquet as pq
from parquet_generator import ParquetGenerator

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def validate_config(config_path: str) -> bool:
    """Validate the configuration file."""
    try:
        with open(config_path, 'r') as file:
            config = yaml.safe_load(file)
        
        # Check required sections
        required_sections = ['schema', 'files']
        for section in required_sections:
            if section not in config:
                logger.error(f"Missing required section: {section}")
                return False
        
        # Validate schema section
        schema = config.get('schema', {})
        columns = schema.get('columns', [])
        
        if not columns:
            logger.error("No columns defined in schema")
            return False
        
        # Validate each column
        for i, column in enumerate(columns):
            if 'name' not in column:
                logger.error(f"Column {i} missing 'name' field")
                return False
            
            if 'type' not in column:
                logger.error(f"Column '{column['name']}' missing 'type' field")
                return False
            
            # Validate column type
            valid_types = ['int32', 'int64', 'float32', 'float64', 'string', 'boolean', 'timestamp']
            if column['type'] not in valid_types:
                logger.error(f"Column '{column['name']}' has invalid type: {column['type']}")
                logger.error(f"Valid types are: {', '.join(valid_types)}")
                return False
        
        # Validate files section
        files = config.get('files', {})
        if 'count' not in files or 'rows_per_file' not in files:
            logger.error("Files section must contain 'count' and 'rows_per_file'")
            return False
        
        if files['count'] <= 0 or files['rows_per_file'] <= 0:
            logger.error("File count and rows_per_file must be positive integers")
            return False
        
        logger.info("Configuration file is valid")
        return True
        
    except yaml.YAMLError as e:
        logger.error(f"Invalid YAML syntax: {e}")
        return False
    except FileNotFoundError:
        logger.error(f"Configuration file not found: {config_path}")
        return False
    except Exception as e:
        logger.error(f"Error validating configuration: {e}")
        return False


def generate_files(config_path: str, verbose: bool = False) -> bool:
    """Generate parquet files based on configuration."""
    try:
        if verbose:
            logging.getLogger().setLevel(logging.DEBUG)
        
        # Validate configuration first
        if not validate_config(config_path):
            logger.error("Configuration validation failed")
            return False
        
        # Generate files
        logger.info(f"Starting parquet file generation using config: {config_path}")
        generator = ParquetGenerator(config_path)
        generator.generate_files()
        
        logger.info("Parquet file generation completed successfully")
        return True
        
    except Exception as e:
        logger.error(f"Error generating files: {e}")
        return False


def show_file_info(file_path: str) -> bool:
    """Display information about a parquet file."""
    try:
        if not os.path.exists(file_path):
            logger.error(f"File not found: {file_path}")
            return False
        
        # Read parquet file metadata
        parquet_file = pq.ParquetFile(file_path)
        
        print(f"\n=== Parquet File Information ===")
        print(f"File: {file_path}")
        print(f"Size: {os.path.getsize(file_path) / 1024 / 1024:.2f} MB")
        print(f"Number of rows: {parquet_file.metadata.num_rows:,}")
        print(f"Number of columns: {parquet_file.metadata.num_columns}")
        print(f"Number of row groups: {parquet_file.metadata.num_row_groups}")
        
        # Schema information
        print(f"\n=== Schema ===")
        schema = parquet_file.schema_arrow
        for i, field in enumerate(schema):
            nullable = "nullable" if field.nullable else "not null"
            print(f"  {i+1:2d}. {field.name:<20} {str(field.type):<15} ({nullable})")
        
        # Row group information
        print(f"\n=== Row Groups ===")
        for i in range(parquet_file.metadata.num_row_groups):
            rg = parquet_file.metadata.row_group(i)
            print(f"  Row Group {i+1}: {rg.num_rows:,} rows, {rg.total_byte_size / 1024 / 1024:.2f} MB")
        
        # Compression information
        print(f"\n=== Compression ===")
        compressions = set()
        for i in range(parquet_file.metadata.num_row_groups):
            rg = parquet_file.metadata.row_group(i)
            for j in range(rg.num_columns):
                col = rg.column(j)
                compressions.add(str(col.compression))
        print(f"  Compression types: {', '.join(compressions)}")
        
        # Sample data
        print(f"\n=== Sample Data (first 5 rows) ===")
        nrows = 5
        df = pd.read_parquet(file_path, engine='pyarrow').head(nrows)
        print(df.to_string(index=False))
        
        return True
        
    except Exception as e:
        logger.error(f"Error reading file info: {e}")
        return False


def list_output_files(output_dir: str = "./output") -> bool:
    """List all parquet files in the output directory."""
    try:
        output_path = Path(output_dir)
        if not output_path.exists():
            logger.error(f"Output directory not found: {output_dir}")
            return False
        
        parquet_files = list(output_path.glob("*.parquet"))
        
        if not parquet_files:
            print(f"No parquet files found in {output_dir}")
            return True
        
        print(f"\n=== Parquet Files in {output_dir} ===")
        total_size = 0
        total_rows = 0
        
        for file_path in sorted(parquet_files):
            try:
                parquet_file = pq.ParquetFile(file_path)
                size_mb = file_path.stat().st_size / 1024 / 1024
                rows = parquet_file.metadata.num_rows
                
                print(f"  {file_path.name:<30} {rows:>10,} rows  {size_mb:>8.2f} MB")
                
                total_size += size_mb
                total_rows += rows
                
            except Exception as e:
                print(f"  {file_path.name:<30} ERROR: {e}")
        
        print(f"  {'-'*50}")
        print(f"  {'TOTAL':<30} {total_rows:>10,} rows  {total_size:>8.2f} MB")
        
        return True
        
    except Exception as e:
        logger.error(f"Error listing files: {e}")
        return False


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Generate multiple parquet files with configurable schemas and parameters",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python cli.py generate --config config.yaml
  python cli.py generate --config config.yaml --verbose
  python cli.py validate --config config.yaml
  python cli.py info --file output/data_001.parquet
  python cli.py list --output-dir ./output
        """
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Generate command
    generate_parser = subparsers.add_parser('generate', help='Generate parquet files')
    generate_parser.add_argument('--config', '-c', required=True,
                               help='Path to configuration YAML file')
    generate_parser.add_argument('--verbose', '-v', action='store_true',
                               help='Enable verbose logging')
    
    # Validate command
    validate_parser = subparsers.add_parser('validate', help='Validate configuration file')
    validate_parser.add_argument('--config', '-c', required=True,
                               help='Path to configuration YAML file')
    
    # Info command
    info_parser = subparsers.add_parser('info', help='Show information about a parquet file')
    info_parser.add_argument('--file', '-f', required=True,
                           help='Path to parquet file')
    
    # List command
    list_parser = subparsers.add_parser('list', help='List parquet files in output directory')
    list_parser.add_argument('--output-dir', '-o', default='./output',
                           help='Output directory to scan (default: ./output)')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return 1
    
    success = False
    
    if args.command == 'generate':
        success = generate_files(args.config, args.verbose)
    elif args.command == 'validate':
        success = validate_config(args.config)
    elif args.command == 'info':
        success = show_file_info(args.file)
    elif args.command == 'list':
        success = list_output_files(args.output_dir)
    
    return 0 if success else 1


if __name__ == '__main__':
    sys.exit(main())
