#!/usr/bin/env python3
"""
Test script to verify the parquet generator works correctly
"""

import os
import tempfile
import shutil
from pathlib import Path
import pandas as pd
import pyarrow.parquet as pq
from parquet_generator import ParquetGenerator

def create_test_config():
    """Create a simple test configuration."""
    config = {
        'global': {
            'output_directory': './test_output',
            'file_prefix': 'test_data',
            'random_seed': 42
        },
        'schema': {
            'columns': [
                {
                    'name': 'id',
                    'type': 'int64',
                    'nullable': False,
                    'generator': {
                        'type': 'sequence',
                        'start': 1
                    }
                },
                {
                    'name': 'name',
                    'type': 'string',
                    'nullable': False,
                    'generator': {
                        'type': 'choice',
                        'choices': ['Alice', 'Bob', 'Charlie']
                    }
                },
                {
                    'name': 'score',
                    'type': 'float64',
                    'nullable': True,
                    'generator': {
                        'type': 'normal',
                        'mean': 85.0,
                        'std': 10.0,
                        'min': 0.0,
                        'max': 100.0,
                        'null_probability': 0.1
                    }
                },
                {
                    'name': 'active',
                    'type': 'boolean',
                    'nullable': False,
                    'generator': {
                        'type': 'boolean',
                        'probability': 0.8
                    }
                }
            ]
        },
        'parquet_options': {
            'compression': 'snappy',
            'row_group_size': 1000
        },
        'files': {
            'count': 2,
            'rows_per_file': 5000
        }
    }
    return config

def test_basic_generation():
    """Test basic parquet file generation."""
    print("Testing basic parquet file generation...")
    
    # Create temporary config file
    import yaml
    config = create_test_config()
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        yaml.dump(config, f)
        config_path = f.name
    
    try:
        # Generate files
        generator = ParquetGenerator(config_path)
        generator.generate_files()
        
        # Check if files were created
        output_dir = Path('./test_output')
        parquet_files = list(output_dir.glob('*.parquet'))
        
        assert len(parquet_files) == 2, f"Expected 2 files, got {len(parquet_files)}"
        print(f"✓ Generated {len(parquet_files)} files successfully")
        
        # Check file contents
        for file_path in parquet_files:
            df = pd.read_parquet(file_path)
            assert len(df) == 5000, f"Expected 5000 rows, got {len(df)}"
            assert list(df.columns) == ['id', 'name', 'score', 'active'], f"Unexpected columns: {list(df.columns)}"
            print(f"✓ File {file_path.name} has correct structure and row count")
            
            # Check data types
            assert df['id'].dtype == 'int64', f"Expected int64 for id, got {df['id'].dtype}"
            assert df['name'].dtype == 'object', f"Expected object for name, got {df['name'].dtype}"
            assert df['score'].dtype == 'float64', f"Expected float64 for score, got {df['score'].dtype}"
            assert df['active'].dtype == 'bool', f"Expected bool for active, got {df['active'].dtype}"
            print(f"✓ File {file_path.name} has correct data types")
            
            # Check for null values in score column
            null_count = df['score'].isnull().sum()
            null_percentage = null_count / len(df)
            assert 0.05 <= null_percentage <= 0.15, f"Expected ~10% nulls in score, got {null_percentage:.2%}"
            print(f"✓ File {file_path.name} has appropriate null values ({null_percentage:.2%})")
        
        print("✓ All basic generation tests passed!")
        
    finally:
        # Cleanup
        os.unlink(config_path)
        if output_dir.exists():
            shutil.rmtree(output_dir)

def test_file_properties():
    """Test parquet file properties and metadata."""
    print("\nTesting parquet file properties...")
    
    import yaml
    config = create_test_config()
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        yaml.dump(config, f)
        config_path = f.name
    
    try:
        # Generate files
        generator = ParquetGenerator(config_path)
        generator.generate_files()
        
        # Check file properties
        output_dir = Path('./test_output')
        parquet_files = list(output_dir.glob('*.parquet'))
        
        for file_path in parquet_files:
            parquet_file = pq.ParquetFile(file_path)
            
            # Check compression
            metadata = parquet_file.metadata
            for i in range(metadata.num_row_groups):
                rg = metadata.row_group(i)
                for j in range(rg.num_columns):
                    col = rg.column(j)
                    assert str(col.compression) == 'SNAPPY', f"Expected SNAPPY compression, got {col.compression}"
            
            print(f"✓ File {file_path.name} uses correct compression")
            
            # Check schema
            schema = parquet_file.schema_arrow
            expected_fields = ['id', 'name', 'score', 'active']
            actual_fields = [field.name for field in schema]
            assert actual_fields == expected_fields, f"Expected {expected_fields}, got {actual_fields}"
            print(f"✓ File {file_path.name} has correct schema")
        
        print("✓ All file property tests passed!")
        
    finally:
        # Cleanup
        os.unlink(config_path)
        if output_dir.exists():
            shutil.rmtree(output_dir)

def test_data_generation():
    """Test specific data generation patterns."""
    print("\nTesting data generation patterns...")
    
    import yaml
    config = create_test_config()
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        yaml.dump(config, f)
        config_path = f.name
    
    try:
        # Generate files
        generator = ParquetGenerator(config_path)
        generator.generate_files()
        
        # Read and analyze data
        output_dir = Path('./test_output')
        parquet_files = list(output_dir.glob('*.parquet'))
        
        df = pd.read_parquet(parquet_files[0])
        
        # Test sequence generation
        assert df['id'].iloc[0] == 1, f"Expected first ID to be 1, got {df['id'].iloc[0]}"
        assert df['id'].iloc[-1] == len(df), f"Expected last ID to be {len(df)}, got {df['id'].iloc[-1]}"
        assert df['id'].is_monotonic_increasing, "ID sequence should be monotonic increasing"
        print("✓ Sequence generation works correctly")
        
        # Test choice generation
        unique_names = set(df['name'].unique())
        expected_names = {'Alice', 'Bob', 'Charlie'}
        assert unique_names == expected_names, f"Expected {expected_names}, got {unique_names}"
        print("✓ Choice generation works correctly")
        
        # Test normal distribution
        scores = df['score'].dropna()
        mean_score = scores.mean()
        std_score = scores.std()
        assert 80 <= mean_score <= 90, f"Expected mean ~85, got {mean_score:.2f}"
        assert 8 <= std_score <= 12, f"Expected std ~10, got {std_score:.2f}"
        assert scores.min() >= 0, f"Expected min >= 0, got {scores.min()}"
        assert scores.max() <= 100, f"Expected max <= 100, got {scores.max()}"
        print("✓ Normal distribution generation works correctly")
        
        # Test boolean generation
        active_percentage = df['active'].mean()
        assert 0.75 <= active_percentage <= 0.85, f"Expected ~80% active, got {active_percentage:.2%}"
        print("✓ Boolean generation works correctly")
        
        print("✓ All data generation tests passed!")
        
    finally:
        # Cleanup
        os.unlink(config_path)
        if output_dir.exists():
            shutil.rmtree(output_dir)

def main():
    """Run all tests."""
    print("Running Parquet Generator Tests")
    print("=" * 50)
    
    try:
        test_basic_generation()
        test_file_properties()
        test_data_generation()
        
        print("\n" + "=" * 50)
        print("🎉 All tests passed successfully!")
        print("The parquet generator is working correctly.")
        
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0

if __name__ == '__main__':
    exit(main())
