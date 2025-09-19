#!/usr/bin/env python3
"""
Simple test runner for CLI functions without requiring pytest.
Tests all CLI functions using example configurations.
"""

import os
import sys
import tempfile
import shutil
import subprocess
from pathlib import Path
import yaml

# Add the current directory to Python path to import cli functions
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from cli import validate_config, generate_files, show_file_info, list_output_files


def modify_config_for_testing(config_path: str, test_output_dir: Path) -> str:
    """Modify a config file to use test output directory and smaller file sizes."""
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    # Modify for testing
    config['global']['output_directory'] = str(test_output_dir)
    config['files']['count'] = 2  # Reduce file count for faster testing
    config['files']['rows_per_file'] = 1000  # Reduce rows for faster testing
    
    # Remove file_configs if present to simplify testing
    if 'file_configs' in config:
        del config['file_configs']
    
    # Create temporary config file
    test_config = tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False)
    yaml.dump(config, test_config)
    test_config.close()
    
    return test_config.name


def test_validation():
    """Test validation of all example configs."""
    print("Testing config validation...")
    
    example_configs = [
        "examples/simple_config.yaml",
        "examples/ecommerce_config.yaml", 
        "examples/financial_config.yaml",
        "examples/iot_sensor_config.yaml"
    ]
    
    for config_path in example_configs:
        if os.path.exists(config_path):
            result = validate_config(config_path)
            if result:
                print(f"  ✓ {config_path} - Valid")
            else:
                print(f"  ✗ {config_path} - Invalid")
                return False
        else:
            print(f"  ⚠ {config_path} - File not found")
    
    # Test invalid config
    invalid_config = {
        'schema': {
            'columns': [
                {'name': 'test', 'type': 'invalid_type'}  # Invalid type
            ]
        }
        # Missing 'files' section
    }
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        yaml.dump(invalid_config, f)
        invalid_config_path = f.name
    
    try:
        result = validate_config(invalid_config_path)
        if not result:
            print(f"  ✓ Invalid config correctly rejected")
        else:
            print(f"  ✗ Invalid config incorrectly accepted")
            return False
    finally:
        os.unlink(invalid_config_path)
    
    return True


def test_generation_and_info():
    """Test file generation and info display."""
    print("\nTesting file generation and info display...")

    example_configs = [
        "examples/simple_config.yaml",
        "examples/ecommerce_config.yaml",
        "examples/financial_config.yaml",
        "examples/iot_sensor_config.yaml"
    ]

    for config_path in example_configs:
        if not os.path.exists(config_path):
            print(f"  ⚠ {config_path} not found, skipping generation test")
            continue

        print(f"  Testing {config_path}...")

        test_output_dir = Path(f"./test_cli_output_{Path(config_path).stem}")
        test_output_dir.mkdir(exist_ok=True)

        try:
            test_config = modify_config_for_testing(config_path, test_output_dir)

            try:
                # Test generation
                result = generate_files(test_config)
                if not result:
                    print(f"    ✗ File generation failed for {config_path}")
                    return False

                # Check if files were created
                parquet_files = list(test_output_dir.glob("*.parquet"))
                if len(parquet_files) != 2:
                    print(f"    ✗ Expected 2 files, got {len(parquet_files)} for {config_path}")
                    return False

                print(f"    ✓ Generated {len(parquet_files)} files successfully for {config_path}")

                # Test show_file_info
                test_file = parquet_files[0]
                result = show_file_info(str(test_file))
                if not result:
                    print(f"    ✗ show_file_info failed for {config_path}")
                    return False

                print(f"    ✓ show_file_info worked correctly for {config_path}")

                # Test list_output_files
                result = list_output_files(str(test_output_dir))
                if not result:
                    print(f"    ✗ list_output_files failed for {config_path}")
                    return False

                print(f"    ✓ list_output_files worked correctly for {config_path}")

            finally:
                os.unlink(test_config)

        finally:
            # Cleanup
            if test_output_dir.exists():
                shutil.rmtree(test_output_dir)

    return True


def test_cli_commands():
    """Test CLI commands via subprocess."""
    print("\nTesting CLI commands...")
    
    test_output_dir = Path("./test_cli_cmd_simple_output")
    test_output_dir.mkdir(exist_ok=True)
    
    try:
        # Test validate command
        result = subprocess.run([
            sys.executable, "cli.py", "validate", 
            "--config", "examples/simple_config.yaml"
        ], capture_output=True, text=True)
        
        if result.returncode != 0:
            print(f"  ✗ CLI validate command failed: {result.stderr}")
            return False
        
        print(f"  ✓ CLI validate command worked")
        
        # Test generate command
        test_config = modify_config_for_testing("examples/simple_config.yaml", test_output_dir)
        
        try:
            result = subprocess.run([
                sys.executable, "cli.py", "generate", 
                "--config", test_config
            ], capture_output=True, text=True)
            
            if result.returncode != 0:
                print(f"  ✗ CLI generate command failed: {result.stderr}")
                return False
            
            print(f"  ✓ CLI generate command worked")
            
            # Test info command
            parquet_files = list(test_output_dir.glob("*.parquet"))
            if len(parquet_files) > 0:
                test_file = parquet_files[0]
                
                result = subprocess.run([
                    sys.executable, "cli.py", "info", 
                    "--file", str(test_file)
                ], capture_output=True, text=True)
                
                if result.returncode != 0:
                    print(f"  ✗ CLI info command failed: {result.stderr}")
                    return False
                
                if "Parquet File Information" not in result.stdout:
                    print(f"  ✗ CLI info command output missing expected content")
                    return False
                
                print(f"  ✓ CLI info command worked")
            
            # Test list command
            result = subprocess.run([
                sys.executable, "cli.py", "list", 
                "--output-dir", str(test_output_dir)
            ], capture_output=True, text=True)
            
            if result.returncode != 0:
                print(f"  ✗ CLI list command failed: {result.stderr}")
                return False
            
            if "Parquet Files" not in result.stdout:
                print(f"  ✗ CLI list command output missing expected content")
                return False
            
            print(f"  ✓ CLI list command worked")
            
        finally:
            os.unlink(test_config)
            
    finally:
        # Cleanup
        if test_output_dir.exists():
            shutil.rmtree(test_output_dir)
    
    return True


def main():
    """Run all tests."""
    print("Running CLI Tests")
    print("=" * 50)
    
    tests = [
        test_cli_commands,
        test_validation,
        test_generation_and_info
    ]
    
    all_passed = True
    
    for test_func in tests:
        try:
            if not test_func():
                all_passed = False
                break
        except Exception as e:
            print(f"  ✗ Test {test_func.__name__} failed with exception: {e}")
            import traceback
            traceback.print_exc()
            all_passed = False
            break
    
    print("\n" + "=" * 50)
    if all_passed:
        print("🎉 All CLI tests passed successfully!")
        print("All CLI functions (validate, generate, info, list) are working correctly.")
        return 0
    else:
        print("❌ Some tests failed!")
        return 1


if __name__ == '__main__':
    sys.exit(main())
