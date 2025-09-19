#!/usr/bin/env python3
"""
Comprehensive test script for CLI functions using example configurations.
Tests generate, validate, list, and info commands.
"""

import os
import sys
import tempfile
import shutil
import subprocess
from pathlib import Path
import pytest
import yaml

# Add the current directory to Python path to import cli functions
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from cli import validate_config, generate_files, show_file_info, list_output_files


class TestCLIFunctions:
    """Test class for CLI functions using example configurations."""
    
    @pytest.fixture(autouse=True)
    def setup_and_teardown(self):
        """Setup and teardown for each test."""
        # Setup: Create a temporary output directory
        self.test_output_dir = Path("./test_cli_output")
        self.test_output_dir.mkdir(exist_ok=True)
        
        yield
        
        # Teardown: Clean up test output directory
        if self.test_output_dir.exists():
            shutil.rmtree(self.test_output_dir)
    
    def modify_config_for_testing(self, config_path: str) -> str:
        """Modify a config file to use test output directory and smaller file sizes."""
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)

        # Modify for testing
        config['global']['output_directory'] = str(self.test_output_dir)
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
    
    def test_validate_simple_config(self):
        """Test validation of simple config."""
        config_path = "examples/simple_config.yaml"
        assert validate_config(config_path), "Simple config should be valid"
    
    def test_validate_ecommerce_config(self):
        """Test validation of ecommerce config."""
        config_path = "examples/ecommerce_config.yaml"
        assert validate_config(config_path), "Ecommerce config should be valid"
    
    def test_validate_financial_config(self):
        """Test validation of financial config."""
        config_path = "examples/financial_config.yaml"
        assert validate_config(config_path), "Financial config should be valid"
    
    def test_validate_iot_config(self):
        """Test validation of IoT config."""
        config_path = "examples/iot_sensor_config.yaml"
        assert validate_config(config_path), "IoT config should be valid"
    
    def test_validate_invalid_config(self):
        """Test validation of invalid config."""
        # Create an invalid config
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
            assert not validate_config(invalid_config_path), "Invalid config should fail validation"
        finally:
            os.unlink(invalid_config_path)
    
    def test_generate_simple_config(self):
        """Test file generation with simple config."""
        original_config = "examples/simple_config.yaml"
        test_config = self.modify_config_for_testing(original_config)
        
        try:
            # Generate files
            assert generate_files(test_config), "File generation should succeed"
            
            # Check if files were created
            parquet_files = list(self.test_output_dir.glob("*.parquet"))
            assert len(parquet_files) == 2, f"Expected 2 files, got {len(parquet_files)}"
            
            # Check file sizes
            for file_path in parquet_files:
                assert file_path.stat().st_size > 0, f"File {file_path.name} should not be empty"
                
        finally:
            os.unlink(test_config)
    
    def test_generate_ecommerce_config(self):
        """Test file generation with ecommerce config."""
        original_config = "examples/ecommerce_config.yaml"
        test_config = self.modify_config_for_testing(original_config)
        
        try:
            # Generate files
            assert generate_files(test_config), "File generation should succeed"
            
            # Check if files were created
            parquet_files = list(self.test_output_dir.glob("*.parquet"))
            assert len(parquet_files) == 2, f"Expected 2 files, got {len(parquet_files)}"
                
        finally:
            os.unlink(test_config)
    
    def test_show_file_info(self):
        """Test show_file_info function."""
        # First generate a test file
        original_config = "examples/simple_config.yaml"
        test_config = self.modify_config_for_testing(original_config)
        
        try:
            # Generate files
            assert generate_files(test_config), "File generation should succeed"
            
            # Get the first generated file
            parquet_files = list(self.test_output_dir.glob("*.parquet"))
            assert len(parquet_files) > 0, "Should have generated at least one file"
            
            test_file = parquet_files[0]
            
            # Test show_file_info
            assert show_file_info(str(test_file)), "show_file_info should succeed"
            
            # Test with non-existent file
            assert not show_file_info("non_existent_file.parquet"), "show_file_info should fail for non-existent file"
                
        finally:
            os.unlink(test_config)
    
    def test_list_output_files(self):
        """Test list_output_files function."""
        # First generate test files
        original_config = "examples/simple_config.yaml"
        test_config = self.modify_config_for_testing(original_config)
        
        try:
            # Generate files
            assert generate_files(test_config), "File generation should succeed"
            
            # Test list_output_files
            assert list_output_files(str(self.test_output_dir)), "list_output_files should succeed"
            
            # Test with non-existent directory
            assert not list_output_files("non_existent_directory"), "list_output_files should fail for non-existent directory"
                
        finally:
            os.unlink(test_config)


class TestCLICommands:
    """Test class for CLI commands via subprocess."""
    
    @pytest.fixture(autouse=True)
    def setup_and_teardown(self):
        """Setup and teardown for each test."""
        # Setup: Create a temporary output directory
        self.test_output_dir = Path("./test_cli_cmd_output")
        self.test_output_dir.mkdir(exist_ok=True)
        
        yield
        
        # Teardown: Clean up test output directory
        if self.test_output_dir.exists():
            shutil.rmtree(self.test_output_dir)
    
    def modify_config_for_testing(self, config_path: str) -> str:
        """Modify a config file to use test output directory and smaller file sizes."""
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)

        # Modify for testing
        config['global']['output_directory'] = str(self.test_output_dir)
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
    
    def test_cli_validate_command(self):
        """Test CLI validate command."""
        result = subprocess.run([
            sys.executable, "cli.py", "validate", 
            "--config", "examples/simple_config.yaml"
        ], capture_output=True, text=True)
        
        assert result.returncode == 0, f"Validate command failed: {result.stderr}"
    
    def test_cli_generate_command(self):
        """Test CLI generate command."""
        test_config = self.modify_config_for_testing("examples/simple_config.yaml")
        
        try:
            result = subprocess.run([
                sys.executable, "cli.py", "generate", 
                "--config", test_config
            ], capture_output=True, text=True)
            
            assert result.returncode == 0, f"Generate command failed: {result.stderr}"
            
            # Check if files were created
            parquet_files = list(self.test_output_dir.glob("*.parquet"))
            assert len(parquet_files) == 2, f"Expected 2 files, got {len(parquet_files)}"
            
        finally:
            os.unlink(test_config)
    
    def test_cli_info_command(self):
        """Test CLI info command."""
        # First generate a test file
        test_config = self.modify_config_for_testing("examples/simple_config.yaml")
        
        try:
            # Generate files
            result = subprocess.run([
                sys.executable, "cli.py", "generate", 
                "--config", test_config
            ], capture_output=True, text=True)
            
            assert result.returncode == 0, f"Generate command failed: {result.stderr}"
            
            # Get the first generated file
            parquet_files = list(self.test_output_dir.glob("*.parquet"))
            assert len(parquet_files) > 0, "Should have generated at least one file"
            
            test_file = parquet_files[0]
            
            # Test info command
            result = subprocess.run([
                sys.executable, "cli.py", "info", 
                "--file", str(test_file)
            ], capture_output=True, text=True)
            
            assert result.returncode == 0, f"Info command failed: {result.stderr}"
            assert "Parquet File Information" in result.stdout, "Info output should contain file information"
            
        finally:
            os.unlink(test_config)
    
    def test_cli_list_command(self):
        """Test CLI list command."""
        # First generate test files
        test_config = self.modify_config_for_testing("examples/simple_config.yaml")
        
        try:
            # Generate files
            result = subprocess.run([
                sys.executable, "cli.py", "generate", 
                "--config", test_config
            ], capture_output=True, text=True)
            
            assert result.returncode == 0, f"Generate command failed: {result.stderr}"
            
            # Test list command
            result = subprocess.run([
                sys.executable, "cli.py", "list", 
                "--output-dir", str(self.test_output_dir)
            ], capture_output=True, text=True)
            
            assert result.returncode == 0, f"List command failed: {result.stderr}"
            assert "Parquet Files" in result.stdout, "List output should contain file listing"
            
        finally:
            os.unlink(test_config)


if __name__ == "__main__":
    # Run tests with pytest
    pytest.main([__file__, "-v"])
