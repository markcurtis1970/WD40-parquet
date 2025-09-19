#!/usr/bin/env python3
"""
Specific tests for directory scanning functionality.
This tests the fix for the relative path issue we encountered.
"""

import tempfile
import shutil
from pathlib import Path
from unittest.mock import patch
import pytest

from azure_uploader import AzureUploader


class TestDirectoryScanning:
    """Test cases specifically for directory scanning edge cases."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.temp_dir = Path(tempfile.mkdtemp())
        
        # Create a directory structure similar to the real scenario
        self.output_dir = self.temp_dir / "WD40-parquet" / "output"
        self.output_dir.mkdir(parents=True)
        
        # Create test parquet files
        (self.output_dir / "simple_data_001.parquet").write_text("Parquet data 1")
        (self.output_dir / "simple_data_002.parquet").write_text("Parquet data 2")
        (self.output_dir / "simple_data_003.parquet").write_text("Parquet data 3")
        
        # Create a subdirectory with files
        iot_dir = self.output_dir / "iot_sensors"
        iot_dir.mkdir()
        (iot_dir / "sensor_data.parquet").write_text("IoT sensor data")
        
        # Create some files that should be excluded
        (self.output_dir / ".hidden_file").write_text("Hidden file")
        (self.output_dir / "temp.tmp").write_text("Temporary file")
        (self.output_dir / "debug.log").write_text("Log file")
    
    def teardown_method(self):
        """Clean up test fixtures."""
        if self.temp_dir.exists():
            shutil.rmtree(self.temp_dir)
    
    @patch('azure_uploader.BlobServiceClient')
    def test_relative_path_scanning(self, mock_blob_service):
        """Test scanning with relative paths (the original issue)."""
        uploader = AzureUploader("test_connection", "test-container")
        
        # Change to the parent directory to simulate the original scenario
        import os
        original_cwd = os.getcwd()
        try:
            os.chdir(self.temp_dir)
            
            # Scan using relative path like "../WD40-parquet/output"
            relative_path = Path("WD40-parquet/output")
            files = uploader.scan_directory(relative_path)
            
            # Should find all parquet files
            file_names = [f.name for f in files]
            assert "simple_data_001.parquet" in file_names
            assert "simple_data_002.parquet" in file_names
            assert "simple_data_003.parquet" in file_names
            assert "sensor_data.parquet" in file_names
            
            # Should include hidden and temp files when no exclude patterns
            assert ".hidden_file" in file_names
            assert "temp.tmp" in file_names
            assert "debug.log" in file_names
            
        finally:
            os.chdir(original_cwd)
    
    @patch('azure_uploader.BlobServiceClient')
    def test_relative_path_with_default_excludes(self, mock_blob_service):
        """Test scanning with relative paths and default exclude patterns."""
        uploader = AzureUploader("test_connection", "test-container")
        
        import os
        original_cwd = os.getcwd()
        try:
            os.chdir(self.temp_dir)
            
            relative_path = Path("WD40-parquet/output")
            
            # Use default exclude patterns
            default_excludes = ['*.tmp', '*.log', '.*', '__pycache__']
            files = uploader.scan_directory(
                relative_path,
                exclude_patterns=default_excludes
            )
            
            file_names = [f.name for f in files]
            
            # Should find parquet files
            assert "simple_data_001.parquet" in file_names
            assert "simple_data_002.parquet" in file_names
            assert "simple_data_003.parquet" in file_names
            assert "sensor_data.parquet" in file_names
            
            # Should exclude hidden, temp, and log files
            assert ".hidden_file" not in file_names
            assert "temp.tmp" not in file_names
            assert "debug.log" not in file_names
            
        finally:
            os.chdir(original_cwd)
    
    @patch('azure_uploader.BlobServiceClient')
    def test_parent_directory_references(self, mock_blob_service):
        """Test that parent directory references (..) don't cause exclusion."""
        uploader = AzureUploader("test_connection", "test-container")
        
        # Create a nested directory structure
        nested_dir = self.temp_dir / "level1" / "level2"
        nested_dir.mkdir(parents=True)
        
        import os
        original_cwd = os.getcwd()
        try:
            os.chdir(nested_dir)
            
            # Scan using path with parent references
            relative_path = Path("../../WD40-parquet/output")
            files = uploader.scan_directory(
                relative_path,
                exclude_patterns=['.*']  # Should only exclude actual hidden files
            )
            
            file_names = [f.name for f in files]
            
            # Should find parquet files despite .. in path
            assert "simple_data_001.parquet" in file_names
            assert "simple_data_002.parquet" in file_names
            assert "simple_data_003.parquet" in file_names
            
            # Should still exclude actual hidden files
            assert ".hidden_file" not in file_names
            
        finally:
            os.chdir(original_cwd)
    
    @patch('azure_uploader.BlobServiceClient')
    def test_parquet_file_patterns(self, mock_blob_service):
        """Test scanning specifically for parquet files."""
        uploader = AzureUploader("test_connection", "test-container")
        
        files = uploader.scan_directory(
            self.output_dir,
            file_patterns=['*.parquet']
        )
        
        file_names = [f.name for f in files]
        
        # Should only find parquet files
        assert "simple_data_001.parquet" in file_names
        assert "simple_data_002.parquet" in file_names
        assert "simple_data_003.parquet" in file_names
        assert "sensor_data.parquet" in file_names
        
        # Should not find non-parquet files
        assert ".hidden_file" not in file_names
        assert "temp.tmp" not in file_names
        assert "debug.log" not in file_names
    
    @patch('azure_uploader.BlobServiceClient')
    def test_empty_directory(self, mock_blob_service):
        """Test scanning an empty directory."""
        uploader = AzureUploader("test_connection", "test-container")
        
        empty_dir = self.temp_dir / "empty"
        empty_dir.mkdir()
        
        files = uploader.scan_directory(empty_dir)
        
        assert len(files) == 0
    
    @patch('azure_uploader.BlobServiceClient')
    def test_directory_with_only_subdirectories(self, mock_blob_service):
        """Test scanning a directory that only contains subdirectories."""
        uploader = AzureUploader("test_connection", "test-container")
        
        dirs_only = self.temp_dir / "dirs_only"
        dirs_only.mkdir()
        (dirs_only / "subdir1").mkdir()
        (dirs_only / "subdir2").mkdir()
        
        files = uploader.scan_directory(dirs_only)
        
        assert len(files) == 0  # Should not include directories
    
    @patch('azure_uploader.BlobServiceClient')
    def test_mixed_file_types_with_patterns(self, mock_blob_service):
        """Test scanning with multiple file patterns."""
        uploader = AzureUploader("test_connection", "test-container")
        
        # Add some additional file types
        (self.output_dir / "data.json").write_text('{"test": "data"}')
        (self.output_dir / "config.yaml").write_text("config: value")
        (self.output_dir / "script.py").write_text("print('hello')")
        
        files = uploader.scan_directory(
            self.output_dir,
            file_patterns=['*.parquet', '*.json', '*.py']
        )
        
        file_names = [f.name for f in files]
        
        # Should find files matching any of the patterns
        assert "simple_data_001.parquet" in file_names
        assert "data.json" in file_names
        assert "script.py" in file_names
        
        # Should not find files not matching patterns
        assert "config.yaml" not in file_names
        assert "temp.tmp" not in file_names


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
