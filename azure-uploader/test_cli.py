#!/usr/bin/env python3
"""
Test suite for CLI functionality.
"""

import os
import tempfile
import shutil
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
import pytest
from click.testing import CliRunner

# Import the CLI module
import cli
from config import Config
from azure_uploader import AzureUploader


class TestCLI:
    """Test cases for CLI commands."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.runner = CliRunner()
        self.temp_dir = Path(tempfile.mkdtemp())
        
        # Create test files
        self.test_file = self.temp_dir / "test_file.txt"
        self.test_file.write_text("Test content")
        
        self.test_dir = self.temp_dir / "test_directory"
        self.test_dir.mkdir()
        (self.test_dir / "file1.txt").write_text("File 1 content")
        (self.test_dir / "file2.jpg").write_text("Fake image content")
        (self.test_dir / "file3.tmp").write_text("Temporary file")
        
        # Create subdirectory
        sub_dir = self.test_dir / "subdir"
        sub_dir.mkdir()
        (sub_dir / "nested.txt").write_text("Nested file")
    
    def teardown_method(self):
        """Clean up test fixtures."""
        if self.temp_dir.exists():
            shutil.rmtree(self.temp_dir)
    
    @patch.dict(os.environ, {
        'AZURE_STORAGE_CONNECTION_STRING': 'test_connection_string',
        'AZURE_CONTAINER_NAME': 'test-container'
    })
    def test_config_info_command(self):
        """Test the config-info command."""
        result = self.runner.invoke(cli.cli, ['config-info'])
        
        assert result.exit_code == 0
        assert "Current Configuration:" in result.output
        assert "Container: test-container" in result.output
        assert "Max Workers:" in result.output
        assert "Chunk Size:" in result.output
    
    def test_config_info_missing_config(self):
        """Test config-info command with missing configuration."""
        with patch.dict(os.environ, {}, clear=True):
            # Also patch Path.exists to prevent loading from .env file
            with patch('config.Path') as mock_path:
                mock_path.return_value.exists.return_value = False
                mock_path.home.return_value = Path('/fake/home')

                result = self.runner.invoke(cli.cli, ['config-info'])

                # Should exit with non-zero code
                assert result.exit_code != 0
                # Should contain configuration error message
                assert "Configuration error:" in result.output
    
    @patch.dict(os.environ, {
        'AZURE_STORAGE_CONNECTION_STRING': 'test_connection_string',
        'AZURE_CONTAINER_NAME': 'test-container'
    })
    @patch('cli.AzureUploader')
    def test_upload_file_success(self, mock_uploader_class):
        """Test successful file upload."""
        # Mock the uploader
        mock_uploader = Mock()
        mock_uploader.upload_file.return_value = True
        mock_uploader_class.return_value = mock_uploader
        
        result = self.runner.invoke(cli.cli, [
            'upload-file', 
            str(self.test_file)
        ])
        
        assert result.exit_code == 0
        assert f"Successfully uploaded: {self.test_file}" in result.output
        
        # Verify uploader was called correctly
        mock_uploader_class.assert_called_once()
        mock_uploader.upload_file.assert_called_once_with(
            file_path=self.test_file,
            blob_name=None,
            overwrite=False,
            metadata=None
        )
    
    @patch.dict(os.environ, {
        'AZURE_STORAGE_CONNECTION_STRING': 'test_connection_string',
        'AZURE_CONTAINER_NAME': 'test-container'
    })
    @patch('cli.AzureUploader')
    def test_upload_file_with_options(self, mock_uploader_class):
        """Test file upload with custom options."""
        mock_uploader = Mock()
        mock_uploader.upload_file.return_value = True
        mock_uploader_class.return_value = mock_uploader
        
        result = self.runner.invoke(cli.cli, [
            'upload-file',
            str(self.test_file),
            '--blob-name', 'custom/blob/name.txt',
            '--overwrite',
            '--metadata', 'key1=value1',
            '--metadata', 'key2=value2'
        ])
        
        assert result.exit_code == 0
        
        # Verify metadata was parsed correctly
        mock_uploader.upload_file.assert_called_once_with(
            file_path=self.test_file,
            blob_name='custom/blob/name.txt',
            overwrite=True,
            metadata={'key1': 'value1', 'key2': 'value2'}
        )
    
    @patch.dict(os.environ, {
        'AZURE_STORAGE_CONNECTION_STRING': 'test_connection_string',
        'AZURE_CONTAINER_NAME': 'test-container'
    })
    @patch('cli.AzureUploader')
    def test_upload_file_failure(self, mock_uploader_class):
        """Test file upload failure."""
        mock_uploader = Mock()
        mock_uploader.upload_file.return_value = False
        mock_uploader_class.return_value = mock_uploader
        
        result = self.runner.invoke(cli.cli, [
            'upload-file',
            str(self.test_file)
        ])
        
        assert result.exit_code == 1
        assert f"Failed to upload: {self.test_file}" in result.output
    
    def test_upload_file_invalid_metadata(self):
        """Test file upload with invalid metadata format."""
        result = self.runner.invoke(cli.cli, [
            'upload-file',
            str(self.test_file),
            '--metadata', 'invalid_format'
        ])
        
        assert result.exit_code == 1
        assert "Invalid metadata format" in result.output
    
    @patch.dict(os.environ, {
        'AZURE_STORAGE_CONNECTION_STRING': 'test_connection_string',
        'AZURE_CONTAINER_NAME': 'test-container'
    })
    @patch('cli.AzureUploader')
    def test_upload_directory_success(self, mock_uploader_class):
        """Test successful directory upload."""
        mock_uploader = Mock()
        mock_uploader.upload_directory.return_value = {
            'total': 4,
            'successful': 4,
            'failed': 0
        }
        mock_uploader_class.return_value = mock_uploader
        
        result = self.runner.invoke(cli.cli, [
            'upload-directory',
            str(self.test_dir)
        ])
        
        assert result.exit_code == 0
        assert "Upload Summary:" in result.output
        assert "Total files: 4" in result.output
        assert "Successful: 4" in result.output
        assert "Failed: 0" in result.output
        assert "All uploads completed successfully!" in result.output
        
        # Verify uploader was called correctly
        mock_uploader.upload_directory.assert_called_once()
        call_args = mock_uploader.upload_directory.call_args
        assert call_args[1]['directory_path'] == self.test_dir
        assert call_args[1]['preserve_structure'] is True
        assert call_args[1]['overwrite'] is False
    
    @patch.dict(os.environ, {
        'AZURE_STORAGE_CONNECTION_STRING': 'test_connection_string',
        'AZURE_CONTAINER_NAME': 'test-container'
    })
    @patch('cli.AzureUploader')
    def test_upload_directory_with_options(self, mock_uploader_class):
        """Test directory upload with custom options."""
        mock_uploader = Mock()
        mock_uploader.upload_directory.return_value = {
            'total': 2,
            'successful': 2,
            'failed': 0
        }
        mock_uploader_class.return_value = mock_uploader
        
        result = self.runner.invoke(cli.cli, [
            'upload-directory',
            str(self.test_dir),
            '--blob-prefix', 'backup/',
            '--flatten',
            '--overwrite',
            '--include', '*.txt',
            '--include', '*.jpg',
            '--exclude', '*.tmp',
            '--max-size', '1000000',
            '--no-progress'
        ])
        
        assert result.exit_code == 0
        
        # Verify options were passed correctly
        call_args = mock_uploader.upload_directory.call_args
        assert call_args[1]['blob_prefix'] == 'backup/'
        assert call_args[1]['preserve_structure'] is False  # --flatten
        assert call_args[1]['overwrite'] is True
        assert call_args[1]['file_patterns'] == ['*.txt', '*.jpg']
        assert call_args[1]['exclude_patterns'] == ['*.tmp']
        assert call_args[1]['max_file_size'] == 1000000
        assert call_args[1]['show_progress'] is False  # --no-progress
    
    @patch.dict(os.environ, {
        'AZURE_STORAGE_CONNECTION_STRING': 'test_connection_string',
        'AZURE_CONTAINER_NAME': 'test-container'
    })
    @patch('cli.AzureUploader')
    def test_upload_directory_with_failures(self, mock_uploader_class):
        """Test directory upload with some failures."""
        mock_uploader = Mock()
        mock_uploader.upload_directory.return_value = {
            'total': 4,
            'successful': 2,
            'failed': 2
        }
        mock_uploader_class.return_value = mock_uploader
        
        result = self.runner.invoke(cli.cli, [
            'upload-directory',
            str(self.test_dir)
        ])
        
        assert result.exit_code == 1
        assert "Failed: 2" in result.output
        assert "Some uploads failed" in result.output
    
    @patch.dict(os.environ, {
        'AZURE_STORAGE_CONNECTION_STRING': 'test_connection_string',
        'AZURE_CONTAINER_NAME': 'test-container'
    })
    @patch('cli.AzureUploader')
    def test_upload_directory_exception(self, mock_uploader_class):
        """Test directory upload with exception."""
        mock_uploader_class.side_effect = Exception("Connection failed")
        
        result = self.runner.invoke(cli.cli, [
            'upload-directory',
            str(self.test_dir)
        ])
        
        assert result.exit_code == 1
        assert "Error: Connection failed" in result.output
    
    def test_upload_nonexistent_file(self):
        """Test uploading a non-existent file."""
        result = self.runner.invoke(cli.cli, [
            'upload-file',
            '/nonexistent/file.txt'
        ])
        
        assert result.exit_code == 2  # Click's exit code for invalid path
        assert "does not exist" in result.output
    
    def test_upload_nonexistent_directory(self):
        """Test uploading a non-existent directory."""
        result = self.runner.invoke(cli.cli, [
            'upload-directory',
            '/nonexistent/directory'
        ])
        
        assert result.exit_code == 2  # Click's exit code for invalid path
        assert "does not exist" in result.output
    
    @patch('cli.setup_logging')
    def test_logging_setup(self, mock_setup_logging):
        """Test that logging is set up correctly."""
        with patch.dict(os.environ, {
            'AZURE_STORAGE_CONNECTION_STRING': 'test_connection_string',
            'AZURE_CONTAINER_NAME': 'test-container'
        }):
            result = self.runner.invoke(cli.cli, [
                '--log-level', 'DEBUG',
                '--log-file', '/tmp/test.log',
                'config-info'
            ])
            
            mock_setup_logging.assert_called_once_with('DEBUG', '/tmp/test.log')


def test_setup_logging():
    """Test the setup_logging function."""
    import logging
    import sys
    from io import StringIO
    
    # Capture log output
    log_capture = StringIO()
    
    with patch('sys.stdout', log_capture):
        cli.setup_logging('INFO')
        
        # Test that logging works
        logger = logging.getLogger('test')
        logger.info("Test message")
    
    # Note: This is a basic test. In practice, you might want to test
    # the actual log configuration more thoroughly


if __name__ == "__main__":
    # Run tests with pytest
    pytest.main([__file__, "-v"])
