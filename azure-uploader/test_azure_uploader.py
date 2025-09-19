#!/usr/bin/env python3
"""
Test suite for AzureUploader functionality.
"""

import os
import tempfile
import shutil
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock, mock_open
import pytest
from azure.core.exceptions import AzureError, ResourceExistsError

from azure_uploader import AzureUploader


class TestAzureUploader:
    """Test cases for AzureUploader class."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.temp_dir = Path(tempfile.mkdtemp())
        
        # Create test files
        self.test_file = self.temp_dir / "test_file.txt"
        self.test_file.write_text("Test content for hashing")
        
        self.test_dir = self.temp_dir / "test_directory"
        self.test_dir.mkdir()
        (self.test_dir / "file1.txt").write_text("File 1 content")
        (self.test_dir / "file2.jpg").write_text("Fake image content")
        (self.test_dir / ".hidden_file").write_text("Hidden file")
        (self.test_dir / "temp.tmp").write_text("Temporary file")
        
        # Create subdirectory
        sub_dir = self.test_dir / "subdir"
        sub_dir.mkdir()
        (sub_dir / "nested.txt").write_text("Nested file")
    
    def teardown_method(self):
        """Clean up test fixtures."""
        if self.temp_dir.exists():
            shutil.rmtree(self.temp_dir)
    
    @patch('azure_uploader.BlobServiceClient')
    def test_uploader_initialization(self, mock_blob_service):
        """Test AzureUploader initialization."""
        mock_container_client = Mock()
        mock_blob_service.from_connection_string.return_value.get_container_client.return_value = mock_container_client
        
        uploader = AzureUploader(
            connection_string="test_connection",
            container_name="test-container",
            max_workers=3,
            chunk_size=1024,
            max_retries=2
        )
        
        assert uploader.connection_string == "test_connection"
        assert uploader.container_name == "test-container"
        assert uploader.max_workers == 3
        assert uploader.chunk_size == 1024
        assert uploader.max_retries == 2
        
        # Verify Azure client setup
        mock_blob_service.from_connection_string.assert_called_once_with("test_connection")
    
    @patch('azure_uploader.BlobServiceClient')
    def test_container_creation_success(self, mock_blob_service):
        """Test successful container creation."""
        mock_container_client = Mock()
        mock_blob_service.from_connection_string.return_value.get_container_client.return_value = mock_container_client
        
        uploader = AzureUploader("test_connection", "test-container")
        
        mock_container_client.create_container.assert_called_once()
    
    @patch('azure_uploader.BlobServiceClient')
    def test_container_already_exists(self, mock_blob_service):
        """Test container already exists scenario."""
        mock_container_client = Mock()
        mock_container_client.create_container.side_effect = ResourceExistsError("Container exists")
        mock_blob_service.from_connection_string.return_value.get_container_client.return_value = mock_container_client
        
        # Should not raise an exception
        uploader = AzureUploader("test_connection", "test-container")
        
        mock_container_client.create_container.assert_called_once()
    
    @patch('azure_uploader.BlobServiceClient')
    def test_container_creation_failure(self, mock_blob_service):
        """Test container creation failure."""
        mock_container_client = Mock()
        mock_container_client.create_container.side_effect = AzureError("Access denied")
        mock_blob_service.from_connection_string.return_value.get_container_client.return_value = mock_container_client
        
        with pytest.raises(RuntimeError):
            AzureUploader("test_connection", "test-container")
    
    def test_calculate_file_hash(self):
        """Test file hash calculation."""
        with patch('azure_uploader.BlobServiceClient'):
            uploader = AzureUploader("test_connection", "test-container")
            
            file_hash = uploader._calculate_file_hash(self.test_file)
            
            # Hash should be consistent
            assert len(file_hash) == 32  # MD5 hash length
            assert file_hash == uploader._calculate_file_hash(self.test_file)
    
    def test_calculate_file_hash_nonexistent(self):
        """Test file hash calculation for non-existent file."""
        with patch('azure_uploader.BlobServiceClient'):
            uploader = AzureUploader("test_connection", "test-container")
            
            nonexistent_file = self.temp_dir / "nonexistent.txt"
            file_hash = uploader._calculate_file_hash(nonexistent_file)
            
            assert file_hash == ""  # Should return empty string on error
    
    def test_get_blob_name_preserve_structure(self):
        """Test blob name generation with preserved structure."""
        with patch('azure_uploader.BlobServiceClient'):
            uploader = AzureUploader("test_connection", "test-container")
            
            file_path = self.test_dir / "subdir" / "nested.txt"
            blob_name = uploader._get_blob_name(file_path, self.test_dir, preserve_structure=True)
            
            assert blob_name == "subdir/nested.txt"
    
    def test_get_blob_name_flatten(self):
        """Test blob name generation without preserved structure."""
        with patch('azure_uploader.BlobServiceClient'):
            uploader = AzureUploader("test_connection", "test-container")
            
            file_path = self.test_dir / "subdir" / "nested.txt"
            blob_name = uploader._get_blob_name(file_path, self.test_dir, preserve_structure=False)
            
            assert blob_name == "nested.txt"
    
    def test_get_content_type(self):
        """Test MIME type detection."""
        with patch('azure_uploader.BlobServiceClient'):
            uploader = AzureUploader("test_connection", "test-container")
            
            # Test known file types
            assert uploader._get_content_type(Path("test.txt")) == "text/plain"
            assert uploader._get_content_type(Path("test.jpg")) == "image/jpeg"
            assert uploader._get_content_type(Path("test.json")) == "application/json"
            
            # Test unknown file type
            assert uploader._get_content_type(Path("test.unknown")) == "application/octet-stream"
    
    def test_scan_directory_all_files(self):
        """Test directory scanning without filters."""
        with patch('azure_uploader.BlobServiceClient'):
            uploader = AzureUploader("test_connection", "test-container")
            
            files = uploader.scan_directory(self.test_dir)
            
            # Should find all files (excluding directories)
            file_names = [f.name for f in files]
            assert "file1.txt" in file_names
            assert "file2.jpg" in file_names
            assert ".hidden_file" in file_names
            assert "temp.tmp" in file_names
            assert "nested.txt" in file_names
    
    def test_scan_directory_with_patterns(self):
        """Test directory scanning with file patterns."""
        with patch('azure_uploader.BlobServiceClient'):
            uploader = AzureUploader("test_connection", "test-container")
            
            files = uploader.scan_directory(
                self.test_dir,
                file_patterns=["*.txt"]
            )
            
            file_names = [f.name for f in files]
            assert "file1.txt" in file_names
            assert "nested.txt" in file_names
            assert "file2.jpg" not in file_names
            assert "temp.tmp" not in file_names
    
    def test_scan_directory_with_excludes(self):
        """Test directory scanning with exclude patterns."""
        with patch('azure_uploader.BlobServiceClient'):
            uploader = AzureUploader("test_connection", "test-container")
            
            files = uploader.scan_directory(
                self.test_dir,
                exclude_patterns=["*.tmp", ".*"]
            )
            
            file_names = [f.name for f in files]
            assert "file1.txt" in file_names
            assert "file2.jpg" in file_names
            assert "nested.txt" in file_names
            assert "temp.tmp" not in file_names
            assert ".hidden_file" not in file_names
    
    def test_scan_directory_with_size_limit(self):
        """Test directory scanning with file size limit."""
        with patch('azure_uploader.BlobServiceClient'):
            uploader = AzureUploader("test_connection", "test-container")
            
            # Set a very small size limit
            files = uploader.scan_directory(
                self.test_dir,
                max_file_size=5  # 5 bytes
            )
            
            # Should exclude files larger than 5 bytes
            assert len(files) == 0  # All our test files are larger than 5 bytes
    
    def test_scan_nonexistent_directory(self):
        """Test scanning non-existent directory."""
        with patch('azure_uploader.BlobServiceClient'):
            uploader = AzureUploader("test_connection", "test-container")
            
            files = uploader.scan_directory(Path("/nonexistent/directory"))
            
            assert files == []
    
    def test_handle_azure_error(self):
        """Test Azure error handling."""
        with patch('azure_uploader.BlobServiceClient'):
            uploader = AzureUploader("test_connection", "test-container")
            
            from azure.core.exceptions import ClientAuthenticationError, HttpResponseError
            
            # Test authentication error
            auth_error = ClientAuthenticationError("Auth failed")
            message = uploader._handle_azure_error(auth_error, "test operation")
            assert "Authentication failed" in message
            
            # Test HTTP error with specific status codes
            http_error_403 = HttpResponseError("Forbidden")
            http_error_403.status_code = 403
            message = uploader._handle_azure_error(http_error_403, "test operation")
            assert "Access denied" in message
            
            # Test generic error
            generic_error = Exception("Generic error")
            message = uploader._handle_azure_error(generic_error, "test operation")
            assert "Unexpected error" in message
    
    @patch('azure_uploader.BlobServiceClient')
    def test_upload_file_success(self, mock_blob_service):
        """Test successful file upload."""
        # Setup mocks
        mock_container_client = Mock()
        mock_blob_client = Mock()
        mock_container_client.get_blob_client.return_value = mock_blob_client
        mock_blob_service.from_connection_string.return_value.get_container_client.return_value = mock_container_client
        
        # Mock blob doesn't exist (AzureError on get_blob_properties)
        mock_blob_client.get_blob_properties.side_effect = AzureError("Not found")
        
        # Mock successful upload
        mock_blob_client.upload_blob.return_value = None
        
        # Mock verification
        mock_properties = Mock()
        mock_properties.size = self.test_file.stat().st_size
        mock_blob_client.get_blob_properties.side_effect = [AzureError("Not found"), mock_properties]
        
        uploader = AzureUploader("test_connection", "test-container")
        
        with patch('builtins.open', mock_open(read_data=b"test content")):
            result = uploader.upload_file(self.test_file, "test_blob.txt", overwrite=True)
        
        assert result is True
        mock_blob_client.upload_blob.assert_called_once()


def test_integration_scan_and_upload():
    """Integration test for scanning and uploading."""
    # This would be a more complex test that combines scanning and uploading
    # For now, we'll keep it simple
    pass


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
