"""
Azure Blob Storage Uploader

A simple utility to upload files and directories to Azure Blob Storage.
"""

import os
import logging
import mimetypes
from pathlib import Path
from typing import Optional, List, Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import hashlib

from azure.storage.blob import BlobServiceClient, BlobClient, ContentSettings
from azure.core.exceptions import (
    AzureError,
    ResourceExistsError,
    ClientAuthenticationError,
    ResourceNotFoundError,
    HttpResponseError
)
from tqdm import tqdm


class AzureUploader:
    """Azure Blob Storage uploader with directory traversal support."""
    
    def __init__(
        self,
        connection_string: str,
        container_name: str,
        max_workers: int = 5,
        chunk_size: int = 4 * 1024 * 1024,  # 4MB
        max_retries: int = 3
    ):
        """
        Initialize the Azure uploader.
        
        Args:
            connection_string: Azure Storage connection string
            container_name: Name of the blob container
            max_workers: Maximum number of concurrent upload threads
            chunk_size: Size of chunks for large file uploads
            max_retries: Maximum number of retry attempts for failed uploads
        """
        self.connection_string = connection_string
        self.container_name = container_name
        self.max_workers = max_workers
        self.chunk_size = chunk_size
        self.max_retries = max_retries
        
        # Initialize Azure client
        self.blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        self.container_client = self.blob_service_client.get_container_client(container_name)
        
        # Setup logging
        self.logger = logging.getLogger(__name__)
        
        # Ensure container exists
        self._ensure_container_exists()

    def _calculate_file_hash(self, file_path: Path) -> str:
        """Calculate MD5 hash of a file for integrity checking."""
        hash_md5 = hashlib.md5()
        try:
            with open(file_path, "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hash_md5.update(chunk)
            return hash_md5.hexdigest()
        except Exception as e:
            self.logger.warning(f"Failed to calculate hash for {file_path}: {e}")
            return ""

    def _handle_azure_error(self, error: Exception, operation: str, file_path: Path = None) -> str:
        """
        Handle Azure-specific errors and return user-friendly error messages.

        Args:
            error: The exception that occurred
            operation: Description of the operation that failed
            file_path: Optional file path for context

        Returns:
            User-friendly error message
        """
        context = f" for {file_path}" if file_path else ""

        if isinstance(error, ClientAuthenticationError):
            return f"Authentication failed{context}. Please check your connection string and credentials."
        elif isinstance(error, ResourceNotFoundError):
            return f"Resource not found{context}. Please check container name and permissions."
        elif isinstance(error, HttpResponseError):
            if error.status_code == 403:
                return f"Access denied{context}. Please check your permissions."
            elif error.status_code == 404:
                return f"Container or resource not found{context}."
            elif error.status_code == 409:
                return f"Conflict occurred{context}. Resource may already exist."
            elif error.status_code == 413:
                return f"File too large{context}. Consider using chunked upload."
            else:
                return f"HTTP error {error.status_code} during {operation}{context}: {error.message}"
        elif isinstance(error, AzureError):
            return f"Azure error during {operation}{context}: {str(error)}"
        else:
            return f"Unexpected error during {operation}{context}: {str(error)}"
    
    def _ensure_container_exists(self) -> None:
        """Create the container if it doesn't exist."""
        try:
            self.container_client.create_container()
            self.logger.info(f"Created container: {self.container_name}")
        except ResourceExistsError:
            self.logger.debug(f"Container already exists: {self.container_name}")
        except Exception as e:
            error_msg = self._handle_azure_error(e, "container creation/access")
            self.logger.error(error_msg)
            raise RuntimeError(error_msg) from e
    
    def _get_blob_name(self, file_path: Path, base_path: Path, preserve_structure: bool = True) -> str:
        """
        Generate blob name from file path.
        
        Args:
            file_path: Path to the file
            base_path: Base directory path
            preserve_structure: Whether to preserve directory structure in blob names
            
        Returns:
            Blob name for Azure storage
        """
        if preserve_structure:
            # Preserve directory structure
            relative_path = file_path.relative_to(base_path)
            return str(relative_path).replace(os.sep, '/')
        else:
            # Use just the filename
            return file_path.name
    
    def _get_content_type(self, file_path: Path) -> str:
        """Get MIME type for the file."""
        content_type, _ = mimetypes.guess_type(str(file_path))
        return content_type or 'application/octet-stream'
    
    def upload_file(
        self,
        file_path: Path,
        blob_name: Optional[str] = None,
        overwrite: bool = False,
        metadata: Optional[Dict[str, str]] = None
    ) -> bool:
        """
        Upload a single file to Azure Blob Storage.
        
        Args:
            file_path: Path to the file to upload
            blob_name: Name for the blob (defaults to filename)
            overwrite: Whether to overwrite existing blobs
            metadata: Optional metadata to attach to the blob
            
        Returns:
            True if upload successful, False otherwise
        """
        if not file_path.exists() or not file_path.is_file():
            self.logger.error(f"File not found or not a file: {file_path}")
            return False
        
        if blob_name is None:
            blob_name = file_path.name
        
        # Calculate file hash for integrity checking
        file_hash = self._calculate_file_hash(file_path)
        file_size = file_path.stat().st_size

        # Retry logic
        for attempt in range(self.max_retries):
            try:
                blob_client = self.container_client.get_blob_client(blob_name)

                # Check if blob exists and overwrite is False
                if not overwrite:
                    try:
                        properties = blob_client.get_blob_properties()
                        # Check if file has changed by comparing size and hash
                        existing_hash = properties.metadata.get('file_hash', '') if properties.metadata else ''
                        if existing_hash == file_hash and properties.size == file_size:
                            self.logger.info(f"File unchanged, skipping: {blob_name}")
                            return True
                        elif existing_hash != file_hash:
                            self.logger.info(f"File changed, will upload: {blob_name}")
                        else:
                            self.logger.warning(f"Blob already exists, skipping: {blob_name}")
                            return True
                    except (AzureError, AttributeError):
                        # Blob doesn't exist or error accessing properties, proceed with upload
                        pass

                # Prepare upload parameters
                upload_kwargs = {
                    'overwrite': overwrite,
                    'content_settings': ContentSettings(
                        content_type=self._get_content_type(file_path)
                    ),
                    'metadata': {
                        'file_hash': file_hash,
                        'original_path': str(file_path),
                        'upload_timestamp': str(int(time.time()))
                    }
                }

                # Add user metadata
                if metadata:
                    upload_kwargs['metadata'].update(metadata)

                # Upload the file
                self.logger.debug(f"Uploading {file_path} ({file_size:,} bytes) -> {blob_name}")
                with open(file_path, 'rb') as data:
                    blob_client.upload_blob(data, **upload_kwargs)

                # Verify upload by checking blob properties
                try:
                    properties = blob_client.get_blob_properties()
                    if properties.size != file_size:
                        raise RuntimeError(f"Upload verification failed: size mismatch ({properties.size} != {file_size})")
                except Exception as verify_error:
                    self.logger.warning(f"Upload verification failed for {blob_name}: {verify_error}")

                self.logger.info(f"Successfully uploaded: {file_path} -> {blob_name} ({file_size:,} bytes)")
                return True

            except Exception as e:
                error_msg = self._handle_azure_error(e, "file upload", file_path)
                self.logger.warning(f"Upload attempt {attempt + 1} failed: {error_msg}")

                if attempt < self.max_retries - 1:
                    backoff_time = (2 ** attempt) + (attempt * 0.1)  # Exponential backoff with jitter
                    self.logger.debug(f"Retrying in {backoff_time:.1f} seconds...")
                    time.sleep(backoff_time)
                else:
                    self.logger.error(f"Failed to upload {file_path} after {self.max_retries} attempts: {error_msg}")
                    return False
        
        return False

    def scan_directory(
        self,
        directory_path: Path,
        file_patterns: Optional[List[str]] = None,
        exclude_patterns: Optional[List[str]] = None,
        max_file_size: Optional[int] = None
    ) -> List[Path]:
        """
        Recursively scan directory for files to upload.

        Args:
            directory_path: Path to the directory to scan
            file_patterns: List of file patterns to include (e.g., ['*.jpg', '*.png'])
            exclude_patterns: List of patterns to exclude (e.g., ['*.tmp', '.*'])
            max_file_size: Maximum file size in bytes (None for no limit)

        Returns:
            List of file paths to upload
        """
        if not directory_path.exists() or not directory_path.is_dir():
            self.logger.error(f"Directory not found: {directory_path}")
            return []

        files_to_upload = []

        # Use glob patterns if provided, otherwise get all files
        if file_patterns:
            for pattern in file_patterns:
                files_to_upload.extend(directory_path.rglob(pattern))
        else:
            files_to_upload.extend(directory_path.rglob('*'))

        # Filter out directories and apply exclusions
        filtered_files = []
        for file_path in files_to_upload:
            if not file_path.is_file():
                continue

            # Check exclude patterns
            if exclude_patterns:
                excluded = False
                for exclude_pattern in exclude_patterns:
                    # Check if the file matches the exclude pattern
                    if file_path.match(exclude_pattern):
                        excluded = True
                        break
                    # Special handling for .* pattern to exclude hidden files/directories
                    if exclude_pattern == '.*' and any(
                        part.startswith('.') and part not in ['.', '..']
                        for part in file_path.parts
                    ):
                        excluded = True
                        break
                if excluded:
                    continue

            # Check file size limit
            if max_file_size and file_path.stat().st_size > max_file_size:
                self.logger.warning(f"File too large, skipping: {file_path}")
                continue

            filtered_files.append(file_path)

        self.logger.info(f"Found {len(filtered_files)} files to upload in {directory_path}")
        return filtered_files

    def upload_directory(
        self,
        directory_path: Path,
        blob_prefix: str = "",
        preserve_structure: bool = True,
        overwrite: bool = False,
        file_patterns: Optional[List[str]] = None,
        exclude_patterns: Optional[List[str]] = None,
        max_file_size: Optional[int] = None,
        show_progress: bool = True
    ) -> Dict[str, Any]:
        """
        Upload an entire directory to Azure Blob Storage.

        Args:
            directory_path: Path to the directory to upload
            blob_prefix: Prefix to add to all blob names
            preserve_structure: Whether to preserve directory structure
            overwrite: Whether to overwrite existing blobs
            file_patterns: List of file patterns to include
            exclude_patterns: List of patterns to exclude
            max_file_size: Maximum file size in bytes
            show_progress: Whether to show progress bar

        Returns:
            Dictionary with upload statistics
        """
        directory_path = Path(directory_path)

        # Scan for files
        files_to_upload = self.scan_directory(
            directory_path, file_patterns, exclude_patterns, max_file_size
        )

        if not files_to_upload:
            self.logger.warning("No files found to upload")
            return {"total": 0, "successful": 0, "failed": 0, "skipped": 0}

        # Upload statistics
        stats = {"total": len(files_to_upload), "successful": 0, "failed": 0, "skipped": 0}

        # Setup progress bar
        progress_bar = None
        if show_progress:
            progress_bar = tqdm(
                total=len(files_to_upload),
                desc="Uploading files",
                unit="file"
            )

        # Upload files with threading
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit upload tasks
            future_to_file = {}
            for file_path in files_to_upload:
                blob_name = self._get_blob_name(file_path, directory_path, preserve_structure)
                if blob_prefix:
                    blob_name = f"{blob_prefix.rstrip('/')}/{blob_name}"

                future = executor.submit(self.upload_file, file_path, blob_name, overwrite)
                future_to_file[future] = file_path

            # Process completed uploads
            for future in as_completed(future_to_file):
                file_path = future_to_file[future]
                try:
                    success = future.result()
                    if success:
                        stats["successful"] += 1
                    else:
                        stats["failed"] += 1
                except Exception as e:
                    self.logger.error(f"Unexpected error uploading {file_path}: {e}")
                    stats["failed"] += 1

                if progress_bar:
                    progress_bar.update(1)

        if progress_bar:
            progress_bar.close()

        # Log summary
        self.logger.info(
            f"Upload complete. Total: {stats['total']}, "
            f"Successful: {stats['successful']}, "
            f"Failed: {stats['failed']}"
        )

        return stats
