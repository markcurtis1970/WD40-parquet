"""
Configuration management for Azure Uploader.
"""

import os
from pathlib import Path
from typing import Optional, List
from dotenv import load_dotenv


class Config:
    """Configuration class for Azure Uploader."""
    
    def __init__(self, env_file: Optional[str] = None):
        """
        Initialize configuration.
        
        Args:
            env_file: Path to .env file (optional)
        """
        # Load environment variables from .env file if it exists
        if env_file:
            load_dotenv(env_file)
        else:
            # Try to load from default locations
            for env_path in ['.env', Path.home() / '.azure_uploader.env']:
                if Path(env_path).exists():
                    load_dotenv(env_path)
                    break
    
    @property
    def azure_connection_string(self) -> str:
        """Get Azure Storage connection string."""
        connection_string = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
        if not connection_string:
            raise ValueError(
                "AZURE_STORAGE_CONNECTION_STRING environment variable is required. "
                "Please set it in your .env file or environment."
            )
        return connection_string
    
    @property
    def azure_container_name(self) -> str:
        """Get Azure container name."""
        container_name = os.getenv('AZURE_CONTAINER_NAME')
        if not container_name:
            raise ValueError(
                "AZURE_CONTAINER_NAME environment variable is required. "
                "Please set it in your .env file or environment."
            )
        return container_name
    
    @property
    def max_workers(self) -> int:
        """Get maximum number of worker threads."""
        return int(os.getenv('MAX_WORKERS', '5'))
    
    @property
    def chunk_size(self) -> int:
        """Get chunk size for uploads."""
        return int(os.getenv('CHUNK_SIZE', str(4 * 1024 * 1024)))  # 4MB default
    
    @property
    def max_retries(self) -> int:
        """Get maximum number of retry attempts."""
        return int(os.getenv('MAX_RETRIES', '3'))
    
    @property
    def upload_batch_size(self) -> int:
        """Get upload batch size."""
        return int(os.getenv('UPLOAD_BATCH_SIZE', '10'))
    
    @property
    def default_file_patterns(self) -> Optional[List[str]]:
        """Get default file patterns to include."""
        patterns = os.getenv('DEFAULT_FILE_PATTERNS')
        if patterns:
            return [p.strip() for p in patterns.split(',')]
        return None
    
    @property
    def default_exclude_patterns(self) -> Optional[List[str]]:
        """Get default exclude patterns."""
        patterns = os.getenv('DEFAULT_EXCLUDE_PATTERNS', '*.tmp,*.log,.*,__pycache__')
        if patterns:
            return [p.strip() for p in patterns.split(',')]
        return None
    
    @property
    def max_file_size(self) -> Optional[int]:
        """Get maximum file size limit."""
        size = os.getenv('MAX_FILE_SIZE')
        if size:
            return int(size)
        return None
    
    @property
    def log_level(self) -> str:
        """Get logging level."""
        return os.getenv('LOG_LEVEL', 'INFO').upper()
    
    @property
    def log_file(self) -> Optional[str]:
        """Get log file path."""
        return os.getenv('LOG_FILE')
    
    def validate(self) -> None:
        """Validate configuration."""
        # This will raise ValueError if required settings are missing
        self.azure_connection_string
        self.azure_container_name
        
        # Validate numeric values
        if self.max_workers <= 0:
            raise ValueError("MAX_WORKERS must be greater than 0")
        
        if self.chunk_size <= 0:
            raise ValueError("CHUNK_SIZE must be greater than 0")
        
        if self.max_retries < 0:
            raise ValueError("MAX_RETRIES must be non-negative")
    
    def __str__(self) -> str:
        """String representation of config (without sensitive data)."""
        return (
            f"Config("
            f"container={self.azure_container_name}, "
            f"max_workers={self.max_workers}, "
            f"chunk_size={self.chunk_size}, "
            f"max_retries={self.max_retries}, "
            f"log_level={self.log_level}"
            f")"
        )
