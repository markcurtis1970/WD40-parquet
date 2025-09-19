#!/usr/bin/env python3
"""
Example usage of the Azure Uploader.

This script demonstrates how to use the Azure Uploader programmatically.
"""

import logging
from pathlib import Path
from azure_uploader import AzureUploader
from config import Config


def setup_logging():
    """Setup basic logging configuration."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )


def example_single_file_upload():
    """Example: Upload a single file."""
    print("=== Single File Upload Example ===")
    
    # Load configuration
    config = Config()
    
    # Create uploader
    uploader = AzureUploader(
        connection_string=config.azure_connection_string,
        container_name=config.azure_container_name,
        max_workers=config.max_workers,
        max_retries=config.max_retries
    )
    
    # Create a test file
    test_file = Path("test_file.txt")
    test_file.write_text("Hello, Azure Blob Storage!")
    
    try:
        # Upload the file
        success = uploader.upload_file(
            file_path=test_file,
            blob_name="examples/test_file.txt",
            overwrite=True,
            metadata={
                "source": "example_script",
                "purpose": "testing"
            }
        )
        
        if success:
            print(f"✅ Successfully uploaded {test_file}")
        else:
            print(f"❌ Failed to upload {test_file}")
            
    finally:
        # Clean up test file
        if test_file.exists():
            test_file.unlink()


def example_directory_upload():
    """Example: Upload a directory with filtering."""
    print("\n=== Directory Upload Example ===")
    
    # Create a test directory structure
    test_dir = Path("test_directory")
    test_dir.mkdir(exist_ok=True)
    
    # Create some test files
    (test_dir / "document.txt").write_text("This is a text document")
    (test_dir / "image.jpg").write_text("Fake image content")
    (test_dir / "temp.tmp").write_text("Temporary file")
    
    # Create subdirectory
    sub_dir = test_dir / "subdirectory"
    sub_dir.mkdir(exist_ok=True)
    (sub_dir / "nested_file.txt").write_text("Nested file content")
    
    try:
        # Load configuration
        config = Config()
        
        # Create uploader
        uploader = AzureUploader(
            connection_string=config.azure_connection_string,
            container_name=config.azure_container_name,
            max_workers=config.max_workers,
            max_retries=config.max_retries
        )
        
        # Upload directory with filtering
        stats = uploader.upload_directory(
            directory_path=test_dir,
            blob_prefix="examples/directory_upload/",
            preserve_structure=True,
            overwrite=True,
            file_patterns=["*.txt", "*.jpg"],  # Only upload text and image files
            exclude_patterns=["*.tmp"],        # Exclude temporary files
            show_progress=True
        )
        
        print(f"\n📊 Upload Statistics:")
        print(f"   Total files: {stats['total']}")
        print(f"   Successful: {stats['successful']}")
        print(f"   Failed: {stats['failed']}")
        
    finally:
        # Clean up test directory
        import shutil
        if test_dir.exists():
            shutil.rmtree(test_dir)


def example_with_error_handling():
    """Example: Demonstrate error handling."""
    print("\n=== Error Handling Example ===")
    
    try:
        # Try to create uploader with invalid connection string
        uploader = AzureUploader(
            connection_string="invalid_connection_string",
            container_name="test-container"
        )
        
        # This should fail
        uploader.upload_file(Path("nonexistent_file.txt"))
        
    except Exception as e:
        print(f"✅ Caught expected error: {e}")


def main():
    """Run all examples."""
    setup_logging()
    
    print("Azure Uploader Examples")
    print("=" * 50)
    
    try:
        # Check if configuration is available
        config = Config()
        config.validate()
        
        # Run examples
        example_single_file_upload()
        example_directory_upload()
        example_with_error_handling()
        
        print("\n🎉 All examples completed!")
        
    except ValueError as e:
        print(f"❌ Configuration error: {e}")
        print("\nPlease ensure you have:")
        print("1. Created a .env file with your Azure credentials")
        print("2. Set AZURE_STORAGE_CONNECTION_STRING and AZURE_CONTAINER_NAME")
        print("\nSee README.md for setup instructions.")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")


if __name__ == "__main__":
    main()
