# Azure Blob Storage Uploader

A simple, robust Python utility for uploading files and directories to Azure Blob Storage with support for recursive directory traversal, concurrent uploads, and comprehensive error handling.

## Features

- 🚀 **Concurrent uploads** with configurable thread pool
- 📁 **Recursive directory traversal** with pattern matching
- 🔄 **Automatic retry logic** with exponential backoff
- 🛡️ **Comprehensive error handling** with user-friendly messages
- 📊 **Progress tracking** with visual progress bars
- 🔍 **File integrity checking** with MD5 hashing
- ⚙️ **Flexible configuration** via environment variables or config files
- 🖥️ **Command-line interface** for easy usage
- 📝 **Detailed logging** with configurable levels

## Installation

1. Clone this repository:
```bash
git clone <repository-url>
cd WD40-azure
```

2. Install dependencies:
```bash
pip install -r ../requirements.txt
```

3. Set up your Azure configuration by copying the example environment file:
```bash
cp .env.example .env
```

4. Edit `.env` with your Azure Storage credentials:
```bash
# Azure Storage Configuration
AZURE_STORAGE_CONNECTION_STRING=DefaultEndpointsProtocol=https;AccountName=your_account_name;AccountKey=your_account_key;EndpointSuffix=core.windows.net
AZURE_CONTAINER_NAME=your-container-name
```

## Quick Start

### Command Line Usage

Upload a single file:
```bash
python cli.py upload-file /path/to/file.txt
```

Upload a directory:
```bash
python cli.py upload-directory /path/to/directory
```

Upload with custom options:
```bash
python cli.py upload-directory /path/to/directory \
    --blob-prefix "backup/2024/" \
    --include "*.jpg" --include "*.png" \
    --exclude "*.tmp" \
    --overwrite
```

### Python API Usage

```python
from pathlib import Path
from azure_uploader import AzureUploader
from config import Config

# Load configuration
config = Config()

# Create uploader instance
uploader = AzureUploader(
    connection_string=config.azure_connection_string,
    container_name=config.azure_container_name,
    max_workers=5,
    max_retries=3
)

# Upload a single file
success = uploader.upload_file(
    file_path=Path("example.txt"),
    blob_name="uploads/example.txt",
    overwrite=True
)

# Upload a directory
stats = uploader.upload_directory(
    directory_path=Path("/path/to/directory"),
    blob_prefix="backup/",
    preserve_structure=True,
    file_patterns=["*.jpg", "*.png"],
    exclude_patterns=["*.tmp", ".*"]
)

print(f"Uploaded {stats['successful']} files successfully")
```

## Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `AZURE_STORAGE_CONNECTION_STRING` | Azure Storage connection string | Required |
| `AZURE_CONTAINER_NAME` | Azure blob container name | Required |
| `MAX_WORKERS` | Maximum concurrent upload threads | 5 |
| `CHUNK_SIZE` | Upload chunk size in bytes | 4194304 (4MB) |
| `MAX_RETRIES` | Maximum retry attempts for failed uploads | 3 |
| `DEFAULT_FILE_PATTERNS` | Default file patterns to include (comma-separated) | None |
| `DEFAULT_EXCLUDE_PATTERNS` | Default exclude patterns (comma-separated) | `*.tmp,*.log,.*,__pycache__` |
| `MAX_FILE_SIZE` | Maximum file size limit in bytes | None |
| `LOG_LEVEL` | Logging level (DEBUG, INFO, WARNING, ERROR) | INFO |
| `LOG_FILE` | Log file path | None (stdout only) |

### Configuration File

You can also use a custom configuration file:
```bash
python cli.py --config /path/to/custom.env upload-directory /path/to/directory
```

## Command Line Interface

### Global Options

- `--config, -c`: Path to configuration file (.env)
- `--log-level`: Logging level (DEBUG, INFO, WARNING, ERROR)
- `--log-file`: Log file path

### Commands

#### `upload-file`
Upload a single file to Azure Blob Storage.

```bash
python cli.py upload-file [OPTIONS] FILE_PATH
```

Options:
- `--blob-name`: Custom blob name (defaults to filename)
- `--overwrite`: Overwrite existing blobs
- `--metadata`: Metadata key=value pairs (can be used multiple times)

#### `upload-directory`
Upload a directory and its contents to Azure Blob Storage.

```bash
python cli.py upload-directory [OPTIONS] DIRECTORY_PATH
```

Options:
- `--blob-prefix`: Prefix for blob names
- `--preserve-structure/--flatten`: Preserve directory structure in blob names (default: preserve)
- `--overwrite`: Overwrite existing blobs
- `--include`: File patterns to include (can be used multiple times)
- `--exclude`: File patterns to exclude (can be used multiple times)
- `--max-size`: Maximum file size in bytes
- `--no-progress`: Disable progress bar

#### `config-info`
Display current configuration.

```bash
python cli.py config-info
```

## Examples

### Upload specific file types from a directory
```bash
python cli.py upload-directory /home/user/photos \
    --include "*.jpg" --include "*.jpeg" --include "*.png" \
    --blob-prefix "photos/2024/" \
    --preserve-structure
```

### Upload with size limit and exclusions
```bash
python cli.py upload-directory /home/user/documents \
    --exclude "*.tmp" --exclude "*.log" --exclude ".*" \
    --max-size 104857600 \
    --overwrite
```

### Upload with custom metadata
```bash
python cli.py upload-file important-document.pdf \
    --blob-name "documents/important-document.pdf" \
    --metadata "department=finance" \
    --metadata "classification=confidential" \
    --overwrite
```

## Error Handling

The uploader includes comprehensive error handling for common scenarios:

- **Authentication errors**: Invalid connection strings or credentials
- **Permission errors**: Insufficient access rights
- **Network errors**: Connection timeouts and interruptions
- **File system errors**: Missing files or permission issues
- **Azure service errors**: Rate limiting, service unavailability

All errors are logged with detailed information and user-friendly messages.

## Logging

Configure logging level and output:

```bash
# Log to file with DEBUG level
python cli.py --log-level DEBUG --log-file upload.log upload-directory /path/to/directory

# Quiet mode (ERROR level only)
python cli.py --log-level ERROR upload-directory /path/to/directory
```

## Performance Tips

1. **Adjust worker threads**: Increase `MAX_WORKERS` for better concurrency (but watch for rate limits)
2. **Optimize chunk size**: Larger chunks for big files, smaller for many small files
3. **Use file patterns**: Filter files at scan time rather than uploading everything
4. **Enable overwrite carefully**: Checking existing files adds overhead

## Troubleshooting

### Common Issues

1. **Authentication failed**: Check your connection string and ensure the storage account exists
2. **Container not found**: Verify the container name and ensure it exists (or will be created)
3. **Permission denied**: Ensure your account has appropriate permissions (Storage Blob Data Contributor)
4. **Files not found**: Check file paths and permissions on the local file system

### Debug Mode

Enable debug logging for detailed information:
```bash
python cli.py --log-level DEBUG upload-directory /path/to/directory
```

## Testing

The project includes comprehensive tests for both the CLI and core uploader functionality.

### Running Tests

1. **Install test dependencies**:
```bash
pip install -r ../requirements.txt
```

2. **Run all tests**:
```bash
python run_tests.py
```

3. **Run specific test file**:
```bash
python -m pytest test_cli.py -v
python -m pytest test_azure_uploader.py -v
python -m pytest test_directory_scanning.py -v
```

4. **Run specific test**:
```bash
python run_tests.py test_upload_file_success
```

### Test Coverage

The test suite covers:

- **CLI functionality**: Command parsing, option handling, error cases
- **Core uploader**: File upload, directory scanning, error handling
- **Directory scanning**: Pattern matching, exclude logic, relative paths
- **Configuration**: Environment variable handling, validation
- **Error handling**: Azure service errors, network issues, file system errors

### Test Structure

- `test_cli.py`: Tests for command-line interface
- `test_azure_uploader.py`: Tests for core uploader functionality
- `test_directory_scanning.py`: Specific tests for directory scanning edge cases
- `run_tests.py`: Test runner script with colored output

## License

This project is licensed under the MIT License - see the LICENSE file for details.
