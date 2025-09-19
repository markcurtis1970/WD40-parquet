# Parquet File Generator

A configurable system for generating multiple parquet files with custom schemas, data distributions, and file parameters. Perfect for testing, development, and creating sample datasets.

## Features

- **Configurable Schema**: Define custom column types, names, and data generation rules
- **Multiple Data Types**: Support for integers, floats, strings, booleans, timestamps, and UUIDs
- **Realistic Data Generation**: Various distribution types (normal, uniform, choice-based) with realistic patterns
- **Flexible File Parameters**: Control compression, row group size, file count, and size variations
- **Multiple File Configurations**: Generate different file sets with different parameters in one run
- **Advanced Data Generators**: Support for time series, correlated data, and hierarchical categories
- **CLI Interface**: Easy-to-use command line interface with validation and file inspection tools

## Installation

1. Clone or download this repository
2. Install the required dependencies:

```bash
pip install -r requirements.txt
```

## Quick Start

1. **Create a configuration file** (or use one of the examples):

```yaml
# simple_config.yaml
global:
  output_directory: "./output"
  file_prefix: "data"
  random_seed: 42

schema:
  columns:
    - name: "id"
      type: "int64"
      nullable: false
      generator:
        type: "sequence"
        start: 1
    - name: "name"
      type: "string"
      nullable: false
      generator:
        type: "choice"
        choices: ["Alice", "Bob", "Charlie"]

parquet_options:
  compression: "snappy"

files:
  count: 3
  rows_per_file: 10000
```

2. **Generate parquet files**:

```bash
python cli.py generate --config simple_config.yaml
```

3. **Inspect the generated files**:

```bash
python cli.py list --output-dir ./output
python cli.py info --file ./output/data_001.parquet
```

## Configuration Reference

### Global Settings

```yaml
global:
  output_directory: "./output"    # Where to save files
  file_prefix: "data"            # Prefix for generated files
  random_seed: 42                # For reproducible generation (optional)
```

### Schema Definition

```yaml
schema:
  columns:
    - name: "column_name"         # Column name
      type: "data_type"           # int32, int64, float32, float64, string, boolean, timestamp
      nullable: true/false        # Whether column can contain nulls
      generator:                  # How to generate data for this column
        type: "generator_type"    # See generator types below
        # ... generator-specific parameters
```

### Data Generator Types

#### Sequence Generator
```yaml
generator:
  type: "sequence"
  start: 1                      # Starting number
```

#### UUID Generator
```yaml
generator:
  type: "uuid"                  # Generates UUID strings
```

#### DateTime Range Generator
```yaml
generator:
  type: "datetime_range"
  start: "2023-01-01"          # Start date (YYYY-MM-DD)
  end: "2024-12-31"            # End date (YYYY-MM-DD)
```

#### Normal Distribution Generator
```yaml
generator:
  type: "normal"
  mean: 100.0                  # Mean value
  std: 25.0                    # Standard deviation
  min: 0.0                     # Minimum value (optional)
  max: 1000.0                  # Maximum value (optional)
```

#### Choice Generator
```yaml
generator:
  type: "choice"
  choices: ["A", "B", "C"]     # List of possible values
  weights: [0.5, 0.3, 0.2]     # Probability weights (optional)
```

#### Boolean Generator
```yaml
generator:
  type: "boolean"
  probability: 0.7             # Probability of True
```

#### Uniform Integer Generator
```yaml
generator:
  type: "uniform_int"
  min: 1                       # Minimum value
  max: 100                     # Maximum value
```

### Parquet Options

```yaml
parquet_options:
  compression: "snappy"         # snappy, gzip, lz4, brotli, none
  row_group_size: 50000        # Rows per row group
  page_size: 8192              # Page size in bytes
  use_dictionary: true         # Enable dictionary encoding
  write_statistics: true       # Write column statistics
```

### File Generation Settings

```yaml
files:
  count: 5                     # Number of files to generate
  rows_per_file: 100000        # Rows in each file
  size_variation: 0.1          # Random size variation (0.0 to 1.0)

# Optional: Multiple file configurations
file_configs:
  - file_suffix: "_small"      # Suffix for file names
    count: 3                   # Number of files
    rows_per_file: 10000       # Rows per file
    parquet_options:           # Override parquet options
      compression: "gzip"
```

## CLI Commands

### Generate Files
```bash
python cli.py generate --config config.yaml [--verbose]
```

### Validate Configuration
```bash
python cli.py validate --config config.yaml
```

### List Generated Files
```bash
python cli.py list [--output-dir ./output]
```

### Inspect File Details
```bash
python cli.py info --file path/to/file.parquet
```

## Example Use Cases

The `examples/` directory contains configuration files for common scenarios:

- **`simple_config.yaml`**: Basic example with minimal configuration
- **`ecommerce_config.yaml`**: E-commerce transaction data with realistic distributions
- **`iot_sensor_config.yaml`**: IoT sensor time-series data with multiple sensor types
- **`financial_config.yaml`**: Financial trading data with market-realistic patterns

## Advanced Features

### Nullable Columns
Add `null_probability` to any generator to introduce null values:

```yaml
generator:
  type: "normal"
  mean: 100.0
  std: 25.0
  null_probability: 0.05      # 5% null values
```

### File Size Variation
Control variation in file sizes:

```yaml
files:
  size_variation: 0.2         # ±20% variation in file sizes
```

### Multiple Compression Types
Generate files with different compression for testing:

```yaml
file_configs:
  - file_suffix: "_snappy"
    parquet_options:
      compression: "snappy"
  - file_suffix: "_gzip"
    parquet_options:
      compression: "gzip"
```

## Performance Tips

- Use `snappy` compression for fastest write/read performance
- Use `gzip` for smallest file sizes
- Adjust `row_group_size` based on your query patterns
- Use appropriate data types (int32 vs int64, float32 vs float64)
- Enable dictionary encoding for categorical data

## Troubleshooting

### Common Issues

1. **Import Error**: Make sure all dependencies are installed with `pip install -r requirements.txt`
2. **Permission Error**: Ensure the output directory is writable
3. **Memory Error**: Reduce `rows_per_file` for large datasets
4. **Invalid Configuration**: Use `python cli.py validate --config your_config.yaml` to check syntax

### Getting Help

Run any command with `--help` for detailed usage information:

```bash
python cli.py --help
python cli.py generate --help
```

## License

This project is open source and available under the MIT License.
