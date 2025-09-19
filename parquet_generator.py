"""
Parquet File Generator

A configurable system for generating multiple parquet files with custom schemas,
data distributions, and file parameters.
"""

import os
import yaml
import random
import logging
from typing import Dict, List, Any, Optional
from pathlib import Path
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timedelta
import uuid
import numpy as np

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class DataGenerator:
    """Handles generation of sample data based on column specifications."""
    
    def __init__(self, random_seed: Optional[int] = None):
        if random_seed is not None:
            random.seed(random_seed)
            np.random.seed(random_seed)
    
    def generate_column_data(self, column_spec: Dict[str, Any], num_rows: int) -> List[Any]:
        """Generate data for a single column based on its specification."""
        generator_config = column_spec.get('generator', {})
        generator_type = generator_config.get('type', 'random')
        nullable = column_spec.get('nullable', False)
        
        # Generate base data
        if generator_type == 'sequence':
            data = self._generate_sequence(generator_config, num_rows)
        elif generator_type == 'uuid':
            data = self._generate_uuid(num_rows)
        elif generator_type == 'datetime_range':
            data = self._generate_datetime_range(generator_config, num_rows)
        elif generator_type == 'normal':
            data = self._generate_normal(generator_config, num_rows)
        elif generator_type == 'choice':
            data = self._generate_choice(generator_config, num_rows)
        elif generator_type == 'boolean':
            data = self._generate_boolean(generator_config, num_rows)
        elif generator_type == 'uniform_int':
            data = self._generate_uniform_int(generator_config, num_rows)
        else:
            raise ValueError(f"Unknown generator type: {generator_type}")
        
        # Add null values if column is nullable
        if nullable:
            null_probability = generator_config.get('null_probability', 0.05)
            data = self._add_nulls(data, null_probability)
        
        return data
    
    def _generate_sequence(self, config: Dict[str, Any], num_rows: int) -> List[int]:
        """Generate sequential integers."""
        start = config.get('start', 1)
        return list(range(start, start + num_rows))
    
    def _generate_uuid(self, num_rows: int) -> List[str]:
        """Generate UUID strings."""
        return [str(uuid.uuid4()) for _ in range(num_rows)]
    
    def _generate_datetime_range(self, config: Dict[str, Any], num_rows: int) -> List[datetime]:
        """Generate random timestamps within a range."""
        start_str = config.get('start', '2023-01-01')
        end_str = config.get('end', '2024-12-31')
        
        start_date = datetime.strptime(start_str, '%Y-%m-%d')
        end_date = datetime.strptime(end_str, '%Y-%m-%d')
        
        time_between = end_date - start_date
        days_between = time_between.days
        
        return [start_date + timedelta(days=random.randint(0, days_between),
                                     hours=random.randint(0, 23),
                                     minutes=random.randint(0, 59),
                                     seconds=random.randint(0, 59))
                for _ in range(num_rows)]
    
    def _generate_normal(self, config: Dict[str, Any], num_rows: int) -> List[float]:
        """Generate normally distributed float values."""
        mean = config.get('mean', 0.0)
        std = config.get('std', 1.0)
        min_val = config.get('min', float('-inf'))
        max_val = config.get('max', float('inf'))
        
        data = np.random.normal(mean, std, num_rows)
        data = np.clip(data, min_val, max_val)
        return data.tolist()
    
    def _generate_choice(self, config: Dict[str, Any], num_rows: int) -> List[str]:
        """Generate random choices from a list."""
        choices = config.get('choices', ['A', 'B', 'C'])
        weights = config.get('weights', None)
        
        if weights:
            weights = np.array(weights) / np.sum(weights)  # Normalize weights
        
        return np.random.choice(choices, size=num_rows, p=weights).tolist()
    
    def _generate_boolean(self, config: Dict[str, Any], num_rows: int) -> List[bool]:
        """Generate random boolean values."""
        probability = config.get('probability', 0.5)
        return [random.random() < probability for _ in range(num_rows)]
    
    def _generate_uniform_int(self, config: Dict[str, Any], num_rows: int) -> List[int]:
        """Generate uniformly distributed integers."""
        min_val = config.get('min', 0)
        max_val = config.get('max', 100)
        return [random.randint(min_val, max_val) for _ in range(num_rows)]
    
    def _add_nulls(self, data: List[Any], null_probability: float) -> List[Any]:
        """Add null values to data based on probability."""
        return [None if random.random() < null_probability else value for value in data]


class ParquetGenerator:
    """Main class for generating parquet files based on configuration."""
    
    def __init__(self, config_path: str):
        self.config = self._load_config(config_path)
        self.data_generator = DataGenerator(self.config.get('global', {}).get('random_seed'))
        
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Load configuration from YAML file."""
        with open(config_path, 'r') as file:
            return yaml.safe_load(file)
    
    def generate_files(self):
        """Generate all parquet files according to configuration."""
        global_config = self.config.get('global', {})
        output_dir = Path(global_config.get('output_directory', './output'))
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Generate files based on main configuration
        files_config = self.config.get('files', {})
        if files_config:
            self._generate_file_set(files_config, output_dir, "")
        
        # Generate files based on specific file configurations
        file_configs = self.config.get('file_configs', [])
        for file_config in file_configs:
            suffix = file_config.get('file_suffix', '')
            self._generate_file_set(file_config, output_dir, suffix)
    
    def _generate_file_set(self, file_config: Dict[str, Any], output_dir: Path, suffix: str):
        """Generate a set of files based on specific configuration."""
        count = file_config.get('count', 1)
        rows_per_file = file_config.get('rows_per_file', 10000)
        size_variation = file_config.get('size_variation', 0.0)
        
        global_config = self.config.get('global', {})
        file_prefix = global_config.get('file_prefix', 'data')
        
        for i in range(count):
            # Calculate actual rows for this file (with variation)
            if size_variation > 0:
                variation = random.uniform(-size_variation, size_variation)
                actual_rows = int(rows_per_file * (1 + variation))
            else:
                actual_rows = rows_per_file
            
            # Generate filename
            filename = f"{file_prefix}{suffix}_{i+1:03d}.parquet"
            filepath = output_dir / filename
            
            # Generate and save file
            logger.info(f"Generating {filename} with {actual_rows} rows...")
            self._generate_single_file(filepath, actual_rows, file_config)
    
    def _generate_single_file(self, filepath: Path, num_rows: int, file_config: Dict[str, Any]):
        """Generate a single parquet file."""
        # Generate data for all columns
        data = {}
        schema_config = self.config.get('schema', {})
        columns = schema_config.get('columns', [])
        
        for column in columns:
            column_name = column['name']
            data[column_name] = self.data_generator.generate_column_data(column, num_rows)
        
        # Create DataFrame
        df = pd.DataFrame(data)
        
        # Convert to PyArrow table with proper schema
        arrow_schema = self._create_arrow_schema(columns)
        table = pa.Table.from_pandas(df, schema=arrow_schema)
        
        # Get parquet options
        parquet_options = file_config.get('parquet_options', 
                                        self.config.get('parquet_options', {}))
        
        # Write parquet file
        pq.write_table(
            table,
            filepath,
            compression=parquet_options.get('compression', 'snappy'),
            row_group_size=parquet_options.get('row_group_size', 50000),
            data_page_size=parquet_options.get('page_size', 8192),
            use_dictionary=parquet_options.get('use_dictionary', True),
            write_statistics=parquet_options.get('write_statistics', True)
        )
        
        logger.info(f"Generated {filepath} ({num_rows} rows, {filepath.stat().st_size / 1024 / 1024:.2f} MB)")
    
    def _create_arrow_schema(self, columns: List[Dict[str, Any]]) -> pa.Schema:
        """Create PyArrow schema from column specifications."""
        fields = []
        
        for column in columns:
            name = column['name']
            col_type = column['type']
            nullable = column.get('nullable', True)
            
            # Map column types to PyArrow types
            if col_type == 'int32':
                arrow_type = pa.int32()
            elif col_type == 'int64':
                arrow_type = pa.int64()
            elif col_type == 'float32':
                arrow_type = pa.float32()
            elif col_type == 'float64':
                arrow_type = pa.float64()
            elif col_type == 'string':
                arrow_type = pa.string()
            elif col_type == 'boolean':
                arrow_type = pa.bool_()
            elif col_type == 'timestamp':
                arrow_type = pa.timestamp('us')
            else:
                raise ValueError(f"Unsupported column type: {col_type}")
            
            fields.append(pa.field(name, arrow_type, nullable=nullable))
        
        return pa.schema(fields)
