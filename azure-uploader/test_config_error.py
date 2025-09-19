#!/usr/bin/env python3
"""
Simple test to debug the configuration error handling.
"""

import os
from unittest.mock import patch
from click.testing import CliRunner
import cli


def test_config_error():
    """Test what happens when config is missing."""
    runner = CliRunner()
    
    # Clear all environment variables
    with patch.dict(os.environ, {}, clear=True):
        result = runner.invoke(cli.cli, ['config-info'])
        
        print(f"Exit code: {result.exit_code}")
        print(f"Output: {repr(result.output)}")
        print(f"Exception: {result.exception}")
        
        if hasattr(result, 'stderr_bytes') and result.stderr_bytes:
            print(f"Stderr: {result.stderr_bytes.decode()}")


if __name__ == "__main__":
    test_config_error()
