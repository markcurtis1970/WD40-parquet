#!/usr/bin/env python3
"""
Test runner script for the Azure Uploader project.
"""

import sys
import subprocess
from pathlib import Path


def run_tests():
    """Run all tests with pytest."""
    print("🧪 Running Azure Uploader Tests")
    print("=" * 50)
    
    # Check if pytest is available
    try:
        import pytest
    except ImportError:
        print("❌ pytest not found. Please install test dependencies:")
        print("   pip install -r requirements.txt")
        return 1
    
    # Run tests
    test_files = [
        "test_cli.py",
        "test_azure_uploader.py"
    ]
    
    # Check if test files exist
    missing_files = []
    for test_file in test_files:
        if not Path(test_file).exists():
            missing_files.append(test_file)
    
    if missing_files:
        print(f"❌ Missing test files: {', '.join(missing_files)}")
        return 1
    
    # Run pytest with verbose output
    cmd = [
        sys.executable, "-m", "pytest",
        "-v",  # verbose
        "--tb=short",  # shorter traceback format
        "--color=yes",  # colored output
        *test_files
    ]
    
    print(f"Running: {' '.join(cmd)}")
    print()
    
    try:
        result = subprocess.run(cmd, check=False)
        return result.returncode
    except KeyboardInterrupt:
        print("\n❌ Tests interrupted by user")
        return 1
    except Exception as e:
        print(f"❌ Error running tests: {e}")
        return 1


def run_specific_test(test_name):
    """Run a specific test."""
    cmd = [
        sys.executable, "-m", "pytest",
        "-v",
        "-k", test_name,
        "--tb=short",
        "--color=yes"
    ]
    
    print(f"Running specific test: {test_name}")
    print(f"Command: {' '.join(cmd)}")
    print()
    
    try:
        result = subprocess.run(cmd, check=False)
        return result.returncode
    except Exception as e:
        print(f"❌ Error running test: {e}")
        return 1


def main():
    """Main entry point."""
    if len(sys.argv) > 1:
        # Run specific test
        test_name = sys.argv[1]
        return run_specific_test(test_name)
    else:
        # Run all tests
        return run_tests()


if __name__ == "__main__":
    exit_code = main()
    
    if exit_code == 0:
        print("\n✅ All tests passed!")
    else:
        print(f"\n❌ Tests failed with exit code {exit_code}")
    
    sys.exit(exit_code)
