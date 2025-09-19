#!/usr/bin/env python3
"""
Debug the configuration error test.
"""

import os
import sys
from pathlib import Path

# Add current directory to path so we can import our modules
sys.path.insert(0, str(Path(__file__).parent))

try:
    from config import Config
    
    print("Testing Config with no environment variables...")
    
    # Clear environment
    old_env = {}
    for key in ['AZURE_STORAGE_CONNECTION_STRING', 'AZURE_CONTAINER_NAME']:
        if key in os.environ:
            old_env[key] = os.environ[key]
            del os.environ[key]
    
    try:
        print("Creating Config instance...")
        config = Config()
        print("Config created successfully")

        print("Calling validate()...")
        config.validate()
        print("❌ Expected ValueError but none was raised")
    except ValueError as e:
        print(f"✅ Got expected ValueError: {e}")
    except Exception as e:
        print(f"❌ Got unexpected exception: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
    
    # Restore environment
    for key, value in old_env.items():
        os.environ[key] = value
        
except ImportError as e:
    print(f"❌ Import error: {e}")
except Exception as e:
    print(f"❌ Unexpected error: {e}")
