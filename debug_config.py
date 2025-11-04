#!/usr/bin/env python3
"""
Debug script to check configuration file issues
"""

import yaml
from pathlib import Path
import sys

def debug_config():
    print("🔍 Configuration Debug Tool")
    print("=" * 50)
    
    # Check current directory
    current_dir = Path.cwd()
    print(f"Current directory: {current_dir}")
    
    # Check for config file
    config_file = Path("production_config.yaml")
    print(f"Looking for: {config_file}")
    print(f"File exists: {config_file.exists()}")
    
    if config_file.exists():
        print(f"File size: {config_file.stat().st_size} bytes")
        
        # Try to read the file
        try:
            with open(config_file, 'r') as f:
                content = f.read()
            print(f"File content length: {len(content)} characters")
            
            # Show first few lines
            lines = content.split('\n')
            print(f"First 5 lines:")
            for i, line in enumerate(lines[:5], 1):
                print(f"  {i}: {repr(line)}")
            
            # Try to parse YAML
            try:
                config = yaml.safe_load(content)
                if config is None:
                    print("❌ YAML parsed as None (empty or invalid)")
                else:
                    print(f"✅ YAML parsed successfully")
                    print(f"Config keys: {list(config.keys()) if isinstance(config, dict) else 'Not a dict'}")
            except yaml.YAMLError as e:
                print(f"❌ YAML parsing error: {e}")
                
        except Exception as e:
            print(f"❌ Error reading file: {e}")
    else:
        print("❌ Configuration file not found!")
        print("Available YAML files in current directory:")
        yaml_files = list(current_dir.glob("*.yaml"))
        if yaml_files:
            for file in yaml_files:
                print(f"  - {file.name}")
        else:
            print("  No YAML files found")
    
    print("\n" + "=" * 50)

if __name__ == "__main__":
    debug_config()