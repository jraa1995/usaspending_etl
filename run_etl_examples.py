#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Example scripts for running the USASpending ETL with different configurations
"""

import subprocess
import sys
from pathlib import Path

def run_command(cmd):
    """Run a command and print the result."""
    print(f"\n{'='*60}")
    print(f"Running: {' '.join(cmd)}")
    print('='*60)
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.stdout:
        print("STDOUT:")
        print(result.stdout)
    
    if result.stderr:
        print("STDERR:")
        print(result.stderr)
    
    print(f"Exit Code: {result.returncode}")
    return result.returncode == 0

def main():
    """Run various ETL examples."""
    
    # Example 1: Basic ETL with all data
    print("Example 1: Basic ETL processing all data")
    success = run_command([
        "python", "usaspending_etl_enhanced.py",
        "--input-dir", "usaspending_data",
        "--output-dir", "examples/basic_processing"
    ])
    
    if not success:
        print("Example 1 failed!")
        return
    
    # Example 2: Filter by fiscal year range
    print("\nExample 2: Filter by fiscal year 2024-2025")
    run_command([
        "python", "usaspending_etl_enhanced.py",
        "--input-dir", "usaspending_data",
        "--output-dir", "examples/fy_2024_2025",
        "--fiscal-year-start", "2024",
        "--fiscal-year-end", "2025"
    ])
    
    # Example 3: Filter by minimum dollar amount
    print("\nExample 3: Filter contracts >= $100,000")
    run_command([
        "python", "usaspending_etl_enhanced.py",
        "--input-dir", "usaspending_data",
        "--output-dir", "examples/large_contracts",
        "--min-dollars", "100000"
    ])
    
    # Example 4: Filter by specific agencies
    print("\nExample 4: Filter by specific agencies")
    run_command([
        "python", "usaspending_etl_enhanced.py",
        "--input-dir", "usaspending_data",
        "--output-dir", "examples/specific_agencies",
        "--agencies", "Department of Defense", "Department of Agriculture"
    ])
    
    # Example 5: Filter by instrument types
    print("\nExample 5: Filter by specific contract types")
    run_command([
        "python", "usaspending_etl_enhanced.py",
        "--input-dir", "usaspending_data",
        "--output-dir", "examples/specific_contracts",
        "--instrument-types", "DEFINITIVE CONTRACT", "DELIVERY ORDER"
    ])
    
    # Example 6: Combined filters
    print("\nExample 6: Combined filters - Large DOD contracts in FY2025")
    run_command([
        "python", "usaspending_etl_enhanced.py",
        "--input-dir", "usaspending_data",
        "--output-dir", "examples/dod_large_fy2025",
        "--fiscal-year-start", "2025",
        "--fiscal-year-end", "2025",
        "--min-dollars", "500000",
        "--agencies", "Department of Defense"
    ])
    
    print("\n" + "="*60)
    print("All examples completed!")
    print("Check the 'examples/' directory for output files.")
    print("="*60)

if __name__ == "__main__":
    main()