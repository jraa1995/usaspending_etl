#!/usr/bin/env python3
"""
Check the downloaded data files
"""

import pandas as pd
from pathlib import Path

def check_data():
    print("🔍 Checking Downloaded Data")
    print("=" * 40)
    
    # Find the CSV file
    raw_data_dir = Path("raw_data")
    csv_files = list(raw_data_dir.rglob("*.csv"))
    
    if not csv_files:
        print("❌ No CSV files found")
        return
    
    for csv_file in csv_files:
        print(f"\n📁 File: {csv_file}")
        print(f"   Size: {csv_file.stat().st_size} bytes")
        
        if csv_file.stat().st_size == 0:
            print("   ❌ File is empty!")
            continue
        
        try:
            # Try to read the file
            df = pd.read_csv(csv_file, nrows=5)  # Just read first 5 rows
            print(f"   ✅ Readable: {len(df)} rows (sample)")
            print(f"   📊 Columns: {len(df.columns)} total")
            if len(df.columns) > 0:
                print(f"   📋 Sample columns: {list(df.columns)[:5]}")
            
            # Check if it has data
            full_df = pd.read_csv(csv_file)
            print(f"   📈 Total rows: {len(full_df)}")
            
        except Exception as e:
            print(f"   ❌ Error reading file: {e}")

if __name__ == "__main__":
    check_data()