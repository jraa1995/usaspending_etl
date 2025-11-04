#!/usr/bin/env python3
"""
Debug script to test ETL processing step by step
"""

import sys
from pathlib import Path
from usaspending_etl_enhanced import EnhancedUSASpendingETL

def debug_etl():
    print("🔍 ETL Debug Tool")
    print("=" * 50)
    
    try:
        # Initialize ETL
        print("1. Initializing ETL...")
        etl = EnhancedUSASpendingETL("etl_config.yaml")
        print("✅ ETL initialized successfully")
        
        # Check config
        print("2. Checking configuration...")
        if etl.config is None:
            print("❌ Configuration is None")
            return
        else:
            print(f"✅ Configuration loaded with keys: {list(etl.config.keys())}")
        
        # Check input directory
        input_dir = Path("raw_data")
        print(f"3. Checking input directory: {input_dir}")
        if not input_dir.exists():
            print(f"❌ Input directory doesn't exist: {input_dir}")
            return
        
        # Find data files
        print("4. Finding data files...")
        data_files = etl.find_data_files(input_dir)
        print(f"✅ Found {len(data_files)} data files:")
        for file in data_files:
            print(f"   - {file}")
        
        if not data_files:
            print("❌ No data files found")
            return
        
        # Test loading one file
        print("5. Testing file loading...")
        test_file = data_files[0]
        df = etl.load_data_file(test_file)
        if df.empty:
            print("❌ Failed to load data file")
            return
        else:
            print(f"✅ Loaded {len(df)} rows, {len(df.columns)} columns")
            print(f"   Sample columns: {list(df.columns)[:5]}...")
        
        # Test column extraction
        print("6. Testing column extraction...")
        df_extracted = etl.extract_required_columns(df)
        if df_extracted.empty:
            print("❌ Column extraction failed")
            return
        else:
            print(f"✅ Extracted {len(df_extracted.columns)} columns")
            print(f"   Extracted columns: {list(df_extracted.columns)}")
        
        print("\n✅ All ETL components working correctly!")
        
    except Exception as e:
        print(f"❌ Error during ETL debug: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    debug_etl()