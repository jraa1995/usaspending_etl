#!/usr/bin/env python3
"""
Simple test to verify ETL processing works
"""

from pathlib import Path
from usaspending_etl_enhanced import EnhancedUSASpendingETL

def test_processing():
    print("🧪 Testing ETL Processing")
    print("=" * 40)
    
    try:
        # Test with absolute path to config
        config_path = Path("etl_config.yaml").absolute()
        print(f"Config path: {config_path}")
        print(f"Config exists: {config_path.exists()}")
        
        # Initialize ETL
        etl = EnhancedUSASpendingETL(config_path)
        
        # Test processing
        input_dir = Path("raw_data")
        output_dir = Path("test_output")
        
        print(f"Input dir: {input_dir} (exists: {input_dir.exists()})")
        
        if input_dir.exists():
            result = etl.process_files(input_dir, output_dir)
            if result:
                print(f"✅ Processing successful: {result}")
            else:
                print("❌ Processing failed")
        else:
            print("❌ No raw_data directory found")
            
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_processing()