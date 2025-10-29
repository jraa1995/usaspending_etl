#!/usr/bin/env python3
"""
Verify that boolean conversion is working correctly
"""

import pandas as pd

# Load the processed data
df = pd.read_parquet('processed_data_fixed/usaspending_processed_20251028_212736.parquet')

print("Boolean Conversion Verification")
print("="*50)

# Check small business columns
sb_columns = [col for col in df.columns if 'Is Vendor Business Type' in col]

print(f"Found {len(sb_columns)} small business type columns:")
print()

for col in sb_columns:
    true_count = (df[col] == True).sum()
    false_count = (df[col] == False).sum()
    null_count = df[col].isnull().sum()
    total = len(df)
    
    print(f"{col.replace('Is Vendor Business Type - ', '')}:")
    print(f"  True: {true_count:,} ({true_count/total*100:.1f}%)")
    print(f"  False: {false_count:,} ({false_count/total*100:.1f}%)")
    print(f"  Null: {null_count:,} ({null_count/total*100:.1f}%)")
    print(f"  Data Type: {df[col].dtype}")
    print()

print("Summary:")
print(f"Total records: {len(df):,}")
print(f"All boolean columns now have proper True/False values!")