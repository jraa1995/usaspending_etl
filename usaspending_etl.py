#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
USASpending ETL Pipeline - Data Preprocessing and Filtering

This script processes the raw USASpending data files and extracts only the required columns
with proper filtering and data cleaning.

Required columns:
- Fiscal Year
- PIID  
- AAC
- APEX
- Instrument Type
- Referenced IDV PIID
- Modification Number
- Date Signed
- Est. Ultimate Completion Date
- Last Date to Order
- Dollars Obligated
- Base and All Options Value (Total Contract Value)
- Legal Business Name
- Contracting Office Name
- Funding Agency Name
- Description of Requirement
- SBIR/STTR Description
- Contracting Officers Business Size Determination
- Is Vendor Business Type - 8A Program Participant
- Is Vendor Business Type - Economically Disadvantaged Women-Owned Small Business
- Is Vendor Business Type - HUBZone Firm
- Is Vendor Business Type - Self-Certified Small Disadvantaged Business
- Is Vendor Business Type - Service-Disabled Veteran-Owned Business
- Is Vendor Business Type - Veteran-Owned Business
- Is Vendor Business Type - Women-Owned Small Business
"""

import os
import sys
import logging
import pandas as pd
from pathlib import Path
from typing import Dict, List, Optional, Union
import argparse

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("usaspending_etl")

# Column mapping from USASpending API to our required headers
COLUMN_MAPPING = {
    # Required columns mapping
    'action_date_fiscal_year': 'Fiscal Year',
    'award_id_piid': 'PIID',
    'awarding_agency_code': 'AAC',
    # Note: APEX not found in standard columns - may need custom logic
    'award_type': 'Instrument Type',
    'parent_award_id_piid': 'Referenced IDV PIID',
    'modification_number': 'Modification Number',
    'action_date': 'Date Signed',
    'period_of_performance_current_end_date': 'Est. Ultimate Completion Date',
    'ordering_period_end_date': 'Last Date to Order',
    'federal_action_obligation': 'Dollars Obligated',
    'base_and_all_options_value': 'Base and All Options Value (Total Contract Value)',
    'recipient_name': 'Legal Business Name',
    'awarding_office_name': 'Contracting Office Name',
    'funding_agency_name': 'Funding Agency Name',
    'transaction_description': 'Description of Requirement',
    # Note: SBIR/STTR Description not found in standard columns - may need custom logic
    'contracting_officers_determination_of_business_size': 'Contracting Officers Business Size Determination',
    'c8a_program_participant': 'Is Vendor Business Type - 8A Program Participant',
    'economically_disadvantaged_women_owned_small_business': 'Is Vendor Business Type - Economically Disadvantaged Women-Owned Small Business',
    'historically_underutilized_business_zone_hubzone_firm': 'Is Vendor Business Type - HUBZone Firm',
    'self_certified_small_disadvantaged_business': 'Is Vendor Business Type - Self-Certified Small Disadvantaged Business',
    'service_disabled_veteran_owned_business': 'Is Vendor Business Type - Service-Disabled Veteran-Owned Business',
    'veteran_owned_business': 'Is Vendor Business Type - Veteran-Owned Business',
    'women_owned_small_business': 'Is Vendor Business Type - Women-Owned Small Business'
}

# Columns that might not exist in all datasets
OPTIONAL_COLUMNS = [
    'ordering_period_end_date',  # Last Date to Order
    'parent_award_id_piid',      # Referenced IDV PIID
]

class USASpendingETL:
    """ETL processor for USASpending data files."""
    
    def __init__(self, input_dir: Union[str, Path], output_dir: Union[str, Path]):
        self.input_dir = Path(input_dir)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
    def find_data_files(self) -> List[Path]:
        """Find all CSV and Parquet files in the input directory."""
        files = []
        for ext in ['*.csv', '*.parquet']:
            files.extend(self.input_dir.rglob(ext))
        return files
    
    def load_data_file(self, file_path: Path) -> pd.DataFrame:
        """Load a single data file (CSV or Parquet)."""
        try:
            if file_path.suffix.lower() == '.csv':
                # Use dtype=str to avoid parsing issues, we'll convert later
                df = pd.read_csv(file_path, dtype=str, low_memory=False)
            elif file_path.suffix.lower() == '.parquet':
                df = pd.read_parquet(file_path)
            else:
                raise ValueError(f"Unsupported file format: {file_path.suffix}")
            
            logger.info(f"Loaded {len(df)} rows from {file_path.name}")
            return df
            
        except Exception as e:
            logger.error(f"Error loading {file_path}: {e}")
            return pd.DataFrame()
    
    def extract_required_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Extract and rename columns according to our requirements."""
        if df.empty:
            return df
            
        # Check which required columns exist in the dataframe
        available_columns = {}
        missing_columns = []
        
        for source_col, target_col in COLUMN_MAPPING.items():
            if source_col in df.columns:
                available_columns[source_col] = target_col
            else:
                missing_columns.append((source_col, target_col))
        
        if missing_columns:
            logger.warning(f"Missing columns in dataset:")
            for source, target in missing_columns:
                if source not in OPTIONAL_COLUMNS:
                    logger.warning(f"  - {source} -> {target}")
        
        # Extract available columns
        if not available_columns:
            logger.error("No required columns found in dataset")
            return pd.DataFrame()
        
        # Select and rename columns
        extracted_df = df[list(available_columns.keys())].copy()
        extracted_df = extracted_df.rename(columns=available_columns)
        
        # Add missing columns with default values
        for source_col, target_col in COLUMN_MAPPING.items():
            if target_col not in extracted_df.columns:
                extracted_df[target_col] = None
                logger.info(f"Added missing column '{target_col}' with null values")
        
        return extracted_df
    
    def apply_data_filters(self, df: pd.DataFrame, filters: Optional[Dict] = None) -> pd.DataFrame:
        """Apply data filters to the dataset."""
        if df.empty or not filters:
            return df
            
        original_count = len(df)
        
        # Apply fiscal year filter
        if 'fiscal_year_range' in filters:
            start_year, end_year = filters['fiscal_year_range']
            if 'Fiscal Year' in df.columns:
                df = df[
                    (pd.to_numeric(df['Fiscal Year'], errors='coerce') >= start_year) &
                    (pd.to_numeric(df['Fiscal Year'], errors='coerce') <= end_year)
                ]
        
        # Apply minimum dollar threshold
        if 'min_dollars_obligated' in filters:
            min_amount = filters['min_dollars_obligated']
            if 'Dollars Obligated' in df.columns:
                df = df[pd.to_numeric(df['Dollars Obligated'], errors='coerce') >= min_amount]
        
        # Filter by instrument type
        if 'instrument_types' in filters:
            types = filters['instrument_types']
            if 'Instrument Type' in df.columns:
                df = df[df['Instrument Type'].isin(types)]
        
        # Filter by agency
        if 'agencies' in filters:
            agencies = filters['agencies']
            if 'Funding Agency Name' in df.columns:
                df = df[df['Funding Agency Name'].isin(agencies)]
        
        filtered_count = len(df)
        logger.info(f"Applied filters: {original_count} -> {filtered_count} rows ({original_count - filtered_count} filtered out)")
        
        return df
    
    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and standardize the data."""
        if df.empty:
            return df
            
        # Convert date columns to proper datetime format
        date_columns = [
            'Date Signed',
            'Est. Ultimate Completion Date', 
            'Last Date to Order'
        ]
        
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        
        # Convert numeric columns
        numeric_columns = [
            'Fiscal Year',
            'Dollars Obligated',
            'Base and All Options Value (Total Contract Value)'
        ]
        
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Convert boolean columns to proper boolean values
        boolean_columns = [
            'Is Vendor Business Type - 8A Program Participant',
            'Is Vendor Business Type - Economically Disadvantaged Women-Owned Small Business',
            'Is Vendor Business Type - HUBZone Firm',
            'Is Vendor Business Type - Self-Certified Small Disadvantaged Business',
            'Is Vendor Business Type - Service-Disabled Veteran-Owned Business',
            'Is Vendor Business Type - Veteran-Owned Business',
            'Is Vendor Business Type - Women-Owned Small Business'
        ]
        
        for col in boolean_columns:
            if col in df.columns:
                # Convert various representations to boolean
                df[col] = df[col].map({
                    'Y': True, 'N': False, 'YES': True, 'NO': False,
                    'True': True, 'False': False, '1': True, '0': False,
                    't': True, 'f': False, 'T': True, 'F': False,
                    True: True, False: False
                })
        
        # Clean text columns
        text_columns = [
            'PIID', 'AAC', 'Legal Business Name', 'Contracting Office Name',
            'Funding Agency Name', 'Description of Requirement'
        ]
        
        for col in text_columns:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip()
                df[col] = df[col].replace('nan', None)
        
        return df
    
    def process_file(self, file_path: Path, filters: Optional[Dict] = None) -> pd.DataFrame:
        """Process a single data file through the complete ETL pipeline."""
        logger.info(f"Processing file: {file_path}")
        
        # Load data
        df = self.load_data_file(file_path)
        if df.empty:
            return df
        
        # Extract required columns
        df = self.extract_required_columns(df)
        if df.empty:
            return df
        
        # Apply filters
        df = self.apply_data_filters(df, filters)
        
        # Clean data
        df = self.clean_data(df)
        
        logger.info(f"Processed {len(df)} rows from {file_path.name}")
        return df
    
    def process_all_files(self, filters: Optional[Dict] = None, output_format: str = 'parquet') -> Path:
        """Process all data files and combine into a single output file."""
        data_files = self.find_data_files()
        
        if not data_files:
            logger.error(f"No data files found in {self.input_dir}")
            return None
        
        logger.info(f"Found {len(data_files)} data files to process")
        
        all_dataframes = []
        
        for file_path in data_files:
            df = self.process_file(file_path, filters)
            if not df.empty:
                all_dataframes.append(df)
        
        if not all_dataframes:
            logger.error("No data was processed successfully")
            return None
        
        # Combine all dataframes
        logger.info("Combining all processed data...")
        combined_df = pd.concat(all_dataframes, ignore_index=True)
        
        # Remove duplicates based on key columns
        key_columns = ['PIID', 'Modification Number', 'Date Signed']
        key_columns = [col for col in key_columns if col in combined_df.columns]
        
        if key_columns:
            before_dedup = len(combined_df)
            combined_df = combined_df.drop_duplicates(subset=key_columns)
            after_dedup = len(combined_df)
            logger.info(f"Removed {before_dedup - after_dedup} duplicate rows")
        
        # Save output
        timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
        
        if output_format.lower() == 'parquet':
            output_file = self.output_dir / f"usaspending_processed_{timestamp}.parquet"
            combined_df.to_parquet(output_file, index=False)
        else:
            output_file = self.output_dir / f"usaspending_processed_{timestamp}.csv"
            combined_df.to_csv(output_file, index=False)
        
        logger.info(f"Saved processed data to: {output_file}")
        logger.info(f"Final dataset: {len(combined_df)} rows, {len(combined_df.columns)} columns")
        
        return output_file

def main():
    parser = argparse.ArgumentParser(description="USASpending ETL Pipeline - Process and filter data")
    parser.add_argument("--input-dir", required=True, help="Input directory containing USASpending data files")
    parser.add_argument("--output-dir", default="./processed_data", help="Output directory for processed files")
    parser.add_argument("--output-format", choices=['csv', 'parquet'], default='parquet', help="Output file format")
    
    # Filter options
    parser.add_argument("--fiscal-year-start", type=int, help="Start fiscal year for filtering")
    parser.add_argument("--fiscal-year-end", type=int, help="End fiscal year for filtering")
    parser.add_argument("--min-dollars", type=float, help="Minimum dollars obligated threshold")
    parser.add_argument("--instrument-types", nargs="+", help="Filter by instrument types")
    parser.add_argument("--agencies", nargs="+", help="Filter by funding agency names")
    
    args = parser.parse_args()
    
    # Build filters dictionary
    filters = {}
    
    if args.fiscal_year_start and args.fiscal_year_end:
        filters['fiscal_year_range'] = (args.fiscal_year_start, args.fiscal_year_end)
    
    if args.min_dollars:
        filters['min_dollars_obligated'] = args.min_dollars
    
    if args.instrument_types:
        filters['instrument_types'] = args.instrument_types
    
    if args.agencies:
        filters['agencies'] = args.agencies
    
    # Initialize and run ETL
    etl = USASpendingETL(args.input_dir, args.output_dir)
    output_file = etl.process_all_files(filters, args.output_format)
    
    if output_file:
        print(f"\nETL completed successfully!")
        print(f"Output file: {output_file}")
    else:
        print("\nETL failed - no output generated")
        sys.exit(1)

if __name__ == "__main__":
    main()