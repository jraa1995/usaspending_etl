#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Enhanced USASpending ETL Pipeline - Data Preprocessing and Filtering

This enhanced version includes:
- YAML configuration support
- Data validation and quality checks
- Summary statistics and reporting
- Better error handling
- Flexible filtering options
- Data profiling capabilities
"""

import os
import sys
import logging
import pandas as pd
import yaml
from pathlib import Path
from typing import Dict, List, Optional, Union, Tuple
import argparse
from datetime import datetime
import json

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("usaspending_etl_enhanced")

class DataQualityReport:
    """Generate data quality reports and statistics."""
    
    def __init__(self):
        self.report = {
            'processing_timestamp': datetime.now().isoformat(),
            'data_quality_issues': [],
            'summary_statistics': {},
            'column_profiles': {}
        }
    
    def add_issue(self, severity: str, message: str, count: int = None):
        """Add a data quality issue to the report."""
        issue = {
            'severity': severity,
            'message': message,
            'timestamp': datetime.now().isoformat()
        }
        if count is not None:
            issue['count'] = count
        self.report['data_quality_issues'].append(issue)
    
    def add_summary_stats(self, df: pd.DataFrame):
        """Add summary statistics for the dataset."""
        self.report['summary_statistics'] = {
            'total_rows': len(df),
            'total_columns': len(df.columns),
            'memory_usage_mb': df.memory_usage(deep=True).sum() / 1024 / 1024,
            'null_counts': df.isnull().sum().to_dict(),
            'data_types': df.dtypes.astype(str).to_dict()
        }
    
    def profile_columns(self, df: pd.DataFrame):
        """Generate detailed column profiles."""
        for col in df.columns:
            profile = {
                'data_type': str(df[col].dtype),
                'null_count': int(df[col].isnull().sum()),
                'null_percentage': float(df[col].isnull().sum() / len(df) * 100),
                'unique_count': int(df[col].nunique()),
                'unique_percentage': float(df[col].nunique() / len(df) * 100)
            }
            
            # Add type-specific statistics
            if pd.api.types.is_numeric_dtype(df[col]):
                profile.update({
                    'min': float(df[col].min()) if not df[col].isnull().all() else None,
                    'max': float(df[col].max()) if not df[col].isnull().all() else None,
                    'mean': float(df[col].mean()) if not df[col].isnull().all() else None,
                    'median': float(df[col].median()) if not df[col].isnull().all() else None
                })
            elif pd.api.types.is_string_dtype(df[col]) or df[col].dtype == 'object':
                profile.update({
                    'avg_length': float(df[col].astype(str).str.len().mean()) if not df[col].isnull().all() else None,
                    'max_length': int(df[col].astype(str).str.len().max()) if not df[col].isnull().all() else None
                })
            
            self.report['column_profiles'][col] = profile
    
    def save_report(self, output_path: Path):
        """Save the data quality report to a JSON file."""
        report_file = output_path.parent / f"data_quality_report_{output_path.stem}.json"
        with open(report_file, 'w') as f:
            json.dump(self.report, f, indent=2, default=str)
        logger.info(f"Data quality report saved to: {report_file}")
        return report_file

class EnhancedUSASpendingETL:
    """Enhanced ETL processor for USASpending data files."""
    
    def __init__(self, config_path: Union[str, Path] = "etl_config.yaml"):
        self.config_path = Path(config_path)
        self.config = self.load_config()
        self.quality_report = DataQualityReport()
        
    def load_config(self) -> Dict:
        """Load configuration from YAML file."""
        try:
            with open(self.config_path, 'r') as f:
                config = yaml.safe_load(f)
            logger.info(f"Loaded configuration from {self.config_path}")
            return config
        except Exception as e:
            logger.error(f"Error loading config from {self.config_path}: {e}")
            # Return default configuration
            return {
                'column_mapping': {},
                'optional_columns': [],
                'data_types': {},
                'default_filters': {},
                'data_quality': {'required_fields': [], 'validation_rules': {}},
                'output': {'format': 'parquet', 'include_summary': True, 'include_quality_report': True}
            }
    
    def find_data_files(self, input_dir: Path) -> List[Path]:
        """Find all CSV and Parquet files in the input directory."""
        files = []
        for ext in ['*.csv', '*.parquet']:
            files.extend(input_dir.rglob(ext))
        return files
    
    def load_data_file(self, file_path: Path) -> pd.DataFrame:
        """Load a single data file with error handling."""
        try:
            if file_path.suffix.lower() == '.csv':
                df = pd.read_csv(file_path, dtype=str, low_memory=False)
            elif file_path.suffix.lower() == '.parquet':
                df = pd.read_parquet(file_path)
            else:
                raise ValueError(f"Unsupported file format: {file_path.suffix}")
            
            logger.info(f"Loaded {len(df)} rows from {file_path.name}")
            return df
            
        except Exception as e:
            logger.error(f"Error loading {file_path}: {e}")
            self.quality_report.add_issue('ERROR', f"Failed to load file {file_path.name}: {str(e)}")
            return pd.DataFrame()
    
    def extract_required_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Extract and rename columns according to configuration."""
        if df.empty:
            return df
        
        column_mapping = self.config.get('column_mapping', {})
        optional_columns = self.config.get('optional_columns', [])
        
        # Check which required columns exist
        available_columns = {}
        missing_columns = []
        
        for source_col, target_col in column_mapping.items():
            if source_col in df.columns:
                available_columns[source_col] = target_col
            else:
                missing_columns.append((source_col, target_col))
        
        # Report missing columns
        for source, target in missing_columns:
            severity = 'WARNING' if source in optional_columns else 'ERROR'
            self.quality_report.add_issue(severity, f"Missing column: {source} -> {target}")
        
        if not available_columns:
            self.quality_report.add_issue('ERROR', "No required columns found in dataset")
            return pd.DataFrame()
        
        # Extract and rename columns
        extracted_df = df[list(available_columns.keys())].copy()
        extracted_df = extracted_df.rename(columns=available_columns)
        
        # Add missing columns with default values
        for source_col, target_col in column_mapping.items():
            if target_col not in extracted_df.columns:
                extracted_df[target_col] = None
                logger.info(f"Added missing column '{target_col}' with null values")
        
        return extracted_df
    
    def validate_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate data according to quality rules."""
        if df.empty:
            return df
        
        quality_config = self.config.get('data_quality', {})
        required_fields = quality_config.get('required_fields', [])
        validation_rules = quality_config.get('validation_rules', {})
        
        original_count = len(df)
        
        # Check required fields
        for field in required_fields:
            if field in df.columns:
                null_count = df[field].isnull().sum()
                if null_count > 0:
                    self.quality_report.add_issue(
                        'WARNING', 
                        f"Required field '{field}' has {null_count} null values",
                        null_count
                    )
        
        # Apply validation rules
        if 'fiscal_year_range' in validation_rules:
            min_year, max_year = validation_rules['fiscal_year_range']
            if 'Fiscal Year' in df.columns:
                invalid_years = df[
                    (pd.to_numeric(df['Fiscal Year'], errors='coerce') < min_year) |
                    (pd.to_numeric(df['Fiscal Year'], errors='coerce') > max_year)
                ]
                if len(invalid_years) > 0:
                    self.quality_report.add_issue(
                        'WARNING',
                        f"Found {len(invalid_years)} records with invalid fiscal years",
                        len(invalid_years)
                    )
        
        if 'min_dollars_obligated' in validation_rules:
            min_dollars = validation_rules['min_dollars_obligated']
            if 'Dollars Obligated' in df.columns:
                invalid_amounts = df[
                    pd.to_numeric(df['Dollars Obligated'], errors='coerce') < min_dollars
                ]
                if len(invalid_amounts) > 0:
                    self.quality_report.add_issue(
                        'INFO',
                        f"Found {len(invalid_amounts)} records below minimum dollar threshold",
                        len(invalid_amounts)
                    )
        
        return df
    
    def apply_filters(self, df: pd.DataFrame, custom_filters: Optional[Dict] = None) -> pd.DataFrame:
        """Apply data filters from config and custom filters."""
        if df.empty:
            return df
        
        # Combine default and custom filters
        default_filters = self.config.get('default_filters', {})
        filters = default_filters.copy() if default_filters else {}
        if custom_filters:
            filters.update(custom_filters)
        
        if not filters:
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
        if filtered_count != original_count:
            self.quality_report.add_issue(
                'INFO',
                f"Applied filters: {original_count} -> {filtered_count} rows",
                original_count - filtered_count
            )
        
        return df
    
    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and standardize data according to configuration."""
        if df.empty:
            return df
        
        data_types = self.config.get('data_types', {})
        
        # Convert date columns
        for col in data_types.get('date_columns', []):
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
                invalid_dates = df[col].isnull().sum()
                if invalid_dates > 0:
                    self.quality_report.add_issue(
                        'WARNING',
                        f"Column '{col}': {invalid_dates} invalid dates converted to null",
                        invalid_dates
                    )
        
        # Convert numeric columns
        for col in data_types.get('numeric_columns', []):
            if col in df.columns:
                original_nulls = df[col].isnull().sum()
                df[col] = pd.to_numeric(df[col], errors='coerce')
                new_nulls = df[col].isnull().sum()
                if new_nulls > original_nulls:
                    self.quality_report.add_issue(
                        'WARNING',
                        f"Column '{col}': {new_nulls - original_nulls} invalid numeric values converted to null",
                        new_nulls - original_nulls
                    )
        
        # Convert boolean columns
        for col in data_types.get('boolean_columns', []):
            if col in df.columns:
                df[col] = df[col].map({
                    'Y': True, 'N': False, 'YES': True, 'NO': False,
                    'True': True, 'False': False, '1': True, '0': False,
                    't': True, 'f': False, 'T': True, 'F': False,
                    True: True, False: False, 1: True, 0: False
                })
        
        # Clean text columns
        for col in data_types.get('text_columns', []):
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip()
                df[col] = df[col].replace('nan', None)
        
        return df
    
    def process_files(self, input_dir: Union[str, Path], output_dir: Union[str, Path], 
                     custom_filters: Optional[Dict] = None) -> Optional[Path]:
        """Process all files through the complete ETL pipeline."""
        input_dir = Path(input_dir)
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Find data files
        data_files = self.find_data_files(input_dir)
        if not data_files:
            logger.error(f"No data files found in {input_dir}")
            return None
        
        logger.info(f"Found {len(data_files)} data files to process")
        
        all_dataframes = []
        
        # Process each file
        for file_path in data_files:
            logger.info(f"Processing file: {file_path}")
            
            # Load data
            df = self.load_data_file(file_path)
            if df.empty:
                continue
            
            # Extract required columns
            df = self.extract_required_columns(df)
            if df.empty:
                continue
            
            # Validate data
            df = self.validate_data(df)
            
            # Apply filters
            df = self.apply_filters(df, custom_filters)
            
            # Clean data
            df = self.clean_data(df)
            
            if not df.empty:
                all_dataframes.append(df)
                logger.info(f"Processed {len(df)} rows from {file_path.name}")
        
        if not all_dataframes:
            logger.error("No data was processed successfully")
            return None
        
        # Combine all dataframes
        logger.info("Combining all processed data...")
        combined_df = pd.concat(all_dataframes, ignore_index=True)
        
        # Remove duplicates
        key_columns = ['PIID', 'Modification Number', 'Date Signed']
        key_columns = [col for col in key_columns if col in combined_df.columns]
        
        if key_columns:
            before_dedup = len(combined_df)
            combined_df = combined_df.drop_duplicates(subset=key_columns)
            after_dedup = len(combined_df)
            if before_dedup != after_dedup:
                self.quality_report.add_issue(
                    'INFO',
                    f"Removed {before_dedup - after_dedup} duplicate rows",
                    before_dedup - after_dedup
                )
        
        # Generate quality report
        self.quality_report.add_summary_stats(combined_df)
        self.quality_report.profile_columns(combined_df)
        
        # Save output
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_format = self.config.get('output', {}).get('format', 'parquet')
        
        if output_format.lower() == 'parquet':
            output_file = output_dir / f"usaspending_processed_{timestamp}.parquet"
            combined_df.to_parquet(output_file, index=False)
        else:
            output_file = output_dir / f"usaspending_processed_{timestamp}.csv"
            combined_df.to_csv(output_file, index=False)
        
        logger.info(f"Saved processed data to: {output_file}")
        logger.info(f"Final dataset: {len(combined_df)} rows, {len(combined_df.columns)} columns")
        
        # Save quality report if enabled
        if self.config.get('output', {}).get('include_quality_report', True):
            self.quality_report.save_report(output_file)
        
        # Print summary if enabled
        if self.config.get('output', {}).get('include_summary', True):
            self.print_summary(combined_df)
        
        return output_file
    
    def print_summary(self, df: pd.DataFrame):
        """Print a summary of the processed data."""
        print("\n" + "="*60)
        print("ETL PROCESSING SUMMARY")
        print("="*60)
        print(f"Total Records: {len(df):,}")
        print(f"Total Columns: {len(df.columns)}")
        print(f"Memory Usage: {df.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB")
        
        print(f"\nData Quality Issues: {len(self.quality_report.report['data_quality_issues'])}")
        for issue in self.quality_report.report['data_quality_issues'][-5:]:  # Show last 5 issues
            print(f"  {issue['severity']}: {issue['message']}")
        
        print(f"\nTop Agencies by Record Count:")
        if 'Funding Agency Name' in df.columns:
            top_agencies = df['Funding Agency Name'].value_counts().head(5)
            for agency, count in top_agencies.items():
                print(f"  {agency}: {count:,}")
        
        print(f"\nFiscal Year Distribution:")
        if 'Fiscal Year' in df.columns:
            fy_dist = df['Fiscal Year'].value_counts().sort_index()
            for fy, count in fy_dist.items():
                print(f"  FY {fy}: {count:,}")
        
        print("="*60)

def main():
    parser = argparse.ArgumentParser(description="Enhanced USASpending ETL Pipeline")
    parser.add_argument("--input-dir", required=True, help="Input directory containing data files")
    parser.add_argument("--output-dir", default="./processed_data", help="Output directory")
    parser.add_argument("--config", default="etl_config.yaml", help="Configuration file path")
    
    # Filter options (override config)
    parser.add_argument("--fiscal-year-start", type=int, help="Start fiscal year")
    parser.add_argument("--fiscal-year-end", type=int, help="End fiscal year")
    parser.add_argument("--min-dollars", type=float, help="Minimum dollars obligated")
    parser.add_argument("--instrument-types", nargs="+", help="Filter by instrument types")
    parser.add_argument("--agencies", nargs="+", help="Filter by agency names")
    
    args = parser.parse_args()
    
    # Build custom filters
    custom_filters = {}
    if args.fiscal_year_start and args.fiscal_year_end:
        custom_filters['fiscal_year_range'] = (args.fiscal_year_start, args.fiscal_year_end)
    if args.min_dollars:
        custom_filters['min_dollars_obligated'] = args.min_dollars
    if args.instrument_types:
        custom_filters['instrument_types'] = args.instrument_types
    if args.agencies:
        custom_filters['agencies'] = args.agencies
    
    # Initialize and run ETL
    etl = EnhancedUSASpendingETL(args.config)
    output_file = etl.process_files(args.input_dir, args.output_dir, custom_filters)
    
    if output_file:
        print(f"\nETL completed successfully!")
        print(f"Output file: {output_file}")
    else:
        print("\nETL failed - no output generated")
        sys.exit(1)

if __name__ == "__main__":
    main()