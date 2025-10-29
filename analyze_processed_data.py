#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Analysis script for processed USASpending data

This script demonstrates how to analyze the processed ETL output
and generate insights from the standardized data.
"""

import pandas as pd
import numpy as np
from pathlib import Path
import argparse
from typing import Optional
import warnings
warnings.filterwarnings('ignore')

# Optional visualization libraries
try:
    import matplotlib.pyplot as plt
    import seaborn as sns
    HAS_PLOTTING = True
except ImportError:
    HAS_PLOTTING = False

class USASpendingAnalyzer:
    """Analyzer for processed USASpending data."""
    
    def __init__(self, data_file: Path):
        self.data_file = Path(data_file)
        self.df = self.load_data()
        
    def load_data(self) -> pd.DataFrame:
        """Load the processed data file."""
        try:
            if self.data_file.suffix.lower() == '.parquet':
                df = pd.read_parquet(self.data_file)
            elif self.data_file.suffix.lower() == '.csv':
                df = pd.read_csv(self.data_file)
            else:
                raise ValueError(f"Unsupported file format: {self.data_file.suffix}")
            
            print(f"Loaded {len(df):,} records from {self.data_file.name}")
            return df
            
        except Exception as e:
            print(f"Error loading data: {e}")
            return pd.DataFrame()
    
    def basic_statistics(self):
        """Generate basic statistics about the dataset."""
        print("\n" + "="*60)
        print("BASIC DATASET STATISTICS")
        print("="*60)
        
        print(f"Total Records: {len(self.df):,}")
        print(f"Total Columns: {len(self.df.columns)}")
        print(f"Date Range: {self.df['Date Signed'].min()} to {self.df['Date Signed'].max()}")
        
        # Fiscal year distribution
        if 'Fiscal Year' in self.df.columns:
            fy_stats = self.df['Fiscal Year'].value_counts().sort_index()
            print(f"\nFiscal Year Distribution:")
            for fy, count in fy_stats.items():
                print(f"  FY {fy}: {count:,}")
        
        # Dollar statistics
        if 'Dollars Obligated' in self.df.columns:
            dollars = self.df['Dollars Obligated']
            print(f"\nDollars Obligated Statistics:")
            print(f"  Total: ${dollars.sum():,.2f}")
            print(f"  Average: ${dollars.mean():,.2f}")
            print(f"  Median: ${dollars.median():,.2f}")
            print(f"  Min: ${dollars.min():,.2f}")
            print(f"  Max: ${dollars.max():,.2f}")
    
    def agency_analysis(self):
        """Analyze spending by agency."""
        print("\n" + "="*60)
        print("AGENCY ANALYSIS")
        print("="*60)
        
        if 'Funding Agency Name' not in self.df.columns:
            print("Funding Agency Name column not found")
            return
        
        # Top agencies by contract count
        agency_counts = self.df['Funding Agency Name'].value_counts().head(10)
        print("\nTop 10 Agencies by Contract Count:")
        for agency, count in agency_counts.items():
            print(f"  {agency}: {count:,}")
        
        # Top agencies by dollar amount
        if 'Dollars Obligated' in self.df.columns:
            agency_dollars = self.df.groupby('Funding Agency Name')['Dollars Obligated'].sum().sort_values(ascending=False).head(10)
            print(f"\nTop 10 Agencies by Dollar Amount:")
            for agency, amount in agency_dollars.items():
                print(f"  {agency}: ${amount:,.2f}")
    
    def contract_type_analysis(self):
        """Analyze contracts by instrument type."""
        print("\n" + "="*60)
        print("CONTRACT TYPE ANALYSIS")
        print("="*60)
        
        if 'Instrument Type' not in self.df.columns:
            print("Instrument Type column not found")
            return
        
        # Contract type distribution
        type_counts = self.df['Instrument Type'].value_counts()
        print("\nContract Type Distribution:")
        for contract_type, count in type_counts.items():
            percentage = (count / len(self.df)) * 100
            print(f"  {contract_type}: {count:,} ({percentage:.1f}%)")
        
        # Average contract value by type
        if 'Dollars Obligated' in self.df.columns:
            type_avg = self.df.groupby('Instrument Type')['Dollars Obligated'].agg(['count', 'mean', 'sum']).sort_values('sum', ascending=False)
            print(f"\nContract Value by Type:")
            for contract_type, stats in type_avg.iterrows():
                print(f"  {contract_type}:")
                print(f"    Count: {stats['count']:,}")
                print(f"    Average: ${stats['mean']:,.2f}")
                print(f"    Total: ${stats['sum']:,.2f}")
    
    def small_business_analysis(self):
        """Analyze small business participation."""
        print("\n" + "="*60)
        print("SMALL BUSINESS ANALYSIS")
        print("="*60)
        
        # Find small business columns
        sb_columns = [col for col in self.df.columns if 'Is Vendor Business Type' in col]
        
        if not sb_columns:
            print("No small business type columns found")
            return
        
        print("Small Business Type Participation:")
        for col in sb_columns:
            # Count True values (assuming boolean conversion worked)
            true_count = self.df[col].sum() if self.df[col].dtype == bool else 0
            total_count = len(self.df) - self.df[col].isnull().sum()
            
            if total_count > 0:
                percentage = (true_count / total_count) * 100
                business_type = col.replace('Is Vendor Business Type - ', '')
                print(f"  {business_type}: {true_count:,} ({percentage:.1f}%)")
        
        # Small business dollar analysis
        if 'Dollars Obligated' in self.df.columns and sb_columns:
            print(f"\nSmall Business Dollar Analysis:")
            for col in sb_columns[:3]:  # Limit to first 3 for brevity
                if self.df[col].dtype == bool:
                    sb_dollars = self.df[self.df[col] == True]['Dollars Obligated'].sum()
                    total_dollars = self.df['Dollars Obligated'].sum()
                    percentage = (sb_dollars / total_dollars) * 100 if total_dollars > 0 else 0
                    business_type = col.replace('Is Vendor Business Type - ', '')
                    print(f"  {business_type}: ${sb_dollars:,.2f} ({percentage:.1f}%)")
    
    def vendor_analysis(self):
        """Analyze top vendors."""
        print("\n" + "="*60)
        print("VENDOR ANALYSIS")
        print("="*60)
        
        if 'Legal Business Name' not in self.df.columns:
            print("Legal Business Name column not found")
            return
        
        # Top vendors by contract count
        vendor_counts = self.df['Legal Business Name'].value_counts().head(10)
        print("\nTop 10 Vendors by Contract Count:")
        for vendor, count in vendor_counts.items():
            print(f"  {vendor}: {count:,}")
        
        # Top vendors by dollar amount
        if 'Dollars Obligated' in self.df.columns:
            vendor_dollars = self.df.groupby('Legal Business Name')['Dollars Obligated'].sum().sort_values(ascending=False).head(10)
            print(f"\nTop 10 Vendors by Dollar Amount:")
            for vendor, amount in vendor_dollars.items():
                print(f"  {vendor}: ${amount:,.2f}")
    
    def data_quality_summary(self):
        """Summarize data quality issues."""
        print("\n" + "="*60)
        print("DATA QUALITY SUMMARY")
        print("="*60)
        
        # Missing data analysis
        missing_data = self.df.isnull().sum()
        missing_data = missing_data[missing_data > 0].sort_values(ascending=False)
        
        if len(missing_data) > 0:
            print("Columns with Missing Data:")
            for col, count in missing_data.items():
                percentage = (count / len(self.df)) * 100
                print(f"  {col}: {count:,} ({percentage:.1f}%)")
        else:
            print("No missing data found!")
        
        # Data completeness score
        total_cells = len(self.df) * len(self.df.columns)
        missing_cells = self.df.isnull().sum().sum()
        completeness = ((total_cells - missing_cells) / total_cells) * 100
        print(f"\nOverall Data Completeness: {completeness:.1f}%")
    
    def generate_summary_report(self, output_file: Optional[Path] = None):
        """Generate a comprehensive summary report."""
        if output_file is None:
            output_file = self.data_file.parent / f"analysis_report_{self.data_file.stem}.txt"
        
        # Redirect print output to file
        import sys
        original_stdout = sys.stdout
        
        try:
            with open(output_file, 'w') as f:
                sys.stdout = f
                
                print(f"USASpending Data Analysis Report")
                print(f"Generated: {pd.Timestamp.now()}")
                print(f"Source File: {self.data_file}")
                print("="*80)
                
                self.basic_statistics()
                self.agency_analysis()
                self.contract_type_analysis()
                self.small_business_analysis()
                self.vendor_analysis()
                self.data_quality_summary()
                
        finally:
            sys.stdout = original_stdout
        
        print(f"\nSummary report saved to: {output_file}")
        return output_file
    
    def run_full_analysis(self):
        """Run all analysis functions."""
        if self.df.empty:
            print("No data to analyze")
            return
        
        self.basic_statistics()
        self.agency_analysis()
        self.contract_type_analysis()
        self.small_business_analysis()
        self.vendor_analysis()
        self.data_quality_summary()

def main():
    parser = argparse.ArgumentParser(description="Analyze processed USASpending data")
    parser.add_argument("--input-file", required=True, help="Processed data file (CSV or Parquet)")
    parser.add_argument("--output-report", help="Output file for summary report")
    parser.add_argument("--analysis-type", 
                       choices=['basic', 'agency', 'contracts', 'small-business', 'vendors', 'quality', 'full'],
                       default='full',
                       help="Type of analysis to run")
    
    args = parser.parse_args()
    
    # Initialize analyzer
    analyzer = USASpendingAnalyzer(args.input_file)
    
    if analyzer.df.empty:
        print("Failed to load data")
        return
    
    # Run requested analysis
    if args.analysis_type == 'basic':
        analyzer.basic_statistics()
    elif args.analysis_type == 'agency':
        analyzer.agency_analysis()
    elif args.analysis_type == 'contracts':
        analyzer.contract_type_analysis()
    elif args.analysis_type == 'small-business':
        analyzer.small_business_analysis()
    elif args.analysis_type == 'vendors':
        analyzer.vendor_analysis()
    elif args.analysis_type == 'quality':
        analyzer.data_quality_summary()
    else:  # full
        analyzer.run_full_analysis()
    
    # Generate report if requested
    if args.output_report:
        analyzer.generate_summary_report(Path(args.output_report))

if __name__ == "__main__":
    main()