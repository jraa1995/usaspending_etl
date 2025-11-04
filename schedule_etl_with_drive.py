#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Enhanced ETL Scheduler with Google Drive Integration

This script extends the basic scheduler to include:
1. Automatic T-1 (yesterday) date calculation
2. Google Drive upload using service account
3. Google Sheets integration
4. Enhanced error handling and notifications

Usage:
    # Daily run with Google Drive upload
    python schedule_etl_with_drive.py --config production_config.yaml --mode daily --upload-to-drive
    
    # Custom date with Google Drive
    python schedule_etl_with_drive.py --start-date 2024-11-02 --end-date 2024-11-02 --upload-to-drive
"""

import os
import sys
import yaml
import argparse
import subprocess
from datetime import datetime, timedelta
from pathlib import Path
import tempfile
import shutil
import json
import logging

# Import Google Drive uploader
try:
    from google_drive_uploader import GoogleDriveUploader
    DRIVE_AVAILABLE = True
except ImportError:
    DRIVE_AVAILABLE = False

class EnhancedETLScheduler:
    """Enhanced ETL scheduler with Google Drive integration."""
    
    def __init__(self, config_path: Path):
        self.config_path = Path(config_path)
        self.base_config = self.load_config()
        self.logger = logging.getLogger("enhanced_etl_scheduler")
        
        # Google Drive settings
        self.drive_uploader = None
        self.drive_config = self.base_config.get('google_drive', {})
        
        if self.drive_config.get('enabled', False) and DRIVE_AVAILABLE:
            self._init_drive_uploader()
    
    def load_config(self) -> dict:
        """Load the base configuration."""
        try:
            if not self.config_path.exists():
                print(f"❌ Configuration file not found: {self.config_path}")
                print(f"Current directory: {Path.cwd()}")
                print("Available files:")
                for file in Path.cwd().glob("*.yaml"):
                    print(f"  - {file.name}")
                sys.exit(1)
            
            with open(self.config_path, 'r') as f:
                config = yaml.safe_load(f)
                
            if config is None:
                print(f"❌ Configuration file is empty or invalid: {self.config_path}")
                sys.exit(1)
                
            return config
            
        except yaml.YAMLError as e:
            print(f"❌ YAML syntax error in {self.config_path}:")
            print(f"   {str(e)}")
            sys.exit(1)
        except Exception as e:
            print(f"❌ Error loading configuration from {self.config_path}:")
            print(f"   {str(e)}")
            sys.exit(1)
    
    def _init_drive_uploader(self):
        """Initialize Google Drive uploader."""
        try:
            service_account_file = self.drive_config.get('service_account_file')
            folder_id = self.drive_config.get('folder_id')
            
            if not service_account_file:
                self.logger.warning("Google Drive enabled but no service account file specified")
                return
            
            if not Path(service_account_file).exists():
                self.logger.warning(f"Service account file not found: {service_account_file}")
                return
            
            self.drive_uploader = GoogleDriveUploader(service_account_file, folder_id)
            self.logger.info("Google Drive uploader initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize Google Drive uploader: {e}")
            self.drive_uploader = None
    
    def calculate_date_range(self, mode: str, backfill_days: int = 30) -> tuple:
        """Calculate start and end dates based on mode."""
        today = datetime.now().date()
        
        if mode == 'daily':
            # Yesterday's data (T-1) - this is what we want for daily runs
            start_date = today - timedelta(days=1)
            end_date = start_date
        elif mode == 'weekly':
            # Last 7 days ending yesterday
            end_date = today - timedelta(days=1)
            start_date = end_date - timedelta(days=6)
        elif mode == 'monthly':
            # Last 30 days ending yesterday
            end_date = today - timedelta(days=1)
            start_date = end_date - timedelta(days=29)
        elif mode == 'backfill':
            # Backfill specified number of days ending yesterday
            end_date = today - timedelta(days=1)
            start_date = end_date - timedelta(days=backfill_days-1)
        else:
            raise ValueError(f"Unknown mode: {mode}")
        
        return start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")
    
    def create_runtime_config(self, start_date: str, end_date: str, 
                            email_report: bool = False) -> Path:
        """Create a runtime configuration with updated dates."""
        if self.base_config is None:
            raise RuntimeError("Base configuration is None - configuration file failed to load")
        
        runtime_config = self.base_config.copy()
        
        # Update dates
        runtime_config['data_download']['start_date'] = start_date
        runtime_config['data_download']['end_date'] = end_date
        
        # Enable email if requested
        if email_report:
            runtime_config.setdefault('email', {})['enabled'] = True
        
        # Add runtime metadata
        runtime_config['runtime'] = {
            'generated_at': datetime.now().isoformat(),
            'date_range': f"{start_date} to {end_date}",
            'base_config': str(self.config_path)
        }
        
        # Create temporary config file
        temp_dir = Path(tempfile.gettempdir()) / "usaspending_etl"
        temp_dir.mkdir(exist_ok=True)
        
        runtime_config_path = temp_dir / f"runtime_config_{datetime.now().strftime('%Y%m%d_%H%M%S')}.yaml"
        
        with open(runtime_config_path, 'w') as f:
            yaml.dump(runtime_config, f, default_flow_style=False, indent=2)
        
        return runtime_config_path
    
    def run_etl_pipeline(self, start_date: str, end_date: str, 
                        upload_to_drive: bool = False, email_report: bool = False) -> dict:
        """Run the complete ETL pipeline with optional Google Drive upload."""
        print(f"🗓️  Running ETL for date range: {start_date} to {end_date}")
        
        results = {
            'start_date': start_date,
            'end_date': end_date,
            'success': False,
            'processed_file': None,
            'quality_report': None,
            'drive_upload': None,
            'errors': []
        }
        
        try:
            # Step 1: Run ETL using the working method
            print("🔄 Step 1: Running data download and processing...")
            
            # Use the direct ETL method that we know works
            cmd = [
                sys.executable,
                "usaspending_etl_enhanced.py",
                "--input-dir", "raw_data",
                "--output-dir", "processed_data"
            ]
            
            # First download the data
            download_cmd = [
                sys.executable,
                "usaspending_pipeline.py",
                "bulk-backfill",
                "--start-date", start_date,
                "--end-date", end_date,
                "--out", "raw_data",
                "--groups", "contracts",
                "--date-type", "action_date",
                "--file-format", "csv"
            ]
            
            print(f"📥 Downloading data: {' '.join(download_cmd)}")
            download_result = subprocess.run(download_cmd, capture_output=True, text=True, timeout=3600)
            
            if download_result.returncode != 0:
                error_msg = f"Download failed: {download_result.stderr}"
                results['errors'].append(error_msg)
                print(f"❌ {error_msg}")
                return results
            
            print("✅ Data download completed")
            
            # Then process the data
            print(f"🔄 Processing data: {' '.join(cmd)}")
            process_result = subprocess.run(cmd, capture_output=True, text=True, timeout=1800)
            
            if process_result.returncode != 0:
                error_msg = f"Processing failed: {process_result.stderr}"
                results['errors'].append(error_msg)
                print(f"❌ {error_msg}")
                return results
            
            print("✅ Data processing completed")
            
            # Step 2: Find the output files
            processed_data_dir = Path("processed_data")
            if not processed_data_dir.exists():
                results['errors'].append("Processed data directory not found")
                return results
            
            # Find the most recent processed file
            processed_files = list(processed_data_dir.glob("usaspending_processed_*.parquet"))
            if not processed_files:
                processed_files = list(processed_data_dir.glob("usaspending_processed_*.csv"))
            
            if not processed_files:
                results['errors'].append("No processed data files found")
                return results
            
            # Get the most recent file
            processed_file = max(processed_files, key=lambda x: x.stat().st_mtime)
            results['processed_file'] = str(processed_file)
            
            # Find quality report
            quality_files = list(processed_data_dir.glob("data_quality_report_*.json"))
            if quality_files:
                quality_file = max(quality_files, key=lambda x: x.stat().st_mtime)
                results['quality_report'] = str(quality_file)
            
            print(f"📊 Processed file: {processed_file.name}")
            
            # Step 3: Upload to Google Drive if requested
            if upload_to_drive and self.drive_uploader:
                print("☁️  Uploading to Google Drive...")
                try:
                    quality_path = Path(results['quality_report']) if results['quality_report'] else Path("dummy.json")
                    
                    drive_results = self.drive_uploader.upload_etl_results(
                        processed_file,
                        quality_path,
                        start_date
                    )
                    
                    results['drive_upload'] = drive_results
                    print("✅ Google Drive upload completed")
                    
                    # Print upload details
                    if 'data_file' in drive_results:
                        print(f"📁 Data file: {drive_results['data_file']['web_view_link']}")
                    if 'google_sheet' in drive_results:
                        print(f"📊 Google Sheet: {drive_results['google_sheet']['spreadsheet_url']}")
                    
                except Exception as e:
                    error_msg = f"Google Drive upload failed: {str(e)}"
                    results['errors'].append(error_msg)
                    print(f"⚠️  {error_msg}")
            
            elif upload_to_drive and not self.drive_uploader:
                print("⚠️  Google Drive upload requested but not configured")
            
            results['success'] = True
            print("🎉 ETL pipeline completed successfully!")
            
        except subprocess.TimeoutExpired:
            error_msg = "ETL pipeline timed out"
            results['errors'].append(error_msg)
            print(f"❌ {error_msg}")
        except Exception as e:
            error_msg = f"ETL pipeline error: {str(e)}"
            results['errors'].append(error_msg)
            print(f"❌ {error_msg}")
        
        return results

def main():
    parser = argparse.ArgumentParser(description="Enhanced ETL Scheduler with Google Drive")
    parser.add_argument("--config", default="production_config.yaml", help="Configuration file")
    parser.add_argument("--mode", choices=['daily', 'weekly', 'monthly', 'backfill'],
                       help="Scheduling mode")
    parser.add_argument("--start-date", help="Custom start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", help="Custom end date (YYYY-MM-DD)")
    parser.add_argument("--backfill-days", type=int, default=30,
                       help="Number of days to backfill")
    parser.add_argument("--upload-to-drive", action="store_true",
                       help="Upload results to Google Drive")
    parser.add_argument("--email-report", action="store_true",
                       help="Send email report")
    parser.add_argument("--dry-run", action="store_true",
                       help="Show what would be done without running")
    
    args = parser.parse_args()
    
    if not Path(args.config).exists():
        print(f"❌ Configuration file not found: {args.config}")
        sys.exit(1)
    
    scheduler = EnhancedETLScheduler(args.config)
    
    # Determine date range
    if args.start_date and args.end_date:
        start_date, end_date = args.start_date, args.end_date
    elif args.mode:
        start_date, end_date = scheduler.calculate_date_range(args.mode, args.backfill_days)
    else:
        print("❌ Must specify either --mode or both --start-date and --end-date")
        sys.exit(1)
    
    if args.dry_run:
        print(f"🔍 DRY RUN - Would process date range: {start_date} to {end_date}")
        print(f"📤 Upload to Drive: {'Yes' if args.upload_to_drive else 'No'}")
        print(f"📧 Email report: {'Yes' if args.email_report else 'No'}")
        return
    
    # Run ETL pipeline
    results = scheduler.run_etl_pipeline(
        start_date=start_date,
        end_date=end_date,
        upload_to_drive=args.upload_to_drive,
        email_report=args.email_report
    )
    
    # Print summary
    print(f"\n📋 SUMMARY")
    print(f"Date Range: {results['start_date']} to {results['end_date']}")
    print(f"Success: {'✅' if results['success'] else '❌'}")
    
    if results['processed_file']:
        print(f"Processed File: {results['processed_file']}")
    
    if results['drive_upload']:
        print(f"Google Drive: ✅ Uploaded")
    
    if results['errors']:
        print(f"Errors: {len(results['errors'])}")
        for error in results['errors']:
            print(f"  - {error}")
    
    sys.exit(0 if results['success'] else 1)

if __name__ == "__main__":
    main()