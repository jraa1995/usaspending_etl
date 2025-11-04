#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ETL Scheduler Helper Script

This script helps with scheduling the ETL pipeline with dynamic date ranges.
It can be used to:
1. Run daily incremental updates
2. Run weekly/monthly full refreshes
3. Backfill historical data
4. Handle date range calculations automatically

Usage:
    # Daily incremental (yesterday's data)
    python schedule_etl.py --mode daily
    
    # Weekly full refresh (last 7 days)
    python schedule_etl.py --mode weekly
    
    # Monthly full refresh (last 30 days)
    python schedule_etl.py --mode monthly
    
    # Custom date range
    python schedule_etl.py --start-date 2025-09-01 --end-date 2025-09-30
    
    # Backfill mode (fill gaps in data)
    python schedule_etl.py --mode backfill --backfill-days 30
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

class ETLScheduler:
    """Helper class for scheduling ETL runs with dynamic date ranges."""
    
    def __init__(self, config_path: Path):
        self.config_path = Path(config_path)
        self.base_config = self.load_config()
    
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
    
    def calculate_date_range(self, mode: str, backfill_days: int = 30) -> tuple:
        """Calculate start and end dates based on mode."""
        today = datetime.now().date()
        
        if mode == 'daily':
            # Yesterday's data (T-1)
            start_date = today - timedelta(days=1)
            end_date = start_date
        elif mode == 'weekly':
            # Last 7 days
            end_date = today - timedelta(days=1)
            start_date = end_date - timedelta(days=6)
        elif mode == 'monthly':
            # Last 30 days
            end_date = today - timedelta(days=1)
            start_date = end_date - timedelta(days=29)
        elif mode == 'backfill':
            # Backfill specified number of days
            end_date = today - timedelta(days=1)
            start_date = end_date - timedelta(days=backfill_days-1)
        else:
            raise ValueError(f"Unknown mode: {mode}")
        
        return start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")
    
    def create_runtime_config(self, start_date: str, end_date: str, 
                            email_report: bool = False) -> Path:
        """Create a runtime configuration with updated dates."""
        # Create a copy of the base config
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
    
    def run_etl(self, start_date: str, end_date: str, email_report: bool = False,
                dry_run: bool = False) -> bool:
        """Run the ETL pipeline with specified date range."""
        print(f"🗓️  Running ETL for date range: {start_date} to {end_date}")
        
        # Create runtime configuration
        runtime_config_path = self.create_runtime_config(start_date, end_date, email_report)
        
        try:
            # Build command
            cmd = [
                sys.executable,
                str(Path(__file__).parent / "usaspending_production_etl.py"),
                "--config", str(runtime_config_path)
            ]
            
            if dry_run:
                cmd.append("--dry-run")
            
            if email_report:
                cmd.append("--email-report")
            
            print(f"🚀 Executing: {' '.join(cmd)}")
            
            # Run the ETL
            result = subprocess.run(cmd, capture_output=False, text=True)
            
            success = result.returncode == 0
            
            if success:
                print(f"✅ ETL completed successfully")
            else:
                print(f"❌ ETL failed with return code: {result.returncode}")
            
            return success
            
        finally:
            # Cleanup temporary config
            if runtime_config_path.exists():
                runtime_config_path.unlink()
    
    def check_data_gaps(self, days_back: int = 30) -> list:
        """Check for gaps in processed data (placeholder for future implementation)."""
        # This would check the processed_data directory for missing date ranges
        # and return a list of date ranges that need to be backfilled
        print(f"🔍 Checking for data gaps in last {days_back} days...")
        print("📝 Note: Gap detection not yet implemented")
        return []

def main():
    parser = argparse.ArgumentParser(description="ETL Scheduler Helper")
    parser.add_argument("--config", default="production_config.yaml", 
                       help="Base configuration file")
    parser.add_argument("--mode", choices=['daily', 'weekly', 'monthly', 'backfill'],
                       help="Scheduling mode")
    parser.add_argument("--start-date", help="Custom start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", help="Custom end date (YYYY-MM-DD)")
    parser.add_argument("--backfill-days", type=int, default=30,
                       help="Number of days to backfill (for backfill mode)")
    parser.add_argument("--email-report", action="store_true",
                       help="Send email report after completion")
    parser.add_argument("--dry-run", action="store_true",
                       help="Validate configuration without running")
    parser.add_argument("--check-gaps", action="store_true",
                       help="Check for data gaps and suggest backfill")
    
    args = parser.parse_args()
    
    if not Path(args.config).exists():
        print(f"❌ Configuration file not found: {args.config}")
        sys.exit(1)
    
    scheduler = ETLScheduler(args.config)
    
    if args.check_gaps:
        gaps = scheduler.check_data_gaps()
        if gaps:
            print(f"📊 Found {len(gaps)} data gaps")
            for gap in gaps:
                print(f"  • {gap}")
        else:
            print("✅ No data gaps found")
        return
    
    # Determine date range
    if args.start_date and args.end_date:
        start_date, end_date = args.start_date, args.end_date
    elif args.mode:
        start_date, end_date = scheduler.calculate_date_range(args.mode, args.backfill_days)
    else:
        print("❌ Must specify either --mode or both --start-date and --end-date")
        sys.exit(1)
    
    # Run ETL
    success = scheduler.run_etl(
        start_date=start_date,
        end_date=end_date,
        email_report=args.email_report,
        dry_run=args.dry_run
    )
    
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()