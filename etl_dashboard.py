#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ETL Dashboard - Monitor pipeline runs and data quality

This script provides a simple dashboard to monitor ETL pipeline runs,
view data quality metrics, and check system status.

Usage:
    python etl_dashboard.py                    # Show recent runs
    python etl_dashboard.py --detailed         # Show detailed metrics
    python etl_dashboard.py --export-csv      # Export metrics to CSV
"""

import json
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
import argparse
from typing import List, Dict, Optional

class ETLDashboard:
    """Dashboard for monitoring ETL pipeline runs."""
    
    def __init__(self, results_dir: str = "results", processed_dir: str = "processed_data"):
        self.results_dir = Path(results_dir)
        self.processed_dir = Path(processed_dir)
    
    def get_recent_runs(self, days: int = 7) -> List[Dict]:
        """Get recent pipeline runs."""
        if not self.results_dir.exists():
            return []
        
        cutoff_date = datetime.now() - timedelta(days=days)
        recent_runs = []
        
        for results_file in self.results_dir.glob("run_results_*.json"):
            try:
                with open(results_file, 'r') as f:
                    run_data = json.load(f)
                
                # Parse start time
                start_time = datetime.fromisoformat(run_data.get('start_time', ''))
                
                if start_time >= cutoff_date:
                    recent_runs.append(run_data)
            
            except Exception as e:
                print(f"Warning: Could not parse {results_file}: {e}")
        
        # Sort by start time (newest first)
        recent_runs.sort(key=lambda x: x.get('start_time', ''), reverse=True)
        return recent_runs
    
    def get_data_quality_reports(self, days: int = 7) -> List[Dict]:
        """Get recent data quality reports."""
        if not self.processed_dir.exists():
            return []
        
        cutoff_date = datetime.now() - timedelta(days=days)
        quality_reports = []
        
        for report_file in self.processed_dir.glob("data_quality_report_*.json"):
            try:
                with open(report_file, 'r') as f:
                    report_data = json.load(f)
                
                # Parse processing timestamp
                timestamp = datetime.fromisoformat(report_data.get('processing_timestamp', ''))
                
                if timestamp >= cutoff_date:
                    quality_reports.append({
                        'file': report_file.name,
                        'timestamp': timestamp,
                        'data': report_data
                    })
            
            except Exception as e:
                print(f"Warning: Could not parse {report_file}: {e}")
        
        # Sort by timestamp (newest first)
        quality_reports.sort(key=lambda x: x['timestamp'], reverse=True)
        return quality_reports
    
    def print_run_summary(self, runs: List[Dict]):
        """Print summary of recent runs."""
        if not runs:
            print("📊 No recent runs found")
            return
        
        print(f"📊 RECENT PIPELINE RUNS ({len(runs)} runs)")
        print("=" * 80)
        
        success_count = sum(1 for run in runs if run.get('status') == 'SUCCESS')
        failure_count = len(runs) - success_count
        
        print(f"✅ Successful runs: {success_count}")
        print(f"❌ Failed runs: {failure_count}")
        print(f"📈 Success rate: {success_count/len(runs)*100:.1f}%")
        print()
        
        # Show recent runs
        print("Recent Runs:")
        print("-" * 40)
        
        for run in runs[:10]:  # Show last 10 runs
            status_emoji = "✅" if run.get('status') == 'SUCCESS' else "❌"
            start_time = run.get('start_time', '')[:19]  # Remove microseconds
            run_id = run.get('run_id', 'Unknown')
            
            # Calculate duration if available
            duration = ""
            if run.get('end_time'):
                try:
                    start = datetime.fromisoformat(run.get('start_time'))
                    end = datetime.fromisoformat(run.get('end_time'))
                    duration_seconds = (end - start).total_seconds()
                    duration = f" ({duration_seconds/60:.1f}m)"
                except:
                    pass
            
            print(f"{status_emoji} {start_time} | {run_id}{duration}")
            
            # Show errors if any
            if run.get('errors'):
                for error in run.get('errors', [])[:2]:  # Show first 2 errors
                    print(f"    ⚠️  {error[:80]}...")
    
    def print_detailed_run_info(self, runs: List[Dict]):
        """Print detailed information about recent runs."""
        if not runs:
            return
        
        print(f"\n📋 DETAILED RUN INFORMATION")
        print("=" * 80)
        
        for i, run in enumerate(runs[:3]):  # Show details for last 3 runs
            print(f"\nRun #{i+1}: {run.get('run_id', 'Unknown')}")
            print(f"Status: {run.get('status', 'Unknown')}")
            print(f"Start: {run.get('start_time', 'Unknown')}")
            print(f"End: {run.get('end_time', 'Unknown')}")
            
            # Show step details
            steps = run.get('steps', {})
            if steps:
                print("Steps:")
                for step_name, step_info in steps.items():
                    status_emoji = "✅" if step_info.get('status') == 'SUCCESS' else "❌" if step_info.get('status') == 'FAILED' else "⏸️"
                    print(f"  {status_emoji} {step_name}: {step_info.get('status', 'Unknown')}")
            
            # Show output files
            output_files = run.get('output_files', [])
            if output_files:
                print("Output Files:")
                for file_path in output_files:
                    file_name = Path(file_path).name
                    print(f"  📁 {file_name}")
            
            print("-" * 40)
    
    def print_data_quality_summary(self, reports: List[Dict]):
        """Print data quality summary."""
        if not reports:
            print("📊 No recent data quality reports found")
            return
        
        print(f"\n🎯 DATA QUALITY SUMMARY ({len(reports)} reports)")
        print("=" * 80)
        
        # Get latest report for detailed analysis
        latest_report = reports[0]['data']
        
        # Summary statistics
        summary = latest_report.get('summary_statistics', {})
        if summary:
            print(f"Latest Dataset:")
            print(f"  📊 Total Records: {summary.get('total_rows', 0):,}")
            print(f"  📋 Total Columns: {summary.get('total_columns', 0)}")
            print(f"  💾 Memory Usage: {summary.get('memory_usage_mb', 0):.1f} MB")
        
        # Data quality issues
        issues = latest_report.get('data_quality_issues', [])
        if issues:
            print(f"\nRecent Issues:")
            error_count = sum(1 for issue in issues if issue.get('severity') == 'ERROR')
            warning_count = sum(1 for issue in issues if issue.get('severity') == 'WARNING')
            
            print(f"  ❌ Errors: {error_count}")
            print(f"  ⚠️  Warnings: {warning_count}")
            
            # Show recent issues
            for issue in issues[-5:]:  # Show last 5 issues
                severity_emoji = "❌" if issue.get('severity') == 'ERROR' else "⚠️" if issue.get('severity') == 'WARNING' else "ℹ️"
                message = issue.get('message', '')[:60]
                print(f"  {severity_emoji} {message}")
        
        # Column completeness
        null_counts = summary.get('null_counts', {})
        if null_counts:
            print(f"\nColumn Completeness (Top 5 issues):")
            # Sort by null count
            sorted_nulls = sorted(null_counts.items(), key=lambda x: x[1], reverse=True)
            total_rows = summary.get('total_rows', 1)
            
            for col, null_count in sorted_nulls[:5]:
                if null_count > 0:
                    completeness = (total_rows - null_count) / total_rows * 100
                    print(f"  📊 {col}: {completeness:.1f}% complete ({null_count:,} missing)")
    
    def export_to_csv(self, runs: List[Dict], output_file: str = "etl_metrics.csv"):
        """Export run metrics to CSV."""
        if not runs:
            print("No data to export")
            return
        
        # Prepare data for CSV
        csv_data = []
        
        for run in runs:
            # Calculate duration
            duration_minutes = None
            if run.get('start_time') and run.get('end_time'):
                try:
                    start = datetime.fromisoformat(run.get('start_time'))
                    end = datetime.fromisoformat(run.get('end_time'))
                    duration_minutes = (end - start).total_seconds() / 60
                except:
                    pass
            
            # Count step statuses
            steps = run.get('steps', {})
            successful_steps = sum(1 for step in steps.values() if step.get('status') == 'SUCCESS')
            failed_steps = sum(1 for step in steps.values() if step.get('status') == 'FAILED')
            
            csv_data.append({
                'run_id': run.get('run_id'),
                'status': run.get('status'),
                'start_time': run.get('start_time'),
                'end_time': run.get('end_time'),
                'duration_minutes': duration_minutes,
                'successful_steps': successful_steps,
                'failed_steps': failed_steps,
                'total_errors': len(run.get('errors', [])),
                'output_files': len(run.get('output_files', []))
            })
        
        # Create DataFrame and save
        df = pd.DataFrame(csv_data)
        df.to_csv(output_file, index=False)
        print(f"📊 Metrics exported to: {output_file}")
    
    def show_system_status(self):
        """Show system status and disk usage."""
        print(f"\n💻 SYSTEM STATUS")
        print("=" * 80)
        
        # Check directory sizes
        directories = [
            ("Raw Data", self.processed_dir.parent / "raw_data"),
            ("Processed Data", self.processed_dir),
            ("Archive", self.processed_dir.parent / "archive"),
            ("Results", self.results_dir)
        ]
        
        for name, path in directories:
            if path.exists():
                size_mb = sum(f.stat().st_size for f in path.rglob('*') if f.is_file()) / 1024 / 1024
                file_count = len(list(path.rglob('*')))
                print(f"📁 {name}: {size_mb:.1f} MB ({file_count} files)")
            else:
                print(f"📁 {name}: Directory not found")
        
        # Check recent activity
        if self.processed_dir.exists():
            recent_files = sorted(
                [f for f in self.processed_dir.rglob('*') if f.is_file()],
                key=lambda x: x.stat().st_mtime,
                reverse=True
            )[:5]
            
            if recent_files:
                print(f"\nRecent Files:")
                for file_path in recent_files:
                    mod_time = datetime.fromtimestamp(file_path.stat().st_mtime)
                    print(f"  📄 {file_path.name} ({mod_time.strftime('%Y-%m-%d %H:%M')})")

def main():
    parser = argparse.ArgumentParser(description="ETL Pipeline Dashboard")
    parser.add_argument("--days", type=int, default=7, help="Number of days to look back")
    parser.add_argument("--detailed", action="store_true", help="Show detailed run information")
    parser.add_argument("--export-csv", help="Export metrics to CSV file")
    parser.add_argument("--results-dir", default="results", help="Results directory")
    parser.add_argument("--processed-dir", default="processed_data", help="Processed data directory")
    
    args = parser.parse_args()
    
    # Initialize dashboard
    dashboard = ETLDashboard(args.results_dir, args.processed_dir)
    
    # Get data
    runs = dashboard.get_recent_runs(args.days)
    quality_reports = dashboard.get_data_quality_reports(args.days)
    
    # Show information
    dashboard.print_run_summary(runs)
    
    if args.detailed:
        dashboard.print_detailed_run_info(runs)
    
    dashboard.print_data_quality_summary(quality_reports)
    dashboard.show_system_status()
    
    # Export if requested
    if args.export_csv:
        dashboard.export_to_csv(runs, args.export_csv)
    
    print(f"\n🔄 Dashboard updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

if __name__ == "__main__":
    main()