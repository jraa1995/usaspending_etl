#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
USASpending Production ETL Pipeline

This is the main orchestrator script for production deployment.
It handles the complete pipeline:
1. Data download (using existing pipeline)
2. Data processing and transformation
3. Quality validation
4. Analysis and reporting
5. Cleanup and archiving

Usage:
    python usaspending_production_etl.py --config production_config.yaml
    
For scheduling:
    # Daily run
    0 2 * * * /usr/bin/python3 /path/to/usaspending_production_etl.py --config /path/to/config.yaml
    
    # Weekly run with email notifications
    0 2 * * 0 /usr/bin/python3 /path/to/usaspending_production_etl.py --config /path/to/config.yaml --email-report
"""

import os
import sys
import logging
import yaml
import json
import subprocess
import shutil
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import argparse
import traceback
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders

# Import our ETL components
try:
    from usaspending_etl_enhanced import EnhancedUSASpendingETL
    from analyze_processed_data import USASpendingAnalyzer
except ImportError as e:
    print(f"Error importing ETL components: {e}")
    print("Make sure usaspending_etl_enhanced.py and analyze_processed_data.py are in the same directory")
    sys.exit(1)

# Setup logging
def setup_logging(log_level: str = "INFO", log_file: Optional[Path] = None):
    """Setup logging configuration."""
    handlers = [logging.StreamHandler(sys.stdout)]
    
    if log_file:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(log_file))
    
    logging.basicConfig(
        level=getattr(logging, log_level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        handlers=handlers
    )
    
    return logging.getLogger("usaspending_production")

class ProductionETLOrchestrator:
    """Production-grade ETL orchestrator for USASpending data."""
    
    def __init__(self, config_path: Path):
        self.config_path = Path(config_path)
        self.config = self.load_config()
        self.logger = setup_logging(
            self.config.get('logging', {}).get('level', 'INFO'),
            Path(self.config.get('logging', {}).get('file')) if self.config.get('logging', {}).get('file') else None
        )
        self.run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.results = {
            'run_id': self.run_id,
            'start_time': datetime.now().isoformat(),
            'status': 'RUNNING',
            'steps': {},
            'errors': [],
            'output_files': []
        }
        
    def load_config(self) -> Dict:
        """Load production configuration."""
        try:
            with open(self.config_path, 'r') as f:
                config = yaml.safe_load(f)
            return config
        except Exception as e:
            print(f"Error loading config from {self.config_path}: {e}")
            sys.exit(1)
    
    def log_step(self, step_name: str, status: str, details: Optional[Dict] = None):
        """Log a pipeline step."""
        self.results['steps'][step_name] = {
            'status': status,
            'timestamp': datetime.now().isoformat(),
            'details': details or {}
        }
        
        if status == 'SUCCESS':
            self.logger.info(f"✅ {step_name} completed successfully")
        elif status == 'FAILED':
            self.logger.error(f"❌ {step_name} failed")
        else:
            self.logger.info(f"🔄 {step_name}: {status}")
    
    def run_data_download(self) -> bool:
        """Run data download using the existing pipeline."""
        try:
            self.log_step("data_download", "STARTED")
            
            download_config = self.config.get('data_download', {})
            if not download_config.get('enabled', True):
                self.log_step("data_download", "SKIPPED", {"reason": "disabled in config"})
                return True
            
            # Build command for usaspending_pipeline.py
            cmd = [
                sys.executable, 
                str(Path(__file__).parent / "usaspending_pipeline.py"),
                "bulk-backfill"
            ]
            
            # Add required parameters
            cmd.extend([
                "--start-date", download_config['start_date'],
                "--end-date", download_config['end_date'],
                "--out", str(Path(download_config['output_dir']))
            ])
            
            # Add optional parameters
            if download_config.get('groups'):
                cmd.extend(["--groups"] + download_config['groups'])
            
            if download_config.get('agencies'):
                cmd.extend(["--agencies"] + download_config['agencies'])
            
            if download_config.get('date_type'):
                cmd.extend(["--date-type", download_config['date_type']])
            
            if download_config.get('file_format'):
                cmd.extend(["--file-format", download_config['file_format']])
            
            self.logger.info(f"Running download command: {' '.join(cmd)}")
            
            # Run the download
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=3600)  # 1 hour timeout
            
            if result.returncode == 0:
                self.log_step("data_download", "SUCCESS", {
                    "command": ' '.join(cmd),
                    "stdout_lines": len(result.stdout.splitlines()),
                    "stderr_lines": len(result.stderr.splitlines())
                })
                return True
            else:
                self.log_step("data_download", "FAILED", {
                    "command": ' '.join(cmd),
                    "return_code": result.returncode,
                    "stdout": result.stdout[-1000:],  # Last 1000 chars
                    "stderr": result.stderr[-1000:]
                })
                self.results['errors'].append(f"Data download failed: {result.stderr}")
                return False
                
        except subprocess.TimeoutExpired:
            self.log_step("data_download", "FAILED", {"error": "Download timeout (1 hour)"})
            self.results['errors'].append("Data download timed out after 1 hour")
            return False
        except Exception as e:
            self.log_step("data_download", "FAILED", {"error": str(e)})
            self.results['errors'].append(f"Data download error: {str(e)}")
            return False
    
    def run_data_processing(self) -> Optional[Path]:
        """Run data processing using the enhanced ETL."""
        try:
            self.log_step("data_processing", "STARTED")
            
            processing_config = self.config.get('data_processing', {})
            
            # Initialize ETL
            etl_config_path = processing_config.get('etl_config', 'etl_config.yaml')
            etl = EnhancedUSASpendingETL(etl_config_path)
            
            # Build custom filters from config
            custom_filters = {}
            filters_config = processing_config.get('filters', {})
            
            if filters_config.get('fiscal_year_range'):
                custom_filters['fiscal_year_range'] = tuple(filters_config['fiscal_year_range'])
            
            if filters_config.get('min_dollars_obligated'):
                custom_filters['min_dollars_obligated'] = filters_config['min_dollars_obligated']
            
            if filters_config.get('instrument_types'):
                custom_filters['instrument_types'] = filters_config['instrument_types']
            
            if filters_config.get('agencies'):
                custom_filters['agencies'] = filters_config['agencies']
            
            # Run processing
            input_dir = Path(processing_config['input_dir'])
            output_dir = Path(processing_config['output_dir'])
            
            output_file = etl.process_files(input_dir, output_dir, custom_filters)
            
            if output_file:
                self.results['output_files'].append(str(output_file))
                self.log_step("data_processing", "SUCCESS", {
                    "input_dir": str(input_dir),
                    "output_file": str(output_file),
                    "filters_applied": len(custom_filters)
                })
                return output_file
            else:
                self.log_step("data_processing", "FAILED", {"error": "No output file generated"})
                self.results['errors'].append("Data processing failed - no output generated")
                return None
                
        except Exception as e:
            self.log_step("data_processing", "FAILED", {"error": str(e)})
            self.results['errors'].append(f"Data processing error: {str(e)}")
            return None
    
    def run_analysis(self, data_file: Path) -> Optional[Path]:
        """Run data analysis and generate reports."""
        try:
            self.log_step("analysis", "STARTED")
            
            analysis_config = self.config.get('analysis', {})
            if not analysis_config.get('enabled', True):
                self.log_step("analysis", "SKIPPED", {"reason": "disabled in config"})
                return None
            
            # Initialize analyzer
            analyzer = USASpendingAnalyzer(data_file)
            
            if analyzer.df.empty:
                self.log_step("analysis", "FAILED", {"error": "No data to analyze"})
                self.results['errors'].append("Analysis failed - no data loaded")
                return None
            
            # Generate comprehensive report
            report_file = data_file.parent / f"analysis_report_{self.run_id}.txt"
            analyzer.generate_summary_report(report_file)
            
            self.results['output_files'].append(str(report_file))
            self.log_step("analysis", "SUCCESS", {
                "data_file": str(data_file),
                "report_file": str(report_file),
                "records_analyzed": len(analyzer.df)
            })
            
            return report_file
            
        except Exception as e:
            self.log_step("analysis", "FAILED", {"error": str(e)})
            self.results['errors'].append(f"Analysis error: {str(e)}")
            return None
    
    def run_cleanup(self) -> bool:
        """Clean up temporary files and archive outputs."""
        try:
            self.log_step("cleanup", "STARTED")
            
            cleanup_config = self.config.get('cleanup', {})
            if not cleanup_config.get('enabled', True):
                self.log_step("cleanup", "SKIPPED", {"reason": "disabled in config"})
                return True
            
            cleaned_files = 0
            
            # Clean up raw download files if configured
            if cleanup_config.get('remove_raw_downloads', False):
                download_dir = Path(self.config.get('data_download', {}).get('output_dir', 'usaspending_data'))
                if download_dir.exists():
                    for file_path in download_dir.rglob('*.zip'):
                        file_path.unlink()
                        cleaned_files += 1
            
            # Archive outputs if configured
            if cleanup_config.get('archive_outputs', False):
                archive_dir = Path(cleanup_config.get('archive_dir', 'archive'))
                archive_dir.mkdir(parents=True, exist_ok=True)
                
                for output_file in self.results['output_files']:
                    output_path = Path(output_file)
                    if output_path.exists():
                        archive_path = archive_dir / f"{self.run_id}_{output_path.name}"
                        shutil.copy2(output_path, archive_path)
            
            self.log_step("cleanup", "SUCCESS", {"files_cleaned": cleaned_files})
            return True
            
        except Exception as e:
            self.log_step("cleanup", "FAILED", {"error": str(e)})
            self.results['errors'].append(f"Cleanup error: {str(e)}")
            return False
    
    def send_email_report(self) -> bool:
        """Send email report if configured."""
        try:
            email_config = self.config.get('email', {})
            if not email_config.get('enabled', False):
                return True
            
            self.log_step("email_report", "STARTED")
            
            # Create email
            msg = MIMEMultipart()
            msg['From'] = email_config['from_email']
            msg['To'] = ', '.join(email_config['to_emails'])
            msg['Subject'] = f"USASpending ETL Report - {self.run_id} - {self.results['status']}"
            
            # Email body
            body = self.generate_email_body()
            msg.attach(MIMEText(body, 'plain'))
            
            # Attach reports if configured
            if email_config.get('attach_reports', False):
                for output_file in self.results['output_files']:
                    if Path(output_file).exists() and Path(output_file).suffix in ['.txt', '.json']:
                        with open(output_file, 'rb') as f:
                            part = MIMEBase('application', 'octet-stream')
                            part.set_payload(f.read())
                            encoders.encode_base64(part)
                            part.add_header(
                                'Content-Disposition',
                                f'attachment; filename= {Path(output_file).name}'
                            )
                            msg.attach(part)
            
            # Send email
            server = smtplib.SMTP(email_config['smtp_server'], email_config.get('smtp_port', 587))
            if email_config.get('use_tls', True):
                server.starttls()
            if email_config.get('username') and email_config.get('password'):
                server.login(email_config['username'], email_config['password'])
            
            server.send_message(msg)
            server.quit()
            
            self.log_step("email_report", "SUCCESS", {"recipients": len(email_config['to_emails'])})
            return True
            
        except Exception as e:
            self.log_step("email_report", "FAILED", {"error": str(e)})
            self.results['errors'].append(f"Email report error: {str(e)}")
            return False
    
    def generate_email_body(self) -> str:
        """Generate email report body."""
        status_emoji = "✅" if self.results['status'] == 'SUCCESS' else "❌"
        
        body = f"""
USASpending ETL Pipeline Report
{status_emoji} Status: {self.results['status']}
🆔 Run ID: {self.run_id}
⏰ Start Time: {self.results['start_time']}
⏰ End Time: {self.results.get('end_time', 'Running...')}

📊 PIPELINE STEPS:
"""
        
        for step_name, step_info in self.results['steps'].items():
            status_emoji = "✅" if step_info['status'] == 'SUCCESS' else "❌" if step_info['status'] == 'FAILED' else "⏸️"
            body += f"{status_emoji} {step_name}: {step_info['status']}\n"
        
        if self.results['output_files']:
            body += f"\n📁 OUTPUT FILES:\n"
            for output_file in self.results['output_files']:
                body += f"• {Path(output_file).name}\n"
        
        if self.results['errors']:
            body += f"\n❌ ERRORS:\n"
            for error in self.results['errors']:
                body += f"• {error}\n"
        
        return body
    
    def save_run_results(self) -> Path:
        """Save run results to JSON file."""
        results_dir = Path(self.config.get('results_dir', 'results'))
        results_dir.mkdir(parents=True, exist_ok=True)
        
        results_file = results_dir / f"run_results_{self.run_id}.json"
        
        with open(results_file, 'w') as f:
            json.dump(self.results, f, indent=2, default=str)
        
        return results_file
    
    def run_pipeline(self) -> bool:
        """Run the complete ETL pipeline."""
        try:
            self.logger.info(f"🚀 Starting USASpending ETL Pipeline - Run ID: {self.run_id}")
            
            # Step 1: Data Download
            if not self.run_data_download():
                self.results['status'] = 'FAILED'
                return False
            
            # Step 2: Data Processing
            processed_file = self.run_data_processing()
            if not processed_file:
                self.results['status'] = 'FAILED'
                return False
            
            # Step 3: Analysis
            self.run_analysis(processed_file)
            
            # Step 4: Cleanup
            self.run_cleanup()
            
            # Step 5: Email Report
            self.send_email_report()
            
            self.results['status'] = 'SUCCESS'
            self.results['end_time'] = datetime.now().isoformat()
            
            self.logger.info(f"🎉 Pipeline completed successfully - Run ID: {self.run_id}")
            return True
            
        except Exception as e:
            self.results['status'] = 'FAILED'
            self.results['end_time'] = datetime.now().isoformat()
            self.results['errors'].append(f"Pipeline error: {str(e)}")
            self.logger.error(f"💥 Pipeline failed: {str(e)}")
            self.logger.error(traceback.format_exc())
            return False
        
        finally:
            # Always save results
            results_file = self.save_run_results()
            self.logger.info(f"📊 Run results saved to: {results_file}")

def main():
    parser = argparse.ArgumentParser(description="USASpending Production ETL Pipeline")
    parser.add_argument("--config", required=True, help="Production configuration file")
    parser.add_argument("--dry-run", action="store_true", help="Validate configuration without running")
    parser.add_argument("--email-report", action="store_true", help="Force send email report")
    
    args = parser.parse_args()
    
    if not Path(args.config).exists():
        print(f"❌ Configuration file not found: {args.config}")
        sys.exit(1)
    
    if args.dry_run:
        print("🔍 Dry run mode - validating configuration...")
        try:
            orchestrator = ProductionETLOrchestrator(args.config)
            print("✅ Configuration is valid")
            print(f"📋 Pipeline steps configured:")
            for step in ['data_download', 'data_processing', 'analysis', 'cleanup', 'email']:
                enabled = orchestrator.config.get(step, {}).get('enabled', True)
                status = "✅ Enabled" if enabled else "⏸️ Disabled"
                print(f"  • {step}: {status}")
            sys.exit(0)
        except Exception as e:
            print(f"❌ Configuration validation failed: {e}")
            sys.exit(1)
    
    # Run the pipeline
    orchestrator = ProductionETLOrchestrator(args.config)
    
    if args.email_report:
        orchestrator.config.setdefault('email', {})['enabled'] = True
    
    success = orchestrator.run_pipeline()
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()