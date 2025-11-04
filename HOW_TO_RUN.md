# How to Run the USASpending ETL System

This guide provides clear, step-by-step instructions for running the USASpending ETL pipeline. **You only need to use 1-2 files** for most operations.

## 🎯 Quick Reference - Which File to Use

| What You Want to Do | File to Run | Command |
|---------------------|-------------|---------|
| **Daily automated run (T-1)** | `schedule_etl_with_drive.py` | `python schedule_etl_with_drive.py --config production_config.yaml --mode daily` |
| **Daily run + Google Drive** | `schedule_etl_with_drive.py` | `python schedule_etl_with_drive.py --config production_config.yaml --mode daily --upload-to-drive` |
| **Test the system** | `schedule_etl_with_drive.py` | `python schedule_etl_with_drive.py --config production_config.yaml --mode daily --dry-run` |
| **Monitor pipeline** | `etl_dashboard.py` | `python etl_dashboard.py` |
| **Custom date range** | `schedule_etl_with_drive.py` | `python schedule_etl_with_drive.py --start-date 2025-11-02 --end-date 2025-11-02` |
| **Process existing data** | `usaspending_etl_enhanced.py` | `python usaspending_etl_enhanced.py --input-dir raw_data --output-dir processed_data` |

## 🚀 Getting Started (First Time Setup)

### Step 1: Install Dependencies

**Option A: Install from requirements file (Recommended)**
```bash
# Make sure your virtual environment is activated
pip install -r requirements.txt
```

**Option B: Install individually**
```bash
pip install pandas pyarrow pyyaml httpx backoff
```

**Note**: If using a virtual environment (recommended), make sure it's activated before installing dependencies.

### Step 2: Test Your Setup
```bash
# Navigate to your ETL folder
cd C:\path\to\your\usaspending_etl

# Test the system (doesn't actually run, just validates)
python schedule_etl.py --config production_config.yaml --mode daily --dry-run
```

**Expected Output:**
```
🔍 Dry run mode - validating configuration...
✅ Configuration is valid
📋 Pipeline steps configured:
  • data_download: ✅ Enabled
  • data_processing: ✅ Enabled
  • analysis: ✅ Enabled
  • cleanup: ✅ Enabled
  • email: ⏸️ Disabled
✅ ETL completed successfully
```

### Step 3: Run Your First ETL Job
```bash
# Process yesterday's data
python schedule_etl.py --config production_config.yaml --mode daily
```

## 📅 Daily Operations

### Run Daily ETL (Most Common)
```bash
# Process yesterday's data automatically
python schedule_etl.py --config production_config.yaml --mode daily --email-report
```

**What this does:**
1. ✅ Downloads yesterday's contract data from USASpending.gov
2. ✅ Processes and standardizes the data (23 required headers)
3. ✅ Generates quality reports
4. ✅ Sends email notification (if configured)
5. ✅ Cleans up temporary files

**Runtime:** ~15-20 minutes for daily data

### Check Pipeline Status
```bash
# View recent pipeline runs and data quality
python etl_dashboard.py
```

**Sample Output:**
```
📊 RECENT PIPELINE RUNS (5 runs)
================================================================================
✅ Successful runs: 4
❌ Failed runs: 1  
📈 Success rate: 80.0%

Recent Runs:
----------------------------------------
✅ 2025-10-28 21:27:24 | 20251028_212724 (15.2m)
✅ 2025-10-27 21:27:24 | 20251027_212724 (12.8m)

🎯 DATA QUALITY SUMMARY
================================================================================
Latest Dataset:
  📊 Total Records: 140,785
  📋 Total Columns: 23
  💾 Memory Usage: 107.6 MB
```

## 📊 Different Run Modes

### Weekly Run (Last 7 Days)
```bash
python schedule_etl.py --config production_config.yaml --mode weekly --email-report
```

### Monthly Run (Last 30 Days)
```bash
python schedule_etl.py --config production_config.yaml --mode monthly --email-report
```

### Custom Date Range
```bash
# Process specific dates
python schedule_etl.py --start-date 2025-09-01 --end-date 2025-09-30 --email-report
```

### Backfill Missing Data
```bash
# Fill in last 30 days of data
python schedule_etl.py --config production_config.yaml --mode backfill --backfill-days 30
```

## 🧪 Testing and Validation

### Test Without Running (Dry Run)
```bash
# Validate configuration and setup
python schedule_etl.py --config production_config.yaml --mode daily --dry-run
```

### Test Individual Components
```bash
# Test just the data processing (if you already have raw data)
python usaspending_etl_enhanced.py --input-dir raw_data --output-dir test_output

# Test just the analysis (if you have processed data)
python analyze_processed_data.py --input-file processed_data/your_file.parquet
```

### View Sample Results
```bash
# See a demo of what the final output looks like
python final_demo.py
```

## 🔧 Configuration

### Basic Configuration (production_config.yaml)
```yaml
# Most important settings to customize:

data_download:
  enabled: true
  groups: ["contracts"]  # What type of data to download
  
email:
  enabled: false  # Set to true and add your email settings
  to_emails: ["your-email@company.com"]
  
cleanup:
  enabled: true
  archive_outputs: true  # Keep processed files
```

### Test Your Email Configuration
```bash
# Run with email enabled to test notifications
python schedule_etl.py --config production_config.yaml --mode daily --email-report
```

## 🪟 Windows Task Scheduler Setup

### For Automated Daily Runs

**Program/Script:**
```
python
```

**Add arguments:**
```
schedule_etl.py --config production_config.yaml --mode daily --email-report
```

**Start in:**
```
C:\path\to\your\etl\folder
```

**Schedule:** Daily at 2:00 AM

### Alternative: Use Batch File
Create `run_etl.bat`:
```batch
@echo off
cd /d "C:\path\to\your\etl\folder"
python schedule_etl.py --config production_config.yaml --mode daily --email-report
pause
```

Then schedule the `.bat` file in Task Scheduler.

## 📁 Understanding the Output

### Where Files Are Created
```
your_etl_folder/
├── raw_data/           # Downloaded data from USASpending
├── processed_data/     # Your final standardized data files
├── archive/           # Archived copies of processed data
├── results/           # Run metadata and statistics
└── logs/             # Log files for troubleshooting
```

### Key Output Files
- **`processed_data/usaspending_processed_YYYYMMDD_HHMMSS.parquet`** - Your main data file with 23 headers
- **`processed_data/data_quality_report_*.json`** - Data quality metrics
- **`results/run_results_*.json`** - Pipeline execution details

## 🚨 Troubleshooting

### Pipeline Failed?
```bash
# 1. Check what went wrong
python etl_dashboard.py

# 2. Look at the logs
type logs\etl_pipeline.log

# 3. Try a dry run to test configuration
python schedule_etl.py --config production_config.yaml --mode daily --dry-run

# 4. Re-run the failed date
python schedule_etl.py --start-date 2025-10-27 --end-date 2025-10-27
```

### Common Issues

**"Configuration file not found"**
```bash
# Make sure you're in the right directory
cd C:\path\to\your\etl\folder
dir production_config.yaml
```

**"No data downloaded"**
- Check your internet connection
- Verify USASpending.gov is accessible
- Try a smaller date range

**"Email not working"**
- Set `email: enabled: false` in config to skip emails
- Check your SMTP settings if you want emails

## 📊 Monitoring Your Data

### View Processing Statistics
```bash
# Detailed dashboard with metrics
python etl_dashboard.py --detailed

# Export metrics to CSV for analysis
python etl_dashboard.py --export-csv my_metrics.csv
```

### Check Data Quality
```bash
# View the latest data quality report
type processed_data\data_quality_report_*.json
```

## 🎯 Production Checklist

### Daily Checklist (2 minutes)
1. ✅ Check email notification (if enabled)
2. ✅ Verify new files in `processed_data/` folder
3. ✅ Run: `python etl_dashboard.py` (quick status check)

### Weekly Checklist (5 minutes)
1. ✅ Run: `python etl_dashboard.py --detailed`
2. ✅ Check disk space in data folders
3. ✅ Review any error patterns

### If Something Goes Wrong
1. 🔍 Run: `python etl_dashboard.py` (see recent failures)
2. 📋 Check: `logs\etl_pipeline.log` (detailed error info)
3. 🧪 Test: `python schedule_etl.py --config production_config.yaml --mode daily --dry-run`
4. 🔄 Retry: Re-run the failed date range

## 💡 Pro Tips

### For Regular Use
- **Use `schedule_etl.py`** - This is your main file for 90% of operations
- **Use `--dry-run`** - Always test changes before running for real
- **Use `etl_dashboard.py`** - Check this regularly for system health

### For Development/Testing
- **Use smaller date ranges** - Test with 1-2 days of data first
- **Check the `results/` folder** - Contains detailed run information
- **Use `final_demo.py`** - Shows you what the final output looks like

### For Troubleshooting
- **Always check the dashboard first** - `python etl_dashboard.py`
- **Use dry-run mode** - Validates without actually running
- **Check one component at a time** - Use individual scripts if needed

---

## 🎉 Summary

**For 99% of your usage, you only need these two commands:**

```bash
# Daily ETL run
python schedule_etl.py --config production_config.yaml --mode daily --email-report

# Check status
python etl_dashboard.py
```

Everything else is for testing, troubleshooting, or special situations. The system is designed to be simple to use while being powerful under the hood!