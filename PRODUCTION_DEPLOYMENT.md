# USASpending ETL - Production Deployment Guide

This guide covers deploying the USASpending ETL pipeline for production use with scheduling, monitoring, and error handling.

## 🏗️ Architecture Overview

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Scheduler     │───▶│  Main ETL        │───▶│   Outputs       │
│  (Cron/Task)    │    │  Orchestrator    │    │  (Data/Reports) │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                                │
                                ▼
                       ┌──────────────────┐
                       │   Components     │
                       │ • Data Download  │
                       │ • Data Processing│
                       │ • Analysis       │
                       │ • Cleanup        │
                       │ • Notifications  │
                       └──────────────────┘
```

## 📁 File Structure

```
usaspending_etl/
├── usaspending_production_etl.py    # Main orchestrator
├── schedule_etl.py                  # Scheduler helper
├── production_config.yaml           # Production configuration
├── etl_config.yaml                  # ETL processing configuration
├── usaspending_etl_enhanced.py      # ETL processing engine
├── analyze_processed_data.py        # Analysis engine
├── usaspending_pipeline.py          # Data download engine
├── logs/                            # Log files
├── raw_data/                        # Downloaded raw data
├── processed_data/                  # Processed output
├── archive/                         # Archived outputs
└── results/                         # Run results and metadata
```

## 🚀 Quick Start

### 1. Installation

```bash
# Clone or copy all ETL files to your server
mkdir /opt/usaspending_etl
cd /opt/usaspending_etl

# Install dependencies
pip install pandas pyarrow pyyaml httpx backoff

# Make scripts executable
chmod +x usaspending_production_etl.py
chmod +x schedule_etl.py
```

### 2. Configuration

```bash
# Copy and customize the production configuration
cp production_config.yaml my_production_config.yaml

# Edit configuration for your environment
nano my_production_config.yaml
```

### 3. Test Run

```bash
# Dry run to validate configuration
python usaspending_production_etl.py --config my_production_config.yaml --dry-run

# Test run with small date range
python schedule_etl.py --config my_production_config.yaml --start-date 2025-09-01 --end-date 2025-09-01
```

## ⚙️ Configuration

### Production Configuration (`production_config.yaml`)

```yaml
# Key settings to customize:

data_download:
  enabled: true
  start_date: "2025-09-01"  # Updated by scheduler
  end_date: "2025-09-30"    # Updated by scheduler
  output_dir: "/data/usaspending/raw"
  groups: ["contracts"]

data_processing:
  input_dir: "/data/usaspending/raw"
  output_dir: "/data/usaspending/processed"
  
email:
  enabled: true
  smtp_server: "your-smtp-server.com"
  username: "etl-notifications@company.com"
  to_emails: ["team@company.com"]

logging:
  level: "INFO"
  file: "/var/log/usaspending_etl/pipeline.log"
```

## 📅 Scheduling Options

### Option 1: Using the Scheduler Helper (Recommended)

```bash
# Daily incremental updates (yesterday's data)
0 2 * * * /usr/bin/python3 /opt/usaspending_etl/schedule_etl.py --config /opt/usaspending_etl/production_config.yaml --mode daily --email-report

# Weekly full refresh (last 7 days)
0 2 * * 0 /usr/bin/python3 /opt/usaspending_etl/schedule_etl.py --config /opt/usaspending_etl/production_config.yaml --mode weekly --email-report

# Monthly full refresh (last 30 days)
0 2 1 * * /usr/bin/python3 /opt/usaspending_etl/schedule_etl.py --config /opt/usaspending_etl/production_config.yaml --mode monthly --email-report
```

### Option 2: Direct Orchestrator (Manual Date Management)

```bash
# You manage dates in the config file
0 2 * * * /usr/bin/python3 /opt/usaspending_etl/usaspending_production_etl.py --config /opt/usaspending_etl/production_config.yaml --email-report
```

### Option 3: Dynamic Date Script

Create a wrapper script that updates dates:

```bash
#!/bin/bash
# update_and_run_etl.sh

CONFIG_FILE="/opt/usaspending_etl/production_config.yaml"
YESTERDAY=$(date -d "yesterday" +%Y-%m-%d)

# Update config with yesterday's date
sed -i "s/start_date: .*/start_date: \"$YESTERDAY\"/" $CONFIG_FILE
sed -i "s/end_date: .*/end_date: \"$YESTERDAY\"/" $CONFIG_FILE

# Run ETL
python3 /opt/usaspending_etl/usaspending_production_etl.py --config $CONFIG_FILE --email-report
```

## 🔧 Usage Examples

### Daily Operations

```bash
# Run daily update
python schedule_etl.py --mode daily

# Run with email notification
python schedule_etl.py --mode daily --email-report

# Dry run to test configuration
python schedule_etl.py --mode daily --dry-run
```

### Backfill Operations

```bash
# Backfill last 30 days
python schedule_etl.py --mode backfill --backfill-days 30

# Backfill specific date range
python schedule_etl.py --start-date 2025-08-01 --end-date 2025-08-31

# Large backfill with email notification
python schedule_etl.py --start-date 2025-01-01 --end-date 2025-12-31 --email-report
```

### Monitoring and Maintenance

```bash
# Check recent run results
ls -la results/

# View latest log
tail -f logs/etl_pipeline.log

# Check data quality report
cat processed_data/data_quality_report_*.json | jq .

# Validate configuration
python usaspending_production_etl.py --config production_config.yaml --dry-run
```

## 📊 Monitoring and Alerting

### Log Monitoring

```bash
# Monitor for errors
tail -f logs/etl_pipeline.log | grep ERROR

# Monitor for completion
tail -f logs/etl_pipeline.log | grep "Pipeline completed"

# Check disk usage
du -sh raw_data/ processed_data/ archive/
```

### Email Notifications

The pipeline automatically sends email reports including:
- ✅ Success/failure status
- 📊 Processing statistics
- 📁 Output file locations
- ❌ Error details (if any)
- ⏱️ Runtime duration

### Custom Monitoring Integration

Add monitoring hooks to the configuration:

```yaml
# Example webhook integration
monitoring:
  webhook_url: "https://your-monitoring-system.com/webhook"
  slack_webhook: "https://hooks.slack.com/services/YOUR/SLACK/WEBHOOK"
```

## 🛠️ Troubleshooting

### Common Issues

1. **Download Timeouts**
   ```bash
   # Increase timeout in config
   data_download:
     timeout_minutes: 120
   ```

2. **Memory Issues**
   ```bash
   # Process smaller date ranges
   python schedule_etl.py --start-date 2025-09-01 --end-date 2025-09-07
   ```

3. **Disk Space**
   ```bash
   # Enable cleanup
   cleanup:
     enabled: true
     remove_raw_downloads: true
   ```

4. **Email Issues**
   ```bash
   # Test email configuration
   python -c "
   import smtplib
   from email.mime.text import MIMEText
   # Test your SMTP settings
   "
   ```

### Debug Mode

```bash
# Run with debug logging
python usaspending_production_etl.py --config production_config.yaml

# Check specific component
python usaspending_etl_enhanced.py --input-dir raw_data --output-dir debug_output
```

### Recovery Procedures

1. **Failed Download**: Re-run with same date range
2. **Failed Processing**: Check raw data integrity, re-run processing only
3. **Partial Success**: Check results JSON file for details

## 🔒 Security Considerations

### File Permissions

```bash
# Secure configuration files
chmod 600 production_config.yaml
chown etl_user:etl_group production_config.yaml

# Secure log directory
chmod 755 logs/
chown etl_user:etl_group logs/
```

### Email Security

- Use app passwords for Gmail
- Store credentials in environment variables
- Consider using OAuth2 for production

### Data Security

- Encrypt sensitive data at rest
- Use secure transfer protocols
- Implement data retention policies

## 📈 Performance Optimization

### Resource Management

```yaml
# Optimize for your environment
environment:
  max_memory_gb: 16
  max_runtime_hours: 4
  parallel_processing: true
```

### Data Partitioning

```bash
# Process data in smaller chunks
python schedule_etl.py --start-date 2025-09-01 --end-date 2025-09-07  # Weekly
python schedule_etl.py --start-date 2025-09-08 --end-date 2025-09-14  # Next week
```

### Storage Optimization

```yaml
cleanup:
  enabled: true
  remove_raw_downloads: true    # Save disk space
  compress_archives: true       # Compress old data
  retention_days: 90           # Keep data for 90 days
```

## 🔄 Maintenance

### Regular Tasks

1. **Weekly**: Check log files and disk usage
2. **Monthly**: Review data quality reports
3. **Quarterly**: Update dependencies and test disaster recovery

### Updates and Upgrades

```bash
# Backup current installation
cp -r /opt/usaspending_etl /opt/usaspending_etl_backup_$(date +%Y%m%d)

# Update code
git pull origin main  # or copy new files

# Test in staging environment first
python usaspending_production_etl.py --config staging_config.yaml --dry-run
```

## 📞 Support

### Getting Help

1. Check the logs first: `tail -f logs/etl_pipeline.log`
2. Review the results JSON: `cat results/run_results_*.json`
3. Test individual components separately
4. Check USASpending API status: https://api.usaspending.gov/

### Reporting Issues

Include in your report:
- Configuration file (sanitized)
- Log files (last 100 lines)
- Results JSON file
- System information (OS, Python version, available memory)

This production deployment setup provides a robust, scalable, and maintainable ETL pipeline for USASpending data processing.