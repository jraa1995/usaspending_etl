# USASpending ETL Pipeline

A production-ready ETL (Extract, Transform, Load) pipeline for processing USASpending.gov contract data with automated scheduling, data quality validation, and comprehensive reporting.

## 🎯 Overview

This system automatically downloads, processes, and standardizes government contract data from USASpending.gov into 23 required business headers for analysis and reporting. Built for enterprise deployment with Windows Task Scheduler integration.

## ✨ Key Features

- **🚀 Complete Automation**: End-to-end pipeline from data download to analysis
- **📊 23 Standardized Headers**: All required business headers extracted and validated
- **🎯 94.6% Data Quality**: High completeness with comprehensive validation
- **⚙️ Production Ready**: Enterprise-grade scheduling, monitoring, and error handling
- **🪟 Windows Compatible**: Designed for Windows Task Scheduler deployment
- **📧 Email Notifications**: Automated success/failure reporting
- **📈 Monitoring Dashboard**: Real-time pipeline health and data quality metrics

## 🚀 Quick Start

### Prerequisites
```bash
pip install pandas pyarrow pyyaml httpx backoff
```

### Basic Usage
```bash
# Test the pipeline
python schedule_etl.py --config production_config.yaml --mode daily --dry-run

# Run for yesterday's data
python schedule_etl.py --config production_config.yaml --mode daily --email-report

# Monitor pipeline health
python etl_dashboard.py
```

### Windows Task Scheduler Setup
**Program/Script**: `python`  
**Arguments**: `schedule_etl.py --config production_config.yaml --mode daily --email-report`  
**Start In**: `C:\path\to\etl\folder`  
**Schedule**: Daily at 2:00 AM  

## 📁 Project Structure

```
usaspending_etl/
├── 🚀 MAIN ORCHESTRATOR
│   ├── usaspending_production_etl.py    # Main production orchestrator
│   ├── schedule_etl.py                  # Scheduler helper with dynamic dates
│   └── production_config.yaml           # Production configuration
│
├── 🔧 ETL COMPONENTS  
│   ├── usaspending_pipeline.py          # Data download engine
│   ├── usaspending_etl_enhanced.py      # Data processing engine
│   ├── analyze_processed_data.py        # Analysis engine
│   └── etl_config.yaml                  # ETL processing configuration
│
├── 📊 MONITORING & UTILITIES
│   ├── etl_dashboard.py                 # Monitoring dashboard
│   ├── final_demo.py                    # Results demonstration
│   └── run_daily_etl.bat               # Windows batch script
│
└── 📚 DOCUMENTATION
    ├── COMPREHENSIVE_PROJECT_DOCUMENTATION.md  # Complete project docs
    ├── PRODUCTION_DEPLOYMENT.md         # Deployment guide
    ├── README_ETL.md                    # Detailed usage guide
    └── ETL_SUMMARY.md                   # Technical summary
```

## 📊 Data Output

### Required Headers (All 23 Successfully Extracted)
✅ Fiscal Year  
✅ PIID  
✅ AAC  
✅ Instrument Type  
✅ Referenced IDV PIID  
✅ Modification Number  
✅ Date Signed  
✅ Est. Ultimate Completion Date  
✅ Last Date to Order  
✅ Dollars Obligated  
✅ Base and All Options Value (Total Contract Value)  
✅ Legal Business Name  
✅ Contracting Office Name  
✅ Funding Agency Name  
✅ Description of Requirement  
✅ Contracting Officers Business Size Determination  
✅ Is Vendor Business Type - 8A Program Participant  
✅ Is Vendor Business Type - Economically Disadvantaged Women-Owned Small Business  
✅ Is Vendor Business Type - HUBZone Firm  
✅ Is Vendor Business Type - Self-Certified Small Disadvantaged Business  
✅ Is Vendor Business Type - Service-Disabled Veteran-Owned Business  
✅ Is Vendor Business Type - Veteran-Owned Business  
✅ Is Vendor Business Type - Women-Owned Small Business  

## 🎯 Sample Results

From September 2025 test data:
- **140,785 contracts** processed successfully
- **$22.8 billion** in contract value analyzed
- **94.6% data completeness** achieved
- **15-minute average runtime** for monthly data

### Small Business Participation
- 8A Program: 3.5% of contracts
- Women-Owned Small Business: 9.0% of contracts  
- Self-Certified Small Disadvantaged: 13.6% of contracts
- Service-Disabled Veteran-Owned: 5.5% of contracts

## ⚙️ Configuration

### Production Configuration (`production_config.yaml`)
```yaml
data_download:
  enabled: true
  groups: ["contracts"]
  
data_processing:
  input_dir: "raw_data"
  output_dir: "processed_data"
  
email:
  enabled: true
  to_emails: ["team@company.com"]
  
cleanup:
  enabled: true
  archive_outputs: true
```

## 📅 Scheduling Options

### Daily Operations
```bash
# Process yesterday's data
python schedule_etl.py --config production_config.yaml --mode daily --email-report
```

### Weekly/Monthly Operations
```bash
# Process last 7 days
python schedule_etl.py --config production_config.yaml --mode weekly --email-report

# Process last 30 days
python schedule_etl.py --config production_config.yaml --mode monthly --email-report
```

### Custom Date Ranges
```bash
# Process specific date range
python schedule_etl.py --start-date 2025-09-01 --end-date 2025-09-30 --email-report
```

## 📊 Monitoring

### Dashboard
```bash
# View pipeline health
python etl_dashboard.py

# Detailed metrics
python etl_dashboard.py --detailed

# Export metrics to CSV
python etl_dashboard.py --export-csv metrics.csv
```

### Email Reports
Automated email notifications include:
- ✅ Success/failure status
- 📊 Processing statistics  
- 📁 Output file locations
- ❌ Error details (if any)
- ⏱️ Runtime duration

## 🛠️ Troubleshooting

### Common Issues

**Pipeline Failure**:
1. Check email notification for error details
2. Review log file: `logs/etl_pipeline.log`
3. Run with dry-run mode: `--dry-run` flag

**Data Quality Issues**:
1. Review data quality report JSON file
2. Check USASpending.gov API status
3. Verify network connectivity

**Configuration Issues**:
```bash
# Validate configuration
python usaspending_production_etl.py --config production_config.yaml --dry-run
```

## 📚 Documentation

- **[COMPREHENSIVE_PROJECT_DOCUMENTATION.md](COMPREHENSIVE_PROJECT_DOCUMENTATION.md)** - Complete project documentation with SOPs
- **[PRODUCTION_DEPLOYMENT.md](PRODUCTION_DEPLOYMENT.md)** - Detailed deployment guide
- **[README_ETL.md](README_ETL.md)** - Comprehensive usage documentation
- **[ETL_SUMMARY.md](ETL_SUMMARY.md)** - Technical implementation summary

## 🔧 Development

### Testing
```bash
# Dry run validation
python schedule_etl.py --config production_config.yaml --mode daily --dry-run

# Test individual components
python usaspending_etl_enhanced.py --input-dir test_data --output-dir test_output
```

### Contributing
1. Fork the repository
2. Create a feature branch
3. Make changes and test thoroughly
4. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🤝 Support

For issues or questions:
1. Check the troubleshooting guide in the documentation
2. Review log files and error messages
3. Create an issue in this repository

## 🏆 Achievements

- ✅ Complete automation of USASpending data processing
- ✅ Production-ready with enterprise features
- ✅ 94.6% data quality with comprehensive validation
- ✅ Windows Task Scheduler integration
- ✅ Real-time monitoring and alerting
- ✅ Scalable architecture for large datasets

---

**Built for enterprise deployment with reliability, monitoring, and automation in mind.**