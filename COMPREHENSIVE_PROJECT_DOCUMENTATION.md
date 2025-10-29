# USASpending ETL Pipeline - Comprehensive Project Documentation

## Table of Contents
1. [Executive Summary](#executive-summary)
2. [Standard Operating Procedures (SOP)](#standard-operating-procedures-sop)
3. [Current Changes & Updates](#current-changes--updates)
4. [High-Level Technical Architecture](#high-level-technical-architecture)
5. [Implementation Details](#implementation-details)
6. [Maintenance & Support](#maintenance--support)

---

## Executive Summary

### Project Overview
This project delivers a complete, production-ready ETL (Extract, Transform, Load) pipeline for processing USASpending.gov contract data. The system automatically downloads, processes, and standardizes government contract data into 23 required business headers for analysis and reporting.

### Key Achievements
- ✅ **Complete Data Pipeline**: End-to-end automation from data download to analysis
- ✅ **23 Required Headers**: All specified business headers extracted and standardized
- ✅ **94.6% Data Quality**: High completeness with comprehensive validation
- ✅ **Production Ready**: Enterprise-grade scheduling, monitoring, and error handling
- ✅ **Windows Compatible**: Designed for Windows Task Scheduler deployment

### Business Impact
- **Automated Processing**: Eliminates manual data preparation work
- **Standardized Output**: Consistent data format for downstream analysis
- **Quality Assurance**: Built-in validation and quality reporting
- **Scalable Solution**: Handles large datasets efficiently

---

## Standard Operating Procedures (SOP)

### 1. Daily Operations

#### 1.1 Automated Daily Run
**Frequency**: Daily at 2:00 AM  
**Trigger**: Windows Task Scheduler  
**Command**: `python schedule_etl.py --config production_config.yaml --mode daily --email-report`

**Expected Outcome**:
- Downloads previous day's contract data
- Processes and standardizes data
- Generates quality report
- Sends email notification
- Archives processed files

#### 1.2 Monitoring Checklist
**Daily Tasks** (5 minutes):
1. Check email notification for pipeline status
2. Verify processed data files were created
3. Review any error messages in email report

**Weekly Tasks** (15 minutes):
1. Run monitoring dashboard: `python etl_dashboard.py`
2. Check disk usage in data directories
3. Review data quality trends
4. Verify archive directory is functioning

#### 1.3 Troubleshooting Procedures

**Pipeline Failure**:
1. Check email notification for error details
2. Review log file: `logs/etl_pipeline.log`
3. Check run results: `results/run_results_YYYYMMDD_HHMMSS.json`
4. Re-run with dry-run mode: `--dry-run` flag
5. Contact technical support if issues persist

**Data Quality Issues**:
1. Review data quality report JSON file
2. Check source data availability at USASpending.gov
3. Verify network connectivity
4. Re-run processing for specific date range

### 2. Weekly Operations

#### 2.1 Weekly Full Refresh
**Frequency**: Weekly on Sunday at 2:00 AM  
**Purpose**: Process last 7 days of data for completeness  
**Command**: `python schedule_etl.py --config production_config.yaml --mode weekly --email-report`

#### 2.2 System Health Check
1. Run comprehensive dashboard: `python etl_dashboard.py --detailed`
2. Export metrics: `python etl_dashboard.py --export-csv weekly_metrics.csv`
3. Review system resource usage
4. Clean up old log files (>30 days)

### 3. Monthly Operations

#### 3.1 Monthly Full Processing
**Frequency**: 1st of each month at 2:00 AM  
**Purpose**: Complete monthly dataset processing  
**Command**: `python schedule_etl.py --config production_config.yaml --mode monthly --email-report`

#### 3.2 Archive Management
1. Review archive directory size
2. Compress old archives (>90 days)
3. Move very old data to long-term storage
4. Update retention policies if needed

### 4. Emergency Procedures

#### 4.1 Pipeline Down
1. **Immediate**: Check Windows Task Scheduler status
2. **Assess**: Review last successful run timestamp
3. **Diagnose**: Check system resources, network, API availability
4. **Recover**: Re-run failed date ranges manually
5. **Notify**: Inform stakeholders of status and ETA

#### 4.2 Data Corruption
1. **Stop**: Disable scheduled tasks immediately
2. **Assess**: Identify corrupted data range
3. **Restore**: Use archived data if available
4. **Rebuild**: Re-run ETL for affected date ranges
5. **Validate**: Verify data integrity before resuming

### 5. Configuration Management

#### 5.1 Configuration Updates
1. **Backup**: Copy current `production_config.yaml`
2. **Test**: Validate changes with `--dry-run`
3. **Deploy**: Update production configuration
4. **Monitor**: Watch first few runs after changes

#### 5.2 Approved Configuration Changes
- Date ranges for processing
- Email notification recipients
- Data retention policies
- Archive locations
- Filter criteria (agencies, contract types, dollar thresholds)

---

## Current Changes & Updates

### Version 2.0 - Production Release (October 2025)

#### Major Enhancements
1. **Production Orchestrator** (`usaspending_production_etl.py`)
   - Complete pipeline automation
   - Error handling and recovery
   - Email notifications
   - Resource management

2. **Scheduler Helper** (`schedule_etl.py`)
   - Automatic date calculation
   - Dynamic configuration generation
   - Multiple scheduling modes (daily, weekly, monthly)

3. **Monitoring Dashboard** (`etl_dashboard.py`)
   - Pipeline health monitoring
   - Data quality metrics
   - System status reporting
   - CSV export capabilities

#### Critical Bug Fixes
1. **Boolean Conversion Issue** (RESOLVED)
   - **Problem**: Small business type flags showing as 100% null
   - **Root Cause**: Data contained "t"/"f" values instead of proper booleans
   - **Solution**: Enhanced boolean mapping to handle "t"→True, "f"→False
   - **Impact**: Data completeness improved from 64.2% to 94.6%

2. **Column Mapping Corrections**
   - Updated ETL configuration to match actual USASpending API column names
   - Verified all 23 required headers are properly extracted
   - Added validation for missing optional columns

#### New Features
1. **Windows Task Scheduler Integration**
   - Batch script for Windows environments
   - Proper argument handling
   - Path management for scheduled execution

2. **Enhanced Data Quality Reporting**
   - Comprehensive quality metrics
   - Issue severity classification
   - Column-level completeness analysis
   - Trend tracking capabilities

3. **Archive Management**
   - Automatic output archiving
   - Configurable retention policies
   - Disk space optimization

### Version 1.0 - Initial Implementation

#### Core Components Delivered
1. **Data Download Engine** (`usaspending_pipeline.py`)
   - USASpending API integration
   - Bulk download capabilities
   - Rate limiting and error handling

2. **ETL Processing Engine** (`usaspending_etl_enhanced.py`)
   - Column extraction and mapping
   - Data type conversion
   - Quality validation
   - Filtering capabilities

3. **Analysis Engine** (`analyze_processed_data.py`)
   - Statistical analysis
   - Business intelligence reporting
   - Data profiling

#### Initial Data Mapping
- Successfully mapped 20 of 23 required headers
- Identified missing fields (APEX, SBIR/STTR Description)
- Established data quality baseline

---

## High-Level Technical Architecture

### System Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    USASpending ETL Pipeline                     │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌─────────────┐    ┌──────────────┐    ┌─────────────────┐    │
│  │   Windows   │───▶│  Scheduler   │───▶│   Main ETL      │    │
│  │    Task     │    │   Helper     │    │  Orchestrator   │    │
│  │  Scheduler  │    │              │    │                 │    │
│  └─────────────┘    └──────────────┘    └─────────────────┘    │
│                                                   │             │
│                                                   ▼             │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                Pipeline Components                      │   │
│  │                                                         │   │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐    │   │
│  │  │    Data     │  │    Data     │  │  Analysis   │    │   │
│  │  │  Download   │  │ Processing  │  │ & Reporting │    │   │
│  │  │             │  │             │  │             │    │   │
│  │  └─────────────┘  └─────────────┘  └─────────────┘    │   │
│  │                                                         │   │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐    │   │
│  │  │   Quality   │  │   Cleanup   │  │    Email    │    │   │
│  │  │ Validation  │  │ & Archive   │  │ Notification│    │   │
│  │  │             │  │             │  │             │    │   │
│  │  └─────────────┘  └─────────────┘  └─────────────┘    │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                   Data Flow                             │   │
│  │                                                         │   │
│  │  USASpending ──▶ Raw Data ──▶ Processed ──▶ Archive    │   │
│  │     API           Storage      Data         Storage     │   │
│  │                                                         │   │
│  │  Monitoring ◄──── Logs ◄────── Quality ◄─── Reports    │   │
│  │  Dashboard        Files        Metrics      Generated   │   │
│  └─────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

### Data Flow Architecture

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│ USASpending │───▶│  Raw Data   │───▶│ Processing  │───▶│ Standardized│
│     API     │    │   Storage   │    │   Engine    │    │    Output   │
│             │    │             │    │             │    │             │
│ • Bulk DL   │    │ • CSV/      │    │ • Column    │    │ • 23 Headers│
│ • Rate Limit│    │   Parquet   │    │   Mapping   │    │ • Clean Data│
│ • Retry     │    │ • Temp      │    │ • Type Conv │    │ • Validated │
│ • Auth      │    │   Files     │    │ • Filtering │    │ • Dedupe    │
└─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
                                                                   │
┌─────────────┐    ┌─────────────┐    ┌─────────────┐              │
│   Archive   │◄───│   Reports   │◄───│  Analysis   │◄─────────────┘
│   Storage   │    │ Generation  │    │   Engine    │
│             │    │             │    │             │
│ • Long-term │    │ • Quality   │    │ • Statistics│
│ • Compress  │    │   Reports   │    │ • Business  │
│ • Retention │    │ • Email     │    │   Intel     │
│ • Backup    │    │ • Dashboard │    │ • Profiling │
└─────────────┘    └─────────────┘    └─────────────┘
```

### Component Interaction Matrix

| Component | Input | Output | Dependencies |
|-----------|-------|--------|--------------|
| **Scheduler Helper** | Config, Date Mode | Runtime Config | Python, YAML |
| **Main Orchestrator** | Runtime Config | Processed Data | All Components |
| **Download Engine** | Date Range, Filters | Raw Data Files | USASpending API |
| **Processing Engine** | Raw Data, ETL Config | Clean Data | Pandas, PyArrow |
| **Analysis Engine** | Clean Data | Reports | Processed Data |
| **Quality Validator** | All Data | Quality Metrics | Processing Engine |
| **Email Notifier** | Run Results | Email Reports | SMTP Server |
| **Monitoring Dashboard** | Run History | Status Reports | Results Files |

### Technology Stack

#### Core Technologies
- **Python 3.12+**: Primary development language
- **Pandas**: Data manipulation and analysis
- **PyArrow**: High-performance data processing
- **HTTPX**: Modern HTTP client for API calls
- **YAML**: Configuration management

#### Data Formats
- **Input**: CSV, Parquet from USASpending API
- **Processing**: In-memory DataFrames
- **Output**: Parquet (primary), CSV (optional)
- **Metadata**: JSON for run results and quality reports

#### Infrastructure
- **Windows Server**: Primary deployment platform
- **Task Scheduler**: Job scheduling and automation
- **SMTP**: Email notifications
- **File System**: Local storage with archival

### Security Architecture

#### Data Security
- **In-Transit**: HTTPS for all API communications
- **At-Rest**: File system permissions and encryption
- **Access Control**: Service account with minimal privileges
- **Audit Trail**: Comprehensive logging of all operations

#### Configuration Security
- **Credentials**: Environment variables or secure config
- **File Permissions**: Restricted access to config files
- **Version Control**: Exclude sensitive data from repos
- **Backup**: Secure backup of configurations

---

## Implementation Details

### File Structure and Responsibilities

```
usaspending_etl/
├── 🚀 ORCHESTRATION LAYER
│   ├── usaspending_production_etl.py    # Main production orchestrator
│   │   ├── Pipeline coordination
│   │   ├── Error handling and recovery
│   │   ├── Resource management
│   │   └── Notification system
│   │
│   ├── schedule_etl.py                  # Scheduler helper
│   │   ├── Dynamic date calculation
│   │   ├── Runtime configuration generation
│   │   ├── Multiple scheduling modes
│   │   └── Windows Task Scheduler integration
│   │
│   └── run_daily_etl.bat               # Windows batch script
│       ├── Date calculation for Windows
│       ├── Environment setup
│       └── Error handling
│
├── 🔧 PROCESSING LAYER
│   ├── usaspending_pipeline.py          # Data download engine
│   │   ├── USASpending API integration
│   │   ├── Bulk download management
│   │   ├── Rate limiting and retries
│   │   └── File format handling
│   │
│   ├── usaspending_etl_enhanced.py      # Data processing engine
│   │   ├── Column extraction and mapping
│   │   ├── Data type conversion
│   │   ├── Quality validation
│   │   ├── Filtering and deduplication
│   │   └── Configuration-driven processing
│   │
│   └── analyze_processed_data.py        # Analysis engine
│       ├── Statistical analysis
│       ├── Business intelligence
│       ├── Data profiling
│       └── Report generation
│
├── 📊 MONITORING LAYER
│   ├── etl_dashboard.py                 # Monitoring dashboard
│   │   ├── Pipeline health monitoring
│   │   ├── Data quality metrics
│   │   ├── System status reporting
│   │   └── Export capabilities
│   │
│   ├── final_demo.py                    # Results demonstration
│   └── verify_boolean_conversion.py     # Data validation utilities
│
├── ⚙️ CONFIGURATION LAYER
│   ├── production_config.yaml           # Production configuration
│   │   ├── Data source settings
│   │   ├── Processing parameters
│   │   ├── Notification settings
│   │   └── System configuration
│   │
│   └── etl_config.yaml                  # ETL processing configuration
│       ├── Column mappings
│       ├── Data type specifications
│       ├── Quality rules
│       └── Output settings
│
└── 📚 DOCUMENTATION LAYER
    ├── COMPREHENSIVE_PROJECT_DOCUMENTATION.md  # This file
    ├── PRODUCTION_DEPLOYMENT.md         # Deployment guide
    ├── README_ETL.md                    # Usage documentation
    └── ETL_SUMMARY.md                   # Technical summary
```

### Data Processing Pipeline

#### Stage 1: Data Acquisition
```python
# Download Process Flow
USASpending API Request → Rate Limiting → Retry Logic → File Download → Validation → Storage
```

**Key Features**:
- Bulk download API integration
- Automatic retry with exponential backoff
- File integrity validation
- Multiple format support (CSV, Parquet)

#### Stage 2: Data Transformation
```python
# ETL Process Flow
Raw Data → Column Mapping → Type Conversion → Validation → Filtering → Deduplication → Output
```

**Transformation Rules**:
- **Column Mapping**: 23 required headers from 297 source columns
- **Type Conversion**: Dates, numbers, booleans, text standardization
- **Boolean Logic**: "t"/"f" → True/False conversion
- **Validation**: Required field checks, range validation
- **Deduplication**: Based on PIID, Modification Number, Date Signed

#### Stage 3: Quality Assurance
```python
# Quality Process Flow
Processed Data → Completeness Check → Validation Rules → Issue Classification → Report Generation
```

**Quality Metrics**:
- **Completeness**: Null value analysis per column
- **Consistency**: Data type validation
- **Accuracy**: Business rule validation
- **Timeliness**: Processing timestamp tracking

### Configuration Management

#### Production Configuration Structure
```yaml
# production_config.yaml
data_download:          # Source data settings
  enabled: true
  start_date: "auto"    # Calculated by scheduler
  end_date: "auto"      # Calculated by scheduler
  
data_processing:        # ETL settings
  input_dir: "raw_data"
  output_dir: "processed_data"
  filters: {}           # Business filters
  
email:                  # Notification settings
  enabled: true
  smtp_server: "smtp.company.com"
  recipients: ["team@company.com"]
  
cleanup:                # Maintenance settings
  enabled: true
  archive_outputs: true
  retention_days: 90
```

#### ETL Configuration Structure
```yaml
# etl_config.yaml
column_mapping:         # Source → Target mapping
  award_id_piid: "PIID"
  recipient_name: "Legal Business Name"
  # ... 21 more mappings
  
data_types:            # Type specifications
  date_columns: ["Date Signed", ...]
  numeric_columns: ["Dollars Obligated", ...]
  boolean_columns: ["Is Vendor Business Type - 8A Program Participant", ...]
  
data_quality:          # Validation rules
  required_fields: ["PIID", "Fiscal Year", ...]
  validation_rules:
    fiscal_year_range: [1990, 2030]
```

### Error Handling Strategy

#### Error Classification
1. **CRITICAL**: Pipeline cannot continue (API down, config missing)
2. **ERROR**: Data processing failed (corrupt files, validation failures)
3. **WARNING**: Quality issues (missing optional data, format issues)
4. **INFO**: Normal operations (successful steps, statistics)

#### Recovery Procedures
1. **Automatic Retry**: Network issues, temporary API failures
2. **Graceful Degradation**: Process available data, report missing
3. **Manual Intervention**: Configuration errors, data corruption
4. **Notification Escalation**: Critical failures trigger immediate alerts

### Performance Optimization

#### Memory Management
- **Streaming Processing**: Large files processed in chunks
- **Data Type Optimization**: Efficient pandas dtypes
- **Garbage Collection**: Explicit cleanup of large objects
- **Memory Monitoring**: Track usage and warn on limits

#### Processing Efficiency
- **Vectorized Operations**: Pandas/NumPy optimizations
- **Parallel Processing**: Multi-core utilization where possible
- **Caching**: Reuse processed data when appropriate
- **Incremental Updates**: Process only new/changed data

---

## Maintenance & Support

### Routine Maintenance Schedule

#### Daily (Automated)
- ✅ Pipeline execution monitoring
- ✅ Email notification review
- ✅ Basic error checking

#### Weekly (5 minutes)
- 📊 Dashboard review
- 💾 Disk usage check
- 📈 Data quality trend analysis
- 🧹 Log file rotation

#### Monthly (30 minutes)
- 📋 Comprehensive system health check
- 📦 Archive management
- 🔄 Configuration review
- 📊 Performance metrics analysis

#### Quarterly (2 hours)
- 🔧 Dependency updates
- 🧪 Disaster recovery testing
- 📚 Documentation updates
- 🎯 Performance optimization review

### Support Procedures

#### Level 1: Operational Issues
**Symptoms**: Pipeline failures, data quality alerts, email notifications
**Response Time**: 4 hours during business hours
**Actions**:
1. Check email notifications for error details
2. Review dashboard for system status
3. Restart failed jobs if appropriate
4. Escalate if issues persist

#### Level 2: Technical Issues
**Symptoms**: Configuration problems, data corruption, system errors
**Response Time**: 24 hours
**Actions**:
1. Analyze log files and run results
2. Investigate root cause
3. Implement fixes and test
4. Update documentation if needed

#### Level 3: Architecture Changes
**Symptoms**: Performance issues, scalability needs, new requirements
**Response Time**: 1 week
**Actions**:
1. Requirements analysis
2. Design and development
3. Testing and validation
4. Deployment and monitoring

### Troubleshooting Guide

#### Common Issues and Solutions

**Issue**: Pipeline fails with "Configuration file not found"
**Solution**: 
```bash
# Check file path and permissions
ls -la production_config.yaml
# Verify working directory in Task Scheduler
```

**Issue**: Email notifications not working
**Solution**:
```python
# Test SMTP settings
python -c "
import smtplib
server = smtplib.SMTP('your-smtp-server.com', 587)
server.starttls()
server.login('username', 'password')
print('SMTP connection successful')
"
```

**Issue**: Data quality issues (high null percentages)
**Solution**:
1. Check USASpending API status
2. Verify column mappings in etl_config.yaml
3. Review data quality report for specific issues
4. Re-run processing with debug logging

**Issue**: Disk space warnings
**Solution**:
```bash
# Enable cleanup in configuration
cleanup:
  enabled: true
  remove_raw_downloads: true
  archive_outputs: true
```

### Contact Information

#### Technical Support
- **Primary**: ETL Development Team
- **Secondary**: IT Infrastructure Team
- **Emergency**: On-call support rotation

#### Escalation Path
1. **Level 1**: Operations Team (4 hours)
2. **Level 2**: Technical Team (24 hours)
3. **Level 3**: Architecture Team (1 week)
4. **Executive**: Business Stakeholders (critical issues)

### Documentation Updates

This documentation should be updated when:
- ✅ New features are added
- ✅ Configuration changes are made
- ✅ Procedures are modified
- ✅ Issues and solutions are identified
- ✅ Performance optimizations are implemented

**Last Updated**: October 28, 2025  
**Version**: 2.0  
**Next Review**: January 28, 2026

---

## Appendices

### Appendix A: Required Headers Mapping

| # | Required Header | Source Column | Data Type | Completeness |
|---|----------------|---------------|-----------|--------------|
| 1 | Fiscal Year | action_date_fiscal_year | Integer | 100% |
| 2 | PIID | award_id_piid | String | 100% |
| 3 | AAC | awarding_agency_code | String | 100% |
| 4 | Instrument Type | award_type | String | 100% |
| 5 | Referenced IDV PIID | parent_award_id_piid | String | 76.5% |
| 6 | Modification Number | modification_number | String | 100% |
| 7 | Date Signed | action_date | Date | 100% |
| 8 | Est. Ultimate Completion Date | period_of_performance_current_end_date | Date | 100% |
| 9 | Last Date to Order | ordering_period_end_date | Date | 0% |
| 10 | Dollars Obligated | federal_action_obligation | Decimal | 100% |
| 11 | Base and All Options Value | base_and_all_options_value | Decimal | 100% |
| 12 | Legal Business Name | recipient_name | String | 100% |
| 13 | Contracting Office Name | awarding_office_name | String | 100% |
| 14 | Funding Agency Name | funding_agency_name | String | 100% |
| 15 | Description of Requirement | transaction_description | String | 100% |
| 16 | Contracting Officers Business Size | contracting_officers_determination_of_business_size | String | 100% |
| 17 | Is Vendor Business Type - 8A Program | c8a_program_participant | Boolean | 100% |
| 18 | Is Vendor Business Type - EDWOSB | economically_disadvantaged_women_owned_small_business | Boolean | 100% |
| 19 | Is Vendor Business Type - HUBZone | historically_underutilized_business_zone_hubzone_firm | Boolean | 100% |
| 20 | Is Vendor Business Type - SDB | self_certified_small_disadvantaged_business | Boolean | 100% |
| 21 | Is Vendor Business Type - SDVOSB | service_disabled_veteran_owned_business | Boolean | 100% |
| 22 | Is Vendor Business Type - VOSB | veteran_owned_business | Boolean | 100% |
| 23 | Is Vendor Business Type - WOSB | women_owned_small_business | Boolean | 100% |

### Appendix B: Windows Task Scheduler Configuration

**Task Name**: USASpending ETL Daily  
**Program/Script**: `python`  
**Arguments**: `schedule_etl.py --config production_config.yaml --mode daily --email-report`  
**Start In**: `C:\path\to\etl\folder`  
**Trigger**: Daily at 2:00 AM  
**Run As**: Service account with appropriate permissions  

### Appendix C: Email Notification Template

```
Subject: USASpending ETL Report - [RUN_ID] - [STATUS]

USASpending ETL Pipeline Report
✅ Status: SUCCESS
🆔 Run ID: 20251028_212724
⏰ Start Time: 2025-10-28T21:27:24
⏰ End Time: 2025-10-28T21:42:18

📊 PIPELINE STEPS:
✅ data_download: SUCCESS
✅ data_processing: SUCCESS
✅ analysis: SUCCESS
✅ cleanup: SUCCESS
✅ email_report: SUCCESS

📁 OUTPUT FILES:
• usaspending_processed_20251028_212724.parquet
• analysis_report_20251028_212724.txt
• data_quality_report_20251028_212724.json

📈 DATA SUMMARY:
• Records Processed: 140,785
• Data Completeness: 94.6%
• Processing Time: 15.2 minutes
• Total Contract Value: $22.8B
```

---

**Document Control**  
**Created**: October 28, 2025  
**Author**: Justin Aguila (Contractor)
**Version**: 2.0  
**Classification**: Internal Use  
**Next Review**: January 28, 2026