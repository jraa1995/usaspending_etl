# USASpending ETL Pipeline

This ETL (Extract, Transform, Load) pipeline processes USASpending data files and extracts the specific columns required for contract analysis.

## Features

- **Column Mapping**: Automatically maps USASpending API columns to standardized business headers
- **Data Filtering**: Filter by fiscal year, dollar amounts, agencies, and contract types
- **Data Validation**: Built-in data quality checks and validation rules
- **Multiple Formats**: Supports both CSV and Parquet input/output formats
- **Quality Reporting**: Generates detailed data quality reports
- **Configurable**: YAML-based configuration for easy customization

## Required Output Columns

The ETL extracts and standardizes these columns:

- Fiscal Year
- PIID (Contract ID)
- AAC (Agency Activity Code)
- Instrument Type
- Referenced IDV PIID
- Modification Number
- Date Signed
- Est. Ultimate Completion Date
- Last Date to Order
- Dollars Obligated
- Base and All Options Value (Total Contract Value)
- Legal Business Name
- Contracting Office Name
- Funding Agency Name
- Description of Requirement
- Contracting Officers Business Size Determination
- Is Vendor Business Type - 8A Program Participant
- Is Vendor Business Type - Economically Disadvantaged Women-Owned Small Business
- Is Vendor Business Type - HUBZone Firm
- Is Vendor Business Type - Self-Certified Small Disadvantaged Business
- Is Vendor Business Type - Service-Disabled Veteran-Owned Business
- Is Vendor Business Type - Veteran-Owned Business
- Is Vendor Business Type - Women-Owned Small Business

## Files

1. **`usaspending_etl.py`** - Basic ETL pipeline
2. **`usaspending_etl_enhanced.py`** - Enhanced ETL with configuration and quality reporting
3. **`etl_config.yaml`** - Configuration file for column mappings and settings
4. **`run_etl_examples.py`** - Example usage scripts

## Installation

```bash
pip install pandas pyarrow pyyaml
```

## Basic Usage

### Simple ETL Processing

```bash
# Process all data files in a directory
python usaspending_etl.py --input-dir usaspending_data --output-dir processed_data

# Specify output format
python usaspending_etl.py --input-dir usaspending_data --output-dir processed_data --output-format csv
```

### Enhanced ETL with Configuration

```bash
# Use default configuration
python usaspending_etl_enhanced.py --input-dir usaspending_data --output-dir processed_data

# Use custom configuration file
python usaspending_etl_enhanced.py --input-dir usaspending_data --output-dir processed_data --config my_config.yaml
```

## Filtering Options

### Filter by Fiscal Year
```bash
python usaspending_etl_enhanced.py \
  --input-dir usaspending_data \
  --output-dir processed_data \
  --fiscal-year-start 2024 \
  --fiscal-year-end 2025
```

### Filter by Dollar Amount
```bash
python usaspending_etl_enhanced.py \
  --input-dir usaspending_data \
  --output-dir processed_data \
  --min-dollars 100000
```

### Filter by Agencies
```bash
python usaspending_etl_enhanced.py \
  --input-dir usaspending_data \
  --output-dir processed_data \
  --agencies "Department of Defense" "Department of Agriculture"
```

### Filter by Contract Types
```bash
python usaspending_etl_enhanced.py \
  --input-dir usaspending_data \
  --output-dir processed_data \
  --instrument-types "DEFINITIVE CONTRACT" "DELIVERY ORDER"
```

### Combined Filters
```bash
python usaspending_etl_enhanced.py \
  --input-dir usaspending_data \
  --output-dir processed_data \
  --fiscal-year-start 2025 \
  --fiscal-year-end 2025 \
  --min-dollars 500000 \
  --agencies "Department of Defense"
```

## Configuration

The `etl_config.yaml` file allows you to customize:

- **Column Mappings**: Map USASpending columns to your required headers
- **Data Types**: Specify how to handle dates, numbers, booleans, and text
- **Default Filters**: Set default filtering criteria
- **Data Quality Rules**: Define validation rules and required fields
- **Output Settings**: Configure output format and reporting options

### Example Configuration

```yaml
# Column mapping from USASpending API to required headers
column_mapping:
  action_date_fiscal_year: "Fiscal Year"
  award_id_piid: "PIID"
  awarding_agency_code: "AAC"
  # ... more mappings

# Default filters
default_filters:
  fiscal_year_range: [2020, 2025]
  min_dollars_obligated: 1000

# Data quality rules
data_quality:
  required_fields:
    - "PIID"
    - "Fiscal Year"
    - "Legal Business Name"
```

## Output

The ETL generates:

1. **Processed Data File**: Parquet or CSV file with cleaned and filtered data
2. **Data Quality Report**: JSON file with quality metrics and issues
3. **Console Summary**: Statistics about processing results

### Data Quality Report

The quality report includes:
- Processing timestamp
- Data quality issues and warnings
- Summary statistics (row count, memory usage, null counts)
- Column profiles (data types, unique values, statistics)

## Examples

Run the example script to see various filtering scenarios:

```bash
python run_etl_examples.py
```

This will create several example outputs in the `examples/` directory:
- Basic processing (all data)
- Fiscal year filtering
- Dollar amount filtering
- Agency filtering
- Contract type filtering
- Combined filters

## Integration with Existing Pipeline

The ETL works with data generated by the existing `usaspending_pipeline.py`:

1. Use `usaspending_pipeline.py` to download raw data
2. Use the ETL to process and filter the downloaded data
3. Analyze the processed data for your specific requirements

```bash
# Step 1: Download data
python usaspending_pipeline.py bulk-backfill --start-date 2025-09-01 --end-date 2025-09-30 --groups contracts

# Step 2: Process data
python usaspending_etl_enhanced.py --input-dir usaspending_data --output-dir processed_data

# Step 3: Analyze processed data
python your_analysis_script.py --input processed_data/usaspending_processed_*.parquet
```

## Troubleshooting

### Common Issues

1. **Missing Columns**: Some USASpending datasets may not include all expected columns. The ETL handles this gracefully and reports missing columns.

2. **Data Type Conversion**: Invalid dates or numbers are converted to null values and reported in the quality report.

3. **Memory Usage**: Large datasets may require significant memory. Consider processing in smaller chunks or using a machine with more RAM.

### Performance Tips

- Use Parquet format for better performance and smaller file sizes
- Filter data early to reduce processing time
- Process files individually if memory is limited

## Support

For issues or questions:
1. Check the data quality report for processing issues
2. Review the console output for error messages
3. Verify your configuration file syntax
4. Ensure input data files are in the expected format (CSV or Parquet)