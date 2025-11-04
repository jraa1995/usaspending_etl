# Google Drive Integration Setup Guide

This guide shows how to set up automatic Google Drive uploads for your USASpending ETL pipeline.

## 🔧 Prerequisites

1. **Google Cloud Project** with Drive and Sheets APIs enabled
2. **Service Account** with appropriate permissions
3. **Google Drive folder** (optional) for organized uploads

## 📋 Step-by-Step Setup

### Step 1: Create Google Cloud Project

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create a new project or select existing one
3. Enable the following APIs:
   - Google Drive API
   - Google Sheets API

### Step 2: Create Service Account

1. Go to **IAM & Admin** → **Service Accounts**
2. Click **Create Service Account**
3. Fill in details:
   - **Name**: `usaspending-etl-service`
   - **Description**: `Service account for USASpending ETL pipeline`
4. Click **Create and Continue**
5. Skip role assignment (we'll handle permissions in Drive)
6. Click **Done**

### Step 3: Generate Service Account Key

1. Click on your newly created service account
2. Go to **Keys** tab
3. Click **Add Key** → **Create New Key**
4. Select **JSON** format
5. Download the key file
6. **Important**: Keep this file secure and never commit to version control

### Step 4: Set Up Google Drive Permissions

1. Open Google Drive
2. Create a folder for ETL uploads (optional but recommended)
3. Right-click the folder → **Share**
4. Add the service account email (from the JSON key file)
5. Give **Editor** permissions
6. Copy the folder ID from the URL (the long string after `/folders/`)

### Step 5: Configure ETL Pipeline

1. **Place the service account key file** in your ETL directory:
   ```
   usaspending_etl/
   ├── service-account-key.json  # Your downloaded key file
   ├── production_config.yaml
   └── ...
   ```

2. **Update production_config.yaml**:
   ```yaml
   google_drive:
     enabled: true
     service_account_file: "service-account-key.json"
     folder_id: "1ABC123DEF456GHI789JKL"  # Your folder ID
     upload_formats: ["parquet", "csv"]
     create_google_sheets: true
   ```

3. **Install Google libraries**:
   ```bash
   pip install gspread google-auth google-api-python-client
   ```

## 🚀 Usage

### Daily Run with Google Drive Upload
```bash
# Run for yesterday's data and upload to Google Drive
python schedule_etl_with_drive.py --config production_config.yaml --mode daily --upload-to-drive
```

### Custom Date with Google Drive
```bash
# Process specific date and upload
python schedule_etl_with_drive.py --start-date 2024-11-02 --end-date 2024-11-02 --upload-to-drive
```

### Test Google Drive Connection
```bash
# Test upload with a sample file
python google_drive_uploader.py --service-account service-account-key.json --test-file sample.csv
```

## 📁 What Gets Uploaded

For each ETL run, the following files are uploaded to Google Drive:

1. **Processed Data File**: `usaspending_data_YYYY-MM-DD.parquet`
2. **Quality Report**: `quality_report_YYYY-MM-DD.json`
3. **Google Sheet** (if CSV format): Interactive spreadsheet with the data

### Folder Structure
```
Google Drive/
└── USASpending_ETL_2024-11/
    ├── usaspending_data_2024-11-01.parquet
    ├── quality_report_2024-11-01.json
    ├── usaspending_data_2024-11-02.parquet
    ├── quality_report_2024-11-02.json
    └── ...
```

## 🔒 Security Best Practices

### Service Account Key Security
- **Never commit** the service account key to version control
- **Restrict file permissions**: `chmod 600 service-account-key.json`
- **Use environment variables** in production:
  ```bash
  export GOOGLE_SERVICE_ACCOUNT_FILE="/secure/path/to/key.json"
  ```

### Google Drive Permissions
- **Use dedicated folder** for ETL uploads
- **Limit service account access** to only necessary folders
- **Regular audit** of shared permissions

## 🪟 Windows Task Scheduler Integration

### Task Configuration
**Program/Script**: `python`  
**Arguments**: `schedule_etl_with_drive.py --config production_config.yaml --mode daily --upload-to-drive`  
**Start In**: `C:\path\to\etl\folder`  
**Schedule**: Daily at 2:00 AM  

### Example Batch Script
```batch
@echo off
cd /d "C:\path\to\etl\folder"
python schedule_etl_with_drive.py --config production_config.yaml --mode daily --upload-to-drive
if %errorlevel% neq 0 (
    echo ETL failed with error code %errorlevel%
    exit /b %errorlevel%
)
echo ETL completed successfully
```

## 🔍 Monitoring and Troubleshooting

### Check Upload Status
```bash
# View recent uploads in Google Drive
# Check the ETL logs for upload confirmation
```

### Common Issues

**"Service account key not found"**
- Verify the file path in `production_config.yaml`
- Check file permissions

**"Permission denied"**
- Ensure service account has access to the Google Drive folder
- Verify the folder ID is correct

**"API not enabled"**
- Enable Google Drive API and Google Sheets API in Google Cloud Console

### Debug Mode
```bash
# Run with detailed logging
python schedule_etl_with_drive.py --mode daily --upload-to-drive --dry-run
```

## 📊 Expected Results

After successful setup, each daily run will:

1. ✅ **Download** yesterday's USASpending data
2. ✅ **Process** and standardize to 23 required headers
3. ✅ **Upload** processed data to Google Drive
4. ✅ **Create** Google Sheet for easy viewing
5. ✅ **Organize** files by month in Drive folders

### Sample Success Output
```
🗓️  Running ETL for date range: 2024-11-02 to 2024-11-02
📥 Downloading data...
✅ Data download completed
🔄 Processing data...
✅ Data processing completed
📊 Processed file: usaspending_processed_20241103_120000.parquet
☁️  Uploading to Google Drive...
✅ Google Drive upload completed
📁 Data file: https://drive.google.com/file/d/1ABC123.../view
📊 Google Sheet: https://docs.google.com/spreadsheets/d/1XYZ789.../edit
🎉 ETL pipeline completed successfully!
```

This setup ensures your USASpending data is automatically processed daily and uploaded to Google Drive for easy access and sharing!