#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Google Drive Uploader for USASpending ETL Pipeline

This module handles uploading processed data files to Google Drive
using a service account and gspread.

Requirements:
- Google service account JSON key file
- gspread and google-auth libraries
- Google Drive API enabled
"""

import os
import json
from pathlib import Path
from datetime import datetime
from typing import Optional, List, Dict
import logging

try:
    import gspread
    from google.oauth2.service_account import Credentials
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaFileUpload
    GOOGLE_LIBS_AVAILABLE = True
except ImportError:
    GOOGLE_LIBS_AVAILABLE = False

logger = logging.getLogger(__name__)

class GoogleDriveUploader:
    """Upload files to Google Drive using service account."""
    
    def __init__(self, service_account_file: str, folder_id: Optional[str] = None):
        """
        Initialize Google Drive uploader.
        
        Args:
            service_account_file: Path to service account JSON key file
            folder_id: Google Drive folder ID to upload to (optional)
        """
        if not GOOGLE_LIBS_AVAILABLE:
            raise ImportError(
                "Google libraries not installed. Run: pip install gspread google-auth google-api-python-client"
            )
        
        self.service_account_file = Path(service_account_file)
        self.folder_id = folder_id
        self.drive_service = None
        self.sheets_client = None
        
        if not self.service_account_file.exists():
            raise FileNotFoundError(f"Service account file not found: {service_account_file}")
        
        self._authenticate()
    
    def _authenticate(self):
        """Authenticate with Google services."""
        try:
            # Define the scopes
            scopes = [
                'https://www.googleapis.com/auth/drive',
                'https://www.googleapis.com/auth/spreadsheets'
            ]
            
            # Load credentials
            credentials = Credentials.from_service_account_file(
                self.service_account_file, 
                scopes=scopes
            )
            
            # Initialize services
            self.drive_service = build('drive', 'v3', credentials=credentials)
            self.sheets_client = gspread.authorize(credentials)
            
            logger.info("Successfully authenticated with Google services")
            
        except Exception as e:
            logger.error(f"Failed to authenticate with Google services: {e}")
            raise
    
    def create_folder(self, folder_name: str, parent_folder_id: Optional[str] = None) -> str:
        """
        Create a folder in Google Drive.
        
        Args:
            folder_name: Name of the folder to create
            parent_folder_id: Parent folder ID (optional)
            
        Returns:
            Created folder ID
        """
        try:
            folder_metadata = {
                'name': folder_name,
                'mimeType': 'application/vnd.google-apps.folder'
            }
            
            if parent_folder_id:
                folder_metadata['parents'] = [parent_folder_id]
            
            folder = self.drive_service.files().create(
                body=folder_metadata,
                fields='id'
            ).execute()
            
            folder_id = folder.get('id')
            logger.info(f"Created folder '{folder_name}' with ID: {folder_id}")
            return folder_id
            
        except Exception as e:
            logger.error(f"Failed to create folder '{folder_name}': {e}")
            raise
    
    def upload_file(self, file_path: Path, folder_id: Optional[str] = None, 
                   custom_name: Optional[str] = None) -> Dict:
        """
        Upload a file to Google Drive.
        
        Args:
            file_path: Path to the file to upload
            folder_id: Google Drive folder ID to upload to
            custom_name: Custom name for the uploaded file
            
        Returns:
            Dictionary with file information
        """
        try:
            if not file_path.exists():
                raise FileNotFoundError(f"File not found: {file_path}")
            
            # Determine file name
            file_name = custom_name or file_path.name
            
            # File metadata
            file_metadata = {'name': file_name}
            
            # Set parent folder if specified
            if folder_id or self.folder_id:
                parent_id = folder_id or self.folder_id
                file_metadata['parents'] = [parent_id]
            
            # Determine MIME type based on file extension
            mime_types = {
                '.csv': 'text/csv',
                '.parquet': 'application/octet-stream',
                '.json': 'application/json',
                '.txt': 'text/plain',
                '.xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            }
            
            mime_type = mime_types.get(file_path.suffix.lower(), 'application/octet-stream')
            
            # Create media upload
            media = MediaFileUpload(str(file_path), mimetype=mime_type, resumable=True)
            
            # Upload file
            file = self.drive_service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id,name,size,createdTime,webViewLink'
            ).execute()
            
            result = {
                'id': file.get('id'),
                'name': file.get('name'),
                'size': int(file.get('size', 0)),
                'created_time': file.get('createdTime'),
                'web_view_link': file.get('webViewLink'),
                'local_path': str(file_path)
            }
            
            logger.info(f"Uploaded file '{file_name}' ({result['size']} bytes) to Google Drive")
            return result
            
        except Exception as e:
            logger.error(f"Failed to upload file '{file_path}': {e}")
            raise
    
    def upload_to_sheets(self, file_path: Path, sheet_name: str, 
                        worksheet_name: str = "Sheet1") -> Dict:
        """
        Upload CSV data to Google Sheets.
        
        Args:
            file_path: Path to CSV file
            sheet_name: Name of the Google Sheet
            worksheet_name: Name of the worksheet within the sheet
            
        Returns:
            Dictionary with sheet information
        """
        try:
            if not file_path.exists():
                raise FileNotFoundError(f"File not found: {file_path}")
            
            if file_path.suffix.lower() != '.csv':
                raise ValueError("Only CSV files can be uploaded to Google Sheets")
            
            # Read CSV data
            import pandas as pd
            df = pd.read_csv(file_path)
            
            # Create or open spreadsheet
            try:
                spreadsheet = self.sheets_client.open(sheet_name)
                logger.info(f"Opened existing spreadsheet: {sheet_name}")
            except gspread.SpreadsheetNotFound:
                spreadsheet = self.sheets_client.create(sheet_name)
                logger.info(f"Created new spreadsheet: {sheet_name}")
            
            # Get or create worksheet
            try:
                worksheet = spreadsheet.worksheet(worksheet_name)
                worksheet.clear()  # Clear existing data
            except gspread.WorksheetNotFound:
                worksheet = spreadsheet.add_worksheet(
                    title=worksheet_name, 
                    rows=len(df) + 1, 
                    cols=len(df.columns)
                )
            
            # Upload data
            # Convert DataFrame to list of lists (including headers)
            data = [df.columns.tolist()] + df.values.tolist()
            worksheet.update('A1', data)
            
            result = {
                'spreadsheet_id': spreadsheet.id,
                'spreadsheet_url': spreadsheet.url,
                'worksheet_name': worksheet_name,
                'rows': len(df),
                'columns': len(df.columns)
            }
            
            logger.info(f"Uploaded {len(df)} rows to Google Sheets: {sheet_name}")
            return result
            
        except Exception as e:
            logger.error(f"Failed to upload to Google Sheets: {e}")
            raise
    
    def upload_etl_results(self, processed_file: Path, quality_report_file: Path,
                          run_date: str) -> Dict:
        """
        Upload ETL results to Google Drive with organized folder structure.
        
        Args:
            processed_file: Path to processed data file
            quality_report_file: Path to quality report JSON
            run_date: Date string (YYYY-MM-DD) for folder organization
            
        Returns:
            Dictionary with upload results
        """
        try:
            # Create date-based folder structure
            year_month = run_date[:7]  # YYYY-MM
            folder_name = f"USASpending_ETL_{year_month}"
            
            # Create or find folder
            try:
                # Try to find existing folder
                query = f"name='{folder_name}' and mimeType='application/vnd.google-apps.folder'"
                if self.folder_id:
                    query += f" and '{self.folder_id}' in parents"
                
                results = self.drive_service.files().list(q=query).execute()
                folders = results.get('files', [])
                
                if folders:
                    upload_folder_id = folders[0]['id']
                    logger.info(f"Using existing folder: {folder_name}")
                else:
                    upload_folder_id = self.create_folder(folder_name, self.folder_id)
                    
            except Exception as e:
                logger.warning(f"Could not create/find folder, uploading to root: {e}")
                upload_folder_id = self.folder_id
            
            results = {}
            
            # Upload processed data file
            if processed_file.exists():
                data_name = f"usaspending_data_{run_date}{processed_file.suffix}"
                results['data_file'] = self.upload_file(
                    processed_file, 
                    upload_folder_id, 
                    data_name
                )
            
            # Upload quality report
            if quality_report_file.exists():
                report_name = f"quality_report_{run_date}.json"
                results['quality_report'] = self.upload_file(
                    quality_report_file, 
                    upload_folder_id, 
                    report_name
                )
            
            # Upload to Google Sheets if CSV available
            if processed_file.suffix.lower() == '.csv':
                sheet_name = f"USASpending_Data_{run_date}"
                try:
                    results['google_sheet'] = self.upload_to_sheets(
                        processed_file, 
                        sheet_name
                    )
                except Exception as e:
                    logger.warning(f"Could not upload to Google Sheets: {e}")
            
            logger.info(f"Successfully uploaded ETL results for {run_date}")
            return results
            
        except Exception as e:
            logger.error(f"Failed to upload ETL results: {e}")
            raise

def main():
    """Test the Google Drive uploader."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Test Google Drive uploader")
    parser.add_argument("--service-account", required=True, help="Service account JSON file")
    parser.add_argument("--test-file", required=True, help="Test file to upload")
    parser.add_argument("--folder-id", help="Google Drive folder ID")
    
    args = parser.parse_args()
    
    try:
        uploader = GoogleDriveUploader(args.service_account, args.folder_id)
        
        test_file = Path(args.test_file)
        result = uploader.upload_file(test_file)
        
        print("Upload successful!")
        print(f"File ID: {result['id']}")
        print(f"View Link: {result['web_view_link']}")
        
    except Exception as e:
        print(f"Upload failed: {e}")

if __name__ == "__main__":
    main()