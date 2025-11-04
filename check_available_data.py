#!/usr/bin/env python3
"""
Check what data is available from USASpending API
"""

import httpx
from datetime import datetime, timedelta
import json

def check_data_availability():
    print("🔍 Checking USASpending Data Availability")
    print("=" * 50)
    
    # Test different months to see what's available
    test_dates = [
        ("2024-09-01", "2024-09-30", "September 2024"),
        ("2024-08-01", "2024-08-31", "August 2024"), 
        ("2024-07-01", "2024-07-31", "July 2024"),
        ("2024-10-01", "2024-10-15", "October 2024 (partial)"),
        ("2025-09-01", "2025-09-30", "September 2025 (future)")
    ]
    
    client = httpx.Client(timeout=30.0)
    
    for start_date, end_date, description in test_dates:
        print(f"\n📅 Testing {description} ({start_date} to {end_date})")
        
        try:
            # Test with a small bulk download request
            payload = {
                "filters": {
                    "prime_award_types": ["A", "B", "C", "D"],  # Contracts
                    "date_type": "action_date",
                    "date_range": {
                        "start_date": start_date,
                        "end_date": end_date
                    }
                },
                "file_format": "csv",
                "columns": []
            }
            
            response = client.post(
                "https://api.usaspending.gov/api/v2/bulk_download/awards/",
                json=payload,
                headers={
                    "User-Agent": "justinaguilab2@gmail.com",
                    "Content-Type": "application/json"
                }
            )
            
            if response.status_code == 200:
                result = response.json()
                if "file_name" in result:
                    print(f"   ✅ Data available - Download initiated")
                    print(f"   📁 File: {result.get('file_name', 'Unknown')}")
                else:
                    print(f"   ⚠️  Unexpected response: {result}")
            else:
                print(f"   ❌ Request failed: HTTP {response.status_code}")
                if response.status_code == 400:
                    try:
                        error_detail = response.json()
                        print(f"   📋 Error: {error_detail}")
                    except:
                        print(f"   📋 Error: {response.text[:200]}")
                        
        except Exception as e:
            print(f"   ❌ Error: {str(e)}")
    
    client.close()
    
    print(f"\n💡 Recommendation:")
    print(f"   Use September 2024 or August 2024 for reliable, complete data")
    print(f"   Future dates (2025) will not have data available")

if __name__ == "__main__":
    check_data_availability()