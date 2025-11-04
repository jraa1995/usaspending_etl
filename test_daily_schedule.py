#!/usr/bin/env python3
"""
Test script to verify T-1 (yesterday) date calculation
"""

from datetime import datetime, timedelta
from schedule_etl_with_drive import EnhancedETLScheduler
from pathlib import Path

def test_date_calculation():
    print("🧪 Testing T-1 Date Calculation")
    print("=" * 40)
    
    # Mock today's date for testing
    today = datetime.now().date()
    yesterday = today - timedelta(days=1)
    
    print(f"Today: {today}")
    print(f"Expected T-1 (yesterday): {yesterday}")
    
    # Test the scheduler
    try:
        scheduler = EnhancedETLScheduler("production_config.yaml")
        start_date, end_date = scheduler.calculate_date_range("daily")
        
        print(f"Scheduler calculated: {start_date} to {end_date}")
        
        if start_date == yesterday.strftime("%Y-%m-%d") and end_date == yesterday.strftime("%Y-%m-%d"):
            print("✅ Date calculation is correct!")
        else:
            print("❌ Date calculation is incorrect!")
            
    except Exception as e:
        print(f"❌ Error: {e}")
    
    # Test different modes
    print(f"\n📅 Testing different modes:")
    try:
        scheduler = EnhancedETLScheduler("production_config.yaml")
        
        modes = ['daily', 'weekly', 'monthly']
        for mode in modes:
            start, end = scheduler.calculate_date_range(mode)
            print(f"  {mode}: {start} to {end}")
            
    except Exception as e:
        print(f"❌ Error testing modes: {e}")

if __name__ == "__main__":
    test_date_calculation()