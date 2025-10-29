@echo off
REM Windows batch script for running daily ETL
REM This script calculates yesterday's date and runs the ETL

setlocal enabledelayedexpansion

REM Get yesterday's date
for /f "tokens=2 delims==" %%a in ('wmic OS Get localdatetime /value') do set "dt=%%a"
set "YY=%dt:~2,2%" & set "YYYY=%dt:~0,4%" & set "MM=%dt:~4,2%" & set "DD=%dt:~6,2%"

REM Calculate yesterday (simplified - doesn't handle month/year boundaries perfectly)
set /a DD=%DD%-1

REM Pad with zero if needed
if %DD% LSS 10 set DD=0%DD%

REM Handle day 0 (would need more complex logic for production)
if %DD%==00 (
    set DD=01
    echo Warning: Simple date calculation used. Consider using schedule_etl.py instead.
)

set YESTERDAY=%YYYY%-%MM%-%DD%

echo Running ETL for date: %YESTERDAY%

REM Run the scheduler helper (recommended)
python schedule_etl.py --config production_config.yaml --mode daily --email-report

REM Alternative: Run production ETL directly with updated config
REM python usaspending_production_etl.py --config production_config.yaml --email-report

echo ETL run completed.
pause