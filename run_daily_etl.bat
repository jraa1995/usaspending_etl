@echo off
REM USASpending ETL - Daily Run with Google Drive Upload
REM This batch file runs the ETL for yesterday's data (T-1) and uploads to Google Drive

echo ========================================
echo USASpending ETL - Daily Run
echo ========================================
echo Start Time: %date% %time%
echo.

REM Change to ETL directory (adjust path as needed)
cd /d "%~dp0"

REM Check if Python is available
python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo ERROR: Python is not installed or not in PATH
    echo Please install Python and try again
    pause
    exit /b 1
)

REM Check if config file exists
if not exist "production_config.yaml" (
    echo ERROR: production_config.yaml not found
    echo Please ensure you're in the correct directory
    pause
    exit /b 1
)

REM Run the ETL pipeline with Google Drive upload
echo Running ETL for yesterday's data with Google Drive upload...
echo.
python schedule_etl_with_drive.py --config production_config.yaml --mode daily --upload-to-drive

REM Check if the command was successful
if %errorlevel% equ 0 (
    echo.
    echo ========================================
    echo ETL COMPLETED SUCCESSFULLY
    echo ========================================
    echo End Time: %date% %time%
) else (
    echo.
    echo ========================================
    echo ETL FAILED - Error Code: %errorlevel%
    echo ========================================
    echo End Time: %date% %time%
    echo.
    echo Check the logs for more details
)

REM Pause only if running interactively (double-clicked)
echo %cmdcmdline% | find /i "%~0" >nul
if not errorlevel 1 pause

exit /b %errorlevel%