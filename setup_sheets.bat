@echo off
REM Quick setup script for Google Sheets export functionality (Windows)

echo =======================================================================
echo Google Sheets Export - Quick Setup
echo =======================================================================
echo.

REM Check if Python is installed
python --version >nul 2>&1
if errorlevel 1 (
    echo Error: Python is not installed
    echo    Please install Python 3.8 or higher
    exit /b 1
)

echo Python found
python --version
echo.

REM Install dependencies
echo Installing dependencies...
pip install -r requirements.txt

if errorlevel 1 (
    echo Error: Failed to install dependencies
    exit /b 1
)

echo.
echo Dependencies installed
echo.

REM Run verification
echo Running verification...
python verify_setup.py

echo.
echo =======================================================================
echo Next Steps:
echo =======================================================================
echo.
echo 1. Set up Google Cloud credentials:
echo    Follow the guide: docs/GOOGLE_SHEETS_SETUP.md
echo.
echo 2. Once credentials are set up, run:
echo    python export_to_sheets.py
echo.
echo =======================================================================
pause
