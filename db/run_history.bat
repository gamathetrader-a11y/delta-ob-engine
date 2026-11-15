@echo off
setlocal EnableDelayedExpansion
title Delta Exchange History Loader

echo ============================================
echo  Delta Exchange Manual History Loader
echo ============================================
echo.

REM Move to script's folder (db)
cd /d "%~dp0"

REM Check if manual_fetch.py exists
if not exist "manual_fetch.py" (
    echo ERROR: manual_fetch.py not found in %CD%
    pause
    exit /b 1
)

REM Find Python executable (checks current and parent R2 folder for .venv)
if exist ".venv\Scripts\python.exe" (
    set "PY=.venv\Scripts\python.exe"
    echo Using local virtual environment Python.
) else if exist "..\.venv\Scripts\python.exe" (
    set "PY=..\.venv\Scripts\python.exe"
    echo Using root virtual environment Python.
) else (
    set "PY=python"
    echo Using system Python.
)
echo.

REM Install missing requirements automatically
echo Checking dependencies...
"%PY%" -c "import requests, urllib3, numpy, pandas" >nul 2>&1
if errorlevel 1 (
    echo Installing required packages: requests, urllib3, numpy, pandas...
    "%PY%" -m pip install requests urllib3 numpy pandas
    if errorlevel 1 (
        echo ERROR: Failed to install dependencies.
        pause
        exit /b 1
    )
)
echo Dependencies OK.
echo.

REM Launch the manual history fetcher
echo ============================================
echo  Starting Manual History Fetcher...
echo ============================================
echo.

"%PY%" manual_fetch.py

echo.
echo History fetch process finished.
pause
endlocal
