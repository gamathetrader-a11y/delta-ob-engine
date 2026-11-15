@echo off
setlocal EnableDelayedExpansion
title Delta Exchange Indicator Loader

echo ============================================
echo  Delta Exchange OB + R7 Indicator Loader
echo ============================================
echo.

REM Move to script's folder (indicator)
cd /d "%~dp0"

REM Check if indicator_loader.py exists
if not exist "indicator_loader.py" (
    echo ERROR: indicator_loader.py not found in %CD%
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
"%PY%" -c "import numpy, pandas, requests" >nul 2>&1
if errorlevel 1 (
    echo Installing required packages: numpy, pandas, requests...
    "%PY%" -m pip install numpy pandas requests
    if errorlevel 1 (
        echo ERROR: Failed to install dependencies.
        pause
        exit /b 1
    )
)
echo Dependencies OK.
echo.

"%PY%" indicator_loader.py
if errorlevel 1 (
    echo.
    echo The indicator loader stopped with an error.
    pause
) else (
    echo.
    echo Indicator overlays and Telegram alerts processed successfully!
    pause
)
endlocal
