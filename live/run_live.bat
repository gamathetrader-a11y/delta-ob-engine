@echo off
setlocal EnableDelayedExpansion
title Delta Exchange Live Chart Launcher

echo ============================================
echo  Delta Exchange Live Chart Launcher
echo ============================================
echo.

REM Move to script's folder (live)
cd /d "%~dp0"

REM Check if app.py exists
if not exist "app.py" (
    echo ERROR: app.py not found in %CD%
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
"%PY%" -c "import flask, websockets, requests, numpy, pandas" >nul 2>&1
if errorlevel 1 (
    echo Installing required packages: flask, websockets, requests, numpy, pandas...
    "%PY%" -m pip install flask websockets requests numpy pandas
    if errorlevel 1 (
        echo ERROR: Failed to install dependencies.
        pause
        exit /b 1
    )
)
echo Dependencies OK.
echo.

REM Launch the standalone application
echo ============================================
echo  Starting Live Chart Server...
echo  Open http://127.0.0.1:5001 in your browser
echo ============================================
echo.

"%PY%" app.py

echo.
echo Server stopped.
pause
endlocal
