@echo off
setlocal enableextensions enabledelayedexpansion

REM --- Go to the folder where this .bat lives (so paths work even from Task Scheduler)
pushd "%~dp0"

REM --- Set mode for the script
set RUN_MODE=WEEKLY

REM --- Choose Python (prefer venv, else fallback to system Python)
set PYTHON=".venv\Scripts\python.exe"
if not exist %PYTHON% set PYTHON=python

REM --- Make sure logs directory exists (script also does this, but belt & braces)
if not exist "logs" mkdir "logs"

REM --- Build a yyyymmdd stamp (locale-safe enough for most AU Windows setups)
for /f "tokens=1-3 delims=/- " %%a in ("%date%") do (
  set y=%%c
  set m=00%%a & set m=!m:~-2!
  set d=00%%b & set d=!d:~-2!
)
set STAMP=!y!!m!!d!

REM --- Run the job; append stdout/stderr to a dated task log
%PYTHON% run.py >> "logs\%STAMP%_task.log" 2>&1

REM --- Preserve exit code for Task Scheduler
set ERR=%ERRORLEVEL%

popd
exit /b %ERR%
