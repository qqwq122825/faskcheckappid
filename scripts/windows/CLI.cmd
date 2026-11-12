@echo off
chcp 65001 >nul
set PYTHONUTF8=1
cd /d "%~dp0..\.."
if not exist .venv\Scripts\python.exe py -3.11 -m venv .venv
if errorlevel 1 goto failed
.venv\Scripts\python.exe -c "import portalocker,requests,websockets,win32file,zoneinfo; zoneinfo.ZoneInfo('Asia/Shanghai')" >nul 2>&1
if errorlevel 1 .venv\Scripts\python.exe -m pip install -r requirements.txt
if errorlevel 1 goto failed
.venv\Scripts\python.exe cli.py %*
set task_status=%errorlevel%
pause
exit /b %task_status%
:failed
pause
exit /b 1
