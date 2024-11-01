@echo off
setlocal enabledelayedexpansion
set "SCRIPT_NAME=run_update.py"
set "SCRIPT_PATH=main_scripts\run_update.py"
set "FLAG_FILE=script_running.flag"
set "MAX_AGE_SECONDS=20"  :: Increased to 20 seconds to allow for ZIP processing
set "CHECK_INTERVAL=3"

:start
if exist "%FLAG_FILE%" del "%FLAG_FILE%"
start /b python "%SCRIPT_PATH%"

:wait_loop
timeout /t %CHECK_INTERVAL% /nobreak > nul

tasklist /FI "IMAGENAME eq python.exe" 2>NUL | find /I /N "python.exe">NUL
if "%ERRORLEVEL%"=="1" (
    echo Python process not found. Restarting script.
    goto :restart
)

if not exist "%FLAG_FILE%" (
    echo Flag file not found. Restarting script.
    goto :restart
)

for /f %%A in ('powershell -command "$fileAge = (Get-Date) - (Get-Item '%FLAG_FILE%').LastWriteTime; [math]::Round($fileAge.TotalSeconds * 100)"') do set "age_int=%%A"
set /a age_seconds=%age_int% / 100
set /a age_decimal=%age_int% %% 100
echo Flag file age: %age_seconds%.%age_decimal% seconds

set /a max_age_int=%MAX_AGE_SECONDS% * 100
if %age_int% gtr %max_age_int% (
    echo Flag file is too old ^(%age_seconds%.%age_decimal% seconds^). MAX_AGE_SECONDS is %MAX_AGE_SECONDS%. Restarting script.
    goto :restart
)

goto :wait_loop

:restart
taskkill /F /IM python.exe /T
timeout /t 5 /nobreak > nul
goto :start
