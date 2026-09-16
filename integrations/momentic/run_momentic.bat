@echo off
setlocal

set "NODE24=%NVM_HOME%\v24.15.0\node.exe"
set "MOMENTIC_CLI=%~dp0node_modules\momentic\bin\cli.js"

if not exist "%NODE24%" (
    echo [ERROR] Node 24 not found: %NODE24%
    exit /b 1
)

if not exist "%MOMENTIC_CLI%" (
    echo [ERROR] Momentic CLI not found: %MOMENTIC_CLI%
    exit /b 1
)

"%NODE24%" "%MOMENTIC_CLI%" %*

set "EXIT_CODE=%ERRORLEVEL%"
endlocal & exit /b %EXIT_CODE%
