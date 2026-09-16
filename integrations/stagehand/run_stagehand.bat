@echo off
setlocal

set "NODE24=%NVM_HOME%\v24.15.0\node.exe"
set "TSX_CLI=%~dp0node_modules\tsx\dist\cli.mjs"

if not exist "%NODE24%" (
    echo [ERROR] Node 24 not found: %NODE24%
    exit /b 1
)

if not exist "%TSX_CLI%" (
    echo [ERROR] tsx CLI not found: %TSX_CLI%
    exit /b 1
)

cd /d "%~dp0"

"%NODE24%" "%TSX_CLI%" %*

set "EXIT_CODE=%ERRORLEVEL%"
endlocal & exit /b %EXIT_CODE%
