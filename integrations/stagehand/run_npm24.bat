@echo off
setlocal

set "NODE24=%NVM_HOME%\v24.15.0\node.exe"
set "NPM24=%NVM_HOME%\v24.15.0\node_modules\npm\bin\npm-cli.js"

if not exist "%NODE24%" (
    echo [ERROR] Node 24 not found: %NODE24%
    exit /b 1
)

if not exist "%NPM24%" (
    echo [ERROR] npm for Node 24 not found: %NPM24%
    exit /b 1
)

cd /d "%~dp0"

"%NODE24%" "%NPM24%" %*

set "EXIT_CODE=%ERRORLEVEL%"
endlocal & exit /b %EXIT_CODE%
