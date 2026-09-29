@echo off
setlocal

set "HARNESS_ROOT=D:\kkm_test"
set "PROJECT_ROOT=D:\inha_dev"
set "PROFILE_NAME=e4u"
set "PROFILE_DIR=D:\agents\profiles\%PROFILE_NAME%"
set "FACTORY_AGENTS=%USERPROFILE%\.factory\AGENTS.md"
set "SHARED_SKILLS=%USERPROFILE%\.agents\skills"

if not exist "%HARNESS_ROOT%\" (
    echo [ERROR] Harness root not found: %HARNESS_ROOT%
    exit /b 2
)

if not exist "%PROJECT_ROOT%\" (
    echo [ERROR] Project root not found: %PROJECT_ROOT%
    exit /b 3
)

if not exist "%PROFILE_DIR%\" (
    echo [ERROR] Project profile directory not found: %PROFILE_DIR%
    exit /b 4
)

if not exist "%PROFILE_DIR%\project.json" (
    echo [ERROR] Project profile not found: %PROFILE_DIR%\project.json
    exit /b 5
)

if not exist "%PROFILE_DIR%\project-rules.md" (
    echo [ERROR] Project rules not found: %PROFILE_DIR%\project-rules.md
    exit /b 6
)

if not exist "%FACTORY_AGENTS%" (
    echo [ERROR] Factory global AGENTS.md not found: %FACTORY_AGENTS%
    exit /b 7
)

if not exist "%SHARED_SKILLS%\" (
    echo [ERROR] Shared user skills directory not found: %SHARED_SKILLS%
    exit /b 8
)

set "AGENT_PROFILE_DIR=%PROFILE_DIR%"

if /I "%~1"=="--check-profile" goto check_profile

droid --version >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Droid CLI is not executable in CMD.
    exit /b 9
)

pushd "%PROJECT_ROOT%"
if errorlevel 1 (
    echo [ERROR] Failed to enter project directory.
    exit /b 10
)

if not defined AGENT_PROFILE_DIR (
    echo [ERROR] AGENT_PROFILE_DIR is not set.
    popd
    exit /b 11
)

droid %*
set "EXIT_CODE=%ERRORLEVEL%"
popd
exit /b %EXIT_CODE%

:check_profile
pushd "%HARNESS_ROOT%"
if errorlevel 1 (
    echo [ERROR] Failed to enter harness directory.
    exit /b 12
)

python run.py "%PROJECT_ROOT%" --profile "%PROFILE_DIR%" --check-profile
set "EXIT_CODE=%ERRORLEVEL%"
popd
exit /b %EXIT_CODE%
