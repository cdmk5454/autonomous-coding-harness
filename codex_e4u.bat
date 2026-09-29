@echo off
setlocal

set "PROJECT_ROOT=D:\inha_dev"
set "PROFILE_DIR=D:\agents\profiles\e4u"

if not exist "%PROJECT_ROOT%\" (
    echo [ERROR] Project root not found: %PROJECT_ROOT%
    exit /b 2
)

if not exist "%PROFILE_DIR%\project.json" (
    echo [ERROR] Project profile not found: %PROFILE_DIR%\project.json
    exit /b 3
)

if not exist "%USERPROFILE%\.codex\AGENTS.md" (
    echo [ERROR] Codex global AGENTS.md not found.
    exit /b 4
)

where codex >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Codex CLI not found in PATH.
    exit /b 5
)

set "AGENT_PROFILE_DIR=%PROFILE_DIR%"

pushd "%PROJECT_ROOT%"
if errorlevel 1 (
    echo [ERROR] Failed to enter project directory.
    exit /b 6
)

codex %*
set "EXIT_CODE=%ERRORLEVEL%"

popd
exit /b %EXIT_CODE%
