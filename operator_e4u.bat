@echo off
setlocal

set "PROJECT_ROOT=D:\inha_dev"
set "PROFILE_DIR=D:\agents\profiles\e4u"
set "PYTHON_EXE=%~dp0.venv\Scripts\python.exe"

set "PYTHONHOME="
set "PYTHONPATH="
set "PYTHONSTARTUP="
set "PYTHONINSPECT="
set "PYTHONBREAKPOINT="
set "CONTROL_PLANE_API_KEY="
set "OPENAI_ADMIN_KEY="
set "CONTROL_PLANE_EXTRA_HEADERS="
set "MCP_EXTRA_HEADERS="
set "MCP_DISCOVERY_EXTRA_HEADERS="
set "CLOUDFLARED_TUNNEL_TOKEN="
set "TUNNEL_TOKEN="
set "OPENAI_API_KEY="
set "OPENAI_ORG_ID="
set "OPENAI_PROJECT_ID="
set "KKM_ENFORCE_TRUSTED_EXECUTABLES=1"

if not exist "%PYTHON_EXE%" (
    echo [ERROR] Trusted control venv Python not found: %PYTHON_EXE% 1>&2
    exit /b 8
)

if not exist "%PROJECT_ROOT%\" (
    echo [ERROR] Project root not found: %PROJECT_ROOT% 1>&2
    exit /b 2
)

pushd "%~dp0"
if errorlevel 1 (
    echo [ERROR] Failed to enter harness directory. 1>&2
    exit /b 7
)

"%PYTHON_EXE%" -E -s "%~dp0control_operator.py" --working-dir "%PROJECT_ROOT%" --profile "%PROFILE_DIR%" %*
set "EXIT_CODE=%ERRORLEVEL%"

popd
exit /b %EXIT_CODE%
