@echo off
setlocal

set "PROJECT_ROOT=D:\inha_dev"
set "PROFILE_DIR=D:\agents\profiles\e4u"
set "AGENTS_DIR=D:\agents"
set "CODEX_HOME=%USERPROFILE%\.codex"
set "PYTHON_EXE=%~dp0.venv\Scripts\python.exe"
set "CONTROL_HOST=127.0.0.1"
set "CONTROL_PORT=8765"

set "PYTHONHOME="
set "PYTHONPATH="
set "PYTHONSTARTUP="
set "PYTHONINSPECT="
set "PYTHONBREAKPOINT="
rem Tunnel/control-plane credentials must terminate at the tunnel parent.
rem Codex inside the harness uses CODEX_HOME or a separate CODEX_API_KEY.
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

if not exist "%PROFILE_DIR%\project.json" (
    echo [ERROR] Project profile not found: %PROFILE_DIR%\project.json 1>&2
    exit /b 3
)

if not exist "%AGENTS_DIR%\AGENTS.md" (
    echo [ERROR] Source AGENTS.md not found. 1>&2
    exit /b 4
)

if not exist "%CODEX_HOME%\AGENTS.md" (
    echo [ERROR] Codex global AGENTS.md not found. 1>&2
    exit /b 5
)

if not exist "%USERPROFILE%\.factory\AGENTS.md" (
    echo [ERROR] Factory global AGENTS.md not found. 1>&2
    exit /b 6
)

set "AGENT_PROFILE_DIR=%PROFILE_DIR%"

pushd "%PROJECT_ROOT%"
if errorlevel 1 (
    echo [ERROR] Failed to enter project directory. 1>&2
    exit /b 7
)

if /I "%~1"=="--check-profile" (
    "%PYTHON_EXE%" -E -s "%~dp0run.py" "%PROJECT_ROOT%" %* --profile "%PROFILE_DIR%"
) else (
    "%PYTHON_EXE%" -E -s "%~dp0chatgpt_control_mcp.py" %* --working-dir "%PROJECT_ROOT%" --profile "%PROFILE_DIR%" --host "%CONTROL_HOST%" --port "%CONTROL_PORT%" --transport stdio
)
set "EXIT_CODE=%ERRORLEVEL%"

popd
exit /b %EXIT_CODE%
