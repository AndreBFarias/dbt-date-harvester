@echo off
setlocal EnableDelayedExpansion

set "SCRIPT_DIR=%~dp0"
if "%SCRIPT_DIR:~-1%"=="\" set "SCRIPT_DIR=%SCRIPT_DIR:~0,-1%"
set "VENV_DIR=%SCRIPT_DIR%\venv"

where python >nul 2>nul
if %ERRORLEVEL% neq 0 (
    echo Erro: Python nao encontrado. Instale Python 3.8+ em https://python.org
    exit /b 1
)

python -c "import sys; sys.exit(0 if sys.version_info >= (3, 8) else 1)" 2>nul
if %ERRORLEVEL% neq 0 (
    echo Erro: Python 3.8+ necessario.
    exit /b 1
)

if not exist "%VENV_DIR%\Scripts\python.exe" (
    echo Criando virtualenv em %VENV_DIR%...
    python -m venv "%VENV_DIR%"
    echo Instalando dependencias...
    "%VENV_DIR%\Scripts\pip" install --quiet --upgrade pip
    "%VENV_DIR%\Scripts\pip" install --quiet -r "%SCRIPT_DIR%\requirements.txt"
    echo Instalacao concluida.
    echo.
)

"%VENV_DIR%\Scripts\python" "%SCRIPT_DIR%\main.py" %*
