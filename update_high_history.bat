@echo off
rem ---------------------------------------------------------------------------
rem  Update the "joujourai-takane" data. Just double-click this file.
rem
rem  Keep this file ASCII-only. cmd.exe parses a batch file using the console
rem  codepage that is active BEFORE chcp runs, so any Japanese text placed here
rem  is decoded as cp932, breaks the line, and cmd tries to run the fragments as
rem  commands. All Japanese output comes from the Python script instead, which
rem  is safe once the codepage and PYTHONIOENCODING are set below.
rem ---------------------------------------------------------------------------

rem Use UTF-8 for both the console and Python so Japanese output is not mangled.
chcp 65001 > nul
set PYTHONIOENCODING=utf-8

rem Run from this file's own folder, whatever directory it was launched from.
cd /d "%~dp0"

where python >nul 2>&1
if errorlevel 1 (
    echo.
    echo [ERROR] Python not found on PATH.
    echo         Install Python, or run the script manually from PowerShell.
    echo.
    pause
    exit /b 1
)

rem %* passes through extra options, e.g. --deep to reach further back in time.
python update_high_history.py %*
set EXITCODE=%ERRORLEVEL%

echo.
if %EXITCODE% neq 0 (
    echo ----------------------------------------------------------------
    echo  FAILED. See the message above.
    echo ----------------------------------------------------------------
) else (
    echo ----------------------------------------------------------------
    echo  Done. Reopen the site in a few minutes to see the update.
    echo  https://turtle-tool-4em5dzkz7yvqtma8gvpuxf.streamlit.app/
    echo ----------------------------------------------------------------
)

echo.
pause
exit /b %EXITCODE%
