@echo off
rem ---------------------------------------------------------------------------
rem  Append the analyst rating data to rating_snapshot.json.
rem  Just double-click this file, or register it in Task Scheduler.
rem
rem  The source site only lists the current month (about three weeks), so run
rem  this at least every two weeks or older ratings are lost for good.
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

rem %* passes through extra options, e.g. --no-price to skip the price lookup.
python update_rating.py %*
set EXITCODE=%ERRORLEVEL%

echo.
if %EXITCODE% neq 0 (
    echo ----------------------------------------------------------------
    echo  FAILED. See the message above. The archive was left untouched.
    echo ----------------------------------------------------------------
) else (
    echo ----------------------------------------------------------------
    echo  Done. Commit and push rating_snapshot.json to publish it.
    echo    git add rating_snapshot.json
    echo    git commit -m "レーティングのアーカイブを更新する"
    echo    git push
    echo ----------------------------------------------------------------
)

echo.
pause
exit /b %EXITCODE%
