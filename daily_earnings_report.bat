@echo off
:: daily_earnings_report.bat
:: PowerShell スクリプトのラッパー（ダブルクリックまたはタスクスケジューラから起動）
powershell -ExecutionPolicy Bypass -File "%~dp0daily_earnings_report.ps1" %*
pause
