@echo off
setlocal
cd /d "%~dp0.."
cargo build --release
if errorlevel 1 exit /b 1
echo.
echo Release binary is in target\release\dutawalletd.exe
