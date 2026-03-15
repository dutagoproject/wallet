@echo off
setlocal
cd /d "%~dp0.."
set "DATA_DIR=%~1"
if "%DATA_DIR%"=="" set "DATA_DIR=.\data\mainnet"
cargo run -- --datadir "%DATA_DIR%"
