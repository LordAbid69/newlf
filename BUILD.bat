@echo off
setlocal EnableDelayedExpansion
title MaStR Scraper — Build

echo.
echo  =====================================================
echo    MaStR Scraper  ^|  Build Script
echo  =====================================================
echo.

:: ── Check Node.js ────────────────────────────────────────────────────────────
where node >nul 2>&1
if errorlevel 1 (
    echo  [ERROR] Node.js is not installed or not in PATH.
    echo          Download it from https://nodejs.org/
    pause & exit /b 1
)
for /f "tokens=*" %%v in ('node -v') do echo  Node.js %%v detected.
echo.

:: ── Step 1: Install Node dependencies ────────────────────────────────────────
echo  [1/3] Installing Node.js dependencies...
echo  -------------------------------------------------------
call npm install
if errorlevel 1 (
    echo.
    echo  [ERROR] npm install failed! Check your internet connection.
    pause & exit /b 1
)
echo  Done.
echo.

:: ── Step 2: Download Playwright Chromium into project folder ─────────────────
echo  [2/3] Downloading Playwright Chromium browser...
echo  (This is ~180 MB and only needs to run once)
echo  -------------------------------------------------------
set PLAYWRIGHT_BROWSERS_PATH=%cd%\browsers
call npx playwright install chromium
if errorlevel 1 (
    echo.
    echo  [ERROR] Browser download failed! Check your internet connection.
    pause & exit /b 1
)
echo  Browser downloaded to: %cd%\browsers
echo  Done.
echo.

:: ── Step 3: Build NSIS installer with electron-builder ───────────────────────
echo  [3/3] Building Windows installer (this may take a few minutes)...
echo  -------------------------------------------------------
call npm run build
if errorlevel 1 (
    echo.
    echo  [ERROR] Build failed! See output above for details.
    pause & exit /b 1
)

echo.
echo  =====================================================
echo    BUILD COMPLETE!
echo    Installer is located in:  dist\
echo    Double-click the .exe in dist\ to install.
echo  =====================================================
echo.
pause
