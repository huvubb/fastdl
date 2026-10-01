@echo off
setlocal
cd /d "%~dp0"

set "PYTHON=%LocalAppData%\Programs\Python\Python311\python.exe"
set "PYINSTALLER=%LocalAppData%\Programs\Python\Python311\Scripts\pyinstaller.exe"

if not exist "%PYTHON%" (
  echo [ERROR] Python not found: %PYTHON%
  pause
  exit /b 1
)
if not exist "%PYINSTALLER%" (
  echo [ERROR] PyInstaller not found: %PYINSTALLER%
  pause
  exit /b 1
)

echo [1/3] Checking Python sources...
for %%F in (fastdl\*.py) do "%PYTHON%" -m py_compile "%%F"
if errorlevel 1 goto :failed

echo [2/3] Cleaning and packaging...
"%PYINSTALLER%" --clean --noconfirm fastdl.spec
if errorlevel 1 goto :failed

echo [3/3] Verifying executable...
dist\fastdl.exe --version
if errorlevel 1 goto :failed

echo.
echo [OK] dist\fastdl.exe created.
pause
exit /b 0

:failed
echo.
echo [FAILED] Packaging failed. Check the errors above.
pause
exit /b 1
