@echo off
setlocal
cd /d "%~dp0"

set "PYTHON=C:\Users\邓涛\AppData\Local\Programs\Python\Python311\python.exe"
set "PYINSTALLER=C:\Users\邓涛\AppData\Local\Programs\Python\Python311\Scripts\pyinstaller.exe"

if not exist "%PYTHON%" (
  echo [错误] 找不到 Python: %PYTHON%
  pause
  exit /b 1
)
if not exist "%PYINSTALLER%" (
  echo [错误] 找不到 PyInstaller: %PYINSTALLER%
  pause
  exit /b 1
)

echo [1/3] 检查 Python 源码...
for %%F in (fastdl\*.py) do "%PYTHON%" -m py_compile "%%F"
if errorlevel 1 goto :failed

echo [2/3] 清理并重新打包...
"%PYINSTALLER%" --clean --noconfirm fastdl.spec
if errorlevel 1 goto :failed

echo [3/3] 验证程序...
dist\fastdl.exe --version
if errorlevel 1 goto :failed

echo.
echo [完成] dist\fastdl.exe 已生成。
pause
exit /b 0

:failed
echo.
echo [失败] 打包未完成，请检查上面的错误信息。
pause
exit /b 1
