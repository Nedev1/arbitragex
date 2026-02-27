@echo off
setlocal

where dotnet >nul 2>nul
if errorlevel 1 (
  echo .NET SDK not found. Please install .NET SDK.
  pause
  exit /b 1
)

pushd %~dp0

echo Building ArbitrageX (HFT .NET 4.8 / x64 Architecture)...
dotnet build ArbitrageX\ArbitrageX.csproj -c Release
if errorlevel 1 (
  echo Build failed! Check the errors above.
  pause
  exit /b 1
)

echo Starting ArbitrageX...
start "" "%~dp0ArbitrageX\bin\Release\net48\ArbitrageX.exe"

popd
endlocal