$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $repoRoot

$env:WATCHFILES_FORCE_POLLING = "true"
$env:PYTHONPATH = if ($env:PYTHONPATH) { "$repoRoot;$env:PYTHONPATH" } else { "$repoRoot" }
if (-not $env:FYERS_LOG_PATH) { $env:FYERS_LOG_PATH = "logs/fyers" }
New-Item -ItemType Directory -Force -Path "$repoRoot\logs\fyers" | Out-Null

& ".\.venv\Scripts\python.exe" -m uvicorn app.main:app `
  --host 127.0.0.1 `
  --port 8000 `
  --reload `
  --app-dir "$repoRoot" `
  --reload-dir app
