$ErrorActionPreference = "Stop"
$Root = Split-Path $PSScriptRoot -Parent
Set-Location (Join-Path $Root "docker")
docker compose down
Write-Host "Stopped. Data volume kept (postgres_data). Use 'docker compose down -v' to wipe."
