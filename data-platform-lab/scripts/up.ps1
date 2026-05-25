# Start lab infrastructure
$ErrorActionPreference = "Stop"
$Root = Split-Path $PSScriptRoot -Parent
Set-Location (Join-Path $Root "docker")

Write-Host "Starting Postgres + Redpanda..."
docker compose up -d postgres redpanda

Write-Host "Waiting for healthchecks..."
$deadline = (Get-Date).AddMinutes(3)
while ((Get-Date) -lt $deadline) {
    $pg = docker inspect -f "{{.State.Health.Status}}" lab-postgres 2>$null
    $rp = docker inspect -f "{{.State.Health.Status}}" lab-redpanda 2>$null
    if ($pg -eq "healthy" -and $rp -eq "healthy") {
        Write-Host "All services healthy."
        Set-Location $Root
        return
    }
    Start-Sleep -Seconds 3
}

Set-Location $Root
Write-Warning "Timed out waiting for health. Run: docker compose -f docker/docker-compose.yml ps"
