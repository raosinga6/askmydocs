# Quick sanity check — not a test suite
$ErrorActionPreference = "Stop"
$Root = Split-Path $PSScriptRoot -Parent

Write-Host "1) Docker containers"
docker ps --filter "name=lab-" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

Write-Host "`n2) Postgres"
docker exec lab-postgres psql -U lab -d lab -c "SELECT COUNT(*) AS tables FROM information_schema.tables WHERE table_schema='raw';"

Write-Host "`n3) Redpanda topic (auto-created on first produce if missing)"
docker exec lab-redpanda rpk topic list 2>$null

Write-Host "`nSmoke done. Next: pip install -r kafka/requirements.txt && python kafka/producer.py"
