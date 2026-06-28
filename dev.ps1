# FluxTape local runner (Windows). Launches each service in its own window.
# Usage: ./dev.ps1 init   (first time)   then   ./dev.ps1 all
param([string]$cmd = "all")

function Refresh-Path { $env:Path = [Environment]::GetEnvironmentVariable("Path","Machine") + ";" + [Environment]::GetEnvironmentVariable("Path","User") }
function Start-Svc($title, $dir, $run) { Start-Process pwsh -ArgumentList "-NoExit","-Command","cd '$dir'; $run" -WindowStyle Normal }

Refresh-Path
switch ($cmd) {
  "init" {
    docker compose up -d
    Get-Content infra/db/schema.sql | docker exec -i fluxtape-timescaledb psql -U fluxtape -d fluxtape
    docker exec fluxtape-redpanda rpk topic create trades -p 3
    docker exec fluxtape-redpanda rpk topic create bars_1s -p 3
  }
  "all" {
    Start-Svc "ingestion"  "$PWD" "cargo run --manifest-path services/ingestion/Cargo.toml"
    Start-Svc "processor"  "$PWD/services/processor" "go run ."
    Start-Svc "bar-sink"   "$PWD/services/bar-sink" "go run ."
    Start-Svc "trade-sink" "$PWD/services/trade-sink" "go run ."
    Start-Svc "api"        "$PWD/services/api" "go run ."
    Start-Svc "web"        "$PWD/web" "npm run dev"
    Write-Host "Started 6 services. Web: http://localhost:5173"
  }
  "down" { docker compose down }
  "stop" {
    # Stop the 6 local services (Rust/Go/web), leave infra running.
    Get-Process -Name ingestion,processor,bar-sink,trade-sink,api -ErrorAction SilentlyContinue | Stop-Process -Force
    Get-CimInstance Win32_Process -Filter "name='node.exe'" | Where-Object { $_.CommandLine -like '*vite*' } | ForEach-Object { Stop-Process -Id $_.ProcessId -Force }
    Get-CimInstance Win32_Process -Filter "name='cargo.exe'" | Where-Object { $_.CommandLine -like '*ingestion*' } | ForEach-Object { Stop-Process -Id $_.ProcessId -Force }
    Write-Host "Stopped local services (infra still up; use ./dev.ps1 down for infra)."
  }
  default { Write-Host "usage: ./dev.ps1 [init|all|stop|down]" }
}
