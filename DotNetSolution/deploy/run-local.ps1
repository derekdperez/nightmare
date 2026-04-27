<#
.SYNOPSIS
  Starts the full Nightmare v2 stack locally (Docker Compose) for development/debugging.

.DESCRIPTION
  Brings up Postgres, Redis, RabbitMQ, Command Center, Gatekeeper, Spider, Enum, and PortScan
  using deploy/docker-compose.yml. Requires Docker Desktop (Windows) with Compose V2, or
  docker-compose v1 on PATH.

.PARAMETER Action
  up    Build images and start containers detached (default).
  down  Stop and remove containers.
  logs  Follow all service logs (blocks until Ctrl+C).
  ps    Show container status.

.EXAMPLE
  .\deploy\run-local.ps1
  .\deploy\run-local.ps1 -Action logs
#>
param(
  [ValidateSet("up", "down", "logs", "ps")]
  [string] $Action = "up"
)

$ErrorActionPreference = "Stop"

if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
  Write-Error "Docker is not on PATH. Install Docker Desktop for Windows."
}

$ScriptRoot = $PSScriptRoot
if (-not $ScriptRoot) { $ScriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Path }

$ComposeFile = Join-Path $ScriptRoot "docker-compose.yml"
$Root = (Resolve-Path (Join-Path $ScriptRoot "..")).Path
Set-Location $Root

function Invoke-NightmareCompose {
  param([Parameter(ValueFromRemainingArguments = $true)][string[]] $ComposeArgs)
  docker compose version *>$null
  $composeV2 = ($LASTEXITCODE -eq 0)

  if ($composeV2) {
    & docker compose -f $ComposeFile @ComposeArgs
  }
  elseif (Get-Command docker-compose -ErrorAction SilentlyContinue) {
    & docker-compose -f $ComposeFile @ComposeArgs
  }
  else {
    Write-Error "Docker Compose not found. Install Docker Desktop (Compose V2) or docker-compose v1."
  }
}

switch ($Action) {
  "down" {
    Write-Host "Stopping stack in: $Root"
    Invoke-NightmareCompose @("down", "--remove-orphans")
    Write-Host "Stopped."
  }
  "logs" {
    Write-Host "Following logs (Ctrl+C stops tail only). Project: $Root"
    Invoke-NightmareCompose @("logs", "-f")
  }
  "ps" {
    Invoke-NightmareCompose @("ps")
  }
  default {
    Write-Host "Building and starting stack from: $Root"
    Invoke-NightmareCompose @("up", "-d", "--build", "--remove-orphans")
    Write-Host ""
    Write-Host "Stack is up. URLs:"
    Write-Host "  Command Center   http://localhost:8080/"
    Write-Host "  RabbitMQ UI    http://localhost:15672/  (nightmare / nightmare)"
    Write-Host "  Postgres       localhost:5432  db=nightmare_v2  user=nightmare"
    Write-Host "  Redis          localhost:6379"
    Write-Host ""
    Write-Host "Debug:"
    Write-Host "  .\deploy\run-local.ps1 -Action logs"
    Write-Host "  .\deploy\run-local.ps1 -Action ps"
    Write-Host "  .\deploy\run-local.ps1 -Action down"
    Write-Host "  docker compose -f deploy/docker-compose.yml logs -f command-center worker-spider"
  }
}
