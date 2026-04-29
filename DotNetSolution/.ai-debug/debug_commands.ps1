# =============================================================================
# Windows PowerShell diagnostic commands for .\debug.ps1 (run from DotNetSolution/)
# =============================================================================
# Future AI agents: add ONE PowerShell command per line below.
# - Blank lines are ignored.
# - Lines starting with # are ignored.
# - Working directory is DotNetSolution/ when commands run.
# - Use only safe, read-only diagnostics unless the user explicitly approves.
# - Do not print secrets, full environment dumps, or .env contents.
# - Remove commands when no longer needed.
# =============================================================================

# Harmless workflow validation:
Get-Location
if (Test-Path -LiteralPath 'deploy/docker-compose.yml') { 'compose_file_ok' }
if (Get-Command dotnet -ErrorAction SilentlyContinue) { dotnet --version } else { 'dotnet_skip: not on PATH (use Docker build images for SDK)' }

# Docker (each line is its own process — set COMPOSE_BAKE per line; non-zero exit if Docker is down):
$env:COMPOSE_BAKE = 'false'; docker compose -f deploy/docker-compose.yml version
$env:COMPOSE_BAKE = 'false'; docker compose -f deploy/docker-compose.yml config -q; if ($LASTEXITCODE -eq 0) { 'compose_config_ok' }

# Examples (commented — uncomment or copy when needed):
# git status -sb
# git rev-parse HEAD
# dotnet --info
# docker compose -f deploy/docker-compose.yml ps -a
# docker logs --tail 80 nightmare-v2-command-center-1 2>&1
