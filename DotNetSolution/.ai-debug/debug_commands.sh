# =============================================================================
# Unix / macOS / Linux diagnostic commands for ./debug.sh (run from DotNetSolution/)
# =============================================================================
# Future AI agents: add ONE shell command per line below.
# - Blank lines are ignored.
# - Lines starting with # are ignored.
# - Working directory is DotNetSolution/ when commands run.
# - Use only safe, read-only diagnostics unless the user explicitly approves.
# - Do not print secrets, full environment dumps, or .env contents.
# - Remove commands when no longer needed.
# =============================================================================

# Harmless workflow validation (paths + optional Docker Compose file check):
pwd
test -f deploy/docker-compose.yml && echo compose_file_ok
if command -v dotnet >/dev/null 2>&1; then dotnet --version; else echo "dotnet_skip: not on PATH (common on Docker-only hosts; builds use the SDK inside Dockerfiles)"; fi

# Docker (real exit codes — may be non-zero if the daemon is down or the user lacks permission):
COMPOSE_BAKE=false docker compose -f deploy/docker-compose.yml version
COMPOSE_BAKE=false docker compose -f deploy/docker-compose.yml config -q && echo compose_config_ok

# Examples (commented — uncomment or copy when needed):
# git status -sb
# git rev-parse HEAD
# dotnet --info
# docker compose -f deploy/docker-compose.yml ps -a
# docker logs --tail 80 nightmare-v2-command-center-1 2>&1
