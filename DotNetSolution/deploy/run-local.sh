#!/usr/bin/env bash
# Start the full Nightmare v2 stack locally via Docker Compose (Postgres, Redis, RabbitMQ,
# Command Center, Gatekeeper, Spider, Enum, PortScan) for development and debugging.
#
# Re-runnable: each "up" rebuilds from the current working tree and recreates app containers.
#
# Usage:
#   chmod +x deploy/run-local.sh
#   ./deploy/run-local.sh              # build + start detached (latest code)
#   ./deploy/run-local.sh logs         # follow all service logs (no build)
#   ./deploy/run-local.sh down         # stop and remove containers
#
# Optional environment (for "up" only):
#   NIGHTMARE_GIT_PULL=1   git pull --ff-only before build
#   NIGHTMARE_NO_CACHE=1   docker compose build --no-cache
#   NIGHTMARE_SKIP_INSTALL=1   Do not auto-install Docker; fail if missing
#
# Requires: Docker Engine + Compose V2 ("docker compose") or V1 ("docker-compose").
# On Linux, missing Docker/Compose is installed automatically (see deploy/lib-install-deps.sh).
set -euo pipefail

DEPLOY_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$DEPLOY_DIR/.." && pwd)"
cd "$ROOT"

# shellcheck source=deploy/lib-nightmare-compose.sh
source "$DEPLOY_DIR/lib-nightmare-compose.sh"
# shellcheck source=deploy/lib-install-deps.sh
source "$DEPLOY_DIR/lib-install-deps.sh"

nightmare_ensure_runtime_dependencies

case "${1:-up}" in
  down)
    echo "Stopping stack from: $ROOT"
    compose down --remove-orphans
    echo "Stopped."
    ;;
  logs)
    echo "Following logs (Ctrl+C stops tail only). Project: $ROOT"
    compose logs -f
    ;;
  ps|status)
    compose ps
    ;;
  up|""|*)
    nightmare_maybe_git_pull "$ROOT"
    nightmare_export_build_stamp "$ROOT"
    echo "Building images (with --pull) and recreating containers from: $ROOT"
    nightmare_compose_full_redeploy
    echo ""
    echo "Stack is up (images match current BUILD_SOURCE_STAMP). URLs:"
    echo "  Command Center   http://localhost:8080/"
    echo "  RabbitMQ UI    http://localhost:15672/  (nightmare / nightmare)"
    echo "  Postgres       localhost:5432  db=nightmare_v2  user=nightmare"
    echo "  Redis          localhost:6379"
    echo ""
    echo "Debug commands (run from $ROOT):"
    echo "  ./deploy/run-local.sh logs     # all services"
    echo "  ./deploy/run-local.sh ps       # container status"
    echo "  ./deploy/run-local.sh down     # stop stack"
    echo "  docker compose -f deploy/docker-compose.yml logs -f command-center worker-spider"
    ;;
esac
