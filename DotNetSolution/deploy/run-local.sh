#!/usr/bin/env bash
# Start the full Nightmare v2 stack locally via Docker Compose (Postgres, Redis, RabbitMQ,
# Command Center, Gatekeeper, Spider, Enum, PortScan) for development and debugging.
#
# Re-runnable: "up" uses the same incremental fingerprint as deploy.sh (see deploy/.last-deploy-stamp).
#
# Usage:
#   chmod +x deploy/run-local.sh
#   ./deploy/run-local.sh              # incremental up (skip build when fingerprint unchanged)
#   ./deploy/run-local.sh -fresh       # full rebuild (--pull --no-cache) then up
#   ./deploy/run-local.sh logs         # follow all service logs (no build)
#   ./deploy/run-local.sh down         # stop and remove containers
#
# Optional environment (for "up" only):
#   NIGHTMARE_GIT_PULL=1   git pull --ff-only before build
#   NIGHTMARE_NO_CACHE=1   docker compose build --no-cache
#   NIGHTMARE_SKIP_INSTALL=1   Do not auto-install Docker; fail if missing
#   NIGHTMARE_DEPLOY_FRESH=1   Same as -fresh
#   COMPOSE_BAKE=true|false   deploy/lib-nightmare-compose.sh defaults to false (see docker-compose.yml header).
#
# Requires: Docker Engine + Compose V2 ("docker compose") or V1 ("docker-compose").
# On Linux, missing Docker/Compose is installed automatically (see deploy/lib-install-deps.sh).
set -euo pipefail

DEPLOY_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$DEPLOY_DIR/.." && pwd)"
cd "$ROOT"

NIGHTMARE_DEPLOY_FRESH="${NIGHTMARE_DEPLOY_FRESH:-0}"
CMD="up"
while [[ $# -gt 0 ]]; do
  case "$1" in
    -fresh | --fresh)
      NIGHTMARE_DEPLOY_FRESH=1
      shift
      ;;
    -h | --help)
      cat <<'EOF'
Usage: ./deploy/run-local.sh [-fresh] [up|down|logs|ps|status]

  up (default)  Incremental: skip docker compose build when fingerprint matches deploy/.last-deploy-stamp.
  -fresh        Full rebuild: build --pull --no-cache, then up.
  down / logs / ps / status   No image build.
EOF
      exit 0
      ;;
    down | logs | ps | status | up)
      CMD="$1"
      shift
      ;;
    *)
      echo "Unknown argument: $1" >&2
      exit 1
      ;;
  esac
done
export NIGHTMARE_DEPLOY_FRESH

# shellcheck source=deploy/lib-nightmare-compose.sh
source "$DEPLOY_DIR/lib-nightmare-compose.sh"
# shellcheck source=deploy/lib-install-deps.sh
source "$DEPLOY_DIR/lib-install-deps.sh"

nightmare_ensure_runtime_dependencies

case "$CMD" in
  down)
    echo "Stopping stack from: $ROOT"
    compose down --remove-orphans
    echo "Stopped."
    ;;
  logs)
    echo "Following logs (Ctrl+C stops tail only). Project: $ROOT"
    compose logs -f
    ;;
  ps | status)
    compose ps
    ;;
  up)
    nightmare_maybe_git_pull "$ROOT"
    nightmare_export_build_stamp "$ROOT"
    nightmare_decide_incremental_deploy
    echo "Applying stack from: $ROOT"
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
    echo "  ./deploy/run-local.sh -fresh   # full image rebuild"
    echo "  docker compose -f deploy/docker-compose.yml logs -f command-center worker-spider"
    ;;
esac
