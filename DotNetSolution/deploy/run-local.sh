#!/usr/bin/env bash
# Start the full Nightmare v2 stack locally via Docker Compose (Postgres, Redis, RabbitMQ,
# Command Center, Gatekeeper, Spider, Enum, PortScan) for development and debugging.
#
# Usage:
#   chmod +x deploy/run-local.sh
#   ./deploy/run-local.sh              # build + start detached
#   ./deploy/run-local.sh logs         # follow all service logs (no build)
#   ./deploy/run-local.sh down         # stop and remove containers
#
# Requires: Docker Engine + Compose V2 ("docker compose") or V1 ("docker-compose").
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="$ROOT/deploy/docker-compose.yml"
cd "$ROOT"

if ! command -v docker >/dev/null 2>&1; then
  echo "Docker is required. Install Docker Engine, then re-run." >&2
  exit 1
fi

compose() {
  if docker compose version >/dev/null 2>&1; then
    docker compose -f "$COMPOSE_FILE" "$@"
  elif command -v docker-compose >/dev/null 2>&1; then
    docker-compose -f "$COMPOSE_FILE" "$@"
  else
    echo "Docker Compose is not available." >&2
    echo "  Install V2: https://docs.docker.com/compose/install/linux/" >&2
    exit 1
  fi
}

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
    echo "Building and starting stack from: $ROOT"
    compose up -d --build --remove-orphans
    echo ""
    echo "Stack is up. URLs:"
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
