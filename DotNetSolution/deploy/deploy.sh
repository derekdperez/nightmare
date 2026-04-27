#!/usr/bin/env bash
# One-command local / EC2 deploy for the full Nightmare v2 .NET stack (Docker Compose).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if ! command -v docker >/dev/null 2>&1; then
  echo "Docker is required. Install Docker Engine + Compose plugin, then re-run." >&2
  exit 1
fi

export COMPOSE_FILE="$ROOT/deploy/docker-compose.yml"
cd "$ROOT"

echo "Building images and starting stack from: $ROOT"
docker compose up -d --build

echo ""
echo "Nightmare v2 is running."
echo "  Command Center:  http://localhost:8080/  (use host public IP on EC2)"
echo "  RabbitMQ admin:  http://localhost:15672/  (user/pass: nightmare / nightmare)"
echo "  Postgres:        localhost:5432  db=nightmare_v2  user=nightmare"
echo ""
echo "Useful commands (from $ROOT):"
echo "  COMPOSE_FILE=deploy/docker-compose.yml docker compose logs -f worker-spider"
echo "  COMPOSE_FILE=deploy/docker-compose.yml docker compose down"
echo ""
