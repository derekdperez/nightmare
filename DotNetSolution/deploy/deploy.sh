#!/usr/bin/env bash
# One-command local / EC2 deploy for the full Nightmare v2 .NET stack (Docker Compose).
# Re-runnable: safe to run again; rebuilds images from the current repo tree and recreates containers.
#
# Requires either:
#   - Docker Compose V2:  "docker compose version" works (install docker-compose-plugin), or
#   - Docker Compose V1:  standalone "docker-compose" on PATH.
#
# Optional environment:
#   NIGHTMARE_GIT_PULL=1   Run git pull --ff-only in the repo before building (remote must be ff-only).
#   NIGHTMARE_NO_CACHE=1   docker compose build --no-cache (slow; strongest guarantee against stale cache).
#
# If you see: unknown shorthand flag: 'd' in -d
#   you ran "docker compose ..." without the Compose plugin — "compose" was ignored and
#   "up -d" was parsed as invalid global docker flags. Install the plugin or use docker-compose.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

# shellcheck source=deploy/lib-nightmare-compose.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib-nightmare-compose.sh"

if ! command -v docker >/dev/null 2>&1; then
  echo "Docker is required. Install Docker Engine, then re-run." >&2
  exit 1
fi

nightmare_maybe_git_pull "$ROOT"
nightmare_export_build_stamp "$ROOT"

echo "Building images (with --pull) and recreating containers from: $ROOT"
nightmare_compose_full_redeploy

echo ""
echo "Nightmare v2 is running (images match current BUILD_SOURCE_STAMP)."
echo "  Command Center:  http://localhost:8080/  (use host public IP on EC2)"
echo "  RabbitMQ admin:  http://localhost:15672/  (user/pass: nightmare / nightmare)"
echo "  Postgres:        localhost:5432  db=nightmare_v2  user=nightmare"
echo ""
echo "Useful commands (from $ROOT):"
echo "  docker compose -f deploy/docker-compose.yml logs -f worker-spider"
echo "  docker compose -f deploy/docker-compose.yml down"
echo "(or docker-compose -f deploy/docker-compose.yml ... if you use V1)"
echo ""
