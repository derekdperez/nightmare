#!/usr/bin/env bash
set -euo pipefail

report_central_error() {
  local msg="$1"
  local raw="${2:-$1}"
  if [ -z "${COORDINATOR_BASE_URL:-}" ]; then
    return 0
  fi
  local base="${COORDINATOR_BASE_URL%/}"
  local payload
  payload=$(python3 - "$msg" "$raw" "bootstrap_worker" <<'PY'
import json, os, socket, sys
msg = sys.argv[1]
raw = sys.argv[2]
program = sys.argv[3]
print(json.dumps({
    "severity": "error",
    "description": msg,
    "machine": os.getenv("EC2_INSTANCE_ID") or os.getenv("HOSTNAME") or socket.gethostname(),
    "source_id": f"{program}:shell",
    "source_type": "shell_script",
    "program_name": program,
    "component_name": "shell",
    "raw_line": raw,
    "metadata_json": {"script": program},
}))
PY
)
  curl -ksS --connect-timeout 5 --max-time 10 \
    -H "Content-Type: application/json" \
    ${COORDINATOR_API_TOKEN:+-H "Authorization: Bearer ${COORDINATOR_API_TOKEN}"} \
    -d "$payload" \
    "$base/api/coord/errors/ingest" >/dev/null 2>&1 || true
}

trap 'report_central_error "Shell script error in bootstrap_worker" "line=${LINENO} cmd=${BASH_COMMAND}"' ERR


ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DEPLOY_DIR="${ROOT_DIR}/deploy"

if [[ ! -f "${DEPLOY_DIR}/.env" ]]; then
  echo "Missing ${DEPLOY_DIR}/.env (copy from .env.example and fill COORDINATOR_BASE_URL/COORDINATOR_API_TOKEN)." >&2
  exit 1
fi

cd "${DEPLOY_DIR}"
docker compose -f docker-compose.worker.yml --env-file .env up -d --build
container_id="$(docker compose -f docker-compose.worker.yml --env-file .env ps -q worker | tail -n 1)"
if [[ -z "${container_id}" ]]; then
  echo "Worker failed to start." >&2
  exit 1
fi
docker exec "${container_id}" python3 -m sublist3r -h >/dev/null
docker exec "${container_id}" sh -lc 'mkdir -p /app/output && test -w /app/output'
echo "Worker started and validated."

