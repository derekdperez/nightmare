#!/usr/bin/env bash
set -euo pipefail

DEPLOY_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$DEPLOY_DIR"

ENV_FILE="${DEPLOY_DIR}/.env"
TLS_DIR="${DEPLOY_DIR}/tls"
CERT_FILE="${TLS_DIR}/server.crt"
KEY_FILE="${TLS_DIR}/server.key"

get_env_value() {
  local file_path="$1"
  local key="$2"
  [[ -f "$file_path" ]] || return 1
  local line
  line="$(grep -E "^${key}=" "$file_path" | head -n1 || true)"
  [[ -n "$line" ]] || return 1
  local value="${line#*=}"
  [[ -n "$value" ]] || return 1
  printf '%s' "$value"
}

# Reuse secrets from existing .env so persisted Postgres volumes remain valid.
POSTGRES_PASSWORD="$(get_env_value "$ENV_FILE" "POSTGRES_PASSWORD" || true)"
COORDINATOR_API_TOKEN="$(get_env_value "$ENV_FILE" "COORDINATOR_API_TOKEN" || true)"

if [[ -z "$POSTGRES_PASSWORD" ]]; then
  POSTGRES_PASSWORD="$(openssl rand -hex 32)"
fi
if [[ -z "$COORDINATOR_API_TOKEN" ]]; then
  COORDINATOR_API_TOKEN="$(openssl rand -hex 64)"
fi

# Create TLS directory if needed
mkdir -p "$TLS_DIR"

# Generate self-signed certificate
openssl req -x509 -nodes -newkey rsa:2048 \
  -keyout "$KEY_FILE" \
  -out "$CERT_FILE" \
  -days 365 \
  -subj "/CN=server"

# Write .env file
cat > "$ENV_FILE" <<EOF
POSTGRES_DB=nightmare
POSTGRES_USER=nightmare
POSTGRES_PASSWORD=${POSTGRES_PASSWORD}
COORDINATOR_API_TOKEN=${COORDINATOR_API_TOKEN}
TLS_CERT_FILE=${CERT_FILE}
TLS_KEY_FILE=${KEY_FILE}
COORDINATOR_BASE_URL=https://server:443
EOF

echo "Generated configuration:"
echo "  .env file: $ENV_FILE"
echo "  TLS cert: $CERT_FILE"
echo "  TLS key: $KEY_FILE"
echo "  Coordinator API Token: ${COORDINATOR_API_TOKEN}"
echo ""
echo "Starting local Nightmare cluster (1 central server + 4 workers)..."

# Run docker compose
docker compose -f docker-compose.local.yml --env-file .env up -d --build

echo ""
echo "Nightmare cluster started successfully!"
echo "  Central server: https://localhost (self-signed cert)"
echo "  Dashboard: http://localhost/dashboard"
echo ""
echo "To stop: docker compose -f docker-compose.local.yml --env-file .env down"
echo "To view logs: docker compose -f docker-compose.local.yml --env-file .env logs -f"
