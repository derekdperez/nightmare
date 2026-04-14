#!/usr/bin/env bash
set -euo pipefail

DEPLOY_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$DEPLOY_DIR"

ENV_FILE="${DEPLOY_DIR}/.env"
TLS_DIR="${DEPLOY_DIR}/tls"
CERT_FILE="${TLS_DIR}/server.crt"
KEY_FILE="${TLS_DIR}/server.key"

# Check if .env already exists with valid values
if [[ -f "$ENV_FILE" ]]; then
    echo "Using existing .env configuration..."
    source "$ENV_FILE"
else
    echo "Generating new .env configuration..."

    # Generate random values
    POSTGRES_PASSWORD="$(openssl rand -hex 32)"
    COORDINATOR_API_TOKEN="$(openssl rand -hex 64)"

    # Create TLS directory if needed
    mkdir -p "$TLS_DIR"

    # Generate self-signed certificate
    openssl req -x509 -nodes -newkey rsa:2048 \
      -keyout "$KEY_FILE" \
      -out "$CERT_FILE" \
      -days 365 \
      -subj "/CN=server" 2>/dev/null || true

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

    source "$ENV_FILE"
    echo "Configuration generated."
fi

echo ""
echo "=== Debug Configuration ==="
echo "API Token: ${COORDINATOR_API_TOKEN}"
echo "Database: ${POSTGRES_USER}@nightmare (password in .env)"
echo "Local URL: https://localhost (for local debugging)"
echo "Docker URL: ${COORDINATOR_BASE_URL} (from .env)"
echo "Config file: $(cd .. && pwd)/config/coordinator.json (empty values use env vars)"
echo ""

# Always restart containers to ensure they have latest environment variables
echo "Restarting local Nightmare cluster with updated configuration..."
docker compose -f docker-compose.local.yml --env-file .env down -v 2>/dev/null || true
docker compose -f docker-compose.local.yml --env-file .env up -d --build
echo "Waiting for services to start..."
sleep 10

echo ""
echo "✓ Debug environment ready!"
echo "  - coordinator.json is configured to use environment variables"
echo "  - COORDINATOR_BASE_URL will override to https://localhost in debug mode"
echo "  - API token loaded from .env"
