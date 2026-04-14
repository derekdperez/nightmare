#!/usr/bin/env bash
set -euo pipefail

# Start containers
./deploy/run-local.sh

# Wait a bit for containers to be ready
sleep 10

# Read the API token from .env
API_TOKEN=$(grep COORDINATOR_API_TOKEN deploy/.env | cut -d'=' -f2)

# Register targets
python3 register_targets.py --server-base-url https://localhost --api-token "$API_TOKEN" --targets-file targets.txt