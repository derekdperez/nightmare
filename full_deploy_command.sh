#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

chmod +x ./deploy/*.sh 2>/dev/null || true
chmod +x ./deploy/full_deploy_command.sh 2>/dev/null || true
exec ./deploy/full_deploy_command.sh "$@"
