#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

chmod +x ./deploy/*.sh 2>/dev/null || true
chmod +x ./deploy/bootstrap-central-auto.sh 2>/dev/null || true

./deploy/bootstrap-central-auto.sh \
  --auto-provision-workers 5 \
  --aws-ami-id ami-098e39bafa7e7303d \
  --aws-instance-type t3.micro \
  --aws-subnet-id subnet-054dbecc0059eda75 \
  --aws-security-group-ids sg-0423808f12622fada \
  --aws-iam-instance-profile ec2 \
  --aws-key-name kp1 \
  --aws-region us-east-1 \
  --repo-url https://github.com/derekdperez/nightmare.git \
  --repo-branch main

if [[ -f "deploy/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "deploy/.env"
  set +a
fi

if [[ -z "${COORDINATOR_BASE_URL:-}" || -z "${COORDINATOR_API_TOKEN:-}" ]]; then
  echo "Missing COORDINATOR_BASE_URL and/or COORDINATOR_API_TOKEN in deploy/.env; cannot auto-register targets." >&2
  exit 1
fi

echo "Waiting for coordinator API readiness..."
for _ in $(seq 1 45); do
  code="$(curl -k -s -o /dev/null -w "%{http_code}" -H "Authorization: Bearer ${COORDINATOR_API_TOKEN}" "${COORDINATOR_BASE_URL%/}/api/coord/database-status" || true)"
  if [[ "$code" == "200" ]]; then
    break
  fi
  sleep 2
done

python3 register_targets.py \
  --server-base-url "${COORDINATOR_BASE_URL}" \
  --api-token "${COORDINATOR_API_TOKEN}" \
  --targets-file "targets.txt"
