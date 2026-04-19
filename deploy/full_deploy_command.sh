#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

# Default deployment settings for this environment.
AWS_AMI_ID="ami-098e39bafa7e7303d"
AWS_INSTANCE_TYPE="m7i-flex.large"
AWS_SUBNET_ID="subnet-054dbecc0059eda75"
AWS_SECURITY_GROUP_IDS="sg-0423808f12622fada"
AWS_IAM_INSTANCE_PROFILE="ec2"
AWS_KEY_NAME="kp1"
AWS_REGION="us-east-1"
REPO_URL="https://github.com/derekdperez/nightmare.git"
REPO_BRANCH="main"
WORKER_TAG_FILTER="nightmare-worker*"
DEFAULT_WORKER_COUNT=5

run_as_invoking_user() {
  if [[ "${EUID:-$(id -u)}" -eq 0 && -n "${SUDO_USER:-}" && "${SUDO_USER}" != "root" ]]; then
    sudo -H -u "${SUDO_USER}" "$@"
  else
    "$@"
  fi
}

ids_empty() {
  local value="$1"
  [[ -z "$value" || "$value" == "None" ]]
}

get_worker_instance_ids() {
  local state_values="$1"
  run_as_invoking_user aws ec2 describe-instances \
    --region "$AWS_REGION" \
    --filters "Name=tag:Name,Values=${WORKER_TAG_FILTER}" "Name=instance-state-name,Values=${state_values}" \
    --query "Reservations[].Instances[].InstanceId" \
    --output text 2>/dev/null || true
}

chmod +x ./deploy/*.sh 2>/dev/null || true
chmod +x ./deploy/bootstrap-central-auto.sh 2>/dev/null || true

./deploy/bootstrap-central-auto.sh \
  --aws-ami-id "$AWS_AMI_ID" \
  --aws-instance-type "$AWS_INSTANCE_TYPE" \
  --aws-subnet-id "$AWS_SUBNET_ID" \
  --aws-security-group-ids "$AWS_SECURITY_GROUP_IDS" \
  --aws-iam-instance-profile "$AWS_IAM_INSTANCE_PROFILE" \
  --aws-key-name "$AWS_KEY_NAME" \
  --aws-region "$AWS_REGION" \
  --repo-url "$REPO_URL" \
  --repo-branch "$REPO_BRANCH"

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

run_as_invoking_user python3 register_targets.py \
  --server-base-url "${COORDINATOR_BASE_URL}" \
  --api-token "${COORDINATOR_API_TOKEN}" \
  --targets-file "targets.txt"

echo "Reconciling worker VMs..."
all_worker_ids="$(get_worker_instance_ids "pending,running,stopping,stopped")"
active_worker_ids="$(get_worker_instance_ids "pending,running")"

if ids_empty "$all_worker_ids"; then
  echo "No existing worker VMs found. Provisioning ${DEFAULT_WORKER_COUNT} worker(s)..."
  run_as_invoking_user ./deploy/provision-workers-aws.sh \
    --count "$DEFAULT_WORKER_COUNT" \
    --ami-id "$AWS_AMI_ID" \
    --instance-type "$AWS_INSTANCE_TYPE" \
    --subnet-id "$AWS_SUBNET_ID" \
    --security-group-ids "$AWS_SECURITY_GROUP_IDS" \
    --repo-url "$REPO_URL" \
    --repo-branch "$REPO_BRANCH" \
    --coordinator-base-url "${COORDINATOR_BASE_URL}" \
    --api-token "${COORDINATOR_API_TOKEN}" \
    --coordinator-insecure-tls "${COORDINATOR_INSECURE_TLS:-true}" \
    --iam-instance-profile "$AWS_IAM_INSTANCE_PROFILE" \
    --key-name "$AWS_KEY_NAME" \
    --region "$AWS_REGION"
else
  if ids_empty "$active_worker_ids"; then
    stopped_worker_ids="$(get_worker_instance_ids "stopped")"
    if ! ids_empty "$stopped_worker_ids"; then
      echo "Worker VMs exist but are stopped. Starting them..."
      run_as_invoking_user aws ec2 start-instances --region "$AWS_REGION" --instance-ids $stopped_worker_ids >/dev/null
      run_as_invoking_user aws ec2 wait instance-running --region "$AWS_REGION" --instance-ids $stopped_worker_ids
    fi
  fi
  echo "Existing worker VMs found. Rolling out latest code + docker rebuild on workers..."
  rollout_cmd=(
    python3 client.py rollout
    --server-base-url "${COORDINATOR_BASE_URL}"
    --api-token "${COORDINATOR_API_TOKEN}"
    --aws-region "$AWS_REGION"
    --ssm-target-key "tag:Name"
    --ssm-target-values "$WORKER_TAG_FILTER"
    --repo-dir "/opt/nightmare"
    --branch "$REPO_BRANCH"
  )
  if [[ "${COORDINATOR_INSECURE_TLS:-}" == "true" ]]; then
    rollout_cmd+=(--insecure-tls)
  fi
  run_as_invoking_user "${rollout_cmd[@]}"
fi
