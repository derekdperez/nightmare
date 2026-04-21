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
  payload=$(python3 - "$msg" "$raw" "full_deploy_command" <<'PY'
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

trap 'report_central_error "Shell script error in full_deploy_command" "line=${LINENO} cmd=${BASH_COMMAND}"' ERR


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
DEFAULT_WORKER_COUNT=2

run_as_invoking_user() {
  if [[ "${EUID:-$(id -u)}" -eq 0 && -n "${SUDO_USER:-}" && "${SUDO_USER}" != "root" ]]; then
    sudo -H -u "${SUDO_USER}" "$@"
  else
    "$@"
  fi
}

resolve_compose_cmd() {
  if run_as_invoking_user docker compose version >/dev/null 2>&1 || docker compose version >/dev/null 2>&1; then
    echo "docker compose"
    return 0
  fi
  if run_as_invoking_user which docker-compose >/dev/null 2>&1 || which docker-compose >/dev/null 2>&1; then
    echo "docker-compose"
    return 0
  fi
  return 1
}

detect_docker_access_mode() {
  if run_as_invoking_user docker info >/dev/null 2>&1; then
    echo "invoking_user"
    return 0
  fi
  if docker info >/dev/null 2>&1; then
    echo "current_user"
    return 0
  fi
  if command -v sudo >/dev/null 2>&1 && sudo -n docker info >/dev/null 2>&1; then
    echo "sudo"
    return 0
  fi
  return 1
}

run_compose() {
  local compose_cmd="$1"
  local access_mode="$2"
  shift
  shift

  if [[ "$compose_cmd" == "docker compose" ]]; then
    case "$access_mode" in
      invoking_user)
        run_as_invoking_user docker compose "$@"
        ;;
      current_user)
        docker compose "$@"
        ;;
      sudo)
        sudo -n docker compose "$@"
        ;;
      *)
        return 1
        ;;
    esac
  else
    case "$access_mode" in
      invoking_user)
        run_as_invoking_user docker-compose "$@"
        ;;
      current_user)
        docker-compose "$@"
        ;;
      sudo)
        sudo -n docker-compose "$@"
        ;;
      *)
        return 1
        ;;
    esac
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
api_ready=0
last_code=""
for _ in $(seq 1 45); do
  code="$(curl -k -s -o /dev/null -w "%{http_code}" -H "Authorization: Bearer ${COORDINATOR_API_TOKEN}" "${COORDINATOR_BASE_URL%/}/api/coord/database-status" || true)"
  last_code="$code"
  if [[ "$code" == "200" ]]; then
    api_ready=1
    break
  fi
  sleep 2
done

if [[ "$api_ready" -ne 1 ]]; then
  echo "Coordinator API did not become ready after 90 seconds (last HTTP code: ${last_code:-none})." >&2
  compose_cmd="$(resolve_compose_cmd || true)"
  docker_access_mode="$(detect_docker_access_mode || true)"
  if [[ -n "$compose_cmd" && -n "$docker_access_mode" ]]; then
    echo "Central compose service status:" >&2
    run_compose "$compose_cmd" "$docker_access_mode" -f deploy/docker-compose.central.yml --env-file deploy/.env ps >&2 || true
    echo >&2
    echo "Recent central service logs (tail):" >&2
    run_compose "$compose_cmd" "$docker_access_mode" -f deploy/docker-compose.central.yml --env-file deploy/.env logs --tail 120 server postgres >&2 || true
  elif [[ -n "$compose_cmd" ]]; then
    echo "Central compose diagnostics skipped: Docker daemon is not accessible for invoking user/current user and sudo -n docker is unavailable." >&2
  fi
  echo "Refusing to continue with target registration because coordinator is unreachable." >&2
  exit 1
fi

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
