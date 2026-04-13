#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DEPLOY_DIR="${ROOT_DIR}/deploy"

COUNT=0
AMI_ID=""
INSTANCE_TYPE="t3.small"
SUBNET_ID=""
SECURITY_GROUP_IDS=""
KEY_NAME=""
IAM_INSTANCE_PROFILE=""
REGION=""
REPO_URL=""
REPO_BRANCH="main"
COORDINATOR_BASE_URL=""
API_TOKEN=""
INSTANCE_NAME_PREFIX="nightmare-worker"
WAIT_FOR_RUNNING=1

usage() {
  cat <<'USAGE'
Usage:
  ./deploy/provision-workers-aws.sh \
    --count 20 \
    --ami-id ami-xxxxxxxx \
    --subnet-id subnet-xxxxxxxx \
    --security-group-ids sg-aaaa,sg-bbbb \
    --repo-url https://github.com/<owner>/<repo>.git \
    --coordinator-base-url https://central-host \
    --api-token <token> \
    [--instance-type t3.small] [--repo-branch main] [--key-name keypair] [--iam-instance-profile profile] [--region us-east-1]

This script launches worker EC2 instances and bootstraps each VM with cloud-init to:
  - install docker + git,
  - clone the repo,
  - write deploy/.env with coordinator URL/token,
  - run docker compose worker stack.
USAGE
}

require_cmd() {
  local name="$1"
  if ! command -v "$name" >/dev/null 2>&1; then
    echo "Missing required command: $name" >&2
    exit 1
  fi
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --count)
      COUNT="${2:-0}"
      shift 2
      ;;
    --ami-id)
      AMI_ID="${2:-}"
      shift 2
      ;;
    --instance-type)
      INSTANCE_TYPE="${2:-t3.small}"
      shift 2
      ;;
    --subnet-id)
      SUBNET_ID="${2:-}"
      shift 2
      ;;
    --security-group-ids)
      SECURITY_GROUP_IDS="${2:-}"
      shift 2
      ;;
    --key-name)
      KEY_NAME="${2:-}"
      shift 2
      ;;
    --iam-instance-profile)
      IAM_INSTANCE_PROFILE="${2:-}"
      shift 2
      ;;
    --region)
      REGION="${2:-}"
      shift 2
      ;;
    --repo-url)
      REPO_URL="${2:-}"
      shift 2
      ;;
    --repo-branch)
      REPO_BRANCH="${2:-main}"
      shift 2
      ;;
    --coordinator-base-url)
      COORDINATOR_BASE_URL="${2:-}"
      shift 2
      ;;
    --api-token)
      API_TOKEN="${2:-}"
      shift 2
      ;;
    --instance-name-prefix)
      INSTANCE_NAME_PREFIX="${2:-nightmare-worker}"
      shift 2
      ;;
    --no-wait)
      WAIT_FOR_RUNNING=0
      shift
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if ! [[ "$COUNT" =~ ^[0-9]+$ ]] || [[ "$COUNT" -lt 1 ]]; then
  echo "--count must be an integer >= 1" >&2
  exit 2
fi

for required in AMI_ID SUBNET_ID SECURITY_GROUP_IDS REPO_URL COORDINATOR_BASE_URL API_TOKEN; do
  if [[ -z "${!required}" ]]; then
    echo "Missing required option for provisioning: --$(echo "$required" | tr 'A-Z_' 'a-z-')" >&2
    exit 2
  fi
done

require_cmd aws

region_args=()
if [[ -n "$REGION" ]]; then
  region_args=(--region "$REGION")
fi

mkdir -p "$DEPLOY_DIR"
USER_DATA_FILE="$(mktemp "${DEPLOY_DIR}/worker-user-data.XXXXXX.yaml")"
trap 'rm -f "$USER_DATA_FILE"' EXIT

cat > "$USER_DATA_FILE" <<EOF
#cloud-config
runcmd:
  - [bash, -lc, "set -euo pipefail; if command -v yum >/dev/null 2>&1; then yum makecache -y || true; yum install -y ca-certificates curl git docker openssl; yum install -y docker-compose-plugin || yum install -y docker-compose || true; elif command -v dnf >/dev/null 2>&1; then dnf makecache -y || true; dnf install -y ca-certificates curl git docker openssl; dnf install -y docker-compose-plugin || dnf install -y docker-compose || true; elif command -v apt-get >/dev/null 2>&1; then apt-get update; apt-get install -y ca-certificates curl git docker.io docker-compose-plugin openssl; else echo 'no supported package manager' >&2; exit 1; fi"]
  - [bash, -lc, "systemctl enable --now docker"]
  - [bash, -lc, "if docker compose version >/dev/null 2>&1; then COMPOSE_CMD='docker compose'; elif command -v docker-compose >/dev/null 2>&1; then COMPOSE_CMD='docker-compose'; else echo 'docker compose not installed' >&2; exit 1; fi; echo \"compose=\$COMPOSE_CMD\" >/var/log/nightmare-compose.txt"]
  - [bash, -lc, "mkdir -p /opt/nightmare"]
  - [bash, -lc, "if [ ! -d /opt/nightmare/.git ]; then git clone --depth 1 --branch '${REPO_BRANCH}' '${REPO_URL}' /opt/nightmare; fi"]
  - [bash, -lc, "cd /opt/nightmare && git fetch --all --prune && git checkout '${REPO_BRANCH}' && git pull --ff-only || true"]
  - [bash, -lc, "cat > /opt/nightmare/deploy/.env <<'ENVEOF'\nCOORDINATOR_BASE_URL=${COORDINATOR_BASE_URL}\nCOORDINATOR_API_TOKEN=${API_TOKEN}\nENVEOF"]
  - [bash, -lc, "if docker compose version >/dev/null 2>&1; then COMPOSE_CMD='docker compose'; else COMPOSE_CMD='docker-compose'; fi; cd /opt/nightmare && \$COMPOSE_CMD -f deploy/docker-compose.worker.yml --env-file deploy/.env up -d --build"]
EOF

IFS=',' read -r -a sg_ids <<< "$SECURITY_GROUP_IDS"
if [[ "${#sg_ids[@]}" -eq 0 ]]; then
  echo "No valid security group IDs provided in --security-group-ids" >&2
  exit 2
fi

run_cmd=(
  aws ec2 run-instances
  "${region_args[@]}"
  --image-id "$AMI_ID"
  --count "$COUNT"
  --instance-type "$INSTANCE_TYPE"
  --subnet-id "$SUBNET_ID"
  --security-group-ids "${sg_ids[@]}"
  --user-data "file://${USER_DATA_FILE}"
  --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=${INSTANCE_NAME_PREFIX}}]"
  --query "Instances[].InstanceId"
  --output text
)

if [[ -n "$KEY_NAME" ]]; then
  run_cmd+=(--key-name "$KEY_NAME")
fi
if [[ -n "$IAM_INSTANCE_PROFILE" ]]; then
  run_cmd+=(--iam-instance-profile "Name=${IAM_INSTANCE_PROFILE}")
fi

instance_ids="$("${run_cmd[@]}")"
if [[ -z "$instance_ids" ]]; then
  echo "No instance IDs returned from run-instances." >&2
  exit 1
fi

echo "Launched worker instances:"
echo "$instance_ids"

if [[ "$WAIT_FOR_RUNNING" -eq 1 ]]; then
  aws ec2 wait instance-running "${region_args[@]}" --instance-ids $instance_ids
  echo
  echo "Worker instance networking:"
  aws ec2 describe-instances "${region_args[@]}" --instance-ids $instance_ids \
    --query "Reservations[].Instances[].{id:InstanceId,state:State.Name,public_ip:PublicIpAddress,private_ip:PrivateIpAddress}" \
    --output table
fi

echo
echo "Each worker VM will auto-bootstrap from cloud-init and join coordinator ${COORDINATOR_BASE_URL}."
