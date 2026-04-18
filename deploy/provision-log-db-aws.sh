#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DEPLOY_DIR="${ROOT_DIR}/deploy"

ENV_FILE="${DEPLOY_DIR}/.env"
COMPOSE_FILE="${DEPLOY_DIR}/docker-compose.central.yml"
REGION=""
AMI_ID=""
INSTANCE_TYPE="t3.small"
SUBNET_ID=""
SECURITY_GROUP_IDS=""
KEY_NAME=""
IAM_INSTANCE_PROFILE=""
INSTANCE_NAME_PREFIX="nightmare-log-db"
DB_NAME="nightmare_logs"
DB_USER="nightmare_logs"
DB_PASSWORD=""
WAIT_FOR_RUNNING=1
SKIP_REBUILD_CENTRAL=0

usage() {
  cat <<'USAGE'
Usage:
  ./deploy/provision-log-db-aws.sh \
    --ami-id ami-xxxxxxxx \
    --subnet-id subnet-xxxxxxxx \
    --security-group-ids sg-aaaa,sg-bbbb \
    [--instance-type t3.small] \
    [--key-name my-key] \
    [--iam-instance-profile ec2-role] \
    [--region us-east-1] \
    [--db-name nightmare_logs] \
    [--db-user nightmare_logs] \
    [--db-password <strong-password>] \
    [--env-file deploy/.env] \
    [--compose-file deploy/docker-compose.central.yml] \
    [--skip-rebuild-central]

What it does:
  1. Provisions one EC2 VM for dedicated log/reporting Postgres.
  2. Boots Postgres in Docker on that VM via cloud-init.
  3. Writes/updates LOG_DATABASE_URL in deploy/.env.
  4. Rebuilds central server container unless --skip-rebuild-central is set.
USAGE
}

require_cmd() {
  local name="$1"
  if ! command -v "$name" >/dev/null 2>&1; then
    echo "Missing required command: $name" >&2
    exit 1
  fi
}

restore_invoking_user_ownership() {
  local target_path="$1"
  if [[ "${EUID:-$(id -u)}" -eq 0 && -n "${SUDO_USER:-}" && "${SUDO_USER}" != "root" ]]; then
    local group_name
    group_name="$(id -gn "${SUDO_USER}" 2>/dev/null || true)"
    if [[ -z "$group_name" ]]; then
      group_name="${SUDO_USER}"
    fi
    chown "${SUDO_USER}:${group_name}" "$target_path" 2>/dev/null || true
  fi
}

gen_password() {
  if command -v openssl >/dev/null 2>&1; then
    openssl rand -hex 24
  else
    python3 - <<'PY'
import secrets
print(secrets.token_hex(24))
PY
  fi
}

load_key_from_env_file() {
  local key="$1"
  local file="$2"
  if [[ ! -f "$file" ]]; then
    return 0
  fi
  awk -F= -v k="$key" '$1==k{print substr($0, index($0, "=")+1); exit}' "$file"
}

set_env_key() {
  local file="$1"
  local key="$2"
  local value="$3"
  local tmp
  tmp="$(mktemp "${file}.XXXXXX")"
  if [[ -f "$file" ]]; then
    awk -F= -v k="$key" '$1!=k{print $0}' "$file" >"$tmp"
  fi
  printf "%s=%s\n" "$key" "$value" >>"$tmp"
  mv "$tmp" "$file"
  chmod 600 "$file" 2>/dev/null || true
  restore_invoking_user_ownership "$file"
}

resolve_compose_cmd() {
  if docker compose version >/dev/null 2>&1; then
    echo "docker compose"
    return 0
  fi
  if command -v docker-compose >/dev/null 2>&1; then
    echo "docker-compose"
    return 0
  fi
  return 1
}

run_compose() {
  local compose_cmd="$1"
  shift
  if [[ "$compose_cmd" == "docker compose" ]]; then
    docker compose "$@"
  else
    docker-compose "$@"
  fi
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --ami-id) AMI_ID="${2:-}"; shift 2 ;;
    --instance-type) INSTANCE_TYPE="${2:-t3.small}"; shift 2 ;;
    --subnet-id) SUBNET_ID="${2:-}"; shift 2 ;;
    --security-group-ids) SECURITY_GROUP_IDS="${2:-}"; shift 2 ;;
    --key-name) KEY_NAME="${2:-}"; shift 2 ;;
    --iam-instance-profile) IAM_INSTANCE_PROFILE="${2:-}"; shift 2 ;;
    --region) REGION="${2:-}"; shift 2 ;;
    --instance-name-prefix) INSTANCE_NAME_PREFIX="${2:-nightmare-log-db}"; shift 2 ;;
    --db-name) DB_NAME="${2:-nightmare_logs}"; shift 2 ;;
    --db-user) DB_USER="${2:-nightmare_logs}"; shift 2 ;;
    --db-password) DB_PASSWORD="${2:-}"; shift 2 ;;
    --env-file) ENV_FILE="${2:-}"; shift 2 ;;
    --compose-file) COMPOSE_FILE="${2:-}"; shift 2 ;;
    --skip-rebuild-central) SKIP_REBUILD_CENTRAL=1; shift ;;
    --no-wait) WAIT_FOR_RUNNING=0; shift ;;
    --help|-h) usage; exit 0 ;;
    *)
      echo "Unknown option: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ -z "$REGION" ]]; then
  REGION="$(load_key_from_env_file "AWS_REGION" "$ENV_FILE")"
fi
region_args=()
if [[ -n "$REGION" ]]; then
  region_args=(--region "$REGION")
fi

if [[ -z "$DB_PASSWORD" ]]; then
  DB_PASSWORD="$(gen_password)"
fi

existing_log_db_url="$(load_key_from_env_file "LOG_DATABASE_URL" "$ENV_FILE")"
if [[ -n "$existing_log_db_url" ]]; then
  echo "LOG_DATABASE_URL already exists in ${ENV_FILE}; skipping VM provisioning."
  if [[ "$SKIP_REBUILD_CENTRAL" -eq 0 ]]; then
    compose_cmd="$(resolve_compose_cmd || true)"
    if [[ -z "$compose_cmd" ]]; then
      echo "docker compose/docker-compose is required to rebuild central server." >&2
      exit 1
    fi
    run_compose "$compose_cmd" -f "$COMPOSE_FILE" --env-file "$ENV_FILE" up -d --build server
  fi
  exit 0
fi

for required in AMI_ID SUBNET_ID SECURITY_GROUP_IDS; do
  if [[ -z "${!required}" ]]; then
    echo "Missing required option: --$(echo "$required" | tr 'A-Z_' 'a-z-')" >&2
    exit 2
  fi
done

require_cmd aws
require_cmd bash
require_cmd mktemp

if ! aws sts get-caller-identity "${region_args[@]}" >/dev/null 2>&1; then
  echo "AWS credentials are not configured for this shell." >&2
  exit 1
fi

USER_DATA_FILE="$(mktemp "${DEPLOY_DIR}/log-db-user-data.XXXXXX.yaml")"
trap 'rm -f "$USER_DATA_FILE"' EXIT

cat > "$USER_DATA_FILE" <<EOF
#cloud-config
runcmd:
  - [bash, -lc, "set -euo pipefail; if command -v yum >/dev/null 2>&1; then yum makecache -y || true; yum install -y ca-certificates curl-minimal docker; elif command -v dnf >/dev/null 2>&1; then dnf makecache -y || true; dnf install -y ca-certificates curl-minimal docker; elif command -v apt-get >/dev/null 2>&1; then apt-get update; apt-get install -y ca-certificates curl docker.io; else echo 'no supported package manager' >&2; exit 1; fi"]
  - [bash, -lc, "systemctl enable --now docker"]
  - [bash, -lc, "docker volume create nightmare-log-db-data || true"]
  - [bash, -lc, "docker rm -f nightmare-log-postgres >/dev/null 2>&1 || true"]
  - [bash, -lc, "docker run -d --name nightmare-log-postgres --restart unless-stopped -e POSTGRES_DB='${DB_NAME}' -e POSTGRES_USER='${DB_USER}' -e POSTGRES_PASSWORD='${DB_PASSWORD}' -p 5432:5432 -v nightmare-log-db-data:/var/lib/postgresql/data postgres:16"]
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
  --count 1
  --instance-type "$INSTANCE_TYPE"
  --subnet-id "$SUBNET_ID"
  --security-group-ids "${sg_ids[@]}"
  --user-data "file://${USER_DATA_FILE}"
  --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=${INSTANCE_NAME_PREFIX}}]"
  --query "Instances[0].InstanceId"
  --output text
)

if [[ -n "$KEY_NAME" ]]; then
  run_cmd+=(--key-name "$KEY_NAME")
fi
if [[ -n "$IAM_INSTANCE_PROFILE" ]]; then
  run_cmd+=(--iam-instance-profile "Name=${IAM_INSTANCE_PROFILE}")
fi

instance_id="$("${run_cmd[@]}")"
if [[ -z "$instance_id" || "$instance_id" == "None" ]]; then
  echo "Failed to provision log DB instance." >&2
  exit 1
fi

echo "Provisioned log DB VM: ${instance_id}"
if [[ "$WAIT_FOR_RUNNING" -eq 1 ]]; then
  aws ec2 wait instance-running "${region_args[@]}" --instance-ids "$instance_id"
fi

private_ip="$(aws ec2 describe-instances "${region_args[@]}" --instance-ids "$instance_id" --query "Reservations[0].Instances[0].PrivateIpAddress" --output text)"
if [[ -z "$private_ip" || "$private_ip" == "None" ]]; then
  echo "Could not determine private IP for ${instance_id}" >&2
  exit 1
fi

log_database_url="postgresql://${DB_USER}:${DB_PASSWORD}@${private_ip}:5432/${DB_NAME}"
set_env_key "$ENV_FILE" "LOG_DATABASE_URL" "$log_database_url"
if [[ -n "$REGION" ]]; then
  set_env_key "$ENV_FILE" "AWS_REGION" "$REGION"
fi

echo "Wrote LOG_DATABASE_URL to ${ENV_FILE}"

if [[ "$SKIP_REBUILD_CENTRAL" -eq 0 ]]; then
  compose_cmd="$(resolve_compose_cmd || true)"
  if [[ -z "$compose_cmd" ]]; then
    echo "docker compose/docker-compose is required to rebuild central server." >&2
    exit 1
  fi
  run_compose "$compose_cmd" -f "$COMPOSE_FILE" --env-file "$ENV_FILE" up -d --build server
  echo "Rebuilt central server with dedicated logging DB configuration."
fi

echo "instance_id=${instance_id}"
echo "private_ip=${private_ip}"
echo "log_database_url=${log_database_url}"
