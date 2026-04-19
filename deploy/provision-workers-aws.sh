#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DEPLOY_DIR="${ROOT_DIR}/deploy"

COUNT=0
AMI_ID=""
INSTANCE_TYPE="m7i-flex.large"
ROOT_VOLUME_SIZE_GB=50
SUBNET_ID=""
SECURITY_GROUP_IDS=""
KEY_NAME=""
IAM_INSTANCE_PROFILE=""
REGION=""
REPO_URL=""
REPO_BRANCH="main"
COORDINATOR_BASE_URL=""
API_TOKEN=""
COORDINATOR_INSECURE_TLS=""
INSTANCE_NAME_PREFIX="nightmare-worker"
WAIT_FOR_RUNNING=1
region_args=()

CENTRAL_ENV_FILE="${DEPLOY_DIR}/.env"
WORKER_ENV_FILE="${DEPLOY_DIR}/worker.env.generated"

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
    [--coordinator-insecure-tls true] \
    [--instance-type m7i-flex.large] [--repo-branch main] [--key-name keypair] [--iam-instance-profile profile] [--region us-east-1]

This script launches worker EC2 instances and bootstraps each VM with cloud-init to:
  - install docker + git,
  - clone the repo,
  - write deploy/.env with coordinator URL/token,
  - run docker compose worker stack.

Most required values can be provided either as CLI flags or in deploy/.env
(for example AWS_AMI_ID, AWS_SUBNET_ID, AWS_SECURITY_GROUP_IDS, REPO_URL,
COORDINATOR_BASE_URL, COORDINATOR_API_TOKEN, AUTO_PROVISION_WORKERS).
USAGE
}

require_cmd() {
  local name="$1"
  if ! command -v "$name" >/dev/null 2>&1; then
    echo "Missing required command: $name" >&2
    exit 1
  fi
}

load_key_from_env_file() {
  local key="$1"
  local env_file="$2"
  if [[ ! -f "$env_file" ]]; then
    return 0
  fi
  awk -F= -v k="$key" '$1==k{print substr($0, index($0, "=")+1); exit}' "$env_file"
}

load_key_from_env_files() {
  local key="$1"
  shift
  local f
  for f in "$@"; do
    local value
    value="$(load_key_from_env_file "$key" "$f")"
    if [[ -n "$value" ]]; then
      echo "$value"
      return 0
    fi
  done
  return 0
}

ensure_aws_credentials() {
  if aws sts get-caller-identity "${region_args[@]}" >/dev/null 2>&1; then
    return 0
  fi
  cat >&2 <<'EOF'
AWS credentials are not configured for this shell/instance.

Recommended on EC2:
  - Attach an IAM instance profile role to this central VM.
  - Include permissions: ec2:RunInstances, ec2:DescribeInstances, ec2:CreateTags, ec2:DescribeSubnets, ec2:DescribeSecurityGroups, iam:PassRole (if --iam-instance-profile is used).

Alternatives:
  - Configure AWS CLI credentials (`aws configure`), or
  - Configure IAM Identity Center profile (`aws configure sso`) and login (`aws sso login --profile <name>`), then set `AWS_PROFILE=<name>`.
EOF
  return 1
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
      INSTANCE_TYPE="${2:-m7i-flex.large}"
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
    --coordinator-insecure-tls)
      COORDINATOR_INSECURE_TLS="${2:-}"
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
  env_count="$(load_key_from_env_files "AUTO_PROVISION_WORKERS" "$CENTRAL_ENV_FILE" "$WORKER_ENV_FILE")"
  if [[ -z "$env_count" ]]; then
    env_count="$(load_key_from_env_files "WORKER_COUNT" "$CENTRAL_ENV_FILE" "$WORKER_ENV_FILE")"
  fi
  if [[ -n "$env_count" ]]; then
    COUNT="$env_count"
  fi
fi
if [[ -z "$AMI_ID" ]]; then
  AMI_ID="$(load_key_from_env_files "AWS_AMI_ID" "$CENTRAL_ENV_FILE" "$WORKER_ENV_FILE")"
fi
if [[ -z "$AMI_ID" ]]; then
  AMI_ID="$(load_key_from_env_files "AMI_ID" "$CENTRAL_ENV_FILE" "$WORKER_ENV_FILE")"
fi
if [[ -z "$INSTANCE_TYPE" || "$INSTANCE_TYPE" == "m7i-flex.large" ]]; then
  env_instance_type="$(load_key_from_env_files "AWS_INSTANCE_TYPE" "$CENTRAL_ENV_FILE" "$WORKER_ENV_FILE")"
  if [[ -z "$env_instance_type" ]]; then
    env_instance_type="$(load_key_from_env_files "INSTANCE_TYPE" "$CENTRAL_ENV_FILE" "$WORKER_ENV_FILE")"
  fi
  if [[ -n "$env_instance_type" ]]; then
    INSTANCE_TYPE="$env_instance_type"
  fi
fi
if [[ -z "$SUBNET_ID" ]]; then
  SUBNET_ID="$(load_key_from_env_files "AWS_SUBNET_ID" "$CENTRAL_ENV_FILE" "$WORKER_ENV_FILE")"
fi
if [[ -z "$SUBNET_ID" ]]; then
  SUBNET_ID="$(load_key_from_env_files "SUBNET_ID" "$CENTRAL_ENV_FILE" "$WORKER_ENV_FILE")"
fi
if [[ -z "$SECURITY_GROUP_IDS" ]]; then
  SECURITY_GROUP_IDS="$(load_key_from_env_files "AWS_SECURITY_GROUP_IDS" "$CENTRAL_ENV_FILE" "$WORKER_ENV_FILE")"
fi
if [[ -z "$SECURITY_GROUP_IDS" ]]; then
  SECURITY_GROUP_IDS="$(load_key_from_env_files "SECURITY_GROUP_IDS" "$CENTRAL_ENV_FILE" "$WORKER_ENV_FILE")"
fi
if [[ -z "$KEY_NAME" ]]; then
  KEY_NAME="$(load_key_from_env_files "AWS_KEY_NAME" "$CENTRAL_ENV_FILE" "$WORKER_ENV_FILE")"
fi
if [[ -z "$KEY_NAME" ]]; then
  KEY_NAME="$(load_key_from_env_files "KEY_NAME" "$CENTRAL_ENV_FILE" "$WORKER_ENV_FILE")"
fi
if [[ -z "$IAM_INSTANCE_PROFILE" ]]; then
  IAM_INSTANCE_PROFILE="$(load_key_from_env_files "AWS_IAM_INSTANCE_PROFILE" "$CENTRAL_ENV_FILE" "$WORKER_ENV_FILE")"
fi
if [[ -z "$IAM_INSTANCE_PROFILE" ]]; then
  IAM_INSTANCE_PROFILE="$(load_key_from_env_files "IAM_INSTANCE_PROFILE" "$CENTRAL_ENV_FILE" "$WORKER_ENV_FILE")"
fi
if [[ -z "$REPO_URL" ]]; then
  REPO_URL="$(load_key_from_env_files "REPO_URL" "$CENTRAL_ENV_FILE" "$WORKER_ENV_FILE")"
fi
if [[ -z "$REPO_BRANCH" || "$REPO_BRANCH" == "main" ]]; then
  env_repo_branch="$(load_key_from_env_files "REPO_BRANCH" "$CENTRAL_ENV_FILE" "$WORKER_ENV_FILE")"
  if [[ -n "$env_repo_branch" ]]; then
    REPO_BRANCH="$env_repo_branch"
  fi
fi
if [[ -z "$COORDINATOR_BASE_URL" ]]; then
  COORDINATOR_BASE_URL="$(load_key_from_env_files "COORDINATOR_BASE_URL" "$CENTRAL_ENV_FILE" "$WORKER_ENV_FILE")"
fi
if [[ -z "$API_TOKEN" ]]; then
  API_TOKEN="$(load_key_from_env_files "COORDINATOR_API_TOKEN" "$CENTRAL_ENV_FILE" "$WORKER_ENV_FILE")"
fi
if [[ -z "$COORDINATOR_INSECURE_TLS" ]]; then
  COORDINATOR_INSECURE_TLS="$(load_key_from_env_files "COORDINATOR_INSECURE_TLS" "$CENTRAL_ENV_FILE" "$WORKER_ENV_FILE")"
fi
if [[ -z "$COORDINATOR_INSECURE_TLS" ]]; then
  COORDINATOR_INSECURE_TLS="true"
fi
case "${COORDINATOR_INSECURE_TLS,,}" in
  true|1|yes|y|on)
    COORDINATOR_INSECURE_TLS="true"
    ;;
  false|0|no|n|off)
    COORDINATOR_INSECURE_TLS="false"
    ;;
  *)
    echo "--coordinator-insecure-tls must be a boolean (true/false)" >&2
    exit 2
    ;;
esac
if [[ -z "$REGION" ]]; then
  REGION="$(load_key_from_env_files "AWS_REGION" "$CENTRAL_ENV_FILE" "$WORKER_ENV_FILE")"
fi
if ! [[ "$COUNT" =~ ^[0-9]+$ ]] || [[ "$COUNT" -lt 1 ]]; then
  echo "--count must be an integer >= 1 (or set AUTO_PROVISION_WORKERS/WORKER_COUNT in ${CENTRAL_ENV_FILE})" >&2
  exit 2
fi
if [[ -n "$COORDINATOR_BASE_URL" && -n "$API_TOKEN" ]]; then
  echo "Using coordinator settings from local env files."
fi

for required in AMI_ID SUBNET_ID SECURITY_GROUP_IDS REPO_URL COORDINATOR_BASE_URL API_TOKEN; do
  if [[ -z "${!required}" ]]; then
    echo "Missing required option for provisioning: --$(echo "$required" | tr 'A-Z_' 'a-z-')" >&2
    echo "Tip: this value can be provided by CLI or by these files:" >&2
    echo "  - ${CENTRAL_ENV_FILE}" >&2
    echo "  - ${WORKER_ENV_FILE}" >&2
    exit 2
  fi
done

require_cmd aws
if [[ -n "$REGION" ]]; then
  region_args=(--region "$REGION")
fi
ensure_aws_credentials

mkdir -p "$DEPLOY_DIR"
USER_DATA_FILE="$(mktemp "${DEPLOY_DIR}/worker-user-data.XXXXXX.yaml")"
trap 'rm -f "$USER_DATA_FILE"' EXIT

cat > "$USER_DATA_FILE" <<EOF
#cloud-config
runcmd:
  - [bash, -lc, "set -euo pipefail; if command -v yum >/dev/null 2>&1; then yum makecache -y || true; yum install -y ca-certificates curl-minimal git docker openssl; elif command -v dnf >/dev/null 2>&1; then dnf makecache -y || true; dnf install -y ca-certificates curl-minimal git docker openssl; elif command -v apt-get >/dev/null 2>&1; then apt-get update; apt-get install -y ca-certificates curl git docker.io docker-compose-plugin openssl; else echo 'no supported package manager' >&2; exit 1; fi"]
  - [bash, -lc, "systemctl enable --now docker"]
  - [bash, -lc, "if docker compose version >/dev/null 2>&1; then COMPOSE_CMD='docker compose'; elif command -v docker-compose >/dev/null 2>&1; then COMPOSE_CMD='docker-compose'; else ARCH=\$(uname -m); if [ \"\$ARCH\" = \"x86_64\" ] || [ \"\$ARCH\" = \"amd64\" ]; then ARCH='x86_64'; elif [ \"\$ARCH\" = \"aarch64\" ] || [ \"\$ARCH\" = \"arm64\" ]; then ARCH='aarch64'; else echo \"unsupported arch: \$ARCH\" >&2; exit 1; fi; curl -fL \"https://github.com/docker/compose/releases/download/v2.29.7/docker-compose-linux-\$ARCH\" -o /usr/local/bin/docker-compose; chmod +x /usr/local/bin/docker-compose; COMPOSE_CMD='docker-compose'; fi; echo \"compose=\$COMPOSE_CMD\" >/var/log/nightmare-compose.txt"]
  - [bash, -lc, "mkdir -p /opt/nightmare"]
  - [bash, -lc, "if [ ! -d /opt/nightmare/.git ]; then git clone --depth 1 --branch '${REPO_BRANCH}' '${REPO_URL}' /opt/nightmare; fi"]
  - [bash, -lc, "cd /opt/nightmare && git fetch --all --prune && git checkout '${REPO_BRANCH}' && git pull --ff-only || true"]
  - [bash, -lc, "cat > /opt/nightmare/deploy/.env <<'ENVEOF'\nCOORDINATOR_BASE_URL=${COORDINATOR_BASE_URL}\nCOORDINATOR_API_TOKEN=${API_TOKEN}\nCOORDINATOR_INSECURE_TLS=${COORDINATOR_INSECURE_TLS}\nENVEOF"]
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
  --block-device-mappings "[{\"DeviceName\":\"/dev/xvda\",\"Ebs\":{\"VolumeSize\":${ROOT_VOLUME_SIZE_GB},\"VolumeType\":\"gp3\",\"DeleteOnTermination\":true}}]"
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
