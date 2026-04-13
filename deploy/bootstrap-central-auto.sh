#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DEPLOY_DIR="${ROOT_DIR}/deploy"
TLS_DIR="${DEPLOY_DIR}/tls"
ENV_FILE="${DEPLOY_DIR}/.env"
WORKER_ENV_FILE="${DEPLOY_DIR}/worker.env.generated"

POSTGRES_DB_DEFAULT="nightmare"
POSTGRES_USER_DEFAULT="nightmare"

FORCE_REGEN=0
BASE_URL_OVERRIDE=""
CERT_DAYS=825
AUTO_PROVISION_WORKERS=0
AWS_AMI_ID=""
AWS_INSTANCE_TYPE="t3.small"
AWS_SUBNET_ID=""
AWS_SECURITY_GROUP_IDS=""
AWS_KEY_NAME=""
AWS_IAM_INSTANCE_PROFILE=""
AWS_REGION=""
REPO_URL=""
REPO_BRANCH="main"
COMPOSE_CMD=""
DOCKER_USE_SUDO=0

usage() {
  cat <<'USAGE'
Usage:
  ./deploy/bootstrap-central-auto.sh [--base-url https://host-or-ip] [--force]
  ./deploy/bootstrap-central-auto.sh [--auto-provision-workers 20 --aws-ami-id ami-... --aws-subnet-id subnet-... --aws-security-group-ids sg-... --repo-url https://...]

What it does:
  - generates strong Postgres password + coordinator API token
  - auto-detects public host/IP (EC2 metadata first, external IP fallback)
  - generates self-signed TLS cert/key (unless already present)
  - writes deploy/.env
  - writes deploy/worker.env.generated (for worker VMs)
  - rebuilds and starts central docker compose stack
  - optionally launches worker EC2 instances and auto-configures them via cloud-init
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --base-url)
      BASE_URL_OVERRIDE="${2:-}"
      shift 2
      ;;
    --force)
      FORCE_REGEN=1
      shift
      ;;
    --auto-provision-workers)
      AUTO_PROVISION_WORKERS="${2:-0}"
      shift 2
      ;;
    --aws-ami-id)
      AWS_AMI_ID="${2:-}"
      shift 2
      ;;
    --aws-instance-type)
      AWS_INSTANCE_TYPE="${2:-t3.small}"
      shift 2
      ;;
    --aws-subnet-id)
      AWS_SUBNET_ID="${2:-}"
      shift 2
      ;;
    --aws-security-group-ids)
      AWS_SECURITY_GROUP_IDS="${2:-}"
      shift 2
      ;;
    --aws-key-name)
      AWS_KEY_NAME="${2:-}"
      shift 2
      ;;
    --aws-iam-instance-profile)
      AWS_IAM_INSTANCE_PROFILE="${2:-}"
      shift 2
      ;;
    --aws-region)
      AWS_REGION="${2:-}"
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

if ! [[ "$AUTO_PROVISION_WORKERS" =~ ^[0-9]+$ ]]; then
  echo "--auto-provision-workers must be an integer >= 0" >&2
  exit 2
fi

require_cmd() {
  local name="$1"
  if ! command -v "$name" >/dev/null 2>&1; then
    echo "Missing required command: $name" >&2
    exit 1
  fi
}

resolve_compose_cmd() {
  if docker compose version >/dev/null 2>&1; then
    COMPOSE_CMD="docker compose"
    return 0
  fi
  if command -v docker-compose >/dev/null 2>&1; then
    COMPOSE_CMD="docker-compose"
    return 0
  fi
  COMPOSE_CMD=""
  return 1
}

detect_docker_access_mode() {
  if docker info >/dev/null 2>&1; then
    DOCKER_USE_SUDO=0
    return 0
  fi
  if command -v sudo >/dev/null 2>&1 && sudo docker info >/dev/null 2>&1; then
    DOCKER_USE_SUDO=1
    echo "Docker socket requires elevated access; using sudo for compose commands."
    return 0
  fi
  echo "Docker daemon is unreachable (no direct or sudo access)." >&2
  return 1
}

run_compose() {
  if [[ "$DOCKER_USE_SUDO" -eq 1 ]]; then
    if [[ "$COMPOSE_CMD" == "docker compose" ]]; then
      sudo docker compose "$@"
    else
      sudo docker-compose "$@"
    fi
  else
    if [[ "$COMPOSE_CMD" == "docker compose" ]]; then
      docker compose "$@"
    else
      docker-compose "$@"
    fi
  fi
}

install_standalone_compose_if_needed() {
  if resolve_compose_cmd; then
    return 0
  fi

  local sudo_cmd=()
  if [[ "${EUID:-$(id -u)}" -ne 0 ]]; then
    if command -v sudo >/dev/null 2>&1; then
      sudo_cmd=(sudo)
    else
      echo "Run as root or install sudo to install docker-compose standalone." >&2
      return 1
    fi
  fi

  local arch
  arch="$(uname -m)"
  case "$arch" in
    x86_64|amd64) arch="x86_64" ;;
    aarch64|arm64) arch="aarch64" ;;
    *)
      echo "Unsupported architecture for standalone docker-compose: $arch" >&2
      return 1
      ;;
  esac

  local version="v2.29.7"
  local url="https://github.com/docker/compose/releases/download/${version}/docker-compose-linux-${arch}"
  echo "Installing standalone docker-compose ${version} for ${arch}..."
  "${sudo_cmd[@]}" curl -fL "$url" -o /usr/local/bin/docker-compose
  "${sudo_cmd[@]}" chmod +x /usr/local/bin/docker-compose

  if ! resolve_compose_cmd; then
    echo "docker compose is still unavailable after standalone install." >&2
    return 1
  fi
}

install_deps_if_missing() {
  local missing=()
  local needs_compose=0

  for cmd in docker curl openssl; do
    if ! command -v "$cmd" >/dev/null 2>&1; then
      missing+=("$cmd")
    fi
  done
  if ! docker compose version >/dev/null 2>&1; then
    needs_compose=1
  fi

  if [[ "${#missing[@]}" -eq 0 && "$needs_compose" -eq 0 ]]; then
    return 0
  fi

  echo "Installing missing dependencies (docker/curl/openssl/docker compose)..."
  local sudo_cmd=()
  if [[ "${EUID:-$(id -u)}" -ne 0 ]]; then
    if command -v sudo >/dev/null 2>&1; then
      sudo_cmd=(sudo)
    else
      echo "Run as root or install sudo to auto-install dependencies." >&2
      return 1
    fi
  fi

  if command -v yum >/dev/null 2>&1; then
    "${sudo_cmd[@]}" yum makecache -y || true
    "${sudo_cmd[@]}" yum install -y ca-certificates curl-minimal openssl git docker
  elif command -v dnf >/dev/null 2>&1; then
    "${sudo_cmd[@]}" dnf makecache -y || true
    "${sudo_cmd[@]}" dnf install -y ca-certificates curl-minimal openssl git docker
  elif command -v apt-get >/dev/null 2>&1; then
    "${sudo_cmd[@]}" apt-get update
    "${sudo_cmd[@]}" apt-get install -y ca-certificates curl openssl git docker.io docker-compose-plugin
  else
    echo "No supported package manager found (yum/dnf/apt-get)." >&2
    return 1
  fi

  if command -v systemctl >/dev/null 2>&1; then
    "${sudo_cmd[@]}" systemctl enable --now docker || true
  fi

  if ! install_standalone_compose_if_needed; then
    echo "docker compose is still unavailable after package install." >&2
    return 1
  fi
}

abs_path() {
  local p="$1"
  cd "$(dirname "$p")"
  echo "$(pwd -P)/$(basename "$p")"
}

gen_password() {
  openssl rand -hex 32
}

gen_token() {
  openssl rand -hex 48
}

metadata_get() {
  local path="$1"
  local token=""
  token="$(curl -fsS -m 2 -X PUT "http://169.254.169.254/latest/api/token" -H "X-aws-ec2-metadata-token-ttl-seconds: 60" || true)"
  if [[ -n "$token" ]]; then
    curl -fsS -m 2 -H "X-aws-ec2-metadata-token: ${token}" "http://169.254.169.254/latest/meta-data/${path}" || true
    return 0
  fi
  curl -fsS -m 2 "http://169.254.169.254/latest/meta-data/${path}" || true
}

detect_base_url() {
  if [[ -n "$BASE_URL_OVERRIDE" ]]; then
    echo "$BASE_URL_OVERRIDE"
    return 0
  fi

  local public_hostname public_ip
  public_hostname="$(metadata_get public-hostname | tr -d '\r\n')"
  public_ip="$(metadata_get public-ipv4 | tr -d '\r\n')"

  if [[ -n "$public_hostname" ]]; then
    echo "https://${public_hostname}"
    return 0
  fi
  if [[ -n "$public_ip" ]]; then
    echo "https://${public_ip}"
    return 0
  fi

  public_ip="$(curl -fsS -m 4 https://checkip.amazonaws.com 2>/dev/null | tr -d '\r\n' || true)"
  if [[ -n "$public_ip" ]]; then
    echo "https://${public_ip}"
    return 0
  fi

  local host_name
  host_name="$(hostname -f 2>/dev/null || hostname)"
  host_name="$(echo "$host_name" | tr -d '\r\n')"
  if [[ -n "$host_name" ]]; then
    echo "https://${host_name}"
    return 0
  fi

  echo "https://127.0.0.1"
}

build_san() {
  local host_no_scheme="$1"
  if [[ "$host_no_scheme" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    echo "IP:${host_no_scheme},DNS:localhost,IP:127.0.0.1"
  else
    echo "DNS:${host_no_scheme},DNS:localhost,IP:127.0.0.1"
  fi
}

generate_cert_if_needed() {
  local cert_file="$1"
  local key_file="$2"
  local cn="$3"
  local san="$4"

  if [[ -f "$cert_file" && -f "$key_file" && "$FORCE_REGEN" -ne 1 ]]; then
    return 0
  fi

  mkdir -p "$TLS_DIR"
  openssl req -x509 -newkey rsa:4096 -sha256 -nodes \
    -keyout "$key_file" \
    -out "$cert_file" \
    -days "$CERT_DAYS" \
    -subj "/CN=${cn}" \
    -addext "subjectAltName=${san}" \
    >/dev/null 2>&1
  chmod 600 "$key_file"
}

install_deps_if_missing
require_cmd docker
require_cmd openssl
require_cmd curl
resolve_compose_cmd || {
  echo "docker compose (or docker-compose) is required." >&2
  exit 1
}
detect_docker_access_mode || exit 1

mkdir -p "$TLS_DIR"
POSTGRES_DB="${POSTGRES_DB_DEFAULT}"
POSTGRES_USER="${POSTGRES_USER_DEFAULT}"
POSTGRES_PASSWORD="$(gen_password)"
COORDINATOR_API_TOKEN="$(gen_token)"
COORDINATOR_BASE_URL="$(detect_base_url)"
BASE_HOST="${COORDINATOR_BASE_URL#https://}"
BASE_HOST="${BASE_HOST#http://}"
BASE_HOST="${BASE_HOST%%/*}"

CERT_FILE="${TLS_DIR}/server.crt"
KEY_FILE="${TLS_DIR}/server.key"
generate_cert_if_needed "$CERT_FILE" "$KEY_FILE" "$BASE_HOST" "$(build_san "$BASE_HOST")"

TLS_CERT_FILE="$(abs_path "$CERT_FILE")"
TLS_KEY_FILE="$(abs_path "$KEY_FILE")"

cat >"$ENV_FILE" <<EOF
POSTGRES_DB=${POSTGRES_DB}
POSTGRES_USER=${POSTGRES_USER}
POSTGRES_PASSWORD=${POSTGRES_PASSWORD}
COORDINATOR_API_TOKEN=${COORDINATOR_API_TOKEN}
TLS_CERT_FILE=${TLS_CERT_FILE}
TLS_KEY_FILE=${TLS_KEY_FILE}
COORDINATOR_BASE_URL=${COORDINATOR_BASE_URL}
EOF
chmod 600 "$ENV_FILE"

cat >"$WORKER_ENV_FILE" <<EOF
COORDINATOR_BASE_URL=${COORDINATOR_BASE_URL}
COORDINATOR_API_TOKEN=${COORDINATOR_API_TOKEN}
EOF
chmod 600 "$WORKER_ENV_FILE"

cd "$DEPLOY_DIR"
run_compose -f docker-compose.central.yml --env-file .env up -d --build

echo "Central stack is running."
echo "Generated files:"
echo "  - ${ENV_FILE}"
echo "  - ${WORKER_ENV_FILE}"
echo "  - ${CERT_FILE}"
echo "  - ${KEY_FILE}"
echo
echo "Use ${WORKER_ENV_FILE} on each worker VM as deploy/.env."

if [[ "$AUTO_PROVISION_WORKERS" -gt 0 ]]; then
  PROVISION_SCRIPT="${DEPLOY_DIR}/provision-workers-aws.sh"
  if [[ ! -x "$PROVISION_SCRIPT" ]]; then
    echo "Missing executable ${PROVISION_SCRIPT}" >&2
    exit 1
  fi
  provision_cmd=(
    "$PROVISION_SCRIPT"
    --count "$AUTO_PROVISION_WORKERS"
    --ami-id "$AWS_AMI_ID"
    --instance-type "$AWS_INSTANCE_TYPE"
    --subnet-id "$AWS_SUBNET_ID"
    --security-group-ids "$AWS_SECURITY_GROUP_IDS"
    --repo-url "$REPO_URL"
    --repo-branch "$REPO_BRANCH"
    --coordinator-base-url "$COORDINATOR_BASE_URL"
    --api-token "$COORDINATOR_API_TOKEN"
  )
  if [[ -n "$AWS_KEY_NAME" ]]; then
    provision_cmd+=(--key-name "$AWS_KEY_NAME")
  fi
  if [[ -n "$AWS_IAM_INSTANCE_PROFILE" ]]; then
    provision_cmd+=(--iam-instance-profile "$AWS_IAM_INSTANCE_PROFILE")
  fi
  if [[ -n "$AWS_REGION" ]]; then
    provision_cmd+=(--region "$AWS_REGION")
  fi
  "${provision_cmd[@]}"
fi
