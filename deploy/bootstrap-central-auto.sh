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
LOG_DATABASE_URL=""
COMPOSE_CMD=""
DOCKER_USE_SUDO=0

usage() {
  cat <<'USAGE'
Usage:
  ./deploy/bootstrap-central-auto.sh [--base-url https://host-or-ip] [--force]
  ./deploy/bootstrap-central-auto.sh [--auto-provision-workers 20 --aws-ami-id ami-... --aws-subnet-id subnet-... --aws-security-group-ids sg-... --repo-url https://...]

What it does:
  - reuses existing Postgres credentials from deploy/.env when present
  - only generates new Postgres password + coordinator API token for a fresh install (or --force)
  - auto-detects public host/IP (EC2 metadata first, external IP fallback)
  - generates self-signed TLS cert/key (unless already present)
  - writes deploy/.env
  - writes deploy/worker.env.generated (for worker VMs)
  - ensures a dedicated log/reporting Postgres VM exists and writes LOG_DATABASE_URL
  - installs Python 3, pip, and pip install -r requirements.txt on this host (for client.py, etc.)
  - rebuilds and starts central docker compose stack
  - optionally launches worker EC2 instances and auto-configures them via cloud-init
USAGE
}

ensure_executable() {
  local script_path="$1"
  if [[ ! -f "$script_path" ]]; then
    echo "Missing script ${script_path}" >&2
    exit 1
  fi
  if [[ ! -x "$script_path" ]]; then
    chmod +x "$script_path" 2>/dev/null || true
  fi
  if [[ ! -x "$script_path" ]]; then
    echo "Missing executable ${script_path}" >&2
    exit 1
  fi
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

# Some environments/checkouts drop executable bits; self-heal deploy scripts up-front.
for deploy_script in "${DEPLOY_DIR}"/*.sh; do
  [[ -f "$deploy_script" ]] || continue
  chmod +x "$deploy_script" 2>/dev/null || true
done

if ! [[ "$AUTO_PROVISION_WORKERS" =~ ^[0-9]+$ ]]; then
  echo "--auto-provision-workers must be an integer >= 0" >&2
  exit 2
fi


read_env_value() {
  local file="$1"
  local key="$2"
  if [[ ! -f "$file" ]]; then
    return 0
  fi
  python3 - "$file" "$key" <<'PY'
import sys
from pathlib import Path

path = Path(sys.argv[1])
wanted = sys.argv[2]
for raw_line in path.read_text(encoding="utf-8").splitlines():
    line = raw_line.strip()
    if not line or line.startswith("#") or "=" not in line:
        continue
    key, value = line.split("=", 1)
    if key.strip() != wanted:
        continue
    value = value.strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
        value = value[1:-1]
    print(value)
    break
PY
}

detect_existing_postgres_data() {
  local found=1

  if docker ps -a --format '{{.Names}}' 2>/dev/null | grep -Fxq 'nightmare-postgres'; then
    return 0
  fi

  if docker volume ls --format '{{.Name}}' 2>/dev/null | grep -Eq '(^|[_-])postgres_data$'; then
    return 0
  fi

  return 1
}

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

install_local_python_requirements() {
  local sudo_cmd=()
  if [[ "${EUID:-$(id -u)}" -ne 0 ]]; then
    if command -v sudo >/dev/null 2>&1; then
      sudo_cmd=(sudo)
    fi
  fi

  local need_pkgs=0
  command -v python3 >/dev/null 2>&1 || need_pkgs=1
  if command -v python3 >/dev/null 2>&1 && ! python3 -m pip --version >/dev/null 2>&1; then
    need_pkgs=1
  fi

  if [[ "$need_pkgs" -eq 1 ]]; then
    echo "Installing Python 3 and pip (for client.py and other CLI tools on this host)..."
    if command -v yum >/dev/null 2>&1; then
      "${sudo_cmd[@]}" yum makecache -y || true
      "${sudo_cmd[@]}" yum install -y python3 python3-pip || true
    elif command -v dnf >/dev/null 2>&1; then
      "${sudo_cmd[@]}" dnf makecache -y || true
      "${sudo_cmd[@]}" dnf install -y python3 python3-pip || true
    elif command -v apt-get >/dev/null 2>&1; then
      "${sudo_cmd[@]}" apt-get update
      "${sudo_cmd[@]}" apt-get install -y python3 python3-pip python3-venv
    else
      echo "No yum/dnf/apt-get; trying ensurepip only." >&2
    fi
  fi

  if ! command -v python3 >/dev/null 2>&1; then
    echo "python3 is not available; install Python 3 manually." >&2
    return 1
  fi

  if ! python3 -m pip --version >/dev/null 2>&1; then
    echo "Bootstrapping pip via ensurepip..."
    python3 -m ensurepip --upgrade --default-pip 2>/dev/null || "${sudo_cmd[@]}" python3 -m ensurepip --upgrade --default-pip 2>/dev/null || true
  fi
  if ! python3 -m pip --version >/dev/null 2>&1; then
    echo "pip is not available for python3; install python3-pip manually." >&2
    return 1
  fi

  local req="${ROOT_DIR}/requirements.txt"
  if [[ ! -f "$req" ]]; then
    echo "Missing ${req}" >&2
    return 1
  fi

  echo "Installing Python packages from ${req}..."
  local pip_user=()
  if [[ "${EUID:-$(id -u)}" -ne 0 ]]; then
    pip_user=(--user)
  fi
  # Avoid upgrading distro-managed pip (common on Amazon Linux via rpm), which can fail with:
  # "Cannot uninstall pip ..., RECORD file not found".
  python3 -m pip install "${pip_user[@]}" --disable-pip-version-check setuptools wheel
  python3 -m pip install "${pip_user[@]}" --disable-pip-version-check -r "$req"
  echo "Local Python ready: $(python3 --version 2>&1 | tr -d '\n')"
  if [[ "${EUID:-$(id -u)}" -ne 0 ]]; then
    echo "If a command is not found, add ~/.local/bin to PATH (e.g. export PATH=\"\$HOME/.local/bin:\$PATH\")."
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
install_local_python_requirements
require_cmd docker
require_cmd openssl
require_cmd curl
resolve_compose_cmd || {
  echo "docker compose (or docker-compose) is required." >&2
  exit 1
}
detect_docker_access_mode || exit 1

mkdir -p "$TLS_DIR"

EXISTING_INSTALL_DETECTED=0
if [[ -f "$ENV_FILE" ]] || detect_existing_postgres_data; then
  EXISTING_INSTALL_DETECTED=1
fi

POSTGRES_DB="${POSTGRES_DB_DEFAULT}"
POSTGRES_USER="${POSTGRES_USER_DEFAULT}"
POSTGRES_PASSWORD=""
COORDINATOR_API_TOKEN=""
COORDINATOR_INSECURE_TLS=""

if [[ -f "$ENV_FILE" && "$FORCE_REGEN" -ne 1 ]]; then
  existing_postgres_db="$(read_env_value "$ENV_FILE" POSTGRES_DB)"
  existing_postgres_user="$(read_env_value "$ENV_FILE" POSTGRES_USER)"
  existing_postgres_password="$(read_env_value "$ENV_FILE" POSTGRES_PASSWORD)"
  existing_api_token="$(read_env_value "$ENV_FILE" COORDINATOR_API_TOKEN)"
  existing_base_url="$(read_env_value "$ENV_FILE" COORDINATOR_BASE_URL)"
  existing_insecure_tls="$(read_env_value "$ENV_FILE" COORDINATOR_INSECURE_TLS)"
  existing_log_database_url="$(read_env_value "$ENV_FILE" LOG_DATABASE_URL)"

  if [[ -n "$existing_postgres_db" ]]; then
    POSTGRES_DB="$existing_postgres_db"
  fi
  if [[ -n "$existing_postgres_user" ]]; then
    POSTGRES_USER="$existing_postgres_user"
  fi
  if [[ -n "$existing_postgres_password" ]]; then
    POSTGRES_PASSWORD="$existing_postgres_password"
  fi
  if [[ -n "$existing_api_token" ]]; then
    COORDINATOR_API_TOKEN="$existing_api_token"
  fi
  if [[ -n "$existing_insecure_tls" ]]; then
    COORDINATOR_INSECURE_TLS="$existing_insecure_tls"
  fi
  if [[ -n "$existing_log_database_url" ]]; then
    LOG_DATABASE_URL="$existing_log_database_url"
  fi
  if [[ -z "$BASE_URL_OVERRIDE" && -n "$existing_base_url" ]]; then
    COORDINATOR_BASE_URL="$existing_base_url"
  else
    COORDINATOR_BASE_URL="$(detect_base_url)"
  fi
else
  COORDINATOR_BASE_URL="$(detect_base_url)"
fi

if [[ -z "$POSTGRES_PASSWORD" ]]; then
  if [[ "$EXISTING_INSTALL_DETECTED" -eq 1 && "$FORCE_REGEN" -ne 1 ]]; then
    echo "Detected an existing Nightmare/Postgres installation, but could not find reusable POSTGRES_PASSWORD in ${ENV_FILE}." >&2
    echo "Refusing to generate a new database password automatically because that would break access to the existing database." >&2
    echo "Restore ${ENV_FILE}, or intentionally reset with --force after removing the old Postgres data volume." >&2
    exit 1
  fi
  POSTGRES_PASSWORD="$(gen_password)"
fi

if [[ -z "$COORDINATOR_API_TOKEN" ]]; then
  if [[ "$EXISTING_INSTALL_DETECTED" -eq 1 && "$FORCE_REGEN" -ne 1 && -f "$ENV_FILE" ]]; then
    existing_api_token="$(read_env_value "$ENV_FILE" COORDINATOR_API_TOKEN)"
    if [[ -n "$existing_api_token" ]]; then
      COORDINATOR_API_TOKEN="$existing_api_token"
    fi
  fi
fi
if [[ -z "$COORDINATOR_API_TOKEN" ]]; then
  COORDINATOR_API_TOKEN="$(gen_token)"
fi
if [[ -z "$COORDINATOR_INSECURE_TLS" ]]; then
  # This bootstrap path generates/uses a self-signed TLS cert for the coordinator server.
  # Workers must skip CA verification unless you replace certs with a trusted chain.
  COORDINATOR_INSECURE_TLS="true"
fi

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
COORDINATOR_INSECURE_TLS=${COORDINATOR_INSECURE_TLS}
TLS_CERT_FILE=${TLS_CERT_FILE}
TLS_KEY_FILE=${TLS_KEY_FILE}
COORDINATOR_BASE_URL=${COORDINATOR_BASE_URL}
LOG_DATABASE_URL=${LOG_DATABASE_URL}
EOF
chmod 600 "$ENV_FILE"

cat >"$WORKER_ENV_FILE" <<EOF
COORDINATOR_BASE_URL=${COORDINATOR_BASE_URL}
COORDINATOR_API_TOKEN=${COORDINATOR_API_TOKEN}
COORDINATOR_INSECURE_TLS=${COORDINATOR_INSECURE_TLS}
EOF
chmod 600 "$WORKER_ENV_FILE"

# Host shells: export COORDINATOR_BASE_URL / token from deploy/.env (client.py also reads deploy/.env directly).
HOST_ENV_SH="${DEPLOY_DIR}/coordinator-host-env.sh"
cat >"$HOST_ENV_SH" <<EOF
#!/usr/bin/env bash
# Auto-generated by bootstrap-central-auto.sh — sources deploy/.env on this machine.
set -a
# shellcheck source=/dev/null
source "${ROOT_DIR}/deploy/.env"
set +a
EOF
chmod 755 "$HOST_ENV_SH"

HOST_ENV_SH_ABS="$(abs_path "$HOST_ENV_SH")"
if [[ -n "${HOME:-}" ]]; then
  BASHRC_FILE="${HOME}/.bashrc"
  if [[ ! -f "$BASHRC_FILE" ]] && [[ -w "$(dirname "$BASHRC_FILE")" ]]; then
    touch "$BASHRC_FILE" 2>/dev/null || true
  fi
  if [[ -f "$BASHRC_FILE" ]] && [[ -w "$BASHRC_FILE" ]] && ! grep -qF 'nightmare-coordinator-env' "$BASHRC_FILE" 2>/dev/null; then
    {
      echo ""
      echo "# nightmare-coordinator-env (added by bootstrap-central-auto.sh)"
      printf '[[ -f %s ]] && source %s\n' "$(printf '%q' "$HOST_ENV_SH_ABS")" "$(printf '%q' "$HOST_ENV_SH_ABS")"
    } >>"$BASHRC_FILE"
    echo "Appended coordinator env loader to ${BASHRC_FILE} (open a new shell or: source ${HOST_ENV_SH_ABS})"
  fi
fi

cd "$DEPLOY_DIR"

if [[ -z "$LOG_DATABASE_URL" ]]; then
  LOG_DB_PROVISION_SCRIPT="${DEPLOY_DIR}/provision-log-db-aws.sh"
  ensure_executable "$LOG_DB_PROVISION_SCRIPT"
  if [[ -z "$AWS_AMI_ID" || -z "$AWS_SUBNET_ID" || -z "$AWS_SECURITY_GROUP_IDS" ]]; then
    echo "LOG_DATABASE_URL is not set and log DB VM must be provisioned." >&2
    echo "Provide AWS provisioning parameters: --aws-ami-id, --aws-subnet-id, --aws-security-group-ids" >&2
    exit 2
  fi
  log_db_cmd=(
    "$LOG_DB_PROVISION_SCRIPT"
    --ami-id "$AWS_AMI_ID"
    --instance-type "$AWS_INSTANCE_TYPE"
    --subnet-id "$AWS_SUBNET_ID"
    --security-group-ids "$AWS_SECURITY_GROUP_IDS"
    --env-file "$ENV_FILE"
    --skip-rebuild-central
  )
  if [[ -n "$AWS_KEY_NAME" ]]; then
    log_db_cmd+=(--key-name "$AWS_KEY_NAME")
  fi
  if [[ -n "$AWS_IAM_INSTANCE_PROFILE" ]]; then
    log_db_cmd+=(--iam-instance-profile "$AWS_IAM_INSTANCE_PROFILE")
  fi
  if [[ -n "$AWS_REGION" ]]; then
    log_db_cmd+=(--region "$AWS_REGION")
  fi
  "${log_db_cmd[@]}"
fi

run_compose -f docker-compose.central.yml --env-file .env up -d --build

echo "Central stack is running."
echo "Generated files:"
echo "  - ${ENV_FILE}"
echo "  - ${WORKER_ENV_FILE}"
echo "  - ${HOST_ENV_SH}"
echo "  - ${CERT_FILE}"
echo "  - ${KEY_FILE}"
echo
echo "On this host: COORDINATOR_BASE_URL is in deploy/.env; client.py reads it automatically from the repo."
echo "Interactive shells: source ${HOST_ENV_SH_ABS} or open a new login shell if ~/.bashrc was updated."
echo
echo "Use ${WORKER_ENV_FILE} on each worker VM as deploy/.env."

if [[ "$AUTO_PROVISION_WORKERS" -gt 0 ]]; then
  PROVISION_SCRIPT="${DEPLOY_DIR}/provision-workers-aws.sh"
  ensure_executable "$PROVISION_SCRIPT"
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
