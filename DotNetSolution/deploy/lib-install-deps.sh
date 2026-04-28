#!/usr/bin/env bash
# Dependency bootstrap for deploy.sh / run-local.sh (sourced after lib-nightmare-compose.sh).
#
# Environment:
#   NIGHTMARE_SKIP_INSTALL=1   Do not install packages; only verify docker/compose (fail if missing).
#
# On Linux, installs Docker Engine when the docker CLI is missing:
#   - Amazon Linux (ID=amzn): yum/dnf only — get.docker.com does NOT support amzn.
#   - Debian/Ubuntu / other: https://get.docker.com
# Ensures docker compose v2 (plugin or GitHub binary fallback on AL2), curl/git for minimal AMIs,
# and sets NIGHTMARE_DOCKER_USE_SUDO=1 when the daemon socket is root-only.
#
# macOS / Windows: prints install hints (no silent auto-install).

nightmare_is_linux() {
  [[ "$(uname -s)" == "Linux" ]]
}

nightmare_is_macos() {
  [[ "$(uname -s)" == "Darwin" ]]
}

nightmare_run_privileged() {
  if [[ "$(id -u)" -eq 0 ]]; then
    "$@"
  else
    sudo "$@"
  fi
}

# Prefer non-interactive sudo on CI; may still prompt once on a fresh laptop.
nightmare_sudo_docker() {
  if [[ "$(id -u)" -eq 0 ]]; then
    docker "$@"
  else
    sudo docker "$@"
  fi
}

nightmare_probe_docker_access() {
  unset NIGHTMARE_DOCKER_USE_SUDO
  command -v docker >/dev/null 2>&1 || return 1
  if docker info >/dev/null 2>&1; then
    return 0
  fi
  if nightmare_sudo_docker info >/dev/null 2>&1; then
    export NIGHTMARE_DOCKER_USE_SUDO=1
    return 0
  fi
  return 1
}

nightmare_compose_available() {
  if nightmare_docker compose version >/dev/null 2>&1; then
    return 0
  fi
  if command -v docker-compose >/dev/null 2>&1; then
    if [[ "${NIGHTMARE_DOCKER_USE_SUDO:-}" == "1" ]]; then
      sudo docker-compose version >/dev/null 2>&1
    else
      docker-compose version >/dev/null 2>&1
    fi
    return $?
  fi
  return 1
}

nightmare_ensure_curl() {
  command -v curl >/dev/null 2>&1 && return 0
  if [[ ! -f /etc/os-release ]]; then
    echo "curl is required but not installed, and /etc/os-release is missing; install curl manually." >&2
    return 1
  fi
  # shellcheck source=/dev/null
  source /etc/os-release
  case "${ID:-}" in
    ubuntu | debian)
      nightmare_run_privileged env DEBIAN_FRONTEND=noninteractive apt-get update -qq
      nightmare_run_privileged env DEBIAN_FRONTEND=noninteractive apt-get install -y curl ca-certificates
      ;;
    amzn | rhel | centos | fedora | rocky | almalinux)
      if command -v dnf >/dev/null 2>&1; then
        nightmare_run_privileged dnf install -y curl ca-certificates
      else
        nightmare_run_privileged yum install -y curl ca-certificates
      fi
      ;;
    *)
      echo "curl is required. Install curl for ${ID:-unknown}, then re-run." >&2
      return 1
      ;;
  esac
}

nightmare_ensure_git() {
  command -v git >/dev/null 2>&1 && return 0
  [[ -f /etc/os-release ]] || return 0
  # shellcheck source=/dev/null
  source /etc/os-release
  case "${ID:-}" in
    ubuntu | debian)
      nightmare_run_privileged env DEBIAN_FRONTEND=noninteractive apt-get update -qq
      nightmare_run_privileged env DEBIAN_FRONTEND=noninteractive apt-get install -y git
      ;;
    amzn | rhel | centos | fedora | rocky | almalinux)
      if command -v dnf >/dev/null 2>&1; then
        nightmare_run_privileged dnf install -y git
      else
        nightmare_run_privileged yum install -y git
      fi
      ;;
  esac
}

nightmare_start_docker_service_linux() {
  command -v docker >/dev/null 2>&1 || return 0

  if command -v systemctl >/dev/null 2>&1; then
    nightmare_run_privileged systemctl enable docker 2>/dev/null || true
    if nightmare_run_privileged systemctl start docker 2>/dev/null; then
      return 0
    fi
  fi

  nightmare_run_privileged chkconfig docker on 2>/dev/null || true
  if command -v service >/dev/null 2>&1; then
    nightmare_run_privileged service docker start 2>/dev/null || true
  fi
}

nightmare_install_compose_plugin_debian() {
  nightmare_run_privileged env DEBIAN_FRONTEND=noninteractive apt-get update -qq
  nightmare_run_privileged env DEBIAN_FRONTEND=noninteractive apt-get install -y docker-compose-plugin
}

nightmare_install_compose_plugin_rhel() {
  if command -v dnf >/dev/null 2>&1; then
    nightmare_run_privileged dnf install -y docker-compose-plugin 2>/dev/null \
      || nightmare_run_privileged dnf install -y docker-compose
  else
    nightmare_run_privileged yum install -y docker-compose-plugin 2>/dev/null \
      || nightmare_run_privileged yum install -y docker-compose
  fi
}

# get.docker.com rejects ID=amzn; use Amazon Linux packages only.
nightmare_install_docker_amazon_linux() {
  [[ -f /etc/os-release ]] || return 1
  # shellcheck source=/dev/null
  source /etc/os-release
  local vid="${VERSION_ID:-}"
  local plat="${PLATFORM_ID:-}"
  echo "Installing Docker via yum/dnf (Amazon Linux VERSION_ID=${vid:-?} PLATFORM_ID=${plat:-?})…"

  if [[ "${vid}" == 2023* ]] || [[ "${plat}" == platform:al2023* ]]; then
    nightmare_run_privileged dnf install -y docker
    return $?
  fi

  # Amazon Linux 2
  if command -v amazon-linux-extras >/dev/null 2>&1; then
    echo "Trying amazon-linux-extras install docker…"
    if nightmare_run_privileged amazon-linux-extras install -y docker; then
      return 0
    fi
  fi

  echo "Installing docker package with yum…"
  nightmare_run_privileged yum install -y docker
}

nightmare_install_compose_plugin_github_binary() {
  nightmare_ensure_curl || return 1
  local ver="${NIGHTMARE_COMPOSE_VERSION:-v2.29.7}"
  local uname_s uname_m tmp
  uname_s="$(uname -s)"
  uname_m="$(uname -m)"
  tmp="$(mktemp)"
  echo "Installing docker compose CLI plugin (${ver}) from GitHub…"
  curl -fsSL "https://github.com/docker/compose/releases/download/${ver}/docker-compose-${uname_s}-${uname_m}" -o "$tmp"
  nightmare_run_privileged mkdir -p /usr/local/lib/docker/cli-plugins /usr/libexec/docker/cli-plugins
  nightmare_run_privileged install -m0755 "$tmp" /usr/local/lib/docker/cli-plugins/docker-compose
  nightmare_run_privileged install -m0755 "$tmp" /usr/libexec/docker/cli-plugins/docker-compose 2>/dev/null || true
  rm -f "$tmp"
}

nightmare_install_compose_plugin_amazon() {
  [[ -f /etc/os-release ]] || return 1
  # shellcheck source=/dev/null
  source /etc/os-release
  local vid="${VERSION_ID:-}"
  local plat="${PLATFORM_ID:-}"

  if [[ "${vid}" == 2023* ]] || [[ "${plat}" == platform:al2023* ]]; then
    if nightmare_run_privileged dnf install -y docker-compose-plugin; then
      return 0
    fi
  else
    if nightmare_run_privileged yum install -y docker-compose-plugin; then
      return 0
    fi
  fi

  nightmare_install_compose_plugin_github_binary
}

nightmare_install_docker_engine_linux() {
  [[ -f /etc/os-release ]] || {
    echo "Cannot read /etc/os-release; cannot install Docker automatically." >&2
    return 1
  }
  # shellcheck source=/dev/null
  source /etc/os-release

  if [[ "${ID:-}" == "amzn" ]]; then
    nightmare_install_docker_amazon_linux
    return $?
  fi

  nightmare_ensure_curl || return 1
  echo "Downloading Docker install script (get.docker.com)…"
  local tmp
  tmp="$(mktemp)"
  curl -fsSL https://get.docker.com -o "$tmp"
  nightmare_run_privileged sh "$tmp"
  rm -f "$tmp"
}

nightmare_install_compose_plugin_linux() {
  [[ -f /etc/os-release ]] || return 1
  # shellcheck source=/dev/null
  source /etc/os-release
  case "${ID:-}" in
    ubuntu | debian)
      nightmare_install_compose_plugin_debian
      ;;
    amzn)
      nightmare_install_compose_plugin_amazon
      ;;
    rhel | centos | fedora | rocky | almalinux)
      nightmare_install_compose_plugin_rhel
      ;;
    *)
      echo "Docker is installed but Compose v2 is missing; install docker-compose-plugin for ${ID:-unknown}." >&2
      return 1
      ;;
  esac
}

nightmare_add_user_to_docker_group() {
  [[ "$(id -u)" -eq 0 ]] && return 0
  local u="${SUDO_USER:-$USER}"
  [[ -n "$u" && "$u" != "root" ]] || return 0
  getent group docker >/dev/null 2>&1 || return 0
  nightmare_run_privileged usermod -aG docker "$u" 2>/dev/null || true
}

nightmare_print_non_linux_docker_help() {
  if nightmare_is_macos; then
    cat >&2 <<'EOF'
Docker is not available in PATH.

  macOS: install Docker Desktop, start it, then re-run this script:
    brew install --cask docker
    # open Docker.app once, wait until it says "Docker Desktop is running"

EOF
  else
    cat >&2 <<'EOF'
Docker is not available in PATH.

  Windows: use WSL2 with Ubuntu and run this script inside WSL, or install Docker Desktop for Windows.

EOF
  fi
}

nightmare_ensure_runtime_dependencies() {
  if [[ "${NIGHTMARE_SKIP_INSTALL:-}" == "1" ]]; then
    command -v docker >/dev/null 2>&1 || {
      echo "NIGHTMARE_SKIP_INSTALL=1 but docker is not on PATH." >&2
      exit 1
    }
    nightmare_probe_docker_access || {
      echo "Cannot reach Docker daemon (docker info failed). Fix permissions or start Docker." >&2
      exit 1
    }
    nightmare_compose_available || {
      echo "Docker Compose is not available (need 'docker compose' or docker-compose)." >&2
      exit 1
    }
    return 0
  fi

  if command -v docker >/dev/null 2>&1 && nightmare_is_linux; then
    nightmare_start_docker_service_linux 2>/dev/null || true
  fi

  if nightmare_probe_docker_access; then
    if ! nightmare_compose_available; then
      echo "Docker is present but Compose is missing; installing compose plugin…"
      if nightmare_is_linux && [[ -f /etc/os-release ]]; then
        nightmare_install_compose_plugin_linux || exit 1
      else
        echo "Install Docker Compose v2 manually, then re-run." >&2
        exit 1
      fi
    fi
    [[ "${NIGHTMARE_GIT_PULL:-}" != "1" ]] || nightmare_ensure_git || true
    if [[ "${NIGHTMARE_DOCKER_USE_SUDO:-}" == "1" ]]; then
      echo "Note: using 'sudo docker' for this run (your user is not in the 'docker' group yet). Log out and back in, or run: newgrp docker" >&2
    fi
    return 0
  fi

  if command -v docker >/dev/null 2>&1; then
    if nightmare_is_linux; then
      echo "Docker is on PATH but the daemon is not reachable (docker info failed)." >&2
      echo "Try: sudo systemctl start docker   or log in to the 'docker' group, then re-run." >&2
    else
      nightmare_print_non_linux_docker_help
    fi
    exit 1
  fi

  if ! nightmare_is_linux; then
    nightmare_print_non_linux_docker_help
    exit 1
  fi

  echo "Docker was not found. Installing Docker Engine (Linux)…"
  nightmare_install_docker_engine_linux || exit 1
  nightmare_start_docker_service_linux || true
  nightmare_add_user_to_docker_group

  if ! nightmare_probe_docker_access; then
    echo "Docker was installed but 'docker info' still failed. Try: sudo systemctl start docker" >&2
    exit 1
  fi

  if ! nightmare_compose_available; then
    echo "Installing Docker Compose plugin…"
    nightmare_install_compose_plugin_linux || exit 1
  fi

  if [[ "${NIGHTMARE_GIT_PULL:-}" == "1" ]]; then
    nightmare_ensure_git || true
  fi

  if [[ "${NIGHTMARE_DOCKER_USE_SUDO:-}" == "1" ]]; then
    echo "Note: using 'sudo docker' for this run (your user is not in the 'docker' group yet). Log out and back in, or run: newgrp docker" >&2
  fi
}
