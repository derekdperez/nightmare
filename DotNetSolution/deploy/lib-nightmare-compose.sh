#!/usr/bin/env bash
# Shared helpers for deploy.sh and run-local.sh (source after cd to DotNetSolution root and setting ROOT).
#
# Re-runnable: safe to run repeatedly. Picks up latest working tree via BUILD_SOURCE_STAMP (git HEAD + dirty)
# passed into Docker build args, plus `compose build --pull` and `compose up --force-recreate`.
#
# Optional:
#   NIGHTMARE_GIT_PULL=1   Run git pull --ff-only in ROOT before build.
#   NIGHTMARE_NO_CACHE=1   Add docker compose build --no-cache (slowest, strongest cache bust).
#   NIGHTMARE_DOCKER_USE_SUDO=1   Prefix docker with sudo (set by lib-install-deps.sh when the daemon socket is not user-accessible).
#   NIGHTMARE_DEPLOY_SKIP_BUILD=1   Skip "docker compose build" (set by deploy.sh when fingerprint matches deploy/.last-deploy-stamp).
#   NIGHTMARE_DEPLOY_FRESH=1   Force full rebuild (--no-cache); set by ./deploy.sh -fresh.

nightmare_docker() {
  if [[ "${NIGHTMARE_DOCKER_USE_SUDO:-}" == "1" ]]; then
    sudo docker "$@"
  else
    docker "$@"
  fi
}

# Hash of deploy recipes so compose/Dockerfile edits invalidate the incremental deploy cache without a new git commit.
nightmare_recipe_bundle_hash() {
  local root="${1:-}"
  [[ -n "$root" ]] || return 1
  if command -v sha256sum >/dev/null 2>&1; then
    (cd "$root" && sha256sum deploy/docker-compose.yml deploy/Dockerfile.web deploy/Dockerfile.worker 2>/dev/null || true) | sha256sum | awk '{print $1}'
  else
    (cd "$root" && cat deploy/docker-compose.yml deploy/Dockerfile.web deploy/Dockerfile.worker 2>/dev/null || true) | shasum -a 256 2>/dev/null | awk '{print $1}'
  fi
}

nightmare_export_build_stamp() {
  local root="${1:-}"
  [[ -n "$root" ]] || return 1
  if [[ -d "$root/.git" ]]; then
    local head
    head="$(git -C "$root" rev-parse HEAD 2>/dev/null || echo unknown)"
    if git -C "$root" diff --quiet 2>/dev/null && git -C "$root" diff --cached --quiet 2>/dev/null; then
      export BUILD_SOURCE_STAMP="$head"
    else
      export BUILD_SOURCE_STAMP="${head}-dirty"
    fi
  else
    export BUILD_SOURCE_STAMP="nogit-$(date -u +%Y%m%dT%H%M%SZ)"
  fi
  local recipe
  recipe="$(nightmare_recipe_bundle_hash "$root")"
  export BUILD_SOURCE_STAMP="${BUILD_SOURCE_STAMP}+${recipe:0:16}"
  echo "BUILD_SOURCE_STAMP=${BUILD_SOURCE_STAMP}"
}

nightmare_last_deploy_stamp_path() {
  : "${ROOT:?ROOT must point to DotNetSolution root}"
  echo "$ROOT/deploy/.last-deploy-stamp"
}

nightmare_read_last_deploy_stamp() {
  local p
  p="$(nightmare_last_deploy_stamp_path)"
  [[ -f "$p" ]] && head -n1 "$p" || true
}

nightmare_write_last_deploy_stamp() {
  local p
  p="$(nightmare_last_deploy_stamp_path)"
  printf '%s\n' "${BUILD_SOURCE_STAMP}" >"$p.tmp"
  mv -f "$p.tmp" "$p"
}

nightmare_decide_incremental_deploy() {
  unset NIGHTMARE_DEPLOY_SKIP_BUILD
  if [[ "${NIGHTMARE_DEPLOY_FRESH:-0}" == "1" ]]; then
    export NIGHTMARE_NO_CACHE=1
    echo "Fresh deploy: forcing docker compose build --pull --no-cache."
    return 0
  fi
  local last
  last="$(nightmare_read_last_deploy_stamp)"
  if [[ -n "$last" && "$last" == "${BUILD_SOURCE_STAMP}" ]]; then
    export NIGHTMARE_DEPLOY_SKIP_BUILD=1
    echo "Incremental deploy: fingerprint matches last successful deploy; skipping docker compose build."
    echo "  (Use ./deploy/deploy.sh -fresh to force a full rebuild, or delete deploy/.last-deploy-stamp.)"
  else
    echo "Full image build: first run, changed sources, or changed deploy/docker-compose / Dockerfiles."
  fi
}

nightmare_maybe_git_pull() {
  local root="${1:-}"
  [[ "${NIGHTMARE_GIT_PULL:-}" == "1" ]] || return 0
  [[ -d "$root/.git" ]] || { echo "NIGHTMARE_GIT_PULL=1 but $root has no .git; skipping pull." >&2; return 0; }
  echo "NIGHTMARE_GIT_PULL=1: git pull --ff-only in $root"
  git -C "$root" pull --ff-only
}

compose() {
  : "${ROOT:?ROOT must point to DotNetSolution root}"
  # Compose v2 can delegate multi-service builds to "bake", which has had stability issues on some
  # Linux installs (opaque "failed to execute bake: exit status 1"). Default off; set COMPOSE_BAKE=true to opt in.
  export COMPOSE_BAKE="${COMPOSE_BAKE:-false}"
  local cf="$ROOT/deploy/docker-compose.yml"
  if nightmare_docker compose version >/dev/null 2>&1; then
    nightmare_docker compose -f "$cf" "$@"
  elif command -v docker-compose >/dev/null 2>&1; then
    if [[ "${NIGHTMARE_DOCKER_USE_SUDO:-}" == "1" ]]; then
      sudo docker-compose -f "$cf" "$@"
    else
      docker-compose -f "$cf" "$@"
    fi
  else
    echo "Docker Compose is not available (need 'docker compose' or docker-compose)." >&2
    exit 1
  fi
}

nightmare_compose_build() {
  local args=(build --pull)
  [[ "${NIGHTMARE_NO_CACHE:-}" == "1" ]] && args+=(--no-cache)
  compose "${args[@]}"
}

nightmare_compose_up_redeploy() {
  compose up -d --force-recreate --remove-orphans
}

nightmare_compose_deploy_all() {
  if [[ "${NIGHTMARE_DEPLOY_SKIP_BUILD:-}" == "1" ]]; then
    nightmare_compose_up_redeploy
  else
    nightmare_compose_build
    nightmare_compose_up_redeploy
  fi
  nightmare_write_last_deploy_stamp
}

nightmare_compose_full_redeploy() {
  nightmare_compose_deploy_all
}
