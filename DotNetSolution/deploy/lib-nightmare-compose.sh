#!/usr/bin/env bash
# Shared helpers for deploy.sh and run-local.sh (source after cd to DotNetSolution root and setting ROOT).
#
# Re-runnable: safe to run repeatedly. Picks up latest working tree via BUILD_SOURCE_STAMP (git HEAD + dirty)
# passed into Docker build args, plus `compose build --pull` and `compose up --force-recreate`.
#
# Optional:
#   NIGHTMARE_GIT_PULL=1   Run git pull --ff-only in ROOT before build.
#   NIGHTMARE_NO_CACHE=1   Add docker compose build --no-cache (slowest, strongest cache bust).

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
  echo "BUILD_SOURCE_STAMP=${BUILD_SOURCE_STAMP}"
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
  local cf="$ROOT/deploy/docker-compose.yml"
  if docker compose version >/dev/null 2>&1; then
    docker compose -f "$cf" "$@"
  elif command -v docker-compose >/dev/null 2>&1; then
    docker-compose -f "$cf" "$@"
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

nightmare_compose_full_redeploy() {
  nightmare_compose_build
  nightmare_compose_up_redeploy
}
