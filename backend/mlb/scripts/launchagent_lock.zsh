#!/bin/zsh
# Lightweight atomic lock helpers for LaunchAgent wrappers.
#
# Uses mkdir as the atomic operation so it works on macOS without depending on
# flock. Call acquire_launchagent_lock one or more times, then install
# release_launchagent_locks in the wrapper EXIT trap.

setopt local_options no_unset 2>/dev/null || true

LA_LOCK_ROOT="${LA_LOCK_ROOT:-artifacts/ops/locks}"
typeset -ga LA_ACQUIRED_LOCKS

_la_now_epoch() {
  date +%s
}

_la_mtime_epoch() {
  local lock_path="$1"
  local value=""
  value="$(stat -f %m "$lock_path" 2>/dev/null || true)"
  if [[ "$value" =~ '^[0-9]+$' ]]; then
    echo "$value"
    return 0
  fi
  value="$(stat -c %Y "$lock_path" 2>/dev/null || true)"
  if [[ "$value" =~ '^[0-9]+$' ]]; then
    echo "$value"
    return 0
  fi
  echo 0
}

_la_pid_alive() {
  local pid="$1"
  [[ -n "$pid" ]] && kill -0 "$pid" >/dev/null 2>&1
}

_la_write_lock_metadata() {
  local lock_dir="$1"
  {
    echo "pid=$$"
    echo "ppid=${PPID:-}"
    echo "started_at_utc=$(date -u +%FT%TZ)"
    echo "cwd=$(pwd)"
    echo "script=${LA_WRAPPER_NAME:-${0:-}}"
    echo "xpc_service_name=${XPC_SERVICE_NAME:-}"
    echo "path=${PATH:-}"
    echo "python=$({ command -v python3 || true; } 2>/dev/null)"
    echo "venv_python=$({ command -v .venv/bin/python || true; } 2>/dev/null)"
    echo "make=$({ command -v make || true; } 2>/dev/null)"
  } > "${lock_dir}/owner.env"
}

acquire_launchagent_lock() {
  local name="$1"
  local wait_sec="${2:-0}"
  local stale_sec="${3:-21600}"
  local lock_dir="${LA_LOCK_ROOT}/${name}.lock"
  local deadline=$(( $(_la_now_epoch) + wait_sec ))

  mkdir -p "$LA_LOCK_ROOT"
  while true; do
    if mkdir "$lock_dir" 2>/dev/null; then
      _la_write_lock_metadata "$lock_dir"
      LA_ACQUIRED_LOCKS+=("$lock_dir")
      echo "[$(date -u +%FT%TZ)] INFO acquired lock name=${name} path=${lock_dir} pid=$$"
      return 0
    fi

    local owner_pid=""
    if [[ -f "${lock_dir}/owner.env" ]]; then
      owner_pid="$(awk -F= '$1=="pid"{print $2; exit}' "${lock_dir}/owner.env" 2>/dev/null || true)"
    fi
    local mtime="$(_la_mtime_epoch "$lock_dir")"
    local age=$(( $(_la_now_epoch) - mtime ))
    if [[ "$age" -ge "$stale_sec" ]] && ! _la_pid_alive "$owner_pid"; then
      echo "[$(date -u +%FT%TZ)] WARN removing stale lock name=${name} path=${lock_dir} age_sec=${age} owner_pid=${owner_pid}" >&2
      rm -rf "$lock_dir"
      continue
    fi

    if [[ "$(_la_now_epoch)" -ge "$deadline" ]]; then
      echo "[$(date -u +%FT%TZ)] WARN lock unavailable name=${name} path=${lock_dir} owner_pid=${owner_pid} age_sec=${age} wait_sec=${wait_sec}" >&2
      if [[ -f "${lock_dir}/owner.env" ]]; then
        sed 's/^/[lock-owner] /' "${lock_dir}/owner.env" >&2 || true
      fi
      return 75
    fi
    sleep 15
  done
}

release_launchagent_locks() {
  local lock_dir
  for lock_dir in "${LA_ACQUIRED_LOCKS[@]}"; do
    if [[ -d "$lock_dir" ]]; then
      rm -rf "$lock_dir"
      echo "[$(date -u +%FT%TZ)] INFO released lock path=${lock_dir}"
    fi
  done
  LA_ACQUIRED_LOCKS=()
}
