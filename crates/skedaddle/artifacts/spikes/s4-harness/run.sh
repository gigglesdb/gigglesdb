#!/usr/bin/env bash
# Git Bash on Windows; Python and Docker CLI must be on PATH.
set -Eeuo pipefail
export PYTHONDONTWRITEBYTECODE=1
export MSYS_NO_PATHCONV=1
export SKEDADDLE_RUN_ID="${RUN_ID:-sh-$(date +%Y%m%d-%H%M%S)-$$}"
export SKEDADDLE_PG_MAJOR="${PG_MAJOR:-18}"
export SKEDADDLE_BUDGET_GIB="${BUDGET_GIB:-0}"
# Git Bash $$ is an MSYS pid; /proc maps the live outer shell to its Windows PID.
export SKEDADDLE_OWNER_PID="$(cat /proc/$$/winpid)"
export SKEDADDLE_MSYS_PID="$$"
root="$(cygpath -m "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)")"
engine="$root/harness.py"
started=0
child=''
finish() {
  rc=$?
  trap - EXIT INT TERM
  if [[ -n "$child" ]]; then kill -TERM "$child" 2>/dev/null || true; fi
  if [[ "$started" == 1 ]]; then
    python -B "$engine" export || rc=1
    python -B "$engine" down || rc=1
    python -B "$engine" sweep || rc=1
    python -B "$engine" check-run || rc=1
  fi
  exit "$rc"
}
trap finish EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
python -B "$engine" guard
python -B "$engine" sweep
started=1
python -B "$engine" up
if [[ -n "${WORKLOAD:-}" ]]; then
  bash -c "$WORKLOAD" & child=$!
else
  python -B "$engine" "${MODE:-smoke}" & child=$!
fi
wait "$child"
child=''
