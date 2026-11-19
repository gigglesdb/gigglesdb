#!/usr/bin/env bash
set -euo pipefail
here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
export NUNYA_AGENT_BASH
NUNYA_AGENT_BASH=$(command -v bash)
if command -v cygpath >/dev/null; then NUNYA_AGENT_BASH=$(cygpath -am "$NUNYA_AGENT_BASH"); fi
exec "${NUNYA_PYTHON:-python}" -B -I "$here/scenario.py" "$@"
