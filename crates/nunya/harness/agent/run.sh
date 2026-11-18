#!/usr/bin/env bash
set -euo pipefail
# Never run any docker ... prune command (volume, system, image, container, network, builder).
# Name or label every container, volume and network you create, and remove only those,
# by name or label (docker rm -fv <name>, docker volume rm <name>, docker network rm <name>).
# If you pull an image, remove only that image by its exact tag, and only if nothing else uses it.
here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
umask 077
NUNYA_AGENT_BASH=$(command -v bash)
if command -v cygpath >/dev/null; then NUNYA_AGENT_BASH=$(cygpath -am "$NUNYA_AGENT_BASH"); fi
export NUNYA_AGENT_BASH
root=
args=("$@")
for ((i=0; i<${#args[@]}; i++)); do
  if [[ ${args[i]} == --work-root ]]; then root=${args[i+1]:?missing-work-root}; fi
done
[[ -n $root ]] || { echo 'FAIL setup missing-work-root'; echo '0 passed; 1 failed'; exit 1; }
mkdir -p "$root"
NUNYA_AGENT_RUN_DIR=$(mktemp -d "$root/agent-XXXXXXXX")
if command -v cygpath >/dev/null; then NUNYA_AGENT_RUN_DIR=$(cygpath -am "$NUNYA_AGENT_RUN_DIR"); fi
export NUNYA_AGENT_RUN_DIR
cleanup() {
  local rc=$? name
  trap - EXIT
  for name in "${NUNYA_AGENT_PREFIX:-gd81-d5}-pg" "${NUNYA_AGENT_PREFIX:-gd81-d5}-emulator"; do
    if [[ -f $NUNYA_AGENT_RUN_DIR/owned-$name ]]; then
      docker rm -fv "$name" >/dev/null 2>&1 || rc=1
    fi
  done
  exit "$rc"
}
trap cleanup EXIT
"${NUNYA_PYTHON:-python}" -I "$here/agent.py" "$@"
