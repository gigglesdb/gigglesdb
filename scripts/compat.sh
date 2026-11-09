#!/usr/bin/env bash
set -euo pipefail

cd "$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
# Probe the interpreter: Windows may expose an unusable WindowsApps alias.
for python in python3 python; do
    if command -v "$python" >/dev/null 2>&1 && "$python" -I -c 'import sys; sys.exit(sys.version_info.major != 3)' >/dev/null 2>&1; then
        exec "$python" -I compat/harness.py "$@"
    fi
done
printf '%s\n' 'Python 3 is required to run compat/harness.py.' >&2
exit 1
