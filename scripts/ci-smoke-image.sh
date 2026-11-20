#!/usr/bin/env bash
set -euo pipefail
image=$1
set +e
output=$(timeout 30s docker run --rm "$image" --help 2>&1)
rc=$?
set -e
printf '%s\n' "$output"
if [[ $rc == 0 ]]; then exit 0; fi
if [[ $rc == 2 ]] && grep -qi 'usage:' <<< "$output"; then exit 0; fi
echo "Image smoke test failed (exit $rc): $image" >&2
exit 1
