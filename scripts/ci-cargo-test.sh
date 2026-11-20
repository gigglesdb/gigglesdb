#!/usr/bin/env bash
# Reuse the remote gate's failure-list, aborted-binary and exact-rerun checks.
set -uo pipefail
root=$(cd "$(dirname "$0")/.." && pwd)
source "$root/scripts/gate-remote-runner.sh"
crate=$1
mode=${2:-default}
out=${RUNNER_TEMP:-${TMPDIR:?}}/cargo-$crate-$mode
mkdir -p "$out"
summary=$out/summary.txt
expected=$root/scripts/gate-linux-expected-failures.txt
feat=(--locked)
if [[ $mode == nodef ]]; then feat+=(--no-default-features); fi
: > "$summary"
cd "$root/crates" || exit 1
(cd "$crate" && cargo test --no-fail-fast "${feat[@]}") > "$out/test.log" 2>&1
rc=$?
cat "$out/test.log"
check_tests "$crate" "$out/test.log" "$rc"
result=$?
cat "$summary"
if [[ -n ${GITHUB_STEP_SUMMARY:-} ]]; then cat "$summary" >> "$GITHUB_STEP_SUMMARY"; fi
exit "$result"
