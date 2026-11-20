#!/usr/bin/env bash
# Exercise production SCP helpers and upload exit handling without a box.
set -euo pipefail
here=$(cd "$(dirname "$0")/.." && pwd)
work=$(mktemp -d "${TMPDIR:?}/gate-scp.XXXXXX")
trap 'rm -rf "$work"' EXIT
run=$work/run
mkdir -p "$run"
box=fake
log() { echo "$*" >&2; }
sleep() { echo "$*" >> "$work/sleeps"; }
# The fake boat writes both streams, fails once or persistently.
bj() {
  echo "$*" >> "$work/calls"
  echo "fake stdout"
  echo "fake transport error" >&2
  if [ "$mode" = always ] || [ ! -e "$work/failed" ]; then
    touch "$work/failed"
    return 7
  fi
}
source <(sed -n '/^upload_file() {/,/^}/p' "$here/gate-remote.sh")
source <(sed -n '/^download_logs() {/,/^}/p' "$here/gate-remote.sh")
mode=once
upload_file stack.bundle 2> "$work/retry.err"
test "$(wc -l < "$work/calls")" -eq 2
grep -q 'file=stack.bundle attempt=1 exit=7' "$run/upload-scp.log"
grep -q 'file=stack.bundle attempt=2 exit=0' "$run/upload-scp.log"
grep -q 'fake transport error' "$work/retry.err"
test "$(cat "$work/sleeps")" = 10
mode=always
: > "$work/calls"
# Extract the real production loop so exit 5 cannot drift from the test.
set +e
( eval "$(sed -n '/^( cd "$run" && for f in stack.bundle/,/upload failed"; exit 5; }/p' "$here/gate-remote.sh")" ) 2> "$work/fail.err"
rc=$?
set -e
test "$rc" -eq 5
test "$(wc -l < "$work/calls")" -eq 2
grep -q 'file=stack.bundle attempt=2 exit=7' "$run/upload-scp.log"
grep -q 'fake transport error' "$work/fail.err"
download_logs 2> "$work/download.err"
grep -q 'WARNING: log download failed: gate-logs.tar.gz exit=7' "$work/download.err"
grep -q 'fake transport error' "$run/download-scp.log"
grep -q 'fake transport error' "$work/download.err"
mode=once
download_logs
grep -q 'file=gate-logs.tar.gz exit=0' "$run/download-scp.log"
echo 'PASS gate SCP (retry, exit 5, download warning, download success)'
