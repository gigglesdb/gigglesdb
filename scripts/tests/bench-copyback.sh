#!/usr/bin/env bash
set -euo pipefail
here=$(cd "$(dirname "$0")/.." && pwd)
case $(uname -s) in
  MINGW*|MSYS*) base=J:/gigglesdb-work/perf-b/tmp ;;
  *) base=/mnt/j/gigglesdb-work/perf-b/tmp ;;
esac
export TEMP=$base TMP=$base TMPDIR=$base
mkdir -p "$base"
work=$(mktemp -d "$base/copy-test.XXXXXX")
trap 'rm -rf "$work"' EXIT
mkdir -p "$work/source/bench/profiles/rocksdb-x" "$work/run"
echo a > "$work/source/bench/profiles/a.txt"
echo log > "$work/source/bench/profiles/rocksdb-x/LOG"
echo summary > "$work/source/bench/summary.md"
tar --force-local -czf "$work/run/logs.tar.gz" -C "$work/source" bench
# Exercise the actual production copy_back with remote operations stubbed.
# Production functions are extracted dynamically, never execute runner setup.
# shellcheck disable=SC1090
source <(sed -n '/^copy_back() {/,/^}/p' "$here/bench-remote.sh")
# Variables consumed by the dynamically loaded production function.
# shellcheck disable=SC2034
run=$work/run
# shellcheck disable=SC2034
box=fake
# shellcheck disable=SC2034
copied=0
bexec_t() { :; }
# shellcheck disable=SC2034
B=true
log() { echo "$*" >&2; }
copy_back
cmp "$work/source/bench/profiles/a.txt" "$run/profiles/a.txt"
cmp "$work/source/bench/profiles/rocksdb-x/LOG" "$run/profiles/rocksdb-x/LOG"
cmp "$work/source/bench/summary.md" "$run/summary.md"
test ! -d "$run/bench"
echo "PASS copyback ($run)"
