#!/usr/bin/env bash
# Nightly PostgreSQL-parity measurement of main (GD-0092 T1).
#
# Builds giggles-server (debug, default features) for the current main commit
# in a stable worktree and target directory, runs the differential corpus
# against the pinned PostgreSQL oracle with --keep-scratch in two halves split
# by area, merges the halves, then writes a per-area count table and a delta
# against the previous nightly run.
#
# Usage (the one command to schedule):
#   bash K:/Projects/gigglesdb/scripts/pgparity-nightly.sh
# A whole run takes about 110 minutes. Where one task may run at most 2 hours
# (harness background tasks), run the halves as two tasks, one after the other:
#   bash K:/Projects/gigglesdb/scripts/pgparity-nightly.sh --half a
#   bash K:/Projects/gigglesdb/scripts/pgparity-nightly.sh --half b
# The half that completes the pair merges and reports. Never start either with
# a trailing & in a foreground shell.
# Options:
#   --half a|b|all            a: pg_types,pg_functions,pg_queries,pg_silent;
#                             b: every other area; all (default): a then b
#   --previous <report.json>  compare against this report instead of LATEST
#   --with-checks             also run the client, ORM and GUI tool checks (half a)
#   --force                   measure again even if this commit already has a run
#   --keep-scratch-runs <n>   keep server scratch of the newest n runs (default 2)
#   --out-base / --worktree / --target   override the stable locations below
#
# Output: <out-base>/<UTC date>-<sha12>/
#   build.log, harness-<half>.log, part-<half>/r0-*/report.json (+ corpus/<area>/
#   <id>/ transcripts and kept scratch), report.json (merged), areas.md,
#   delta.md, SUMMARY.md. <out-base>/LATEST holds the newest merged report.
# Exit: 0 done (or half done, or already measured), 3 done with regressions,
#   2 failure.
set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
repo=${GIGGLES_REPO:-$(dirname "$(git -C "$script_dir" rev-parse --path-format=absolute --git-common-dir)")}
out_base=J:/gigglesdb-work/pgparity/gap2/nightly
worktree=J:/gigglesdb-work/wt/pgp-nightly
target=J:/gigglesdb-work/pgparity/gap2/nightly-target
buildslot=J:/gigglesdb-work/r2/buildslot2.sh
half_a='pg_types,pg_functions,pg_queries,pg_silent'
half_b='!pg_types,!pg_functions,!pg_queries,!pg_silent'
half=all previous='' checks=0 force=0 keep_runs=2
while [ $# -gt 0 ]; do
  case "$1" in
    --half) half=$2; shift 2 ;;
    --previous) previous=$2; shift 2 ;;
    --with-checks) checks=1; shift ;;
    --force) force=1; shift ;;
    --keep-scratch-runs) keep_runs=$2; shift 2 ;;
    --out-base) out_base=$2; shift 2 ;;
    --worktree) worktree=$2; shift 2 ;;
    --target) target=$2; shift 2 ;;
    *) sed -n '2,32p' "$0" >&2; exit 2 ;;
  esac
done
case "$half" in a) halves=(a) ;; b) halves=(b) ;; all) halves=(a b) ;;
  *) echo "nightly: --half must be a, b or all" >&2; exit 2 ;; esac
mkdir -p "$out_base"

# One nightly at a time; reclaim a lock whose owner process is gone.
lock=$out_base/.nightly.lock
if ! mkdir "$lock" 2>/dev/null; then
  owner=$(cat "$lock/owner" 2>/dev/null || true)
  if [ -n "$owner" ] && ps -p "${owner%% *}" >/dev/null 2>&1; then
    echo "nightly: another run holds $lock ($owner)" >&2; exit 2
  fi
  rm -rf "$lock"; mkdir "$lock"
fi
echo "$$ $(date -u +%FT%TZ)" > "$lock/owner"
trap 'rm -rf "$lock"' EXIT

# Run directory: reuse this commit's unfinished one (a half ran earlier).
sha=$(git -C "$repo" rev-parse --verify 'refs/heads/main^{commit}')
short=${sha:0:12}
run_dir=''
for existing in $(ls -dt "$out_base"/20??-??-??-"$short"* 2>/dev/null); do
  if [ -f "$existing/SUMMARY.md" ]; then
    [ $force -eq 1 ] && break
    echo "nightly: main $short already measured in $existing"; exit 0
  fi
  run_dir=$existing; break
done
if [ -z "$run_dir" ]; then
  run_dir=$out_base/$(date -u +%F)-$short
  [ -e "$run_dir" ] && run_dir=$run_dir-$(date -u +%H%M%S)
fi
mkdir -p "$run_dir/tmp"
[ -f "$run_dir/started" ] || date -u +%FT%TZ > "$run_dir/started"
echo "nightly: main $sha halves ${halves[*]} -> $run_dir"

# Stable worktree (sccache hits need the same source path and target dir).
if [ -e "$worktree/.git" ]; then
  [ -z "$(git -C "$worktree" status --porcelain --untracked-files=no)" ] || {
    echo "nightly: $worktree has local changes; refusing to move it" >&2; exit 2; }
  git -c core.autocrlf=false -C "$worktree" checkout -q --detach "$sha"
else
  git -c core.autocrlf=false -C "$repo" worktree add -q --detach "$worktree" "$sha"
fi

export RUSTC_WRAPPER=J:/gigglesdb-work/tools/bin/sccache.exe SCCACHE_DIR=J:/gigglesdb-work/sccache-cache \
  SCCACHE_CACHE_SIZE=100G CARGO_INCREMENTAL=0 CARGO_BUILD_JOBS=4 CARGO_TARGET_DIR=$target \
  TEMP=$run_dir/tmp TMP=$run_dir/tmp PYTHONDONTWRITEBYTECODE=1 LIBCLANG_PATH=${LIBCLANG_PATH:-J:/tools/LLVM/bin}
build_start=$SECONDS
if ! bash "$buildslot" cargo build --locked --manifest-path "$worktree/crates/giggles-server/Cargo.toml" \
    --bin giggles-server >> "$run_dir/build.log" 2>&1; then
  echo "nightly: build failed, see $run_dir/build.log" >&2; exit 2
fi
echo "build $((SECONDS - build_start))s" >> "$run_dir/timings"
server=$target/debug/giggles-server.exe
# The measured commit's own harness: a clean tree, unlike the shared checkout.
tools=$worktree/compat/harness.py

part_report() { ls -d "$run_dir/part-$1"/r0-*/report.json 2>/dev/null | head -1 || true; }
for h in "${halves[@]}"; do
  if [ -n "$(part_report "$h")" ]; then echo "nightly: half $h already done"; continue; fi
  rm -rf "$run_dir/part-$h"
  if [ "$h" = a ]; then corpus=$half_a; else corpus=$half_b; fi
  harness_args=(run --server-bin "$server" --commit "$sha" --keep-scratch --corpus "$corpus"
    --out-root "$run_dir/part-$h" --cargo-target "$target")
  if [ $checks -eq 0 ] || [ "$h" = b ]; then harness_args+=(--no-checks); fi
  run_start=$SECONDS
  set +e
  (cd "$worktree" && python -I -B compat/harness.py "${harness_args[@]}") > "$run_dir/harness-$h.log" 2>&1
  harness_rc=$?
  set -e
  echo "half $h rc=$harness_rc $((SECONDS - run_start))s" >> "$run_dir/timings"
  cases=0
  if [ -n "$(part_report "$h")" ]; then
    cases=$(python -I -c 'import json,sys; print(len(json.load(open(sys.argv[1], encoding="utf-8"))["differential"]["cases"]))' "$(part_report "$h")")
  fi
  echo "half $h cases=$cases" >> "$run_dir/timings"
  if [ -z "$(part_report "$h")" ] || [ $harness_rc -eq 2 ] || [ "$cases" -eq 0 ]; then
    echo "nightly: half $h failed (rc=$harness_rc, $cases cases), see $run_dir/harness-$h.log" >&2
    mv "$run_dir/part-$h" "$run_dir/failed-part-$h-$(date -u +%H%M%S)" 2>/dev/null || true
    exit 2
  fi
done

report_a=$(part_report a); report_b=$(part_report b)
if [ -z "$report_a" ] || [ -z "$report_b" ]; then
  if [ -z "$report_a" ]; then other=a; else other=b; fi
  echo "nightly: half done; run --half $other to finish $run_dir"
  rm -rf "$run_dir/tmp"; exit 0
fi

report=$run_dir/report.json
python -I -B "$tools" merge "$report_a" "$report_b" --out "$report" > /dev/null
if [ -z "$previous" ] && [ -f "$out_base/LATEST" ]; then previous=$(cat "$out_base/LATEST"); fi
area_args=(areas "$report" --out "$run_dir/areas.md")
if [ -n "$previous" ]; then area_args+=(--previous "$previous"); fi
python -I -B "$tools" "${area_args[@]}" > /dev/null
delta_rc=0
if [ -n "$previous" ]; then
  set +e
  python -I -B "$tools" delta "$previous" "$report" --out "$run_dir/delta.md" > /dev/null
  delta_rc=$?
  set -e
  if [ $delta_rc -eq 2 ]; then echo "nightly: delta failed" >&2; exit 2; fi
fi

{
  echo "# Nightly parity run $(basename "$run_dir")"
  echo
  echo "- commit: $sha"
  echo "- started: $(cat "$run_dir/started"); finished: $(date -u +%FT%TZ)"
  echo "- timings: $(tr '\n' ';' < "$run_dir/timings")"
  echo "- report: $report (merged from $report_a and $report_b)"
  echo "- previous: ${previous:-none}"
  if [ -n "$previous" ]; then
    if [ $delta_rc -eq 3 ]; then verdict=REGRESSIONS; else verdict='no regressions'; fi
    echo "- delta: $run_dir/delta.md ($verdict)"
  fi
  echo
  cat "$run_dir/areas.md"
} > "$run_dir/SUMMARY.md"
echo "$report" > "$out_base/LATEST"

# Keep server scratch (bin, data, tmp, target, cwd) only for the newest runs;
# reports, transcripts and logs stay.
mapfile -t runs < <(ls -dt "$out_base"/20??-??-??-* 2>/dev/null)
for old in "${runs[@]:$keep_runs}"; do
  for child in bin data tmp target cwd; do
    for path in "$old"/part-*/r0-*/"$child"; do
      if [ -d "$path" ]; then rm -rf "$path"; fi
    done
  done
  rm -rf "$old/tmp"
done
rm -rf "$run_dir/tmp"

cat "$run_dir/SUMMARY.md"
if [ $delta_rc -eq 3 ]; then exit 3; fi
exit 0
