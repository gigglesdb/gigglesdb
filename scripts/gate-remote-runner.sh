#!/usr/bin/env bash
# Runs on the boat.dev box; started by scripts/gate-remote.sh. Do not run locally.
# Args: gate steps (see gate-remote.sh). Env: CLIPPY=0 skips clippy.
# Writes /home/user/gate/{progress,summary.txt,done,<step>-*.log}.
# done holds rc=0 when every step built, clippy passed, and every failing test
# is listed in gate-linux-expected-failures.txt; rc=1 otherwise.
source "$HOME/gate.env"
out=/home/user/gate
rm -rf "$out"; mkdir -p "$out"
expected=/home/user/gate-linux-expected-failures.txt
summary=$out/summary.txt
overall=0
note() { echo "$*" >> "$summary"; }
progress() { echo "$(date -u +%T) $*" >> "$out/progress"; }
# Whole-command rerun only for an infrastructure failure that produced no test results.
flaky() { grep -qE 'LNK1171|out of memory|SIGKILL|signal: 9|^Killed' "$1" && ! grep -qE '^test result' "$1"; }

# run_logged <log> <cmd...>: run once, rerun once on an infrastructure flake.
run_logged() {
  local log=$1; shift
  "$@" > "$log" 2>&1; local rc=$?
  if [ $rc -ne 0 ] && flaky "$log"; then
    mv "$log" "$log.flake1"; progress "flake rerun: $*"
    "$@" > "$log" 2>&1; rc=$?
  fi
  return $rc
}

check_tests() { # check_tests <crate> <log> <rc>
  local crate=$1 log=$2 rc=$3 results fails unexpected
  results=$(grep -E '^test result' "$log")
  local p f
  p=$(echo "$results" | awk '{s+=$4} END {print s+0}')
  f=$(echo "$results" | awk '{s+=$6} END {print s+0}')
  fails=$(awk '/^failures:$/{x=1} x&&/^    [A-Za-z_:0-9]+$/{print $1} /^test result/{x=0}' "$log" | sort -u)
  unexpected='' flakes=''
  for t in $fails; do
    grep -qxF "$crate $t" "$expected" 2>/dev/null && continue
    # One exact rerun: a test that passes alone is recorded as a flake.
    local rr=$out/retry-$(echo "$t" | tr ':' '_').log
    (cd "$crate" && cargo test --no-fail-fast "${feat[@]}" -- --exact "$t") > "$rr" 2>&1
    if [ "$(grep -E '^test result' "$rr" | awk '{p+=$4; f+=$6} END {print (p>=1 && f==0) ? "ok" : "bad"}')" = ok ]; then
      flakes+="$t"$'\n'
    else
      unexpected+="$t"$'\n'
    fi
  done
  if [ -n "$flakes" ]; then
    note "  FLAKY (failed in the full run, passed alone):"; printf '%s' "$flakes" | sed 's/^/    /' >> "$summary"
  fi
  note "  tests: passed=$p failed=$f (expected Linux-only: $(( f - $(printf '%s' "$unexpected$flakes" | grep -c . ) )))"
  if [ -z "$results" ] && [ "$rc" -ne 0 ]; then
    note "  BUILD/RUN ERROR (no test results), first errors:"
    grep -E '^error(\[|:)' "$log" | head -5 | sed 's/^/    /' >> "$summary"
    return 1
  fi
  if [ -n "$unexpected" ]; then
    note "  UNEXPECTED FAILURES:"; printf '%s' "$unexpected" | sed 's/^/    /' >> "$summary"
    return 1
  fi
  # rc != 0 with only expected failures is a pass; rc != 0 with no failures
  # listed (for example a doc-test compile error) is not.
  if [ "$rc" -ne 0 ] && [ -z "$fails" ]; then
    note "  cargo test exited $rc with no failing test listed"; return 1
  fi
  return 0
}

cd /home/user/gigglesdb/crates || exit 1
# Temp files on tmpfs (RocksDB/fsync-heavy tests ran 3-10x slower on the box
# disk). Some tests require TEMP to start with j:/gigglesdb-work/ (the Windows
# storage rule); on Linux that is a relative path, so each crate gets a `j:`
# link to the tmpfs root. TMPDIR (std::env::temp_dir) stays absolute.
jroot=/dev/shm/jroot
mkdir -p "$jroot/gigglesdb-work/tmp"
export TEMP=j:/gigglesdb-work/tmp TMP=j:/gigglesdb-work/tmp TMPDIR=$jroot/gigglesdb-work/tmp
for c in giggles-*; do ln -sfn "$jroot" "$c/j:"; done
note "gate at $(git rev-parse HEAD) on $(uname -m) $(rustc --version)"
for step in "$@"; do
  crate=${step%%:*}; mode=default; files=''
  case "$step" in
    *:nodef) mode=nodef ;;
    *:tests=*) files=${step#*:tests=}; mode=tests ;;
  esac
  feat=(); [ $mode = nodef ] && feat=(--no-default-features)
  tag=$(echo "$step" | tr ':=,' '___')
  note "== $step"
  if [ ! -d "$crate" ]; then note "  no such crate"; overall=1; continue; fi
  s=$(date +%s); progress "test $step"
  if [ $mode = tests ]; then
    : > "$out/$tag-test.log"; trc=0
    for f in ${files//,/ }; do
      (cd "$crate" && run_logged "$out/$tag-test-$f.log" cargo test --no-fail-fast --test all "$f::") || trc=1
      cat "$out/$tag-test-$f.log" >> "$out/$tag-test.log"
    done
  else
    (cd "$crate" && run_logged "$out/$tag-test.log" cargo test --no-fail-fast "${feat[@]}"); trc=$?
  fi
  check_tests "$crate" "$out/$tag-test.log" "$trc" || overall=1
  note "  test time: $(( $(date +%s) - s ))s"
  rm -rf "${jroot:?}/gigglesdb-work/tmp/"* 2>/dev/null
  if [ "${CLIPPY:-1}" = 1 ]; then
    s=$(date +%s); progress "clippy $step"
    if (cd "$crate" && run_logged "$out/$tag-clippy.log" cargo clippy --all-targets "${feat[@]}" -- -D warnings); then
      note "  clippy: ok ($(( $(date +%s) - s ))s)"
    else
      note "  CLIPPY FAILED:"; grep -E '^(error|warning)' "$out/$tag-clippy.log" | head -8 | sed 's/^/    /' >> "$summary"
      overall=1
    fi
  fi
done
note "RESULT: $([ $overall = 0 ] && echo PASS || echo FAIL)"
progress "done rc=$overall"
echo "rc=$overall" > "$out/done"
