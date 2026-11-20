#!/usr/bin/env bash
# Runs on the boat.dev box; started by scripts/gate-remote.sh. Do not run locally.
# Args: gate steps (see gate-remote.sh). Env: CLIPPY=0 skips clippy.
# Writes /home/user/gate/{progress,summary.txt,done,<step>-*.log}.
# done holds rc=0 when every step built, clippy passed, and every failing test
# is listed in gate-linux-expected-failures.txt; rc=1 otherwise.
note() { echo "$*" >> "$summary"; }
progress() { echo "$(date -u +%T) $*" >> "$out/progress"; }
# Whole-command rerun only for an infrastructure failure that produced no test results.
flaky() {
  grep -qE 'LNK1171|out of memory|SIGKILL|signal: 9|^Killed' "$1" &&
    ! grep -qE '^test result' "$1" &&
    ! grep -qE 'has overflowed its stack|fatal runtime error|SIGABRT|signal: 6([, )]|$)' "$1"
}

# Cargo can continue after an aborted binary. Track every Running section,
# including ones followed by successful binaries, rather than just its totals.
aborted_binaries() {
  awk '
    function binary(path) { sub(/^.*[/\\]/, "", path); return path }
    function start(label, path) {
      active = ++count
      names[active] = label
      paths[path] = active
      bins[binary(path)] = active
    }
    /^[[:space:]]*Running[[:space:]]/ {
      label = $0
      sub(/^[[:space:]]*Running[[:space:]]+/, "", label)
      sub(/^unittests[[:space:]]+/, "", label)
      path = label
      sub(/^.* \(/, "", path); sub(/\)$/, "", path)
      sub(/ \(.*$/, "", label)
      start(label " (" binary(path) ")", path)
      next
    }
    /^[[:space:]]*Doc-tests[[:space:]]/ { active = 0; next }
    /^test result:/ { if (active) complete[active] = 1 }
    /^test [^ ]+ \.\.\./ { if (active) last[active] = $2 }
    /has overflowed its stack|fatal runtime error/ {
      if (active) {
        runtime[active] = $0
        sub(/^[[:space:]]+/, "", runtime[active])
        # The thread name may be on its own line when harness output interleaves.
        if ($0 ~ /has overflowed its stack/ && $0 ~ /\047/) {
          test = $0
          sub(/^.*thread \047/, "", test); sub(/\047.*$/, "", test)
          if (test != "") last[active] = test
        }
      }
    }
    /process didn.t exit successfully|\(signal: [0-9]+/ {
      target = active
      if ($0 ~ /process didn.t exit successfully/) {
        path = $0
        sub(/^[^`]*`/, "", path); sub(/`.*$/, "", path)
        sub(/ .*/, "", path)
        target = paths[path]
        if (!target) target = bins[binary(path)]
        if (!target) {
          previous = active
          start(binary(path), path)
          target = active
          active = previous
        }
      }
      if (target) {
        reason = $0
        if (reason ~ /\(signal: [0-9]+/) {
          sub(/^.*\(signal: /, "signal ", reason); sub(/\).*$/, "", reason)
        } else {
          sub(/^[[:space:]]+/, "", reason)
        }
        errors[target] = reason
      }
    }
    END {
      for (i = 1; i <= count; i++) {
        if (complete[i] && runtime[i] == "" && errors[i] == "") continue
        reason = errors[i]
        if (runtime[i] != "") reason = runtime[i] (reason == "" ? "" : "; " reason)
        if (reason == "") reason = "no test result: line"
        print "BINARY ABORTED: " names[i] " " reason ", last test started: " (last[i] == "" ? "unknown" : last[i])
      }
    }
  ' "$1"
}

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

retry_test() {
  (cd "$1" && cargo test --no-fail-fast "${feat[@]}" -- --exact "$2")
}

check_tests() { # check_tests <crate> <log> <rc>
  local crate=$1 log=$2 rc=$3 results fails unexpected flakes aborted t
  results=$(grep -E '^test result' "$log")
  local p f
  p=$(echo "$results" | awk '{s+=$4} END {print s+0}')
  f=$(echo "$results" | awk '{s+=$6} END {print s+0}')
  fails=$(awk '/^failures:$/{x=1} x&&/^    [A-Za-z_:0-9]+$/{print $1} /^test result/{x=0}' "$log" | sort -u)
  aborted=$(aborted_binaries "$log")
  unexpected='' flakes=''
  for t in $fails; do
    grep -qxF "$crate $t" "$expected" 2>/dev/null && continue
    # One exact rerun: a test that passes alone is recorded as a flake.
    local rr=$out/retry-$(echo "$t" | tr ':' '_').log
    if retry_test "$crate" "$t" > "$rr" 2>&1 &&
      [ -z "$(aborted_binaries "$rr")" ] &&
      [ "$(grep -E '^test result' "$rr" | awk '{p+=$4; f+=$6} END {print (p>=1 && f==0) ? "ok" : "bad"}')" = ok ]; then
      flakes+="$t"$'\n'
    else
      unexpected+="$t"$'\n'
    fi
  done
  if [ -n "$flakes" ]; then
    note "  FLAKY (failed in the full run, passed alone):"; printf '%s' "$flakes" | sed 's/^/    /' >> "$summary"
  fi
  note "  tests: passed=$p failed=$f (expected Linux-only: $(( f - $(printf '%s' "$unexpected$flakes" | grep -c . ) )))"
  if [ -n "$unexpected$aborted" ]; then
    note "  UNEXPECTED FAILURES:"
    printf '%s' "$unexpected" | sed 's/^/    /' >> "$summary"
    if [ -n "$aborted" ]; then printf '%s\n' "$aborted" | sed 's/^/    /' >> "$summary"; fi
    return 1
  fi
  if [ -z "$results" ] && [ "$rc" -ne 0 ]; then
    note "  BUILD/RUN ERROR (no test results), first errors:"
    grep -E '^error(\[|:)' "$log" | head -5 | sed 's/^/    /' >> "$summary"
    return 1
  fi
  # rc != 0 with only expected failures is a pass; rc != 0 with no failures
  # listed (for example a doc-test compile error) is not.
  if [ "$rc" -ne 0 ] && [ -z "$fails" ]; then
    note "  cargo test exited $rc with no failing test listed"; return 1
  fi
  return 0
}

# Sourcing exposes the log checks without starting a gate or invoking cargo.
if [[ ${BASH_SOURCE[0]} != "$0" ]]; then return 0; fi
source "$HOME/gate.env"
out=/home/user/gate
rm -rf "$out"; mkdir -p "$out"
expected=/home/user/gate-linux-expected-failures.txt
summary=$out/summary.txt
overall=0

cd /home/user/gigglesdb/crates || exit 1
# Temp files on tmpfs (RocksDB/fsync-heavy tests ran 3-10x slower on the box
# disk). Some tests require TEMP to start with j:/gigglesdb-work/ (the Windows
# storage rule); on Linux that is a relative path, so each crate gets `j:`/`J:`
# link to the tmpfs root. TMPDIR (std::env::temp_dir) stays absolute.
jroot=/dev/shm/jroot
mkdir -p "$jroot/gigglesdb-work/tmp/skedaddle"
export TEMP=j:/gigglesdb-work/tmp TMP=j:/gigglesdb-work/tmp TMPDIR=$jroot/gigglesdb-work/tmp
export SKEDADDLE_TEST_WORK_ROOT="$jroot/gigglesdb-work"
export SKEDADDLE_TEST_ROOT="$SKEDADDLE_TEST_WORK_ROOT/tmp/skedaddle"
for c in *; do
  [[ -f $c/Cargo.toml ]] || continue
  ln -sfn "$jroot" "$c/j:"
  ln -sfn "$jroot" "$c/J:"
done
note "gate at $(git rev-parse HEAD) on $(uname -m) $(rustc --version)"
s=$(date +%s); progress "ICU provisioning"
if icu_dir=$(bash ../scripts/icu/provision.sh 2> "$out/icu-provision.log"); then
  export GIGGLES_ICU_DIR="$icu_dir"
  note "ICU provisioning: ok ($(( $(date +%s) - s ))s) $GIGGLES_ICU_DIR"
else
  note "ICU provisioning failed ($(( $(date +%s) - s ))s); see icu-provision.log"
  note "RESULT: FAIL"
  progress "done rc=1 (ICU provisioning failed)"
  echo "rc=1" > "$out/done"
  exit 1
fi
s=$(date +%s); progress "glibm build"
if glibm_dir=$(bash ../scripts/glibm/build.sh 2> "$out/glibm-build.log"); then
  export GIGGLES_GLIBM_DIR="$glibm_dir"
  note "glibm build: ok ($(( $(date +%s) - s ))s) $GIGGLES_GLIBM_DIR"
else
  note "glibm build failed ($(( $(date +%s) - s ))s); see glibm-build.log"
  note "RESULT: FAIL"
  progress "done rc=1 (glibm build failed)"
  echo "rc=1" > "$out/done"
  exit 1
fi
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
