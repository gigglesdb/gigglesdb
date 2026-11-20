#!/usr/bin/env bash
# Offline regression checks; no cargo, box, or gate environment required.
# Optional argument: an existing real cargo test log to check as well.
set -euo pipefail
here=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
source "$here/gate-remote-runner.sh"
root=${GATE_RUNNER_TEST_TMP:-J:/gigglesdb-work/tmp}
mkdir -p "$root"
out=$(mktemp -d "$root/gate-runner-abort.XXXXXX")
trap 'rm -rf "$out"' EXIT
export TEMP=$out TMP=$out TMPDIR=$out CARGO_TARGET_DIR=$out/target
# Keep synthetic expected failures independent of the production exception list.
expected=$out/expected.txt
echo 'giggles-core storage::replica::projected_admission::tests::ledger_never_reset' > "$expected"
summary=$out/summary.txt
feat=()

cat > "$out/pass.log" <<'LOG'
     Running tests/pass.rs (/target/debug/deps/pass-1234)
running 1 test
test sample::passes ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s
LOG
cat > "$out/fail.log" <<'LOG'
     Running tests/all.rs (/target/debug/deps/all-1234)
running 1 test
test sample::fails ... FAILED
failures:
    sample::fails
test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s
LOG
cat > "$out/expected.log" <<'LOG'
     Running unittests src/lib.rs (/target/debug/deps/giggles_core-1234)
running 1 test
test storage::replica::projected_admission::tests::ledger_never_reset ... FAILED
failures:
    storage::replica::projected_admission::tests::ledger_never_reset
test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s
LOG
# Trimmed from the 20261117T021358Z-da8d56074408 core log. Keep the
# interleaved thread name and a successful binary after the aborted one.
cat > "$out/stack.log" <<'LOG'
     Running tests/all.rs (/home/user/target/debug/deps/all-b4a88211df93a977)
test sql_boolean_expressions::boolean_where_keeps_rows_and_literal_primary_key_lookup ...
thread 'ok
sql_boolean_expressions::boolean_sequence_dependency_and_frozen_parser_guards' (110983) has overflowed its stack
fatal runtime error: stack overflow, aborting
error: test failed, to rerun pass `--test all`

Caused by:
  process didn't exit successfully: `/home/user/target/debug/deps/all-b4a88211df93a977` (signal: 6, SIGABRT: process abort signal)
     Running tests/durable_boundaries.rs (/home/user/target/debug/deps/durable_boundaries-d92d73ce455db979)
test sample::later ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s
LOG
cat "$out/expected.log" "$out/stack.log" > "$out/expected-abort.log"
cat > "$out/missing.log" <<'LOG'
     Running tests/missing.rs (/target/debug/deps/missing-1234)
running 1 test
test sample::never_finishes ...
LOG
cat "$out/missing.log" "$out/pass.log" > "$out/missing-next.log"
cat "$out/missing.log" > "$out/missing-doc.log"
cat >> "$out/missing-doc.log" <<'LOG'
   Doc-tests giggles_core
running 1 test
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s
LOG
cat > "$out/signal.log" <<'LOG'
     Running tests/signal.rs (/target/debug/deps/signal-1234)
test sample::crashes ...
  process didn't exit successfully: `/target/debug/deps/signal-1234` (signal: 11, SIGSEGV: invalid memory reference)
LOG
cat > "$out/exit.log" <<'LOG'
     Running tests/exit.rs (/target/debug/deps/exit-1234)
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s
  process didn't exit successfully: `/target/debug/deps/exit-1234` (exit status: 1)
LOG
cat > "$out/runtime.log" <<'LOG'
     Running tests/runtime.rs (/target/debug/deps/runtime-1234)
fatal runtime error: failed to initiate panic, error 5
LOG
cat > "$out/overflow.log" <<'LOG'
     Running tests/overflow.rs (/target/debug/deps/overflow-1234)
thread 'sample::overflow' has overflowed its stack
LOG
cat > "$out/bare-signal.log" <<'LOG'
     Running tests/signal.rs (/target/debug/deps/signal-1234)
(signal: 15, SIGTERM: termination signal)
LOG
# Cargo diagnostics still identify the binary if a Running line is absent.
grep 'process didn' "$out/signal.log" > "$out/diagnostic.log"

# Override only the exact-test execution seam; checks must never invoke cargo.
retry_log=$out/fail.log
retry_rc=1
retry_test() { cat "$retry_log"; return "$retry_rc"; }
check() {
  local label=$1 log=$2 rc=$3 want=$4 result
  : > "$summary"
  if check_tests giggles-core "$log" "$rc"; then result=PASS; else result=FAIL; fi
  note "RESULT: $result"
  if ! grep -qxF "RESULT: $want" "$summary"; then
    cat "$summary" >&2
    echo "$label: wanted $want" >&2
    exit 1
  fi
  printf '%s: RESULT %s\n' "$label" "$result"
}
require_summary() { grep -qF "$1" "$summary" || { cat "$summary" >&2; exit 1; }; }

check normal-pass "$out/pass.log" 0 PASS
check normal-failure "$out/fail.log" 101 FAIL
check expected-linux-failure "$out/expected.log" 101 PASS
check sigabrt-stack-overflow "$out/stack.log" 101 FAIL
require_summary 'UNEXPECTED FAILURES:'
require_summary 'BINARY ABORTED: tests/all.rs (all-b4a88211df93a977)'
require_summary 'signal 6, SIGABRT'
require_summary 'last test started: sql_boolean_expressions::boolean_sequence_dependency_and_frozen_parser_guards'
check expected-failure-plus-abort "$out/expected-abort.log" 101 FAIL
check missing-result-at-eof "$out/missing.log" 0 FAIL
require_summary 'BINARY ABORTED: tests/missing.rs (missing-1234) no test result: line'
check missing-result-before-next-binary "$out/missing-next.log" 101 FAIL
check missing-result-before-doc-tests "$out/missing-doc.log" 101 FAIL
check sigsegv "$out/signal.log" 101 FAIL
require_summary 'signal 11, SIGSEGV'
check unsuccessful-exit-after-result "$out/exit.log" 101 FAIL
check fatal-runtime-error "$out/runtime.log" 101 FAIL
check overflow-without-cargo-diagnostic "$out/overflow.log" 101 FAIL
check signal-without-cargo-diagnostic "$out/bare-signal.log" 101 FAIL
check cargo-diagnostic-without-running "$out/diagnostic.log" 101 FAIL

retry_log=$out/pass.log
retry_rc=0
check passing-exact-rerun "$out/fail.log" 101 PASS
require_summary 'FLAKY (failed in the full run, passed alone):'
retry_rc=101
check nonzero-exact-rerun "$out/fail.log" 101 FAIL
retry_log=$out/stack.log
retry_rc=0
check aborted-exact-rerun "$out/fail.log" 101 FAIL
if grep -q 'FLAKY' "$summary"; then echo 'aborted rerun counted as flaky' >&2; exit 1; fi

for message in LNK1171 'out of memory' SIGKILL 'signal: 9' Killed; do
  printf '%s\n' "$message" > "$out/infra.log"
  if ! flaky "$out/infra.log"; then echo "missing infrastructure rerun: $message" >&2; exit 1; fi
done
cat "$out/infra.log" "$out/pass.log" > "$out/infra-results.log"
if flaky "$out/infra-results.log"; then echo 'reran infrastructure failure with results' >&2; exit 1; fi
for log in stack overflow runtime; do
  printf 'out of memory\n' > "$out/not-infra.log"
  cat "$out/$log.log" >> "$out/not-infra.log"
  if flaky "$out/not-infra.log"; then echo "$log counted as infrastructure" >&2; exit 1; fi
done
echo 'infrastructure-rerun-classification: PASS'

if [ $# -gt 0 ]; then
  check real-sigabrt-log "$1" 101 FAIL
  require_summary 'BINARY ABORTED: tests/all.rs (all-b4a88211df93a977)'
  require_summary 'signal 6, SIGABRT'
  require_summary 'last test started: sql_boolean_expressions::boolean_sequence_dependency_and_frozen_parser_guards'
  grep 'BINARY ABORTED:' "$summary"
fi
