#!/usr/bin/env bash
set -euo pipefail
mode=pg
if [[ ${1:-} == spanner || ${1:-} == private ]]; then mode=$1; shift; fi
d2=0
if [[ $mode == spanner && ${1:-} == --d2 ]]; then d2=1; shift; fi
root=$(mktemp -d)
nunya_exe=${NUNYA_EXE:-nunya}
linux_bin=
while (( $# )); do
  case "$1" in
    --work-root) root=${2:?missing work root}; shift 2 ;;
    --nunya-exe) nunya_exe=${2:?missing executable}; shift 2 ;;
    --linux-bin) linux_bin=${2:?missing Linux binary}; shift 2 ;;
    *) echo 'usage: run.sh [spanner [--d2]|private] [--work-root DIR] [--nunya-exe PATH] [--linux-bin PATH]' >&2; exit 1 ;;
  esac
done
if (( d2 )); then
  mkdir -p "$root"
  root=$(cd "$root" && pwd -P)
  source_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
  rc=0
  NUNYA_D2_WORK="$root/d2" bash "$source_dir/spanner/d2.sh" all || rc=1
  NUNYA_D2_WORK="$root/d2" NUNYA_D2_MUX_RW=off bash "$source_dir/spanner/d2.sh" all || rc=1
  exit "$rc"
fi
mkdir -p "$root"
root=$(cd "$root" && pwd -P)
printf 'Work root: %s\n' "$root"
run="$root/nunya-harness-$$-$RANDOM"
mkdir "$run" "$run/tmp"
mkdir -p "$run/data/pg16/data" "$run/data/pg17/data" "$run/data/pg18"
export TEMP="$run/tmp" TMP="$run/tmp" TMPDIR="$run/tmp"
source_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
if [[ $mode == spanner ]]; then
  cp "$source_dir/spanner/compose.yaml" "$run/compose.yaml"
  project="nunya-spanner-$$-$RANDOM"
  compose=(docker compose -p "$project" -f "$run/compose.yaml")
  cleanup() { "${compose[@]}" down -v >"$run/down.log" 2>&1; }
  trap cleanup EXIT
  "${compose[@]}" up -d >"$run/up.log" 2>&1
  bash "$source_dir/spanner/setup.sh" >"$run/setup.log" 2>&1
  cat "$run/setup.log"
  printf '%s\n' \
    'export NUNYA_TEST_SPANNER_EMULATOR=127.0.0.1:9010' \
    'export NUNYA_TEST_SPANNER_DB=projects/test-project/instances/test-instance/databases/gsql' \
    'export NUNYA_TEST_SPANNER_PGADAPTER=127.0.0.1:5432'
  printf 'Spanner ready. Press Enter to stop.\n'
  read -r _ || true
  exit
fi
if [[ $mode == private ]]; then
  if ! nunya_exe=$(command -v "$nunya_exe"); then
    echo 'Nunya executable not found; set NUNYA_EXE or --nunya-exe' >&2
    exit 1
  fi
  source "$source_dir/private/run.sh"
  private_main
  exit
fi
cp "$source_dir/compose.yaml" "$source_dir/seed.sql" "$run/"
project="nunya-$$-$RANDOM"
compose=(docker compose -p "$project" -f "$run/compose.yaml" --profile dev)
cleanup() { "${compose[@]}" down -v >"$run/down.log" 2>&1; }
trap cleanup EXIT
"${compose[@]}" up -d --wait --wait-timeout 180 >"$run/up.log" 2>&1
# Clone the already seeded app database: seed.sql creates a cluster-wide role
# and grants CONNECT specifically on app, so replaying it would fail.
"${compose[@]}" exec -T pg18 psql -X -v ON_ERROR_STOP=1 -U postgres -d postgres <<'SQL'
CREATE DATABASE nunya_ni_a TEMPLATE app;
CREATE DATABASE nunya_ni_b TEMPLATE app;
GRANT CONNECT ON DATABASE nunya_ni_a, nunya_ni_b TO nunya;
SQL
for database in nunya_ni_a nunya_ni_b; do
  "${compose[@]}" exec -T pg18 psql -X -v ON_ERROR_STOP=1 -U postgres -d "$database" <<'SQL'
INSERT INTO outbox (to_address, template)
SELECT email, 'ni-reminder' FROM customers WHERE failed_payments >= 2;
INSERT INTO outbox (to_address, template)
SELECT email, 'ni-repeat' FROM customers WHERE id = 3;
INSERT INTO outbox (to_address, template) VALUES (NULL, 'ni-null');
SQL
done
"${compose[@]}" exec -T pg18 psql -X -v ON_ERROR_STOP=1 -U postgres -d nunya_ni_b <"$source_dir/ni_permute.sql"
printf '%s\n' \
  'export NUNYA_TEST_PG_A=postgresql://nunya:nunya-harness-only@127.0.0.1:18432/nunya_ni_a' \
  'export NUNYA_TEST_PG_B=postgresql://nunya:nunya-harness-only@127.0.0.1:18432/nunya_ni_b'
printf 'PostgreSQL 16/17/18 ready on 16432/17432/18432. Press Enter to stop.\n'
read -r _ || true
