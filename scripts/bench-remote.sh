#!/usr/bin/env bash
# Run an end-to-end benchmark (PostgreSQL 18 vs GigglesDB) on a boat.dev Linux
# box, under the same rules as scripts/gate-remote.sh (AGENTS.md "Remote gates
# on boat.dev"): daily start cap, one of max_boxes account-wide slots, a large
# box from the gigglesdb-bench-base snapshot with a TTL, the tip sent as a git
# bundle, results copied back, box deleted and slot released on every exit.
# Design: GD-0082 W2.
#
# Usage:
#   scripts/bench-remote.sh --tip <commit> --who <name> [matrix options]
#   scripts/bench-remote.sh --build-snapshot --tip <commit> --who <name>
# Matrix options (comma lists; defaults are the proper baseline):
#   --targets pg,giggles-sql[,giggles-native]   (default pg,giggles-sql)
#   --clients 8,32          app-loadtest --clients values (default 8,32)
#   --opset common          common = ops that pass on all three targets;
#                           sql-full = every op GigglesDB SQL passes (pg and
#                           giggles-sql only). See scripts/BENCH.md.
#   --mixes read,write,tx   read|write|tx|error = 100% of that kind over the
#                           op set, mixed = 60/25/10/5, or raw weights such as
#                           read=50+write=50 (default read,write,tx)
#   --protocols all         all|simple|extended|prepared, or a+b (default all;
#                           giggles-native runs once per mix, protocol=native)
#   --warmup 30 --duration 300 --repeats 3 --rows 1000   (seconds / counts)
#   --server-cpus 0-5 --client-cpus 6-7   taskset split (default: all but the
#                           last two cores for servers, last two for the client)
# Other options:
#   --profile               extra diagnostic repeat per GigglesDB target/client
#                           after normal repeats (first mix/protocol group);
#                           separate fp build <=1510s, profile phase <=1800s
#   --ttl <seconds>         box lifetime, at most 7200 (default 7200)
#   --slot-wait <seconds>   how long to wait for a free slot (default 1800)
#   --data-root <box path>  server data parent dir on the box (default: probe
#                           for a block-device-backed path; /home/user is FUSE)
#   --build-snapshot        start from gigglesdb-gate-base, run
#                           scripts/bench-snapshot-setup.sh, save the box as
#                           gigglesdb-bench-base (never touches gigglesdb-gate-base)
# A matrix whose estimated run time exceeds TTL minus 20 minutes (release
# build and setup), including the bounded profile build/phase, is refused;
# split it into several runs.
#
# Exit: 0 every run passed (0 errors, invariants held), 1 some run failed, 2 usage error or matrix too
# long, 3 daily start cap reached, 4 no slot free in time, 5 infrastructure
# error. Results land in J:/gigglesdb-work/boat/bench/<utc>-<tip12>-<who>-<pid>/
# (summary.md, results.jsonl, meta.json, logs.tar.gz).
set -euo pipefail

repo=K:/Projects/gigglesdb
boatdir=J:/gigglesdb-work/boat
B=${BOAT:-$HOME/.ascii/bin/boat.exe}
snapshot=gigglesdb-bench-base
gate_snapshot=gigglesdb-gate-base
max_starts=200   # plan-included starts per day, all agents
max_boxes=${GATE_MAX_BOXES:-10}   # concurrent boxes, account-wide, shared with gates
type=large       # benches run on large only
tip='' who='' ttl=7200 slot_wait=1800 build_snapshot=0 profile=0
targets=pg,giggles-sql clients=8,32 mixes=read,write,tx protocols=all opset=common data_root=''
warmup=30 duration=300 repeats=3 rows=1000 server_cpus='' client_cpus=''
while [ $# -gt 0 ]; do
  case "$1" in
    --tip) tip=$2; shift 2 ;;
    --who) who=$2; shift 2 ;;
    --ttl) ttl=$2; shift 2 ;;
    --slot-wait) slot_wait=$2; shift 2 ;;
    --profile) profile=1; shift ;;
    --build-snapshot) build_snapshot=1; shift ;;
    --targets) targets=$2; shift 2 ;;
    --clients) clients=$2; shift 2 ;;
    --mixes) mixes=$2; shift 2 ;;
    --protocols) protocols=$2; shift 2 ;;
    --opset) opset=$2; shift 2 ;;
    --data-root) data_root=$2; shift 2 ;;
    --warmup) warmup=$2; shift 2 ;;
    --duration) duration=$2; shift 2 ;;
    --repeats) repeats=$2; shift 2 ;;
    --rows) rows=$2; shift 2 ;;
    --server-cpus) server_cpus=$2; shift 2 ;;
    --client-cpus) client_cpus=$2; shift 2 ;;
    *) echo "unknown argument $1" >&2; exit 2 ;;
  esac
done
usage() { sed -n '2,40p' "$0" >&2; exit 2; }
[ -n "$tip" ] && [ -n "$who" ] || usage
[[ "$who" =~ ^[A-Za-z0-9_.-]+$ ]] || { echo "bad --who" >&2; exit 2; }
who=bench-${who#bench-}
[ "$ttl" -le 7200 ] 2>/dev/null || { echo "--ttl must be at most 7200" >&2; exit 2; }
[[ "$targets" =~ ^(pg|giggles-sql|giggles-native)(,(pg|giggles-sql|giggles-native))*$ ]] || { echo "bad --targets" >&2; exit 2; }
[[ "$clients" =~ ^[0-9]+(,[0-9]+)*$ ]] || { echo "bad --clients" >&2; exit 2; }
[[ "$mixes" =~ ^[a-z0-9=+]+(,[a-z0-9=+]+)*$ ]] || { echo "bad --mixes" >&2; exit 2; }
[[ "$protocols" =~ ^(all|simple|extended|prepared)([+,](all|simple|extended|prepared))*$ ]] || { echo "bad --protocols" >&2; exit 2; }
[[ "$opset" =~ ^(common|sql-full)$ ]] || { echo "bad --opset (common|sql-full)" >&2; exit 2; }
if [ "$opset" = sql-full ] && [[ ",$targets," == *",giggles-native,"* ]]; then
  echo "--opset sql-full is valid only for --targets pg,giggles-sql (giggles-native cannot run it)" >&2; exit 2
fi
[[ -z "$data_root" || "$data_root" =~ ^/[A-Za-z0-9_./-]+$ ]] || { echo "bad --data-root (absolute box path)" >&2; exit 2; }
for v in "$warmup" "$duration" "$repeats" "$rows"; do [[ "$v" =~ ^[0-9]+$ ]] || { echo "bad number $v" >&2; exit 2; }; done
[ "$duration" -gt 0 ] && [ "$repeats" -gt 0 ] && [ "$rows" -ge 10 ] || { echo "duration/repeats must be > 0, rows >= 10" >&2; exit 2; }
for v in "$server_cpus" "$client_cpus"; do [[ -z "$v" || "$v" =~ ^[0-9,-]+$ ]] || { echo "bad cpu list $v" >&2; exit 2; }; done
[ -x "$B" ] || { echo "boat CLI not found at $B (set BOAT)" >&2; exit 5; }

# Time estimate: each measured run costs warmup + duration + ~30s for server
# restart, schema setup, seeding and teardown. Build and setup get a fixed 20
# minutes. giggles-native runs once per (clients, mix), not per protocol.
# Profiling adds 1510s for its build plus min(1800, 270 + targets * 900)s
# for the phase, where targets counts non-pg target/client pairs (270s =
# apt-get 190s plus perf version/sysctl checks; 900s = per-target worst case).
count() { echo "$1" | tr ',' '\n' | grep -c .; }
nt=$(count "$targets"); per=$(( nt * $(count "$protocols") ))
[[ ",$targets," == *",giggles-native,"* ]] && per=$(( (nt - 1) * $(count "$protocols") + 1 ))
runs=$(( per * $(count "$clients") * $(count "$mixes") * repeats ))
est=$(( runs * (warmup + duration + 30) ))
measured_est=$est
# Bounded diagnostic build, installation and per-target allowance.
profile_runs=0
if [ "$profile" = 1 ]; then
  for t in ${targets//,/ }; do
    [ "$t" = pg ] || profile_runs=$(( profile_runs + $(count "$clients") ))
  done
fi
profile_extra=0
if [ "$profile" = 1 ] && [ "$profile_runs" -gt 0 ]; then
  phase=$(( 270 + profile_runs * 900 ))
  [ "$phase" -gt 1800 ] && phase=1800
  profile_extra=$(( 1510 + phase ))
fi
est=$(( est + profile_extra ))
budget=$(( ttl - 1200 ))
if [ $build_snapshot = 0 ] && [ "$est" -gt "$budget" ]; then
  echo "matrix of $runs runs needs ~$(( est / 60 )) min, over the $(( budget / 60 )) min a ${ttl}s box leaves after 20 min of build/setup." >&2
  echo "Split it (for example one --clients value or one --mixes value per run) or lower --repeats/--duration." >&2
  exit 2
fi

g() { git -C "$repo" "$@"; }
tip=$(g rev-parse --verify "$tip^{commit}")
short=${tip:0:12}
stamp=$(date -u +%Y%m%dT%H%M%SZ)
if [ $build_snapshot = 1 ]; then run="$boatdir/bench/snapshot-$stamp-$short-$who-$$"; else run="$boatdir/bench/$stamp-$short-$who-$$"; fi
mkdir -p "$run" "$boatdir/slots"
log() { echo "[bench-remote $(date -u +%T)] $*" >&2; }
bj() { "$B" --no-update "$@"; }
jfield() { python -c "import json,sys
lines=[l for l in sys.stdin.read().splitlines() if l.strip().startswith('{')]
d=json.loads(lines[-1]) if lines else {}
print(d.get(sys.argv[1],'') if sys.argv[1] else json.dumps(d))" "$1"; }
bexec() { bj exec "$1" --timeout 600 --json -- bash -lc "$2" | jfield stdout; }
bexec_t() { # <seconds> <box> <command>: bound both boat and its remote command
  local secs=$1; shift
  timeout --kill-after=10 "$(( secs + 30 ))" "$B" --no-update exec "$1" --timeout "$secs" --json -- bash -lc "$2" | jfield stdout
}
copied=0 bench_started=0
copy_back() { # Best effort, including incomplete benches; never prevents deletion.
  [ "$copied" = 1 ] && return 0
  copied=1
  bexec_t 120 "$box" "cd /home/user && timeout --kill-after=10 90 tar --exclude='*.data' --exclude='*.data.old' --exclude='raw-*' -czf bench-logs.tar.gz bench bench.out" >/dev/null || log "WARNING: log archive incomplete"
  ( cd "$run" && timeout --kill-after=10 150 "$B" --no-update scp "$box:/home/user/bench-logs.tar.gz" logs.tar.gz >/dev/null 2>&1 ) || log "WARNING: log download failed"
  local f
  for f in summary.md results.jsonl meta.json progress; do
    tar -xzOf "$run/logs.tar.gz" "bench/$f" > "$run/$f" 2>/dev/null || true
    [ -s "$run/$f" ] || rm -f "$run/$f"
  done
  tar -xzf "$run/logs.tar.gz" -C "$run" --strip-components=1 bench/profiles 2>/dev/null || true
  return 0
}

# 1. Daily start cap (UTC day, all agents; one START line per box).
today=$(date -u +%F)
used=$(grep -c "^$today.*| START" "$boatdir/usage.log" 2>/dev/null || true)
used=${used:-0}
if [ "$used" -ge "$max_starts" ]; then
  log "daily cap reached ($used/$max_starts starts today)"; exit 3
fi

# 2. Slot (atomic mkdir), released on exit; box deleted on every exit path.
slot='' box='' ref="refs/bench-remote/$stamp-$$" started=$(date +%s)
cleanup() {
  rc=$?
  if [ -n "$box" ]; then
    if [ "$bench_started" = 1 ] && [ "$copied" != 1 ]; then
      log "copying partial results back"
      copy_back || true
    fi
    log "deleting box $box"
    bj delete "$box" --yes >/dev/null 2>&1 || log "WARNING: delete of $box failed; delete it by hand"
    mins=$(( ($(date +%s) - started + 59) / 60 ))
    echo "$(date -u +%FT%TZ) | $who | $box | $type | $short | END bench rc=$rc ${mins}min" >> "$boatdir/usage.log"
  fi
  [ -n "$slot" ] && rm -rf "$boatdir/slots/$slot"
  g update-ref -d "$ref" 2>/dev/null || true
  rm -f "$run/stack.bundle"
  exit $rc
}
trap cleanup EXIT
trap 'exit 5' INT TERM HUP
deadline=$(( $(date +%s) + slot_wait ))
while [ -z "$slot" ]; do
  for n in $(seq 1 "$max_boxes"); do s=slot-$n
    if mkdir "$boatdir/slots/$s" 2>/dev/null; then slot=$s; break; fi
  done
  [ -n "$slot" ] && break
  [ "$(date +%s)" -lt "$deadline" ] || { log "no free slot within ${slot_wait}s"; exit 4; }
  sleep 30
done
echo "pending $who $short $(date -u +%FT%TZ)" > "$boatdir/slots/$slot/owner"
log "took $slot"

# 3. Bundle the tip (commits after the snapshot's base when possible).
if [ $build_snapshot = 1 ]; then from=$gate_snapshot; base=$(cat "$boatdir/base-commit.txt")
else from=$snapshot; base=$(cat "$boatdir/bench-base-commit.txt" 2>/dev/null || cat "$boatdir/base-commit.txt"); fi
g update-ref "$ref" "$tip"
if g merge-base --is-ancestor "$base" "$tip"; then
  g bundle create "$run/stack.bundle" "$ref" "^$base" 2>/dev/null
else
  log "tip does not descend from snapshot base $base; sending full history"
  g bundle create "$run/stack.bundle" "$ref" 2>/dev/null
fi

# 4. Box from the snapshot.
log "starting $type box from $from"
out=$(bj new --from "$from" --type "$type" --no-env --ttl "$ttl" --name "$who-$short" --json 2>&1) || {
  log "boat new failed: $(echo "$out" | tail -2)"; exit 5; }
box=$(echo "$out" | grep -o '"id":"bx_[a-z0-9]*"' | head -1 | cut -d'"' -f4)
[ -n "$box" ] || { log "no box id in: $(echo "$out" | tail -2)"; exit 5; }
echo "$(date -u +%FT%TZ) | $who | $box | $type | $short | START bench" >> "$boatdir/usage.log"
echo "$box $who $short $(date -u +%FT%TZ)" > "$boatdir/slots/$slot/owner"
log "box $box ready"

# 5. Ship the bundle and box-side scripts; check out the tip.
here=$(cd "$(dirname "$0")" && pwd)
cp "$here/bench-remote-runner.sh" "$here/bench-snapshot-setup.sh" "$run/"
( cd "$run" && for f in stack.bundle bench-remote-runner.sh bench-snapshot-setup.sh; do
    bj scp "$f" "$box:/home/user/$f" >/dev/null 2>&1 || exit 1
  done ) || { log "upload failed"; exit 5; }
head=$(bexec "$box" "cd /home/user/gigglesdb && git fetch -q /home/user/stack.bundle '$ref:refs/bench/tip' && git checkout -q --detach $tip && git rev-parse HEAD")
[ "$(echo "$head" | tr -d '[:space:]')" = "$tip" ] || { log "checkout of $tip on box failed: $head"; exit 5; }

wait_for() { # wait_for <done-file> <progress-file>
  local previous_line="" latest_line
  while :; do
    sleep 30
    st=$(bexec_t 60 "$box" "cat $1 2>/dev/null; tail -1 $2 2>/dev/null" || true)
    latest_line=$(echo "$st" | sed '/^rc=/d' | tail -1)
    if [ -n "$latest_line" ] && [ "$latest_line" != "$previous_line" ]; then
      log "box: $latest_line"
      echo "$latest_line" >> "$run/progress.log"
      previous_line=$latest_line
    fi
    if echo "$st" | grep -q '^rc='; then break; fi
    [ $(( $(date +%s) - started )) -lt $(( ttl - 600 )) ] || { log "did not finish before the box TTL"; exit 5; }
  done
}

if [ $build_snapshot = 1 ]; then
  # 6s. Snapshot build: setup script, then save under the bench name only.
  [ "$snapshot" != "$gate_snapshot" ] || exit 5
  bj exec "$box" --detach -- bash -lc "bash /home/user/bench-snapshot-setup.sh > /home/user/bench-setup.log 2>&1; echo rc=\$? > /home/user/bench-setup.done" >/dev/null
  log "snapshot setup running"
  wait_for /home/user/bench-setup.done /home/user/bench-setup.log
  bexec "$box" "tail -40 /home/user/bench-setup.log" > "$run/setup-tail.log"
  bexec "$box" "cat /home/user/bench-base.txt" > "$run/bench-base.txt" || true
  grc=$(echo "$st" | grep -o '^rc=[0-9]*' | cut -d= -f2)
  [ "$grc" = 0 ] || { log "setup failed rc=$grc; see $run/setup-tail.log"; exit 1; }
  bexec "$box" "rm -f /home/user/stack.bundle /home/user/bench-setup.done; sync" >/dev/null
  log "saving snapshot $snapshot"
  bj snapshot "$box" "$snapshot" > "$run/snapshot.out" 2>&1 || { log "boat snapshot failed: $(tail -2 "$run/snapshot.out")"; exit 5; }
  echo "$tip" > "$boatdir/bench-base-commit.txt"
  cat "$run/bench-base.txt"
  log "snapshot $snapshot saved from $box at $tip; results in $run"
  exit 0
fi

# 6. Run the bench in the background on the box and wait for it.
envs="PROFILE=$profile TARGETS=$targets CLIENTS=$clients MIXES=$mixes PROTOCOLS=$protocols OPSET=$opset WARMUP=$warmup DURATION=$duration REPEATS=$repeats ROWS=$rows"
[ -n "$data_root" ] && envs+=" DATA_ROOT=$data_root"
[ -n "$server_cpus" ] && envs+=" SERVER_CPUS=$server_cpus"
[ -n "$client_cpus" ] && envs+=" CLIENT_CPUS=$client_cpus"
bj exec "$box" --detach -- bash -lc "$envs bash /home/user/bench-remote-runner.sh > /home/user/bench.out 2>&1" >/dev/null
bench_started=1
log "bench running: $runs runs, ~$(( measured_est / 60 )) min measured + up to $(( (profile_extra + 59) / 60 )) min profile after the build"
wait_for /home/user/bench/done /home/user/bench/progress

# 7. Copy results back (also used by cleanup on failure/TTL).
copy_back
for f in summary.md results.jsonl meta.json; do
  [ -s "$run/$f" ] || bexec "$box" "cat /home/user/bench/$f" > "$run/$f" || true
done
cat "$run/summary.md"
grc=$(echo "$st" | grep -o '^rc=[0-9]*' | cut -d= -f2)
log "bench rc=$grc; results in $run"
[ "$grc" = 0 ] && exit 0 || exit 1
