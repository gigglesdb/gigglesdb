#!/usr/bin/env bash
# Run a landing gate on a boat.dev Linux box, following the AGENTS.md
# "When to gate" and "Remote gates on boat.dev" rules: daily start cap, one of
# two account-wide slots, a box from the gigglesdb-gate-base snapshot with a
# TTL, the stack sent as a git bundle, results copied back, box deleted.
#
# Usage:
#   scripts/gate-remote.sh --tip <commit> --who <coordinator> [options] -- <step>...
#
# <step> is one of:
#   <crate>                    cargo test --no-fail-fast + clippy, default features
#   <crate>:nodef              the same with --no-default-features
#   <crate>:tests=<f1>,<f2>    test-only stack: cargo test --test all <f>:: for
#                              each file, plus clippy --all-targets (default)
# Options:
#   --type default|large       box size (large needs user approval; default: default)
#   --ttl <seconds>            box lifetime, at most 7200 (default 7200)
#   --slot-wait <seconds>      how long to wait for a free slot (default 1800)
#   --no-clippy                skip clippy (only when the stack has no Rust changes)
#
# Exit: 0 gate passed, 1 gate failed (unexpected test failures or build or
# clippy errors), 2 usage error, 3 daily start cap reached (gate locally),
# 4 no slot free in time, 5 infrastructure error. Results land in
# J:/gigglesdb-work/boat/runs/<utc>-<tip12>/ (summary.txt, logs.tar.gz).
# Linux-only expected failures are listed in scripts/gate-linux-expected-failures.txt.
set -euo pipefail

repo=K:/Projects/gigglesdb
boatdir=J:/gigglesdb-work/boat
B=${BOAT:-$HOME/.ascii/bin/boat.exe}
snapshot=gigglesdb-gate-base
max_starts=15
tip='' who='' type=default ttl=7200 slot_wait=1800 clippy=1
steps=()
while [ $# -gt 0 ]; do
  case "$1" in
    --tip) tip=$2; shift 2 ;;
    --who) who=$2; shift 2 ;;
    --type) type=$2; shift 2 ;;
    --ttl) ttl=$2; shift 2 ;;
    --slot-wait) slot_wait=$2; shift 2 ;;
    --no-clippy) clippy=0; shift ;;
    --) shift; steps+=("$@"); break ;;
    *) echo "unknown argument $1" >&2; exit 2 ;;
  esac
done
usage() { sed -n '2,26p' "$0" >&2; exit 2; }
[ -n "$tip" ] && [ -n "$who" ] && [ ${#steps[@]} -gt 0 ] || usage
case "$type" in default|large) ;; *) echo "--type must be default or large" >&2; exit 2 ;; esac
[ "$ttl" -le 7200 ] 2>/dev/null || { echo "--ttl must be at most 7200" >&2; exit 2; }
for s in "${steps[@]}"; do
  [[ "$s" =~ ^giggles-[a-z0-9-]+(:nodef|:tests=[A-Za-z0-9_,]+)?$ ]] || { echo "bad step: $s" >&2; exit 2; }
done
[ -x "$B" ] || { echo "boat CLI not found at $B (set BOAT)" >&2; exit 5; }

g() { git -C "$repo" "$@"; }
tip=$(g rev-parse --verify "$tip^{commit}")
short=${tip:0:12}
stamp=$(date -u +%Y%m%dT%H%M%SZ)
run="$boatdir/runs/$stamp-$short"
mkdir -p "$run" "$boatdir/slots"
log() { echo "[gate-remote $(date -u +%T)] $*" >&2; }
bj() { "$B" --no-update "$@"; }
# Last JSON line of a boat command, or a field of it.
jfield() { python -c "import json,sys
lines=[l for l in sys.stdin.read().splitlines() if l.strip().startswith('{')]
d=json.loads(lines[-1]) if lines else {}
print(d.get(sys.argv[1],'') if sys.argv[1] else json.dumps(d))" "$1"; }
bexec() { # bexec <id> <command>  (short commands only, 600s max)
  bj exec "$1" --timeout 600 --json -- bash -lc "$2" | jfield stdout; }

# 1. Daily start cap (UTC day, all agents; one START line per box).
today=$(date -u +%F)
used=$(grep -c "^$today.*| START" "$boatdir/usage.log" 2>/dev/null || true)
used=${used:-0}
if [ "$used" -ge "$max_starts" ]; then
  log "daily cap reached ($used/$max_starts starts today): run this gate locally"; exit 3
fi

# 2. Slot (atomic mkdir), released on exit.
slot='' box='' ref="refs/gate-remote/$stamp-$$" started=$(date +%s)
cleanup() {
  rc=$?
  if [ -n "$box" ]; then
    log "deleting box $box"
    bj delete "$box" --yes >/dev/null 2>&1 || log "WARNING: delete of $box failed; delete it by hand"
    mins=$(( ($(date +%s) - started + 59) / 60 ))
    echo "$(date -u +%FT%TZ) | $who | $box | $type | $short | END rc=$rc ${mins}min" >> "$boatdir/usage.log"
  fi
  [ -n "$slot" ] && rm -rf "$boatdir/slots/$slot"
  g update-ref -d "$ref" 2>/dev/null || true
  exit $rc
}
trap cleanup EXIT
trap 'exit 5' INT TERM
deadline=$(( $(date +%s) + slot_wait ))
while [ -z "$slot" ]; do
  for s in slot-1 slot-2; do
    if mkdir "$boatdir/slots/$s" 2>/dev/null; then slot=$s; break; fi
  done
  [ -n "$slot" ] && break
  [ "$(date +%s)" -lt "$deadline" ] || { log "no free slot within ${slot_wait}s: $(cat "$boatdir"/slots/*/owner 2>/dev/null | tr '\n' ';')"; exit 4; }
  sleep 30
done
echo "pending $who $short $(date -u +%FT%TZ)" > "$boatdir/slots/$slot/owner"
log "took $slot"

# 3. Bundle the stack (commits after the snapshot's base when possible).
base=$(cat "$boatdir/base-commit.txt")
g update-ref "$ref" "$tip"
if g merge-base --is-ancestor "$base" "$tip"; then
  g bundle create "$run/stack.bundle" "$ref" "^$base" 2>/dev/null
else
  log "tip does not descend from snapshot base $base; sending full history"
  g bundle create "$run/stack.bundle" "$ref" 2>/dev/null
fi

# 4. Box from the snapshot.
log "starting $type box from $snapshot"
out=$(bj new --from "$snapshot" --type "$type" --no-env --ttl "$ttl" --name "gate-$who-$short" --json 2>&1) || {
  log "boat new failed: $(echo "$out" | tail -2)"; exit 5; }
box=$(echo "$out" | grep -o '"id":"bx_[a-z0-9]*"' | head -1 | cut -d'"' -f4)
[ -n "$box" ] || { log "no box id in: $(echo "$out" | tail -2)"; exit 5; }
echo "$(date -u +%FT%TZ) | $who | $box | $type | $short | START" >> "$boatdir/usage.log"
echo "$box $who $short $(date -u +%FT%TZ)" > "$boatdir/slots/$slot/owner"
log "box $box ready"

# 5. Ship the bundle, runner and expected-failure list; check out the tip.
here=$(cd "$(dirname "$0")" && pwd)
cp "$here/gate-remote-runner.sh" "$here/gate-linux-expected-failures.txt" "$run/"
( cd "$run" && for f in stack.bundle gate-remote-runner.sh gate-linux-expected-failures.txt; do
    bj scp "$f" "$box:/home/user/$f" >/dev/null 2>&1 || exit 1
  done ) || { log "upload failed"; exit 5; }
head=$(bexec "$box" "cd /home/user/gigglesdb && git fetch -q /home/user/stack.bundle '$ref:refs/gate/tip' && git checkout -q --detach $tip && git rev-parse HEAD")
[ "$(echo "$head" | tr -d '[:space:]')" = "$tip" ] || { log "checkout of $tip on box failed: $head"; exit 5; }

# 6. Run the gate in the background on the box and wait for it.
args=$(printf '%q ' "${steps[@]}")
bj exec "$box" --detach -- bash -lc "CLIPPY=$clippy bash /home/user/gate-remote-runner.sh $args > /home/user/gate.out 2>&1" >/dev/null
log "gate running: ${steps[*]}"
while :; do
  sleep 30
  st=$(bexec "$box" "cat /home/user/gate/done 2>/dev/null; tail -1 /home/user/gate/progress 2>/dev/null" || true)
  if echo "$st" | grep -q '^rc='; then break; fi
  [ $(( $(date +%s) - started )) -lt $(( ttl - 300 )) ] || { log "gate did not finish before the box TTL"; exit 5; }
done

# 7. Copy results back.
bexec "$box" "cd /home/user && tar czf gate-logs.tar.gz gate gate.out" >/dev/null
( cd "$run" && bj scp "$box:/home/user/gate-logs.tar.gz" logs.tar.gz >/dev/null 2>&1 ) || log "WARNING: log download failed"
bexec "$box" "cat /home/user/gate/summary.txt" > "$run/summary.txt"
rm -f "$run/stack.bundle"
cat "$run/summary.txt"
grc=$(echo "$st" | grep -o '^rc=[0-9]*' | cut -d= -f2)
log "gate rc=$grc; results in $run"
[ "$grc" = 0 ] && exit 0 || exit 1
