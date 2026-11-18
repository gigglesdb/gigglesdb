#!/usr/bin/env bash
# Run a landing gate on a boat.dev Linux box, following the AGENTS.md
# "When to gate" and "Remote gates on boat.dev" rules: daily start cap, one
# of max_boxes account-wide slots, a box from the gigglesdb-gate-base snapshot with a
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
#   --type default|large       box size (default: large, user-approved for gates)
#   --ttl <seconds>            box lifetime, at most 7200 (default 7200)
#   --slot-wait <seconds>      how long to wait for a free slot (default 1800)
#   --no-clippy                skip clippy (only when the stack has no Rust changes)
#
# Exit: 0 gate passed, 1 gate failed (unexpected test failures or build or
# clippy errors), 2 usage error, 3 daily start cap reached (gate locally),
# 4 no slot free in time, 5 infrastructure error (including box filesystem,
# fetch or checkout hangs). Results land in
# J:/gigglesdb-work/boat/runs/<utc>-<tip12>-<sanitized-who>-<pid>/ (summary.txt, logs.tar.gz).
# Linux-only expected failures are listed in scripts/gate-linux-expected-failures.txt.
set -euo pipefail

repo=K:/Projects/gigglesdb
boatdir=J:/gigglesdb-work/boat
B=${BOAT:-$HOME/.ascii/bin/boat.exe}
snapshot=gigglesdb-gate-base
max_starts=200   # plan-included starts per day; above this is overage
max_boxes=${GATE_MAX_BOXES:-10}   # concurrent gate boxes, account-wide (plan allows 100)
tip='' who='' type=large ttl=7200 slot_wait=1800 clippy=1
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
  [[ "$s" =~ ^(giggles-[a-z0-9-]+(:nodef|:tests=[A-Za-z0-9_,]+)?|nunya(:tests=[A-Za-z0-9_,]+)?|skedaddle)$ ]] || { echo "bad step: $s" >&2; exit 2; }
done
[ -x "$B" ] || { echo "boat CLI not found at $B (set BOAT)" >&2; exit 5; }

g() { git -C "$repo" "$@"; }
tip=$(g rev-parse --verify "$tip^{commit}") || { echo "could not resolve gate tip" >&2; exit 5; }
short=${tip:0:12}
stamp=$(date -u +%Y%m%dT%H%M%SZ)
run="$boatdir/runs/$stamp-$short-${who//[^A-Za-z0-9_.-]/_}-$$"
mkdir -p "$run" "$boatdir/slots"
log() { echo "[gate-remote $(date -u +%T)] $*" >&2; }
bj() { "$B" --no-update "$@"; }
# BEGIN adopt_box (also exercised by the external dry-run driver)
adopt_box() {
  local listing
  listing=$(bj list --all --json 2>/dev/null) || return 0
  # Read only sandbox-level fields; nested objects and quoted braces are data.
  printf '%s\n' "$listing" | awk -v wanted="$boxname" '
    {
      for (i = 1; i <= length($0); i++) {
        c = substr($0, i, 1)
        if (quoted) {
          if (escaped) { token = token c; escaped = 0 }
          else if (c == "\\") { token = token c; escaped = 1 }
          else if (c == "\"") {
            quoted = 0
            if (depth == 2) {
              if (substr($0, i + 1) ~ /^[[:space:]]*:/) {
                key = token
                if (key == "access") sandbox = 1
              } else if (key == "id") id = token
              else if (key == "name") name = token
            }
          } else token = token c
          continue
        }
        if (c == "\"") { quoted = 1; token = "" }
        else if (c == "{") {
          depth++
          if (depth == 2) { sandbox = 0; key = id = name = "" }
        } else if (c == "}") {
          if (depth == 2 && sandbox && !found && name == wanted && id ~ /^bx_[a-z0-9]+$/) {
            print id
            found = 1
          }
          depth--
        }
      }
    }
  ' 2>/dev/null || true
  return 0
}
# END adopt_box
recover_box() {
  box=$(adopt_box)
  if [ -n "$box" ]; then
    echo "$(date -u +%FT%TZ) | $who | $box | $type | $short | START (adopted after failed new)" >> "$boatdir/usage.log"
    log "adopted box $box after failed new"
  fi
  return 0
}
# Last JSON line of a boat command, or a field of it.
jfield() { python -c "import json,sys
lines=[l for l in sys.stdin.read().splitlines() if l.strip().startswith('{')]
d=json.loads(lines[-1]) if lines else {}
print(d.get(sys.argv[1],'') if sys.argv[1] else json.dumps(d))" "$1"; }
bexec() { # bexec <id> <command>  (short commands only, 600s max)
  bj exec "$1" --timeout 600 --json -- bash -lc "$2" | jfield stdout; }
bexec_t() { # <seconds> <box> <command>: bound both boat and its remote command
  local secs=$1; shift
  timeout --kill-after=10 "$(( secs + 30 ))" "$B" --no-update exec "$1" --timeout "$secs" --json -- bash -lc "$2" | jfield stdout
}

# 1. Daily start cap (UTC day, all agents; one START line per box).
today=$(date -u +%F)
used=$(grep -c "^$today.*| START" "$boatdir/usage.log" 2>/dev/null || true)
used=${used:-0}
if [ "$used" -ge "$max_starts" ]; then
  log "daily cap reached ($used/$max_starts starts today): run this gate locally"; exit 3
fi

# 2. Slot (atomic mkdir), released on exit.
slot='' box='' boxname='' new_tried=0 ref="refs/gate-remote/$stamp-$$" started=$(date +%s)
cleanup() {
  rc=$?
  if [ -z "$box" ] && [ "$new_tried" = 1 ] && [ -n "$boxname" ]; then
    recover_box
  fi
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
trap 'exit 5' INT TERM HUP
deadline=$(( $(date +%s) + slot_wait ))
while [ -z "$slot" ]; do
  for n in $(seq 1 "$max_boxes"); do s=slot-$n
    if mkdir "$boatdir/slots/$s" 2>/dev/null; then slot=$s; break; fi
  done
  [ -n "$slot" ] && break
  [ "$(date +%s)" -lt "$deadline" ] || { log "no free slot within ${slot_wait}s: $(cat "$boatdir"/slots/*/owner 2>/dev/null | tr '\n' ';')"; exit 4; }
  sleep 30
done
echo "pending $who $short $(date -u +%FT%TZ)" > "$boatdir/slots/$slot/owner"
log "took $slot"

# 3. Bundle the stack (commits after the snapshot's base when possible).
base=$(cat "$boatdir/base-commit.txt") || { log "could not read snapshot base"; exit 5; }
g update-ref "$ref" "$tip" || { log "could not create bundle ref"; exit 5; }
if g merge-base --is-ancestor "$base" "$tip"; then
  g bundle create "$run/stack.bundle" "$ref" "^$base" 2>/dev/null || { log "could not create stack bundle"; exit 5; }
else
  log "tip does not descend from snapshot base $base; sending full history"
  g bundle create "$run/stack.bundle" "$ref" 2>/dev/null || { log "could not create stack bundle"; exit 5; }
fi

ahead=$(g rev-list --count "$tip" "^$base" 2>/dev/null) || ahead=unknown
bundle_kib=$(( ( $(wc -c < "$run/stack.bundle") + 1023 ) / 1024 ))
rebuild_note=''
if [[ "$ahead" =~ ^[0-9]+$ ]] && [ "$ahead" -gt 1000 ]; then
  rebuild_note=' (consider asking the snapshot coordinator for a rebuild)'
fi
log "tip is $ahead commits ahead of snapshot base ${base:0:12}; bundle $bundle_kib KiB$rebuild_note"

# 4. Box from the snapshot.
log "starting $type box from $snapshot"
# Truncate only the coordinator segment, preserving the unique run suffix.
name_who=${who//[^A-Za-z0-9_.-]/_}
name_suffix="-$short-$stamp-$$"
name_limit=$(( 120 - 5 - ${#name_suffix} ))
boxname="gate-${name_who:0:name_limit}$name_suffix"
new_tried=1
out=$(bj new --from "$snapshot" --type "$type" --no-env --ttl "$ttl" --name "$boxname" --json 2>&1) || {
  log "boat new failed: $(echo "$out" | tail -2)"; recover_box; exit 5; }
box=$(echo "$out" | grep -o '"id":"bx_[a-z0-9]*"' | head -1 | cut -d'"' -f4) || true
[ -n "$box" ] || { log "no box id in: $(echo "$out" | tail -2)"; recover_box; exit 5; }
echo "$(date -u +%FT%TZ) | $who | $box | $type | $short | START" >> "$boatdir/usage.log"
echo "$box $who $short $(date -u +%FT%TZ)" > "$boatdir/slots/$slot/owner"
log "box $box ready"

# 5. Ship the bundle, runner and expected-failure list; check out the tip.
here=$(cd "$(dirname "$0")" && pwd)
cp "$here/gate-remote-runner.sh" "$here/gate-linux-expected-failures.txt" "$run/" || { log "could not prepare runner upload"; exit 5; }
( cd "$run" && for f in stack.bundle gate-remote-runner.sh gate-linux-expected-failures.txt; do
    bj scp "$f" "$box:/home/user/$f" >/dev/null 2>&1 || exit 1
  done ) || { log "upload failed"; exit 5; }
fetch_to=${GATE_FETCH_TIMEOUT:-240}
checkout_to=${GATE_CHECKOUT_TIMEOUT:-240}
step5=$(bexec_t "$(( 60 + fetch_to + checkout_to + 60 ))" "$box" "
export GIT_TERMINAL_PROMPT=0 GIT_ASKPASS=/bin/true SSH_ASKPASS=/bin/true GCM_INTERACTIVE=never
step() {
  local name=\$1 secs=\$2 rc; shift 2
  echo \"GATE-STEP5 \$name\"
  timeout -k 10 \"\$secs\" \"\$@\"
  rc=\$?
  if [ \"\$rc\" != 0 ]; then
    case \$rc in
      124|137) echo \"GATE-STEP5 HUNG \$name\" ;;
      *) echo \"GATE-STEP5 FAILED \$name rc=\$rc\" ;;
    esac
    exit \"\$rc\"
  fi
}
step probe 60 bash -c 'cd /home/user/gigglesdb && cat .git/HEAD && git -c credential.helper= -c core.askPass= rev-parse --git-dir'
step fetch $fetch_to git -C /home/user/gigglesdb -c credential.helper= -c core.askPass= fetch -q /home/user/stack.bundle '$ref:refs/gate/tip'
step checkout $checkout_to git -C /home/user/gigglesdb -c credential.helper= -c core.askPass= checkout -q --detach '$tip'
head=\$(timeout -k 10 60 git -C /home/user/gigglesdb -c credential.helper= -c core.askPass= rev-parse HEAD) || { echo 'GATE-STEP5 FAILED checkout rc='\$?; exit 1; }
echo \"GATE-STEP5 OK \$head\"
") || true
marker=$(printf '%s\n' "$step5" | grep -E '^GATE-STEP5 (OK|HUNG|FAILED) ' | tail -1) || true
if [ "$marker" != "GATE-STEP5 OK $tip" ]; then
  if [ -n "$marker" ]; then
    log "$marker"
  else
    log "no marker: boat exec timed out or failed: $(printf '%s\n' "$step5" | tail -5)"
  fi
  case "$marker" in
    'GATE-STEP5 HUNG probe'|'GATE-STEP5 FAILED probe '*)
      log "box /home/user is not readable: the snapshot's lazy filesystem did not hydrate (boat snapshot store errors?); retry later, a snapshot rebuild will not help unless the store has lost it" ;;
  esac
  bexec_t 30 "$box" "sudo -n journalctl --no-pager 2>/dev/null | grep -E 'lazyfs.*ERROR|lazy\\] ERROR' | tail -5 | cut -c1-300" > "$run/step5-diag.txt" || true
  exit 5
fi

# 6. Run the gate in the background on the box and wait for it.
args=$(printf '%q ' "${steps[@]}")
bj exec "$box" --detach -- bash -lc "CLIPPY=$clippy bash /home/user/gate-remote-runner.sh $args > /home/user/gate.out 2>&1" >/dev/null || { log "could not start the gate runner on the box"; exit 5; }
log "gate running: ${steps[*]}"
poll_fails=0
while :; do
  sleep 30
  if st=$(bexec_t 60 "$box" "cat /home/user/gate/done 2>/dev/null; tail -1 /home/user/gate/progress 2>/dev/null || true"); then
    poll_fails=0
  else
    poll_fails=$(( poll_fails + 1 ))
    log "poll of the gate runner failed ($poll_fails/5)"
    [ "$poll_fails" -lt 5 ] || { log "could not poll the gate runner on the box"; exit 5; }
    continue
  fi
  if echo "$st" | grep -q '^rc='; then break; fi
  [ $(( $(date +%s) - started )) -lt $(( ttl - 300 )) ] || { log "gate did not finish before the box TTL"; exit 5; }
done

# 7. Copy results back.
bexec_t 300 "$box" "cd /home/user && tar czf gate-logs.tar.gz gate gate.out" >/dev/null || { log "could not archive gate results on the box"; exit 5; }
( cd "$run" && bj scp "$box:/home/user/gate-logs.tar.gz" logs.tar.gz >/dev/null 2>&1 ) || log "WARNING: log download failed"
bexec_t 60 "$box" "cat /home/user/gate/summary.txt" > "$run/summary.txt" || { log "could not read gate summary from the box"; exit 5; }
rm -f "$run/stack.bundle"
cat "$run/summary.txt"
grc=$(echo "$st" | grep -o '^rc=[0-9]*' | cut -d= -f2)
log "gate rc=$grc; results in $run"
[ "$grc" = 0 ] && exit 0 || exit 1
