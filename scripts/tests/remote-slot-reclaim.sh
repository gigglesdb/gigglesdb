#!/usr/bin/env bash
# Offline checks of the slot reclaim in scripts/remote-slots.sh: a fake boat
# CLI and a scratch slots dir, usage log and git repo; no provider is
# contacted. Owners are real processes that exit or are killed with
# SIGKILL, including gate-remote.sh and bench-remote.sh themselves.
set -euo pipefail
here=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
root=${REMOTE_SLOT_TEST_TMP:-J:/gigglesdb-work/tmp}
mkdir -p "$root"
work=$(mktemp -d "$root/remote-slot-reclaim.XXXXXX")
fake=$work/fake
cleanup_test() {
  local p
  for p in $(cat "$fake/pids" 2>/dev/null) ${live_pids:-}; do kill -9 "$p" 2>/dev/null || true; done
  rm -rf "$work"
}
trap cleanup_test EXIT
export TEMP=$work TMP=$work TMPDIR=$work
boatdir=$work/boat
mkdir -p "$boatdir/slots" "$fake"
: > "$boatdir/usage.log"; : > "$fake/boxes"; : > "$fake/calls"; : > "$fake/pids"
export FAKE_DIR=$fake

# Fake boat: list/new/delete/scp/exec against $FAKE_DIR/boxes ("id name").
cat > "$fake/boat" <<'FAKE'
#!/usr/bin/env bash
[ "$1" = --no-update ] && shift
echo "$*" >> "$FAKE_DIR/calls"
case "$1" in
  list)
    [ -e "$FAKE_DIR/list-fail" ] && exit 1
    printf '{"items":['
    sep=''
    while read -r id name; do
      [ -n "$id" ] || continue
      printf '%s{"id":"%s","name":"%s","access":{"ssh":"x"},"meta":{"name":"decoy"}}' "$sep" "$id" "$name"; sep=,
    done < "$FAKE_DIR/boxes"
    printf ']}\n' ;;
  new)
    name=''; while [ $# -gt 0 ]; do [ "$1" = --name ] && name=$2; shift; done
    n=$(( $(wc -l < "$FAKE_DIR/boxes") + 1 ))
    id=bx_fake$n$RANDOM
    echo "$id $name" >> "$FAKE_DIR/boxes"
    if [ -e "$FAKE_DIR/new-hang" ]; then echo $$ >> "$FAKE_DIR/pids"; exec sleep 600; fi
    echo "{\"id\":\"$id\",\"name\":\"$name\"}" ;;
  delete)
    [ -e "$FAKE_DIR/delete-fail" ] && exit 1
    grep -q "^$2 " "$FAKE_DIR/boxes" || exit 1
    grep -v "^$2 " "$FAKE_DIR/boxes" > "$FAKE_DIR/boxes.new" || true
    mv "$FAKE_DIR/boxes.new" "$FAKE_DIR/boxes" ;;
  scp)
    if [ -e "$FAKE_DIR/scp-hang" ]; then echo $$ >> "$FAKE_DIR/pids"; exec sleep 600; fi ;;
  exec) echo '{"stdout":""}' ;;
esac
FAKE
chmod +x "$fake/boat"
B=$fake/boat
bj() { "$B" --no-update "$@"; }

repo=$work/repo
git init -q "$repo"
git -C "$repo" -c user.email=t@t -c user.name=t commit -q --allow-empty -m base
git -C "$repo" rev-parse HEAD > "$boatdir/base-commit.txt"
git -C "$repo" -c user.email=t@t -c user.name=t commit -q --allow-empty -m tip   # a non-empty stack bundle
g() { git -C "$repo" "$@"; }

# Production helpers: the shared slot code and the gate's adopt_box parser.
log() { echo "[test] $*" >&2; }
source "$here/remote-slots.sh"
source <(sed -n '/^# BEGIN adopt_box/,/^# END adopt_box/p' "$here/gate-remote.sh")
who=reclaimer type=large short=000000000000 ref='' boxname='' box='' started=$(date +%s) slot=''
max_boxes=4 slot_poll=1

fail() { echo "FAIL: $*" >&2; for f in "$work"/*.err; do [ -s "$f" ] && { echo "--- ${f##*/}" >&2; tail -5 "$f" >&2; }; done; exit 1; }
# Write a lease as a short-lived owner process, which then exits (dead).
dead_lease() { # <slot> <var=value>...
  local s=$1; shift
  mkdir -p "$boatdir/slots/$s"
  env "$@" H="$here/remote-slots.sh" D="$boatdir/slots/$s" bash -c \
    'started=${started:-$(date +%s)}; . "$H"; slot_lease_write "$D"'
}
# Write a lease as an owner that stays alive until the test ends.
live_lease() { # <slot> <var=value>...
  local s=$1 p; shift
  mkdir -p "$boatdir/slots/$s"
  env "$@" H="$here/remote-slots.sh" D="$boatdir/slots/$s" bash -c \
    'started=$(date +%s); . "$H"; slot_lease_write "$D"; while :; do sleep 1; done' &
  p=$!; live_pids="${live_pids:-} $p"
  for _ in $(seq 1 50); do [ -s "$boatdir/slots/$s/lease" ] && return 0; sleep 0.1; done
  fail "live owner did not write its lease"
}
newbox() { echo "bx_$1 $2" >> "$fake/boxes"; }
has_box() { grep -q "^$1 " "$fake/boxes"; }
reset_calls() { : > "$fake/calls"; }

# 1. Dead owner with a box: box deleted, START kept, END reclaimed, ref and slot gone.
newbox dead1 gate-a-1
g update-ref refs/gate-remote/20261121T052258Z-51054 HEAD
echo "2026-11-21T05:22:58Z | lead | bx_dead1 | large | 59a541fb9609 | START" >> "$boatdir/usage.log"
dead_lease slot-1 who=lead type=large short=59a541fb9609 ref=refs/gate-remote/20261121T052258Z-51054 \
  boxname=gate-a-1 box=bx_dead1 started=$(( $(date +%s) - 1500 ))
# 2. Live owner with a box: never touched.
newbox live2 gate-b-2
live_lease slot-2 who=alive type=large short=111111111111 ref=refs/gate-remote/20261121T060000Z-2 boxname=gate-b-2 box=bx_live2
# 3. No lease (cluster-remote.sh, older scripts, or a slot being taken): never touched.
mkdir -p "$boatdir/slots/slot-3"; echo "pending cluster" > "$boatdir/slots/slot-3/owner"
reset_calls
slot_reclaim_stale 2> "$work/r1.err"
[ ! -e "$boatdir/slots/slot-1" ] || fail "dead slot-1 not released"
! has_box bx_dead1 || fail "dead owner's box not deleted"
has_box bx_live2 || fail "live owner's box deleted"
[ -f "$boatdir/slots/slot-2/lease" ] || fail "live slot-2 released"
[ -d "$boatdir/slots/slot-3" ] || fail "lease-less slot-3 released"
grep -q '^delete bx_dead1 --yes$' "$fake/calls" || fail "no delete call"
! grep -q 'bx_live2' "$fake/calls" || fail "live box touched: $(cat "$fake/calls")"
[ "$(grep -c '| bx_dead1 |.*| START' "$boatdir/usage.log")" = 1 ] || fail "START duplicated"
grep -q '| lead | bx_dead1 | large | 59a541fb9609 | END rc=reclaimed 2[5-6]min (owner pid [0-9]* died; reclaimed by reclaimer' "$boatdir/usage.log" \
  || fail "END line: $(cat "$boatdir/usage.log")"
! g rev-parse -q --verify refs/gate-remote/20261121T052258Z-51054 >/dev/null || fail "ref left"
grep -q 'released slot-1: deleted box bx_dead1, removed refs/gate-remote/20261121T052258Z-51054' "$boatdir/reclaim.log" || fail "reclaim.log"
grep -q 'reclaimed slot-1' "$work/r1.err" || fail "reclaim not logged"
[ ! -e "$boatdir/slots/.reclaim.lock" ] || fail "reclaim lock left"

# 4. Dead inside `boat new` (bench): box found by its unique name, START and END written.
newbox decoy4 other-name
newbox died4 bench-x-4
dead_lease slot-1 slot_kind=bench who=bench-x type=large short=222222222222 ref=refs/bench-remote/20261121T070000Z-4 boxname=bench-x-4 box=
slot_reclaim_stale 2>/dev/null
! has_box bx_died4 || fail "box named in lease not deleted"
has_box bx_decoy4 || fail "decoy box deleted"
grep -q '| bench-x | bx_died4 | large | 222222222222 | START bench (reclaimed: owner died before logging it)' "$boatdir/usage.log" || fail "reclaimed START"
grep -q '| bench-x | bx_died4 | large | 222222222222 | END bench rc=reclaimed' "$boatdir/usage.log" || fail "bench END"
[ ! -e "$boatdir/slots/slot-1" ] || fail "slot-1 not released (case 4)"

#    Box name only, box not listed yet (still being created): held until the
#    lease is 10 min old, then released as no box.
dead_lease slot-1 who=slow boxname=gate-slow-4 box=
slot_reclaim_stale 2> "$work/r4.err"
[ -f "$boatdir/slots/slot-1/lease" ] || fail "slot released while its box may still be created"
grep -q 'no box named gate-slow-4 listed yet' "$work/r4.err" || fail "case 4b log"
touch -d '11 minutes ago' "$boatdir/slots/slot-1/lease"
slot_reclaim_stale 2>/dev/null
[ ! -e "$boatdir/slots/slot-1" ] || fail "old name-only lease not released"
grep -q 'released slot-1: no box named gate-slow-4 found' "$boatdir/reclaim.log" || fail "case 4b reclaim.log"
#    An owner killed in its own cleanup after its END line: no second END.
newbox end4 gate-e-4
echo "2026-11-21T05:00:00Z | ended | bx_end4 | large | 333333333333 | START" >> "$boatdir/usage.log"
echo "2026-11-21T05:30:00Z | ended | bx_end4 | large | 333333333333 | END rc=0 30min" >> "$boatdir/usage.log"
dead_lease slot-1 who=ended boxname=gate-e-4 box=bx_end4
slot_reclaim_stale 2>/dev/null
! has_box bx_end4 && [ ! -e "$boatdir/slots/slot-1" ] || fail "case 4c reclaim"
[ "$(grep -c '| bx_end4 |.*| END' "$boatdir/usage.log")" = 1 ] || fail "END duplicated"
#    A slot that changed hands while it was being reclaimed is left alone.
dead_lease slot-1 who=old boxname= box=
old_lease=$(cat "$boatdir/slots/slot-1/lease")
rm -rf "$boatdir/slots/slot-1"; mkdir "$boatdir/slots/slot-1"; slot_lease_write "$boatdir/slots/slot-1"
! slot_reclaim_one "$boatdir/slots/slot-1" "$old_lease" 2> "$work/r4d.err" || fail "reclaimed a slot that changed hands"
grep -q "^pid=$$\$" "$boatdir/slots/slot-1/lease" && grep -q 'changed hands' "$work/r4d.err" || fail "case 4d"
rm -rf "$boatdir/slots/slot-1"

# 5. Dead before `boat new` (no box name): slot released, no boat call.
dead_lease slot-1 who=early ref=refs/gate-remote/20261121T080000Z-5 boxname= box=
reset_calls
slot_reclaim_stale 2>/dev/null
[ ! -e "$boatdir/slots/slot-1" ] || fail "slot-1 not released (case 5)"
[ ! -s "$fake/calls" ] || fail "boat called with no box: $(cat "$fake/calls")"

# 6. Delete fails and the box is still listed: slot stays held, no END.
newbox stuck6 gate-s-6
dead_lease slot-1 who=stuck boxname=gate-s-6 box=bx_stuck6
touch "$fake/delete-fail"
slot_reclaim_stale 2> "$work/r6.err"
[ -f "$boatdir/slots/slot-1/lease" ] || fail "slot released while its box still exists"
grep -q 'still exists; slot-1 stays held' "$work/r6.err" || fail "case 6 warning"
! grep -q 'bx_stuck6.*END' "$boatdir/usage.log" || fail "END written for a live box"
# 7. Delete fails because the box is already gone: released.
grep -v '^bx_stuck6 ' "$fake/boxes" > "$fake/b" || true; mv "$fake/b" "$fake/boxes"
slot_reclaim_stale 2>/dev/null
[ ! -e "$boatdir/slots/slot-1" ] || fail "gone box kept its slot"
grep -q 'released slot-1: box bx_stuck6 already gone' "$boatdir/reclaim.log" || fail "case 7 log"
rm -f "$fake/delete-fail"

# 8. Box name only and the listing fails: slot stays held.
newbox nolist8 gate-n-8
dead_lease slot-1 who=nolist boxname=gate-n-8 box=
touch "$fake/list-fail"
slot_reclaim_stale 2>/dev/null
[ -f "$boatdir/slots/slot-1/lease" ] && has_box bx_nolist8 || fail "reclaimed without a listing"
rm -f "$fake/list-fail"
slot_reclaim_stale 2>/dev/null
! has_box bx_nolist8 && [ ! -e "$boatdir/slots/slot-1" ] || fail "case 8 retry"

# 9. Other host, unreadable pid: never reclaimed even though the pid is dead.
newbox host9 gate-h-9
dead_lease slot-1 who=elsewhere boxname=gate-h-9 box=bx_host9
sed -i 's/^host=.*/host=some-other-machine/' "$boatdir/slots/slot-1/lease"
newbox pid9 gate-p-9
dead_lease slot-4 who=nopid boxname=gate-p-9 box=bx_pid9
sed -i 's/^pid=.*/pid=/' "$boatdir/slots/slot-4/lease"
slot_reclaim_stale 2>/dev/null
has_box bx_host9 && has_box bx_pid9 || fail "reclaimed on missing evidence"
rm -rf "$boatdir/slots/slot-1" "$boatdir/slots/slot-4"

# 10. Live pid with another start time and a gone Windows pid (pid reuse): reclaimed.
#     Live pid with matching start time but a bogus winpid: alive (MSYS pid wins).
newbox reuse10 gate-r-10
mkdir -p "$boatdir/slots/slot-1"
lease_now=$(cat "$boatdir/slots/slot-2/lease")
printf '%s\n' "$lease_now" | sed -e 's/^pid_start=.*/pid_start=1/' -e 's/^winpid=.*/winpid=4194300/' \
  -e 's/^box=.*/box=bx_reuse10/' -e 's/^boxname=.*/boxname=gate-r-10/' > "$boatdir/slots/slot-1/lease"
mkdir -p "$boatdir/slots/slot-4"
printf '%s\n' "$lease_now" | sed -e 's/^winpid=.*/winpid=4194300/' -e 's/^box=.*/box=bx_live2/' > "$boatdir/slots/slot-4/lease"
slot_reclaim_stale 2>/dev/null
! has_box bx_reuse10 && [ ! -e "$boatdir/slots/slot-1" ] || fail "reused pid not reclaimed"
has_box bx_live2 && [ -f "$boatdir/slots/slot-4/lease" ] || fail "live pid with matching start reclaimed"
rm -rf "$boatdir/slots/slot-4"
#     Dead MSYS pid but its Windows pid alive with the same image: alive.
newbox win10 gate-w-10
dead_lease slot-1 who=win boxname=gate-w-10 box=bx_win10
sed -i -e "s/^winpid=.*/winpid=$(cat /proc/$$/winpid)/" "$boatdir/slots/slot-1/lease"
slot_reclaim_stale 2>/dev/null
has_box bx_win10 && [ -f "$boatdir/slots/slot-1/lease" ] || fail "live Windows pid reclaimed"
rm -rf "$boatdir/slots/slot-1"

# 11. Reclaim lock held by a live reclaimer: nothing reclaimed this round.
newbox lock11 gate-l-11
dead_lease slot-1 who=locked boxname=gate-l-11 box=bx_lock11
mkdir "$boatdir/slots/.reclaim.lock"; slot_lease_write "$boatdir/slots/.reclaim.lock"
slot_reclaim_stale 2>/dev/null
has_box bx_lock11 || fail "reclaimed while another reclaim held the lock"
rm -rf "$boatdir/slots/.reclaim.lock"
#     A dead reclaimer's lock older than 15 minutes is taken over.
mkdir "$boatdir/slots/.reclaim.lock"
cp "$boatdir/slots/slot-1/lease" "$boatdir/slots/.reclaim.lock/lease"
touch -d '20 minutes ago' "$boatdir/slots/.reclaim.lock"
slot_reclaim_stale 2>/dev/null
! has_box bx_lock11 && [ ! -e "$boatdir/slots/.reclaim.lock" ] || fail "stale reclaim lock not taken over"
#     A lease-less lock (reclaimer died right after mkdir) older than 15 min too;
#     a fresh lease-less lock is respected.
newbox lock11b gate-l-11b
dead_lease slot-1 who=locked boxname=gate-l-11b box=bx_lock11b
mkdir "$boatdir/slots/.reclaim.lock"
slot_reclaim_stale 2>/dev/null
has_box bx_lock11b || fail "fresh lease-less lock ignored"
touch -d '20 minutes ago' "$boatdir/slots/.reclaim.lock"
slot_reclaim_stale 2>/dev/null
! has_box bx_lock11b && [ ! -e "$boatdir/slots/.reclaim.lock" ] || fail "stale lease-less lock not taken over"
ls -d "$boatdir"/slots/.reclaim.lock.stale.* >/dev/null 2>&1 && fail "renamed stale lock left"

# 12. slot_release never removes a slot leased to another pid.
slot=slot-2
slot_release 2> "$work/r12.err"
[ -f "$boatdir/slots/slot-2/lease" ] || fail "released another owner's slot"
grep -q 'not this run; leaving it' "$work/r12.err" || fail "case 12 warning"
slot=''

# 13. slot_acquire with every slot held: frees the dead one and takes it.
max_boxes=2
dead_lease slot-1 who=full boxname= box=
slot_acquire 0 2>/dev/null || fail "acquire failed with a dead slot"
[ "$slot" = slot-1 ] && grep -q "^pid=$$\$" "$boatdir/slots/slot-1/lease" || fail "acquire did not lease slot-1"
slot_release; slot=''
[ ! -e "$boatdir/slots/slot-1" ] || fail "own release"
mkdir "$boatdir/slots/slot-1"; slot_lease_write "$boatdir/slots/slot-1"   # held by us, alive
! slot_acquire 0 2>/dev/null || fail "acquired with every owner alive"
rm -rf "$boatdir/slots/slot-1"
echo 'PASS slot reclaim units (20 cases)'

# 14. End to end with SIGKILL: a gate is killed with its box up; a bench
#     reclaims that slot, takes it and is killed too; the next gate reclaims
#     the bench's slot; a gate after that leaves the live gate alone.
export GATE_TEST_BOATDIR=$boatdir GIGGLES_REPO=$repo BOAT=$B GATE_MAX_BOXES=2
kill -9 $live_pids 2>/dev/null || true; live_pids=''
rm -rf "$boatdir/slots"/slot-*; : > "$boatdir/usage.log"; : > "$fake/boxes"
wait_lease_box() { # <slot> [not-this-box]: wait until the lease names a (new) box
  local b
  for _ in $(seq 1 600); do
    b=$(sed -n 's/^box=//p' "$boatdir/slots/$1/lease" 2>/dev/null) || b=''
    [[ "$b" == bx_* && "$b" != "${2:-}" ]] && return 0; sleep 0.1
  done
  fail "no new box in $1 lease"
}
lease_of() { sed -n "s/^$2=//p" "$boatdir/slots/$1/lease"; }
kill_group() { kill -9 -- "-$1" 2>/dev/null || kill -9 "$1"; wait "$1" 2>/dev/null || true; }
start_bg() { # <err file> <script> <args...>: own process group, pid in $bg
  set -m; bash "$here/$2" "${@:3}" 2> "$work/$1" & bg=$!; set +m
}
touch "$fake/scp-hang"
start_bg g1.err gate-remote.sh --tip HEAD --who killed-gate --slot-wait 5 -- giggles-core; g1=$bg
wait_lease_box slot-1
gbox=$(lease_of slot-1 box); gref=$(lease_of slot-1 ref)
g rev-parse -q --verify "$gref" >/dev/null || fail "gate ref missing before kill"
kill_group "$g1"
[ -d "$boatdir/slots/slot-1" ] && has_box "$gbox" || fail "SIGKILL ran cleanup?"
! grep -q "| $gbox |.*END" "$boatdir/usage.log" || fail "END before reclaim"
start_bg b1.err bench-remote.sh --tip HEAD --who killed-bench --slot-wait 5 --results-root "$work/bench" \
  --clients 8 --mixes read --repeats 1 --warmup 0 --duration 10; b1=$bg
wait_lease_box slot-1 "$gbox"
bbox=$(lease_of slot-1 box); bref=$(lease_of slot-1 ref)
[ "$(lease_of slot-1 kind)" = bench ] && [ "$(lease_of slot-1 pid)" = "$b1" ] || fail "bench did not take slot-1"
! has_box "$gbox" || fail "killed gate's box not deleted"
grep -q "| killed-gate | $gbox | large | .* | END rc=reclaimed .*reclaimed by bench-killed-bench" "$boatdir/usage.log" || fail "gate END reclaimed"
! g rev-parse -q --verify "$gref" >/dev/null || fail "killed gate's ref left"
grep -q 'reclaimed slot-1' "$work/b1.err" || fail "bench did not log the reclaim"
kill_group "$b1"
start_bg g2.err gate-remote.sh --tip HEAD --who next-gate --slot-wait 5 -- giggles-core; g2=$bg
wait_lease_box slot-1 "$bbox"
nbox=$(lease_of slot-1 box)
! has_box "$bbox" && has_box "$nbox" || fail "killed bench's box not deleted"
grep -q "| bench-killed-bench | $bbox | large | .* | END bench rc=reclaimed .*reclaimed by next-gate" "$boatdir/usage.log" || fail "bench END reclaimed"
! g rev-parse -q --verify "$bref" >/dev/null || fail "killed bench's ref left"
# g2 is alive and waiting on its upload: the next gate must take slot-2.
start_bg g3.err gate-remote.sh --tip HEAD --who third-gate --slot-wait 5 -- giggles-core; g3=$bg
wait_lease_box slot-2
has_box "$nbox" && [ "$(lease_of slot-1 pid)" = "$g2" ] || fail "live gate's slot or box touched"
! grep -q "reclaim" "$work/g3.err" || fail "third gate reclaimed something: $(cat "$work/g3.err")"
kill_group "$g2"; kill_group "$g3"
[ "$(grep -c '| START' "$boatdir/usage.log")" = 4 ] && [ "$(grep -c 'END.*rc=reclaimed' "$boatdir/usage.log")" = 2 ] || fail "usage log: $(cat "$boatdir/usage.log")"
echo 'PASS slot reclaim end to end (gate and bench killed with SIGKILL and reclaimed; live gate untouched)'
