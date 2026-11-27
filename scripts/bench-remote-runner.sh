#!/usr/bin/env bash
# Runs on the boat.dev box; started by scripts/bench-remote.sh. Do not run locally.
# Builds giggles-server --release, then for every (repeat, clients, mix,
# protocol, target) starts a fresh server pinned to SERVER_CPUS (PostgreSQL 18,
# or one GigglesDB config-mode server that serves both giggles-sql over pgwire
# and giggles-native over HTTPS), runs scripts/app-loadtest `load` pinned to
# CLIENT_CPUS, stops the server and deletes its data. Writes
# /home/user/bench/{progress,done,meta.json,results.jsonl,summary.md,logs/}.
# done holds rc=0 when every measured run passed (including --max-error-rate 0
# and the invariants), rc=1 otherwise, rc=2 when setup failed, rc=6 when
# SLOW_DISK=require found a fast disk (nothing ran).
#
# Environment (set by bench-remote.sh): TARGETS CLIENTS MIXES PROTOCOLS WARMUP
# DURATION REPEATS ROWS OPSET [SERVER_CPUS CLIENT_CPUS DATA_ROOT BASE_COMMIT
# PROFILE PROFILE_PHASE_BUDGET BOX_DEADLINE SLOW_DISK MIN_FSYNC_US].
set -uo pipefail
source "$HOME/bench.env"
repo=/home/user/gigglesdb
out=/home/user/bench
OPSET=${OPSET:-common}
PROFILE=${PROFILE:-0}
BASE_COMMIT=${BASE_COMMIT:-}
profile_ran=0
perf_version=""
# Profile phase budget in seconds (bench-remote.sh --profile-budget). 2700 fits
# 4 profiles (2 targets x 2 client counts): a target took about 370 s in the
# GD-0084 slow-disk runs, and each one starts only with PROFILE_TARGET_WORST left.
PROFILE_PHASE_BUDGET=${PROFILE_PHASE_BUDGET:-2700}
PROFILE_TARGET_WORST=900
PROFILE_TARGET_STEPS=840
PROFILE_LOAD_SECONDS=360
# BOX_DEADLINE (epoch, from bench-remote.sh): the profile phase starts no target
# that cannot finish before it, so results are copied back before the TTL.
BOX_DEADLINE=${BOX_DEADLINE:-}
# SLOW_DISK: off (default) | require (stop with rc=6 if the raw fdatasync mean is
# under MIN_FSYNC_US) | auto (emulate a slow device with dm-delay when it is).
SLOW_DISK=${SLOW_DISK:-off}
MIN_FSYNC_US=${MIN_FSYNC_US:-2000}
cap() { # cap <max-secs> <deadline-epoch>; leave room for timeout's kill-after
  local secs=$(( $2 - $(date +%s) - 10 ))
  [ "$secs" -gt "$1" ] && secs=$1
  [ "$secs" -ge 15 ] || return 1
  echo "$secs"
}
rm -rf "$out"; mkdir -p "$out/logs"
progress() { echo "$(date -u +%T) $*" >> "$out/progress"; }
fail_setup() { progress "SETUP FAILED: $*"; echo "setup failed: $*" > "$out/summary.md"; declare -F stop_all >/dev/null && stop_all; echo "rc=2" > "$out/done"; exit 2; }

# Operation sets (app-loadtest op names). common passes on all three targets;
# sql-full is every load op GigglesDB SQL passes (the default load set minus
# count_exists, cte, window, transfer, place_order and cancel_order, which it
# rejects with 0A000 / FOR UPDATE). GD-0082 W3.
OPS_COMMON=pk,email,keyset,range_in,insert_returning,multi_insert,update,optimistic,bulk_delete,rollback,savepoint,unique,fk,check
OPS_SQL_FULL=pk,email,pagination,keyset,search,range_in,join_sum,large_in,insert_returning,multi_insert,update,optimistic,upsert,roundtrip,bulk_delete,rollback,savepoint,unique,fk,check
case "$OPSET" in
  common) OPS=$OPS_COMMON ;;
  sql-full) OPS=$OPS_SQL_FULL
    [[ ",$TARGETS," == *",giggles-native,"* ]] && fail_setup "opset sql-full requires SQL targets" ;;
  *) fail_setup "unknown OPSET $OPSET" ;;
esac

n=$(nproc)
SERVER_CPUS=${SERVER_CPUS:-0-$(( n - 3 ))}
CLIENT_CPUS=${CLIENT_CPUS:-$(( n - 2 ))-$(( n - 1 ))}
export SERVER_CPUS CLIENT_CPUS
progress "start nproc=$n server_cpus=$SERVER_CPUS client_cpus=$CLIENT_CPUS opset=$OPSET"

# 0. Data directory. /home/user is FUSE (ascii-lazyfs), which would distort
# fsync-bound numbers, so look for a writable path on a block device.
{ echo "## findmnt"; findmnt -o TARGET,SOURCE,FSTYPE,OPTIONS; echo "## df -T"; df -T; echo "## lsblk"; lsblk -o NAME,TYPE,SIZE,ROTA,FSTYPE,MOUNTPOINTS; } \
  > "$out/logs/data-probe.log" 2>&1
data_note=''
probe_root() {
  local d src fs
  for d in /mnt /var/tmp /tmp /data /srv /opt /var/lib; do
    [ -d "$d" ] || continue
    src=$(findmnt -n -o SOURCE -T "$d" 2>/dev/null | head -1)
    fs=$(findmnt -n -o FSTYPE -T "$d" 2>/dev/null | head -1)
    case "$fs" in ext4|xfs|btrfs|f2fs) ;; *) continue ;; esac
    [[ "$src" == /dev/* ]] || continue
    if mkdir -p "$d/gigglesdb-bench" 2>/dev/null || { sudo -n mkdir -p "$d/gigglesdb-bench" && sudo -n chown "$(id -u):$(id -g)" "$d/gigglesdb-bench"; } 2>/dev/null; then
      [ -w "$d/gigglesdb-bench" ] || continue
      [ "$(df -Pk "$d/gigglesdb-bench" | awk 'NR==2{print $4}')" -ge 8000000 ] || continue
      echo "$d/gigglesdb-bench"; return 0
    fi
  done
  return 1
}
if [ -n "${DATA_ROOT:-}" ]; then
  data=$DATA_ROOT; data_note="--data-root override"
elif data=$(probe_root); then
  data_note="probed: first writable block-device path (ext4/xfs/btrfs/f2fs) with >= 8 GB free"
else
  data=/home/user/bench-data
  data_note="NO block-device-backed writable path found (see logs/data-probe.log); data on /home/user FUSE"
fi
data=$data/run
rm -rf "$data"; mkdir -p "$data" || fail_setup "cannot create data dir $data"
data_fstype=$(findmnt -n -o FSTYPE -T "$data" | head -1)
data_source=$(findmnt -n -o SOURCE -T "$data" | head -1)
data_mount=$(findmnt -n -o TARGET -T "$data" | head -1)
progress "data dir $data on $data_fstype $data_source ($data_note)"

# 0b. Raw fdatasync probe: mean/p50/p99 us of 400 x (append 1 KB + fdatasync).
fsync_probe() { python3 -I -c '
import os, sys, time
p = os.path.join(sys.argv[1], "fsync-probe.bin")
fd = os.open(p, os.O_CREAT | os.O_WRONLY | os.O_APPEND, 0o600)
ts = []
for _ in range(400):
    os.write(fd, b"x" * 1024); t = time.perf_counter(); os.fdatasync(fd); ts.append(time.perf_counter() - t)
os.close(fd); os.unlink(p); ts.sort()
print("%.0f %.0f %.0f" % (sum(ts) / len(ts) * 1e6, ts[len(ts) // 2] * 1e6, ts[int(len(ts) * .99)] * 1e6))
' "$1"; }
emulate_slow_disk() { # dm-delay over a direct-I/O loop device; echoes the mount point
  local img=$1/slow.img mnt=$1/slowmnt dev sectors w=${SLOW_WRITE_MS:-1} f=${SLOW_FLUSH_MS:-3}
  sudo -n modprobe dm_delay >&2 || return 1
  # Allocate up front: a sparse image could hit ENOSPC mid-run and surface as I/O errors.
  fallocate -l 10G "$img" || return 1
  dev=$(sudo -n losetup --direct-io=on -f --show "$img") || return 1
  sectors=$(sudo -n blockdev --getsz "$dev") || return 1
  echo "0 $sectors delay $dev 0 0 $dev 0 $w $dev 0 $f" | sudo -n dmsetup create ggslow >&2 ||
    echo "0 $sectors delay $dev 0 0 $dev 0 $f" | sudo -n dmsetup create ggslow >&2 || return 1
  sudo -n mkfs.ext4 -q -E lazy_itable_init=0,lazy_journal_init=0 /dev/mapper/ggslow >&2 || return 1
  mkdir -p "$mnt" && sudo -n mount /dev/mapper/ggslow "$mnt" && sudo -n chown "$(id -u):$(id -g)" "$mnt" || return 1
  sudo -n dmsetup table ggslow >&2
  echo "$mnt"
}
[[ "$PROFILE_PHASE_BUDGET" =~ ^[1-9][0-9]{0,4}$ ]] || fail_setup "bad PROFILE_PHASE_BUDGET=$PROFILE_PHASE_BUDGET (1-99999 seconds)"
case "$SLOW_DISK" in off|require|auto) ;; *) fail_setup "bad SLOW_DISK=$SLOW_DISK (off|require|auto)" ;; esac
[[ "$MIN_FSYNC_US" =~ ^[0-9]+$ ]] || fail_setup "bad MIN_FSYNC_US=$MIN_FSYNC_US"
if ! read -r fs_mean fs_p50 fs_p99 < <(fsync_probe "$data"); then
  [ "$SLOW_DISK" = off ] || fail_setup "fsync probe"
  progress "raw fdatasync probe failed on $data (SLOW_DISK=off; continuing)"
  echo "device probe failed" > "$out/logs/fsync-probe.txt"; fs_mean='' fs_p50='' fs_p99=''
else
progress "raw fdatasync probe: mean ${fs_mean}us p50 ${fs_p50}us p99 ${fs_p99}us (SLOW_DISK=$SLOW_DISK min ${MIN_FSYNC_US}us)"
echo "device mean=$fs_mean p50=$fs_p50 p99=$fs_p99" > "$out/logs/fsync-probe.txt"
fi
if [ "$SLOW_DISK" != off ] && [ "$fs_mean" -lt "$MIN_FSYNC_US" ]; then
  if [ "$SLOW_DISK" = require ]; then
    progress "FAST DISK: fdatasync mean ${fs_mean}us < ${MIN_FSYNC_US}us; stopping (SLOW_DISK=require)"
    printf 'fast disk: raw fdatasync mean %sus p50 %sus p99 %sus < %sus; no runs (SLOW_DISK=require)\n' "$fs_mean" "$fs_p50" "$fs_p99" "$MIN_FSYNC_US" > "$out/summary.md"
    echo "rc=6" > "$out/done"; exit 6
  fi
  slow_mnt=$(emulate_slow_disk "${data%/run}" 2> "$out/logs/slow-disk.log") || fail_setup "dm-delay emulation (see logs/slow-disk.log)"
  data=$slow_mnt/run; mkdir -p "$data" || fail_setup "cannot create $data"
  data_note="EMULATED slow device: dm-delay write ${SLOW_WRITE_MS:-1} ms / flush ${SLOW_FLUSH_MS:-3} ms over a direct-I/O loop file; $data_note"
  data_fstype=$(findmnt -n -o FSTYPE -T "$data" | head -1); data_source=$(findmnt -n -o SOURCE -T "$data" | head -1); data_mount=$(findmnt -n -o TARGET -T "$data" | head -1)
  read -r fs_mean fs_p50 fs_p99 < <(fsync_probe "$data") || fail_setup "fsync probe (emulated)"
  progress "EMULATED slow disk on $data_source: fdatasync mean ${fs_mean}us p50 ${fs_p50}us p99 ${fs_p99}us"
  echo "emulated mean=$fs_mean p50=$fs_p50 p99=$fs_p99" >> "$out/logs/fsync-probe.txt"
  [ "$fs_mean" -ge "$MIN_FSYNC_US" ] || fail_setup "emulated device still fast (fdatasync mean ${fs_mean}us < ${MIN_FSYNC_US}us)"
fi

# 1. app-loadtest dependencies (snapshot has them; reinstall if the lock changed).
lt=$repo/scripts/app-loadtest
lock=$(sha256sum "$lt/package-lock.json" | cut -d' ' -f1)
if [ ! -x "$lt/node_modules/.bin/tsx" ] || [ "$lock" != "$(cat /home/user/bench-npm.stamp 2>/dev/null)" ]; then
  progress "npm ci (lock changed)"
  ( cd "$lt" && npm ci --no-audit --no-fund ) > "$out/logs/npm-ci.log" 2>&1 || fail_setup "npm ci"
  echo "$lock" > /home/user/bench-npm.stamp
fi
native_support=0
grep -q -- 'frontend' "$lt/src/cli.ts" && native_support=1

# 2. Release build.
progress "cargo build --release giggles-server"
command -v sccache >/dev/null 2>&1 && export RUSTC_WRAPPER=$(command -v sccache)
s=$(date +%s)
( cd "$repo/crates/giggles-server" && cargo build --release --bin giggles-server ) > "$out/logs/build.log" 2>&1 \
  || fail_setup "release build (see logs/build.log)"
build_s=$(( $(date +%s) - s ))
gs=$CARGO_TARGET_DIR/release/giggles-server
progress "build done in ${build_s}s"
base_build_s=0
gs_base=''
if [ -n "$BASE_COMMIT" ]; then
  base_repo=/home/user/gigglesdb-base
  git -C "$repo" worktree add --detach "$base_repo" "$BASE_COMMIT" > "$out/logs/base-worktree.log" 2>&1 \
    || fail_setup "base checkout (see logs/base-worktree.log)"
  progress "cargo build --release giggles-server (base $BASE_COMMIT)"
  s=$(date +%s)
  ( cd "$base_repo/crates/giggles-server" &&
    CARGO_TARGET_DIR=$CARGO_TARGET_DIR-base cargo build --release --bin giggles-server
  ) > "$out/logs/build-base.log" 2>&1 || fail_setup "base release build (see logs/build-base.log)"
  base_build_s=$(( $(date +%s) - s ))
  gs_base=$CARGO_TARGET_DIR-base/release/giggles-server
  progress "base build done in ${base_build_s}s"
fi
# Profile builds (--profile only; measured runs always use $gs / $gs_base).
# The tip, and the base when giggles-sql-base is a target, get the same build
# type so their profiled CPU/op and stacks compare: both frame pointers with
# line tables (own target dirs $CARGO_TARGET_DIR-profile and -base-profile),
# or, when either fp build fails or times out, both measured release binaries
# with dwarf call graphs. The two builds share 2100 s (tip at most 1500),
# plus 10 s kill-after each; bench-remote.sh budgets 2120 s for them.
fp_build() { # fp_build <src-repo> <target-dir> <timeout-secs> <log-name>
  ( cd "$1/crates/giggles-server" &&
    CARGO_TARGET_DIR=$2 RUSTFLAGS="-C force-frame-pointers=yes" \
    CARGO_PROFILE_RELEASE_DEBUG=line-tables-only timeout --kill-after=10 "$3" cargo build --release --bin giggles-server
  ) > "$out/logs/$4.log" 2>&1
}
profile_builds() {
  local s left limit=1500 base=0
  gs_profile=$gs gs_base_profile=$gs_base profile_callgraph=dwarf profile_base_callgraph=''
  # profile_base_build_seconds stays empty (null in meta.json) unless the base fp build ran.
  profile_build_seconds=0 profile_base_build_seconds='' profile_build_parity='no profile build'
  [ "$PROFILE" = 1 ] && [[ ",$TARGETS," == *",giggles-sql,"* || ",$TARGETS," == *",giggles-native,"* ]] || return 0
  if [[ ",$TARGETS," == *",giggles-sql-base,"* ]] && [ -n "$gs_base" ]; then
    base=1 limit=2100 profile_base_callgraph=dwarf
  fi
  progress "cargo build --release giggles-server (profile: frame pointers)"
  s=$(date +%s)
  if ! fp_build "$repo" "$CARGO_TARGET_DIR-profile" 1500 build-profile; then
    profile_build_seconds=$(( $(date +%s) - s ))
    profile_build_parity='release+dwarf: tip fp build failed/timed out (see logs/build-profile.log)'
    progress "profile build failed/timed out after ${profile_build_seconds}s; profiling the measured binaries with dwarf (see logs/build-profile.log)"
    return 0
  fi
  profile_build_seconds=$(( $(date +%s) - s ))
  progress "profile build done in ${profile_build_seconds}s (fp)"
  if [ "$base" = 0 ]; then
    gs_profile=$CARGO_TARGET_DIR-profile/release/giggles-server profile_callgraph=fp
    profile_build_parity='fp (tip only)'
    return 0
  fi
  left=$(( limit - profile_build_seconds ))
  [ "$left" -le 1500 ] || left=1500
  if [ "$left" -lt 60 ]; then
    profile_build_parity="release+dwarf: no time left for the base fp build (${left}s of ${limit}s)"
    progress "base profile build skipped (${left}s left); profiling base and tip measured binaries with dwarf"
    return 0
  fi
  progress "cargo build --release giggles-server (profile: frame pointers, base $BASE_COMMIT, ${left}s)"
  s=$(date +%s)
  if fp_build "$base_repo" "$CARGO_TARGET_DIR-base-profile" "$left" build-base-profile; then
    profile_base_build_seconds=$(( $(date +%s) - s ))
    gs_profile=$CARGO_TARGET_DIR-profile/release/giggles-server profile_callgraph=fp
    gs_base_profile=$CARGO_TARGET_DIR-base-profile/release/giggles-server profile_base_callgraph=fp
    profile_build_parity='fp (base and tip)'
    progress "base profile build done in ${profile_base_build_seconds}s (fp)"
  else
    profile_base_build_seconds=$(( $(date +%s) - s ))
    profile_build_parity='release+dwarf: base fp build failed/timed out (see logs/build-base-profile.log)'
    progress "base profile build failed/timed out after ${profile_base_build_seconds}s; profiling base and tip measured binaries with dwarf (see logs/build-base-profile.log)"
  fi
}
profile_binary() { # profile_binary <target>: "<binary> <callgraph>" its profile uses
  if [ "$1" = giggles-sql-base ]; then echo "$gs_base_profile $profile_base_callgraph"
  else echo "$gs_profile $profile_callgraph"; fi
}
profile_builds

pin_server() { taskset -c "$SERVER_CPUS" "$@"; }
wait_line() { # wait_line <file> <pattern> <seconds>
  local i; for i in $(seq 1 "$3"); do grep -q "$2" "$1" 2>/dev/null && return 0; sleep 1; done; return 1; }
has() { [[ ",$TARGETS," == *",$1,"* ]]; }
rand() { head -c 12 /dev/urandom | od -An -tx1 | tr -d ' \n'; }

# 3. Long-lived material: passwords, mTLS certificates (generated once).
pg_port=55432; pg_pw=$(rand); pg_url="postgres://postgres:$pg_pw@127.0.0.1:$pg_port/postgres"
gg_pg=55441; gg_http=55442; gg_pw=$(rand); gg_url="postgres://postgres:$gg_pw@127.0.0.1:$gg_pg/postgres"
tls=/home/user/bench-tls
if has giggles-sql || has giggles-native; then
  rm -rf "$tls"; mkdir -p "$tls"
  ( set -e; cd "$tls"
    openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -days 3 -subj /CN=bench-ca \
      -keyout ca.key -out ca.pem -addext basicConstraints=critical,CA:TRUE -addext keyUsage=critical,keyCertSign,cRLSign
    for who in server app; do
      openssl req -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -subj /CN=$who -keyout $who.key -out $who.csr
      if [ $who = server ]; then ext="subjectAltName=DNS:localhost,IP:127.0.0.1
extendedKeyUsage=serverAuth"; else ext="extendedKeyUsage=clientAuth"; fi
      printf 'basicConstraints=CA:FALSE\nkeyUsage=critical,digitalSignature\n%s\n' "$ext" > $who.ext
      openssl x509 -req -in $who.csr -CA ca.pem -CAkey ca.key -CAcreateserial -days 3 -out $who.pem -extfile $who.ext
    done ) > "$out/logs/tls-certs.log" 2>&1 || fail_setup "mTLS certificates"
  fp=$(openssl x509 -in "$tls/app.pem" -outform DER | sha256sum | cut -d' ' -f1)
fi

# 4. Server lifecycle. Every measured config gets a fresh server and data dir.
# GigglesDB: a dev-mode start creates the database (v2 engine, owner password
# from GIGGLES_OWNER_PASSWORD; config mode never creates one), then one
# config-mode server opens it with pgwire (128 connections, TLS off, SCRAM),
# the HTTPS native API (mTLS, 128 sessions) and connections 128. The request
# rate limiter (service_limits, default 4096/s, which a 32-client run exceeds
# with 53300 / HTTP 429) is raised to its maximum, 1,000,000/s. Its stdin is
# a FIFO so it gets a clean "shutdown" command.
# role_statements.concurrent is raised from its default 16 to 31 (GD-0084 Q4,
# E236): with one bench role the per-role cap otherwise bounds commits per WAL
# sync at 32 clients. Results at cap 31 are not comparable with earlier runs.
gg_pid='' gg_fd='' pg_up=0
gg_config_json() {
  cat <<JSON
{"database":"$data/gg/db","native":"127.0.0.1:55440","postgres":"127.0.0.1:55443",
 "postgresql":{"listen":"127.0.0.1:$gg_pg","tls":"disabled","databases":["postgres"],"connections":128},
 "http":"127.0.0.1:$gg_http","api_limits":{"sessions":128},
 "certificate":"$tls/server.pem","private_key":"$tls/server.key","client_ca":"$tls/ca.pem",
 "bindings":[{"fingerprint":"$fp","role":{"role":"Database","name":"postgres"}}],
 "service_limits":{"sessions":128,"burst":1000000,"per_second":1000000},"role_statements":{"concurrent":31},
 "connections":128,"skew_seconds":0,"max_pause_seconds":3600}
JSON
}
stop_proc() { # stop_proc <pid> <fd>: shutdown command, then INT, TERM, KILL
  local pid=$1 fd=$2 i
  [ -n "$pid" ] || return 0
  if [ -n "$fd" ]; then echo shutdown >&"$fd" 2>/dev/null; exec {fd}>&- 2>/dev/null; fi
  for sig in 0 INT TERM KILL; do
    [ "$sig" = 0 ] || kill -"$sig" "$pid" 2>/dev/null
    for i in $(seq 1 20); do kill -0 "$pid" 2>/dev/null || { wait "$pid" 2>/dev/null; return 0; }; sleep 0.5; done
  done
}
start_gg_proc() { # start_gg_proc <binary> <log-prefix> <args...>: sets gg_pid, gg_fd
  local binary=$1 p=$2; shift 2
  rm -f "$data/gg.ctl"; mkfifo "$data/gg.ctl"
  env GIGGLES_OWNER_PASSWORD="$gg_pw" taskset -c "$SERVER_CPUS" "$binary" "$@" \
    < "$data/gg.ctl" > "$p.out" 2> "$p.err" &
  gg_pid=$!
  exec {gg_fd}> "$data/gg.ctl"
}
start_server() { # start_server <target> <tag>
  local t=$1 tag=$2 binary=$gs
  case "$t" in
    pg)
      rm -rf "$data/pg"; echo "$pg_pw" > "$data/pgpw"
      initdb -D "$data/pg" -U postgres --auth=scram-sha-256 --pwfile="$data/pgpw" -E UTF8 --locale=C.UTF-8 \
        > "$out/logs/$tag.initdb.log" 2>&1 || return 1
      cat >> "$data/pg/postgresql.conf" <<CONF
listen_addresses = '127.0.0.1'
port = $pg_port
unix_socket_directories = '$data'
max_connections = 200
shared_buffers = 2GB
fsync = on
synchronous_commit = on
full_page_writes = on
CONF
      pin_server pg_ctl -D "$data/pg" -l "$out/logs/$tag.postgres.log" -w -t 60 start > /dev/null 2>&1 || return 1
      pg_up=1 ;;
    giggles-sql|giggles-sql-base|giggles-native)
      [ "$t" != giggles-sql-base ] || binary=$gs_base
      rm -rf "$data/gg"; mkdir -p "$data/gg"
      start_gg_proc "$binary" "$out/logs/$tag.create" --dev --data "$data/gg" --port $gg_pg --engine v2
      wait_line "$out/logs/$tag.create.out" 'preview=single-host' 60 || { stop_proc "$gg_pid" "$gg_fd"; gg_pid='' gg_fd=''; return 1; }
      stop_proc "$gg_pid" "$gg_fd"; gg_pid='' gg_fd=''
      gg_config_json > "$data/gg/server.json"
      start_gg_proc "$binary" "$out/logs/$tag.server" "$data/gg/server.json"
      wait_line "$out/logs/$tag.server.out" 'preview=single-host' 60 || return 1 ;;
  esac
}
stop_server() {
  if [ $pg_up = 1 ]; then pg_ctl -D "$data/pg" -m fast -w stop >/dev/null 2>&1; pg_up=0; fi
  stop_proc "$gg_pid" "$gg_fd"; gg_pid='' gg_fd=''
  [ -n "${data:-}" ] && rm -rf "$data/pg" "$data/gg"
}
stop_all() { stop_server; return 0; }
trap 'stop_all' EXIT

# 5. Metadata.
pgset=''
if has pg; then
  start_server pg meta || fail_setup "PostgreSQL initdb/start (see logs/meta.*)"
  pgset=$(psql "$pg_url" -AtF= -c "select name, setting||coalesce(unit,'') from pg_settings where name in
    ('fsync','synchronous_commit','full_page_writes','wal_sync_method','shared_buffers','max_connections','wal_level','checkpoint_timeout','max_wal_size') order by 1" 2>&1 | paste -sd';')
  stop_server
fi
if has giggles-sql || has giggles-native; then
  start_server giggles-sql meta || fail_setup "GigglesDB config-mode server did not start (see logs/meta.*)"
  stop_server
fi
gg_cfg_meta=$(fp=app-cert-sha256 gg_config_json)
target_order="listed on every repeat"
[ -z "$BASE_COMMIT" ] || target_order="listed on odd repeats, reversed on even repeats"
python3 - "$out/meta.json" <<PY
import json, sys, subprocess
def sh(c): return subprocess.run(c, shell=True, capture_output=True, text=True).stdout.strip()
meta = {
  "commit": sh("git -C $repo rev-parse HEAD"),
  "base_commit": "$BASE_COMMIT" or None,
  "utc": sh("date -u +%FT%TZ"),
  "nproc": int(sh("nproc")),
  "cpu_model": sh("lscpu | sed -n 's/^Model name:[[:space:]]*//p'"),
  "mem": sh("free -g | awk '/Mem:/{print \$2\"G\"}'"),
  "kernel": sh("uname -r"),
  "server_cpus": "$SERVER_CPUS", "client_cpus": "$CLIENT_CPUS",
  "data_dir": "$data", "data_fstype": "$data_fstype", "data_device": "$data_source",
  "data_mount": "$data_mount", "data_note": "$data_note",
  "data_fs": sh("df -T $data | tail -1"),
  "postgres_version": sh("postgres --version"),
  "postgres_settings": "$pgset",
  "giggles": "one giggles-server --release config-mode server for giggles-sql and giggles-native; database created by --dev --engine v2 (RocksDB), commits WriteOptions.sync=true (Durability::Sync); fresh server and data dir per measured config",
  "giggles_config": json.loads('''$gg_cfg_meta'''),
  "giggles_native": "HTTPS text arm at https://localhost:$gg_http, mTLS (generated CA/server/app cert, binding role Database postgres); app-loadtest native frontend " + ("present" if $native_support else "absent"),
  "opset": "$OPSET", "ops": "$OPS",
  "node": sh("node --version"),
  "release_build_seconds": $build_s,
  "base_release_build_seconds": $base_build_s if "$BASE_COMMIT" else None,
  "target_order": "$target_order",
  "matrix": {"targets": "$TARGETS", "clients": "$CLIENTS", "mixes": "$MIXES", "protocols": "$PROTOCOLS",
             "warmup_s": $WARMUP, "duration_s": $DURATION, "repeats": $REPEATS, "rows": $ROWS},
}
json.dump(meta, open(sys.argv[1], "w"), indent=1)
PY

# 6. Matrix. Order: repeat > clients > mix > protocol > target.
# With a comparison base, reverse the target order on even repeats.
# giggles-native has no
# protocol choice; it runs once per (clients, mix), labelled protocol=native.
mixarg() {
  case "$1" in
    read) echo read=100 ;; write) echo write=100 ;; tx) echo tx=100 ;; error) echo error=100 ;;
    mixed|default) echo read=60,write=25,tx=10,error=5 ;;
    *) echo "${1//+/,}" ;;   # raw weights, e.g. read=50+write=50
  esac
}
protoarg() { [ "$1" = all ] && echo simple,extended,prepared || echo "${1//+/,}"; }
loadtest() { # loadtest <target> <report> <log> <seconds> <clients> <mix> <protocols>
  local target=$1 report=$2 log=$3 secs=$4 c=$5 mix=$6 proto=$7 url extra=() prefix=()
  [ -n "${LOAD_TIMEOUT:-}" ] && prefix=(timeout --kill-after=10 "$LOAD_TIMEOUT")
  case "$target" in
    pg) url=$pg_url; extra=(--protocols "$proto") ;;
    giggles-sql|giggles-sql-base) url=$gg_url; extra=(--protocols "$proto") ;;
    giggles-native)
      # --url is the plaintext pgwire listener of the same server, used for
      # schema setup and seeding; operations go over HTTPS with mTLS.
      url=$gg_url
      extra=(--frontend native --native-endpoint "https://localhost:$gg_http"
             --native-ca "$tls/ca.pem" --native-cert "$tls/app.pem" --native-key "$tls/app.key")
      ;;
  esac
  ( cd "$lt" && "${prefix[@]}" taskset -c "$CLIENT_CPUS" node_modules/.bin/tsx src/cli.ts load --url "$url" \
      --clients "$c" --duration "${secs}s" --rows "$ROWS" --mix "$mix" --ops "$OPS" \
      --max-error-rate 0 --report "$report" "${extra[@]}" ) > "$log" 2>&1
}
sample_prezero_tmpfiles() { # <server pid> <data dir>; run in the background
  exec python3 - "$1" "$2" <<'PY'
import os
import sys
import time

fd_dir = f"/proc/{sys.argv[1]}/fd"
data_prefix = os.path.realpath(sys.argv[2]) + "/"
while True:
    try:
        entries = os.listdir(fd_dir)
    except OSError:
        break
    count = 0
    for entry in entries:
        try:
            target = os.readlink(os.path.join(fd_dir, entry))
        except OSError:
            continue
        if target.startswith(data_prefix) and target.endswith(" (deleted)"):
            count += 1
    print(count, flush=True)
    time.sleep(10)
PY
}

overall=0
IFS=, read -ra T <<< "$TARGETS"; IFS=, read -ra C <<< "$CLIENTS"
IFS=, read -ra M <<< "$MIXES"; IFS=, read -ra P <<< "$PROTOCOLS"
for r in $(seq 1 "$REPEATS"); do
  ordered_targets=("${T[@]}")
  if [ -n "$BASE_COMMIT" ] && (( r % 2 == 0 )); then
    ordered_targets=()
    for (( i=${#T[@]}-1; i>=0; i-- )); do ordered_targets+=("${T[i]}"); done
  fi
  for c in "${C[@]}"; do for m in "${M[@]}"; do for p in "${P[@]}"; do for t in "${ordered_targets[@]}"; do
  pl=$p
  if [ "$t" = giggles-native ]; then
    [ "$p" = "${P[0]}" ] || continue
    pl=native
  fi
  tag="$t-c$c-$m-$pl-r$r"
  rep="$out/logs/$tag.json"; rc=0; skip=''
  if [ "$t" = giggles-native ] && [ $native_support = 0 ]; then
    skip="app-loadtest has no --frontend native"
  elif ! start_server "$t" "$tag"; then
    rc=99; overall=1; progress "server start failed $tag"
    echo "{\"failed\": true, \"blocker\": \"server start failed (see logs/$tag.*)\"}" > "$rep"
    stop_server
  else
    if [ "$WARMUP" -gt 0 ]; then
      progress "warmup $tag"
      loadtest "$t" "$out/logs/$tag.warmup.json" "$out/logs/$tag.warmup.log" "$WARMUP" "$c" "$(mixarg "$m")" "$(protoarg "$p")"
    fi
    progress "run $tag"
    sample_pid=''
    if [[ "$t" == giggles-sql || "$t" == giggles-sql-base ]]; then
      sample_prezero_tmpfiles "$gg_pid" "$data/gg" > "$out/logs/$tag.prezero-fds" &
      sample_pid=$!
    fi
    loadtest "$t" "$rep" "$out/logs/$tag.log" "$DURATION" "$c" "$(mixarg "$m")" "$(protoarg "$p")"; rc=$?
    if [ -n "$sample_pid" ]; then
      kill "$sample_pid" 2>/dev/null || true
      wait "$sample_pid" 2>/dev/null || true
    fi
    [ $rc = 0 ] || overall=1
    stop_server
  fi
  python3 - "$rep" "$out/results.jsonl" "$t" "$c" "$m" "$pl" "$r" "$rc" "$skip" <<'PY'
import json, sys, os
from pathlib import Path
rep, res, t, c, m, p, r, rc, skip = sys.argv[1:]
meta = json.load(open(os.path.join(os.path.dirname(res), "meta.json")))
line = {"target": t, "clients": int(c), "mix": m, "protocol": p, "repeat": int(r), "exit": int(rc),
        "opset": meta["opset"], "duration_s": meta["matrix"]["duration_s"], "warmup_s": meta["matrix"]["warmup_s"],
        "rows": meta["matrix"]["rows"], "nproc": meta["nproc"], "cpu_model": meta["cpu_model"],
        "server_cpus": meta["server_cpus"], "client_cpus": meta["client_cpus"],
        "commit": {"giggles-sql-base": meta["base_commit"], "pg": None}.get(t, meta["commit"]),
        "data_fstype": meta["data_fstype"], "ops": {}, "protocols": {}}
if t in ("giggles-sql", "giggles-sql-base"):
    samples = []
    warnings = []
    try:
        samples = Path(rep).with_suffix(".prezero-fds").read_text().splitlines()
    except OSError:
        pass
    try:
        with Path(rep).with_suffix(".server.err").open(errors="replace") as stderr:
            warnings = [msg.rstrip("\n") for msg in stderr if "WAL prezero pool disabled" in msg]
    except OSError:
        pass
    line.update(prezero_tmpfile_max=max((int(s) for s in samples if s.isdecimal()), default=None),
                prezero_disabled_lines=len(warnings),
                prezero_disabled_first_line=warnings[0] if warnings else None)
if skip:
    line["skipped"] = skip
else:
    try:
        d = json.load(open(rep))
        lat = d.get("latency", {}) or {}
        errs = sum((d.get("errors") or {}).values())
        n = d.get("count") or 0
        line.update({"count": n, "elapsed_ms": d.get("elapsed"), "ops_per_s": d.get("throughput"),
                     "p50_ms": lat.get("p50"), "p95_ms": lat.get("p95"), "p99_ms": lat.get("p99"),
                     "max_ms": lat.get("max"), "errors": errs, "error_detail": d.get("errors"),
                     "error_rate": (errs / n) if n else None,
                     "retries": d.get("retries"), "invariants": d.get("invariants"),
                     "ops": d.get("ops", {}), "protocols": d.get("protocols", {}),
                     "not_applicable": sorted((d.get("notApplicable") or {}).keys()),
                     "failed": bool(d.get("failed")) or int(rc) != 0, "blocker": d.get("blocker")})
    except Exception as e:
        line["failed"] = True; line["blocker"] = "no report: %s" % e
open(res, "a").write(json.dumps(line) + "\n")
PY
  done; done; done; done
done

# Capture and processing run separately so every capture overlaps diagnostic load.
# Redirects intentionally create user-owned artifacts; only collectors need root.
# shellcheck disable=SC2024
bench_profile() ( # <pid> <profile-dir> <key> <deadline> <callgraph> <progress-file> [load-log]
  set -uo pipefail
  local pid=$1 dest=$2 key=$3 tdl=$4 mode=$5 progress_file=$6 loadlog=${7:-} raw s rc
  # Exact per-thread CPU (utime/stime ticks) and context switches, plus the
  # load's completed count, so CPU and switches can be divided per op.
  threads_snap() {
    { echo "epoch $(date +%s.%N) clk_tck $(getconf CLK_TCK)"
      echo "load $( [ -n "$loadlog" ] && grep -o 'completed=[0-9]*' "$loadlog" | tail -1)"
      echo "proc $(sed 's/.*) //' "/proc/$pid/stat" | cut -d' ' -f12,13)"
      for t in /proc/"$pid"/task/*; do
        printf '%s %s %s %s\n' "${t##*/}" "$(sed 's/.*) //' "$t/stat" | cut -d' ' -f12,13)" \
          "$(awk '/ctxt_switches/{printf "%s ", $2}' "$t/status")" "$(tr ' ' _ < "$t/comm")"
      done; } > "$1" 2>/dev/null || true
  }
  local manifest bg
  local -a jobs=()
  mkdir -p "$dest/raw-$key"
  raw=$dest/raw-$key
  manifest=$dest/manifest-$key.txt
  printf 'server_pid=%s\nlive RocksDB stats dump unavailable (no endpoint)\n' "$pid" > "$manifest"
  ps -L -o tid,comm -p "$pid" >> "$manifest" 2>&1 || true
  log() { echo "profile $key: $*" >&2; }
  step() { echo "$(date -u +%T) profile-$key $*" >> "$progress_file"; }
  budget() { cap "$1" "$tdl" || { step "skipped $2: per-target budget spent"; return 1; }; }
  capture() { # name limit output command...; each invocation also works in background
    local name=$1 limit=$2 output=$3 s rc=0 start epoch status sig=TERM
    [ "$name" != pidstat ] || sig=INT  # pidstat prints its Average lines only on SIGINT
    shift 3
    start=$(date -u +%FT%TZ); epoch=$(date +%s)
    if ! command -v "$1" >/dev/null 2>&1; then
      status=skipped; log "$name skipped: $1 missing"
    elif s=$(budget "$limit" "$name"); then
      step "capture $name"
      sudo -n timeout --signal="$sig" --kill-after=10 "$s" "$@" > "$output" 2> "$dest/$name-$key.err" || rc=$?
      status=ok
      # timeout/SIGINT are normal for continuous observers, only with data.
      if [ "$rc" != 0 ] && { [ "$limit" != 310 ] || { [ "$rc" != 124 ] && [ "$rc" != 130 ]; } || [ ! -s "$output" ]; }; then
        status=failed; log "$name failed rc=$rc"
      fi
    else status=skipped; fi
    printf 'capture=%s status=%s duration_seconds=%s start_utc=%s pid=%s\n' \
      "$name" "$status" "$(( $(date +%s) - epoch ))" "$start" "$pid" >> "$manifest"
    [ "$status" = ok ]
  }
  cat > "$raw/fsync.bt" <<BT
tracepoint:syscalls:sys_enter_fsync,
tracepoint:syscalls:sys_enter_fdatasync,
tracepoint:syscalls:sys_enter_sync_file_range /pid == $pid/ { @start[tid] = nsecs; }
tracepoint:syscalls:sys_exit_fsync,
tracepoint:syscalls:sys_exit_fdatasync,
tracepoint:syscalls:sys_exit_sync_file_range /pid == $pid && @start[tid]/ {
  @latency_us[probe, comm] = hist((nsecs - @start[tid]) / 1000);
  delete(@start[tid]);
}
interval:s:300 { exit(); }
END { clear(@start); }
BT
  cat > "$raw/futex.bt" <<BT
tracepoint:syscalls:sys_enter_futex /pid == $pid/ {
  \$op = args->op & 127;
  if (\$op == 0 || \$op == 9 || \$op == 6 || \$op == 11) { @start[tid] = nsecs; }
}
tracepoint:syscalls:sys_exit_futex /pid == $pid && @start[tid]/ {
  @us[comm, ustack(perf, 32)] = sum((nsecs - @start[tid]) / 1000);
  @count[comm, ustack(perf, 32)] = count();
  delete(@start[tid]);
}
interval:s:30 { exit(); }
END { clear(@start); }
BT
  # Observers start first and run through the entire sequential capture window.
  capture pidstat 310 "$dest/pidstat-$key.txt" pidstat -t -u -w -p "$pid" 1 & jobs+=("$!")
  capture iostat 310 "$dest/iostat-$key.txt" iostat -x 1 & jobs+=("$!")
  capture biolatency 310 "$dest/biolatency-$key.txt" biolatency-bpfcc -m 300 1 & jobs+=("$!")
  capture fsync 310 "$dest/fsync-$key.txt" bpftrace "$raw/fsync.bt" & jobs+=("$!")
  capture syscount 310 "$dest/syscount-$key.txt" syscount-bpfcc -p "$pid" -L -d 300 & jobs+=("$!")
  # Clean 30 s window (only the low-overhead observers above attached), then
  # threads2 closes the whole profiled window after the last capture.
  threads_snap "$dest/threads0-$key.txt"; sleep 30; threads_snap "$dest/threads1-$key.txt"
  if [ "$mode" = fp ]; then
    capture cpu 45 "$dest/cpu-$key.txt" perf record -F 499 -g --call-graph fp -p "$pid" -o "$raw/perf.data" -- sleep 30 || true
  else
    capture cpu 45 "$dest/cpu-$key.txt" perf record -F 99 -g --call-graph dwarf,8192 -p "$pid" -o "$raw/perf.data" -- sleep 30 || true
  fi
  if [ -s "$raw/perf.data" ] && grep -q "capture=cpu status=ok" "$manifest"; then touch "$dest/ran-$key"; fi
  capture offcpu 55 "$dest/offcpu-$key.folded" offcputime-bpfcc -f -d -p "$pid" --stack-storage-size 65536 30 || true
  [ ! -s "$dest/offcpu-$key.folded" ] || touch "$dest/ran-offcpu-$key"
  capture futex 40 "$dest/futex-$key.txt" bpftrace "$raw/futex.bt" || true
  # Ubuntu perf commonly advertises this option but lacks BUILD_BPF_SKEL.
  if s=$(budget 15 'lock support') && sudo -n timeout --kill-after=10 "$s" perf lock contention -h > "$raw/lock-help" 2>&1; then :; fi
  if [ -s "$raw/lock-help" ] && ! grep -Eqi 'not built-in|BUILD_BPF_SKEL|not supported' "$raw/lock-help"; then
    capture lock 40 "$dest/lock-$key.txt" perf lock contention -a -b -p "$pid" -- sleep 30 || true
  else
    log 'lock skipped: perf BPF contention support unavailable'
    printf 'capture=lock status=skipped duration_seconds=0 start_utc=%s pid=%s\n' "$(date -u +%FT%TZ)" "$pid" >> "$manifest"
  fi
  capture wakeup 45 "$dest/wakeup-$key.folded" wakeuptime-bpfcc -f -p "$pid" 20 || true
  threads_snap "$dest/threads2-$key.txt"
  for bg in "${jobs[@]}"; do wait "$bg" || true; done
  exit 0
)

bench_profile_rocksdb() { # copy before stop_server deletes the data directory
  local dest=$1 key=$2 file n=0
  mkdir -p "$dest/rocksdb-$key"
  while IFS= read -r -d '' file; do
    [ "$n" -lt 5 ] || break
    n=$(( n + 1 ))
    # Bound growing live LOG files too; find -size alone races with writes.
    head -c 20971520 "$file" > "$dest/rocksdb-$key/LOG-$n" || true
    printf 'rocksdb_LOG=%s copied_as=LOG-%s cap_bytes=20971520\n' "$file" "$n" >> "$dest/manifest-$key.txt"
  done < <(find "$data/gg" -type f -name LOG -print0 2>/dev/null)
}

# Redirects intentionally remain user-owned.
# shellcheck disable=SC2024
bench_profile_post() ( # <profile-dir> <key> <deadline> <progress-file>
  set -uo pipefail
  local dest=$1 key=$2 tdl=$3 progress_file=$4 raw s size kind input
  raw=$dest/raw-$key
  [ -d "$raw" ] || exit 0
  trap 'rm -rf "$raw"' EXIT
  log() { echo "profile $key: $*" >&2; }
  budget() { cap "$1" "$tdl" || return 1; }
  if [ -s "$raw/perf.data" ]; then
    if s=$(budget 60); then
      sudo -n timeout --kill-after=10 "$s" perf report -i "$raw/perf.data" --stdio --no-children --percent-limit 0.5 > "$dest/top-$key.txt" || log 'report failed'
    fi
    if s=$(budget 60); then
      sudo -n timeout --kill-after=10 "$s" perf report -i "$raw/perf.data" --stdio --no-children -g none --sort comm,dso --percent-limit 0.2 > "$dest/comm-$key.txt" || log 'comm report failed'
    fi
    size=1073741825
    if s=$(budget 15); then size=$(sudo -n timeout --kill-after=10 "$s" stat -c %s "$raw/perf.data") || size=1073741825; fi
    if [ "$size" -le 1073741824 ] && s=$(budget 120); then
      if command -v inferno-collapse-perf >/dev/null 2>&1; then
        sudo -n timeout --kill-after=10 "$s" perf script -i "$raw/perf.data" |
          sudo -n timeout --kill-after=10 "$s" inferno-collapse-perf --all > "$dest/folded-$key.txt" || log 'fold failed'
      else
        sudo -n timeout --kill-after=10 "$s" perf script -i "$raw/perf.data" |
          sudo -n timeout --kill-after=10 "$s" awk '
          function flush(    i,s) {
            if(n) { s=comm; for(i=n;i>0;i--) s=s ";" f[i]; count[s]++ } n=0
          }
          /^[[:space:]]*$/ { flush(); next }
          /[[:space:]][0-9]+(\/[0-9]+)?[[:space:]]+(\[[0-9]+\][[:space:]]+)?[0-9]+\.[0-9]+:/ { flush(); comm=$0; sub(/^[[:space:]]+/, "", comm); sub(/[[:space:]]+[0-9]+(\/[0-9]+)?[[:space:]].*$/, "", comm); gsub(/;/, ":", comm); next }
          /^[[:space:]]+[[:xdigit:]]+[[:space:]]/ {
            line=$0; sub(/^[[:space:]]*[[:xdigit:]]+[[:space:]]+/, "", line)
            sub(/[[:space:]]+\(.*$/, "", line); sub(/\+0x[[:xdigit:]]+$/, "", line)
            gsub(/;/, ":", line); f[++n]=line
          }
          END { flush(); for(s in count) print s " " count[s] }
        ' > "$dest/folded-$key.txt" || log 'fold failed'
      fi
    fi
  fi
  for kind in cpu offcpu wakeup; do
    input=$dest/$kind-$key.folded
    [ "$kind" != cpu ] || input=$dest/folded-$key.txt
    [ -s "$input" ] || continue
    if s=$(budget 30); then
      if command -v inferno-flamegraph >/dev/null 2>&1; then
        if [ "$kind" = cpu ]; then
          sudo -n timeout --kill-after=10 "$s" inferno-flamegraph < "$input" > "$dest/flame-cpu-$key.svg" || log 'render failed'
        else
          sudo -n timeout --kill-after=10 "$s" inferno-flamegraph --colors io --countname us --title "$( [ "$kind" = offcpu ] && echo Off-CPU || echo Wakeup ) $key" < "$input" > "$dest/flame-$kind-$key.svg" || log 'render failed'
        fi
      elif command -v flamegraph.pl >/dev/null 2>&1; then
        sudo -n timeout --kill-after=10 "$s" flamegraph.pl < "$input" > "$dest/flame-$kind-$key.svg" || log 'render failed'
      else log 'no renderer; keeping folded stacks'; fi
    fi
  done
  if command -v python3 >/dev/null 2>&1 && s=$(budget 30); then
    timeout --kill-after=10 "$s" python3 -I "$repo/scripts/bench-wait-report.py" "$dest" --key "$key" --out "$dest/wait-report-$key.md" || log 'wait report failed'
  fi
  exit 0
)

# Diagnostics follow ALL normal repeats; one per GigglesDB target/client count.
if [ "$PROFILE" = 1 ] && { has giggles-sql || has giggles-native; }; then
  phase_start=$(date +%s)
  profile_ready=1
  if ! command -v offcputime-bpfcc >/dev/null 2>&1 || ! command -v bpftrace >/dev/null 2>&1; then
    progress "BPF tools missing: snapshot predates P1; run bench-snapshot-setup.sh"
  fi
  # bcc compiles probes against the running kernel's headers; a box booted on
  # a different kernel than the snapshot's leaves offcputime/wakeuptime dead.
  if [ ! -e "/lib/modules/$(uname -r)/build" ]; then
    progress "kernel headers for $(uname -r) missing; bcc captures (offcpu, wakeup, biolatency, syscount) will likely fail"
  fi
  if ! timeout --kill-after=10 15 perf --version > "$out/logs/perf-version.txt" 2>&1; then
    sudo -n timeout --kill-after=10 180 apt-get -o DPkg::Lock::Timeout=60 install -y "linux-tools-$(uname -r)" linux-tools-common > "$out/logs/perf-install.txt" 2>&1 || {
      progress "perf installation failed; continuing with available BPF tools";
    }
  fi
  if [ "$profile_ready" = 1 ]; then
    perf_version=$(timeout --kill-after=10 15 perf --version 2>/dev/null) || perf_version=""
    sudo -n timeout --kill-after=10 15 sysctl kernel.perf_event_paranoid=-1 kernel.kptr_restrict=0 > "$out/logs/perf-sysctl.txt" 2>&1 || progress "perf sysctl failed (captures may be unavailable)"
  fi
  if [ "$profile_ready" = 1 ]; then
    # start_server picks $gs / $gs_base; both now name the profile binaries.
    gs=$gs_profile gs_base=$gs_base_profile
    progress "profile builds: $profile_build_parity"
    for c in "${C[@]}"; do for t in "${T[@]}"; do
      [ "$t" = pg ] && continue
      [ "$t" = giggles-native ] && [ "$native_support" = 0 ] && continue
      tag="profile-$t-c$c"
      read -r _ callgraph < <(profile_binary "$t")
      callgraph=${callgraph:-dwarf}
      elapsed=$(( $(date +%s) - phase_start ))
      if [ -n "$BOX_DEADLINE" ] && [ $(( $(date +%s) + PROFILE_TARGET_WORST )) -gt "$BOX_DEADLINE" ]; then
        progress "box deadline: skipping $tag and later targets ($(( BOX_DEADLINE - $(date +%s) ))s left)"
        break 2
      fi
      if [ $(( elapsed + PROFILE_TARGET_WORST )) -gt "$PROFILE_PHASE_BUDGET" ]; then
        progress "profile budget: skipping $tag and later targets (elapsed ${elapsed}s)"
        break 2
      fi
      tdl=$(( $(date +%s) + PROFILE_TARGET_STEPS ))
      if start_server "$t" "$tag"; then
        progress "$tag mix=${M[0]} protocol=${P[0]}"
        if load_timeout=$(cap 450 "$tdl"); then
          LOAD_TIMEOUT=$load_timeout loadtest "$t" "$out/logs/$tag.json" "$out/logs/$tag.log" "$PROFILE_LOAD_SECONDS" "$c" "$(mixarg "${M[0]}")" "$(protoarg "${P[0]}")" &
          load_pid=$!
          # First progress line is after schema setup and seeding.
          if wait_line "$out/logs/$tag.log" 'completed=' 60; then
            bench_profile "$gg_pid" "$out/profiles" "$t-c$c" "$tdl" "$callgraph" "$out/progress" "$out/logs/$tag.log" > "$out/logs/$tag.capture.txt" 2>&1 || progress "$tag capture failed (nonfatal)"
          else progress "$tag load not ready; skipping captures"
          fi
          wait "$load_pid" || progress "$tag diagnostic load failed (nonfatal)"
        else progress "skipped $tag load: per-target budget spent"
        fi
      else progress "$tag server start failed (nonfatal)"
      fi
      bench_profile_rocksdb "$out/profiles" "$t-c$c"
      stop_server
      bench_profile_post "$out/profiles" "$t-c$c" "$tdl" "$out/progress" >> "$out/logs/$tag.capture.txt" 2>&1 || progress "$tag post-process failed (nonfatal)"
    done; done
    if compgen -G "$out/profiles/ran-*" | grep -v "/ran-offcpu-" >/dev/null; then profile_ran=1; fi
  fi
fi
python3 - "$out/meta.json" "$PROFILE" "$profile_ran" "$perf_version" "$profile_callgraph" "$profile_build_seconds" \
  "$profile_base_callgraph" "$profile_base_build_seconds" "$profile_build_parity" "$PROFILE_PHASE_BUDGET" <<'PYMETA'
import json, sys, glob, os
path, requested, ran, version, callgraph, build_seconds, base_callgraph, base_build_seconds, parity, budget = sys.argv[1:]
meta = json.load(open(path))
# profile_base_callgraph is null without a giggles-sql-base target; with one it
# always equals profile_callgraph (fp for both, or dwarf for both).
meta.update(offcpu_ran=bool(glob.glob(os.path.join(os.path.dirname(path), "profiles", "ran-offcpu-*"))), profile_requested=requested == "1", profiling_ran=ran == "1", perf_version=version or None,
            profile_callgraph=callgraph, profile_build_seconds=int(build_seconds),
            profile_base_callgraph=base_callgraph or None, profile_base_build_seconds=int(base_build_seconds) if base_build_seconds else None,
            profile_build_parity=parity, profile_phase_budget_seconds=int(budget))
json.dump(meta, open(path, "w"), indent=1)
PYMETA

# 7. Summary (median of repeats per config). Failed runs stay in the table.
python3 - "$out" "$build_s" <<'PY'
import json, sys, statistics as st
out, build_s = sys.argv[1], sys.argv[2]
meta = json.load(open(out + "/meta.json"))
rows = [json.loads(l) for l in open(out + "/results.jsonl")]
groups = {}
comparisons = {}
for r in rows:
    config = r["clients"], r["mix"], r["protocol"]
    groups.setdefault((r["target"], *config), []).append(r)
    if r["target"] in ("giggles-sql", "giggles-sql-base"):
        comparisons.setdefault(config, []).append(r)
def med(v):
    v = [x for x in v if x is not None]
    return st.median(v) if v else None
def f(x, d=1):
    return "-" if x is None else f"{x:.{d}f}"
def why(r):
    if r.get("skipped"): return r["skipped"]
    if not (r.get("failed") or r.get("exit")): return ""
    if r.get("blocker"): b = str(r["blocker"])
    elif r.get("errors"): b = "errors " + ", ".join(f"{k}={v}" for k, v in sorted(r["error_detail"].items(), key=lambda kv: -kv[1])[:4])
    elif r.get("invariants") and not all(r["invariants"].values()): b = "invariants " + ",".join(k for k, v in r["invariants"].items() if not v)
    else: b = "exit %s" % r.get("exit")
    return "FAILED: " + b[:120].replace("|", "/")
failed = [r for r in rows if r.get("failed") or r.get("exit")]
L = ["# GigglesDB bench summary", "",
     f"commit `{meta['commit']}`, {meta['utc']}, {meta['cpu_model']}, nproc={meta['nproc']}, mem={meta['mem']}",
     f"server cores {meta['server_cpus']}, client cores {meta['client_cpus']} (taskset); release build {build_s}s",
     f"warmup {meta['matrix']['warmup_s']}s, measured {meta['matrix']['duration_s']}s, repeats {meta['matrix']['repeats']}, rows {meta['matrix']['rows']}",
     f"op set `{meta['opset']}`: {meta['ops']}; --max-error-rate 0",
     f"data dir {meta['data_dir']}: fs **{meta['data_fstype']}** on {meta['data_device']} (mount {meta['data_mount']}; {meta['data_note']})",
     f"PostgreSQL: {meta['postgres_version']}; {meta['postgres_settings']}",
     f"GigglesDB: {meta['giggles']}; pgwire connections {meta['giggles_config']['postgresql']['connections']}, api sessions {meta['giggles_config']['api_limits']['sessions']}, service_limits {meta['giggles_config']['service_limits']}, role_statements {meta['giggles_config'].get('role_statements', 'default (16)')}",
     "",
     f"**{len(rows) - len(failed)} of {len(rows)} runs passed" + (f"; {len(failed)} FAILED (see notes)**" if failed else "**"), "",
     "| target | clients | mix | protocol | ops/s | p50 ms | p99 ms | errors | repeats | ops/s per repeat | prezero_tmpfile_max | prezero_disabled_lines | notes |",
     "|---|---|---|---|---|---|---|---|---|---|---|---|---|"]
if meta["base_commit"]:
    L[3:3] = [f"base `{meta['base_commit']}`, release build {meta['base_release_build_seconds']}s. Targets are {meta['target_order']}."]
for (t, c, m, p), g in groups.items():
    ok = [r for r in g if "skipped" not in r]
    notes = sorted({why(r) for r in g} - {""})
    errs = sum(r.get("errors") or 0 for r in ok)
    tmpfiles = max((r["prezero_tmpfile_max"] for r in g if r.get("prezero_tmpfile_max") is not None), default=None)
    disabled = sum(r["prezero_disabled_lines"] for r in g) if "prezero_disabled_lines" in g[0] else None
    L.append(f"| {t} | {c} | {m} | {p} | {f(med([r.get('ops_per_s') for r in ok]))} | {f(med([r.get('p50_ms') for r in ok]),2)} | "
             f"{f(med([r.get('p99_ms') for r in ok]),2)} | {errs} | {len(ok)} | "
             f"{' / '.join(f(r.get('ops_per_s')) for r in ok)} | {f(tmpfiles, 0)} | {f(disabled, 0)} | {'; '.join(notes)} |")
L += ["", "Medians across repeats. Latency is per app operation (all statements, retries, commit), in ms.",
      "Pool columns show the maximum FD count and the total disabled warnings across repeats.",
      "Compare ratios between targets within one box; absolute numbers vary between cloud boxes."]

metrics = "ops_per_s", "p50_ms", "p99_ms"
def ratio(tip, base):
    return f(tip / base if tip is not None and base else None, 3)

if meta["base_commit"]:
    for (c, m, p), runs in comparisons.items():
        if not any(r["target"] == "giggles-sql-base" for r in runs):
            continue
        L += ["", f"## Base and tip, {c} clients, {m}, {p}", "",
              "| repeat | binary | ops/s | p50 ms | p99 ms | prezero_tmpfile_max | prezero_disabled_lines |",
              "|---|---|---|---|---|---|---|"]
        repeats = {}
        for r in runs:
            binary = "base" if r["target"] == "giggles-sql-base" else "tip"
            L.append(f"| {r['repeat']} | {binary} | {f(r.get('ops_per_s'))} | {f(r.get('p50_ms'), 2)} | "
                     f"{f(r.get('p99_ms'), 2)} | {f(r.get('prezero_tmpfile_max'), 0)} | {f(r.get('prezero_disabled_lines'), 0)} |")
            repeat = repeats.setdefault(r["repeat"], {})
            if not (r.get("failed") or r.get("exit") or r.get("skipped")):
                repeat[binary] = r
        L += ["", "| tip/base | ops/s | p50 | p99 |", "|---|---|---|---|"]
        matched = {metric: [] for metric in metrics}
        for repeat, pair in repeats.items():
            cells = []
            for metric in metrics:
                tip = pair.get("tip", {}).get(metric)
                base = pair.get("base", {}).get(metric)
                cells.append(ratio(tip, base))
                if tip is not None and base is not None:
                    matched[metric].append((tip, base))
            L.append(f"| Repeat {repeat} | " + " | ".join(cells) + " |")
        means = []
        for pairs in matched.values():
            if pairs:
                tips, bases = zip(*pairs)
                means.append(ratio(st.mean(tips), st.mean(bases)))
            else:
                means.append("-")
        L.append("| Mean tip / mean base (matched runs) | " + " | ".join(means) + " |")

for r in rows:
    L += ["", f"## Operations: {r['target']}, {r['clients']} clients, {r['mix']}, {r['protocol']}, repeat {r['repeat']}", "",
          "| target | clients | mix | op | count | p50 ms | p99 ms |",
          "|---|---|---|---|---|---|---|"]
    for op, v in sorted(r.get("ops", {}).items()):
        L.append(f"| {r['target']} | {r['clients']} | {r['mix']} | {op} | {v['count']} | {f(v['p50_ms'],2)} | {f(v['p99_ms'],2)} |")
open(out + "/summary.md", "w").write("\n".join(L) + "\n")
PY
progress "done rc=$overall"
echo "rc=$overall" > "$out/done"
