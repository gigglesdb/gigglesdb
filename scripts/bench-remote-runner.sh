#!/usr/bin/env bash
# Runs on the boat.dev box; started by scripts/bench-remote.sh. Do not run locally.
# Builds giggles-server --release, then for every (repeat, clients, mix,
# protocol, target) starts a fresh server pinned to SERVER_CPUS (PostgreSQL 18,
# or one GigglesDB config-mode server that serves both giggles-sql over pgwire
# and giggles-native over HTTPS), runs scripts/app-loadtest `load` pinned to
# CLIENT_CPUS, stops the server and deletes its data. Writes
# /home/user/bench/{progress,done,meta.json,results.jsonl,summary.md,logs/}.
# done holds rc=0 when every measured run passed (including --max-error-rate 0
# and the invariants), rc=1 otherwise, rc=2 when setup failed.
#
# Environment (set by bench-remote.sh): TARGETS CLIENTS MIXES PROTOCOLS WARMUP
# DURATION REPEATS ROWS OPSET [SERVER_CPUS CLIENT_CPUS DATA_ROOT].
set -uo pipefail
source "$HOME/bench.env"
repo=/home/user/gigglesdb
out=/home/user/bench
OPSET=${OPSET:-common}
PROFILE=${PROFILE:-0}
profile_ran=0
perf_version=""
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
    [[ ",$TARGETS," == *",giggles-native,"* ]] && fail_setup "opset sql-full is valid only for pg,giggles-sql" ;;
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
[ "$PROFILE" = 1 ] && export CARGO_PROFILE_RELEASE_DEBUG=line-tables-only
progress "cargo build --release giggles-server"
command -v sccache >/dev/null 2>&1 && export RUSTC_WRAPPER=$(command -v sccache)
s=$(date +%s)
( cd "$repo/crates/giggles-server" && cargo build --release --bin giggles-server ) > "$out/logs/build.log" 2>&1 \
  || fail_setup "release build (see logs/build.log)"
build_s=$(( $(date +%s) - s ))
gs=$CARGO_TARGET_DIR/release/giggles-server
progress "build done in ${build_s}s"

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
gg_pid='' gg_fd='' pg_up=0
gg_config_json() {
  cat <<JSON
{"database":"$data/gg/db","native":"127.0.0.1:55440","postgres":"127.0.0.1:55443",
 "postgresql":{"listen":"127.0.0.1:$gg_pg","tls":"disabled","databases":["postgres"],"connections":128},
 "http":"127.0.0.1:$gg_http","api_limits":{"sessions":128},
 "certificate":"$tls/server.pem","private_key":"$tls/server.key","client_ca":"$tls/ca.pem",
 "bindings":[{"fingerprint":"$fp","role":{"role":"Database","name":"postgres"}}],
 "service_limits":{"sessions":128,"burst":1000000,"per_second":1000000},
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
start_gg_proc() { # start_gg_proc <log-prefix> <args...>: sets gg_pid, gg_fd
  local p=$1; shift
  rm -f "$data/gg.ctl"; mkfifo "$data/gg.ctl"
  env GIGGLES_OWNER_PASSWORD="$gg_pw" taskset -c "$SERVER_CPUS" "$gs" "$@" \
    < "$data/gg.ctl" > "$p.out" 2> "$p.err" &
  gg_pid=$!
  exec {gg_fd}> "$data/gg.ctl"
}
start_server() { # start_server <target> <tag>
  local t=$1 tag=$2
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
    giggles-sql|giggles-native)
      rm -rf "$data/gg"; mkdir -p "$data/gg"
      start_gg_proc "$out/logs/$tag.create" --dev --data "$data/gg" --port $gg_pg --engine v2
      wait_line "$out/logs/$tag.create.out" 'preview=single-host' 60 || { stop_proc "$gg_pid" "$gg_fd"; gg_pid='' gg_fd=''; return 1; }
      stop_proc "$gg_pid" "$gg_fd"; gg_pid='' gg_fd=''
      gg_config_json > "$data/gg/server.json"
      start_gg_proc "$out/logs/$tag.server" "$data/gg/server.json"
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
python3 - "$out/meta.json" <<PY
import json, sys, subprocess
def sh(c): return subprocess.run(c, shell=True, capture_output=True, text=True).stdout.strip()
meta = {
  "commit": sh("git -C $repo rev-parse HEAD"),
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
  "matrix": {"targets": "$TARGETS", "clients": "$CLIENTS", "mixes": "$MIXES", "protocols": "$PROTOCOLS",
             "warmup_s": $WARMUP, "duration_s": $DURATION, "repeats": $REPEATS, "rows": $ROWS},
}
json.dump(meta, open(sys.argv[1], "w"), indent=1)
PY

# 6. Matrix. Order: repeat > clients > mix > protocol > target, so targets
# alternate and drift over the run hits all alike. giggles-native has no
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
  local target=$1 report=$2 log=$3 secs=$4 c=$5 mix=$6 proto=$7 url extra=()
  case "$target" in
    pg) url=$pg_url; extra=(--protocols "$proto") ;;
    giggles-sql) url=$gg_url; extra=(--protocols "$proto") ;;
    giggles-native)
      # --url is the plaintext pgwire listener of the same server, used for
      # schema setup and seeding; operations go over HTTPS with mTLS.
      url=$gg_url
      extra=(--frontend native --native-endpoint "https://localhost:$gg_http"
             --native-ca "$tls/ca.pem" --native-cert "$tls/app.pem" --native-key "$tls/app.key")
      ;;
  esac
  ( cd "$lt" && taskset -c "$CLIENT_CPUS" node_modules/.bin/tsx src/cli.ts load --url "$url" \
      --clients "$c" --duration "${secs}s" --rows "$ROWS" --mix "$mix" --ops "$OPS" \
      --max-error-rate 0 --report "$report" "${extra[@]}" ) > "$log" 2>&1
}
overall=0
IFS=, read -ra T <<< "$TARGETS"; IFS=, read -ra C <<< "$CLIENTS"
IFS=, read -ra M <<< "$MIXES"; IFS=, read -ra P <<< "$PROTOCOLS"
for r in $(seq 1 "$REPEATS"); do for c in "${C[@]}"; do for m in "${M[@]}"; do for p in "${P[@]}"; do for t in "${T[@]}"; do
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
    loadtest "$t" "$rep" "$out/logs/$tag.log" "$DURATION" "$c" "$(mixarg "$m")" "$(protoarg "$p")"; rc=$?
    [ $rc = 0 ] || overall=1
    stop_server
  fi
  python3 - "$rep" "$out/results.jsonl" "$t" "$c" "$m" "$pl" "$r" "$rc" "$skip" <<'PY'
import json, sys, os
rep, res, t, c, m, p, r, rc, skip = sys.argv[1:]
meta = json.load(open(os.path.join(os.path.dirname(res), "meta.json")))
line = {"target": t, "clients": int(c), "mix": m, "protocol": p, "repeat": int(r), "exit": int(rc),
        "opset": meta["opset"], "duration_s": meta["matrix"]["duration_s"], "warmup_s": meta["matrix"]["warmup_s"],
        "rows": meta["matrix"]["rows"], "nproc": meta["nproc"], "cpu_model": meta["cpu_model"],
        "server_cpus": meta["server_cpus"], "client_cpus": meta["client_cpus"], "commit": meta["commit"],
        "data_fstype": meta["data_fstype"], "ops": {}, "protocols": {}}
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
done; done; done; done; done

bench_profile() (
set -uo pipefail
pid=$1 out=$2 key=$3
mkdir -p "$out"
raw=$(mktemp -d "$out/raw-XXXXXX") || exit 0
trap 'rm -rf "$raw"' EXIT
log() { echo "profile $key: $*" >&2; }
if sudo -n perf record -F 999 -g --call-graph dwarf,16384 -p "$pid" -o "$raw/perf.data" -- sleep 60; then
  touch "$out/ran-$key"
  sudo -n perf report -i "$raw/perf.data" --stdio --no-children --percent-limit 0.5 > "$out/top-$key.txt" || log "report failed"
  # Fold leaf-first frames into root-first stacks without installing tools.
  sudo -n perf script -i "$raw/perf.data" | awk '
    function flush(    i,s) {
      if(n) { s=f[n]; for(i=n-1;i>0;i--) s=s ";" f[i]; count[s]++ }
      n=0
    }
    /^[[:space:]]*$/ { flush(); next }
    /^[[:space:]]+[[:xdigit:]]+[[:space:]]/ {
      line=$0; sub(/^[[:space:]]*[[:xdigit:]]+[[:space:]]+/, "", line)
      sub(/[[:space:]]+\(.*$/, "", line); sub(/\+0x[[:xdigit:]]+$/, "", line)
      gsub(/;/, ":", line); f[++n]=line
    }
    END { flush(); for(s in count) print s " " count[s] }
  ' > "$out/folded-$key.txt" || log "fold failed"
  if [ -s "$out/folded-$key.txt" ]; then
    if command -v inferno-flamegraph >/dev/null 2>&1; then
      inferno-flamegraph < "$out/folded-$key.txt" > "$out/flame-$key.svg" || log "inferno failed"
    elif command -v flamegraph.pl >/dev/null 2>&1; then
      flamegraph.pl < "$out/folded-$key.txt" > "$out/flame-$key.svg" || log "flamegraph failed"
    else log "no renderer; keeping folded stacks"
    fi
  fi
else log "CPU capture failed; trying other captures"
fi
if ! sudo -n perf lock contention -a -b -E 30 -- sleep 30 > "$out/lock-$key.txt"; then
  log "BPF contention unavailable; trying lock record/report"
  sudo -n perf lock record -o "$raw/lock.data" -- sleep 30 &&
    sudo -n perf lock report -i "$raw/lock.data" > "$out/lock-$key.txt" || log "lock fallback failed"
fi
if command -v offcputime-bpfcc >/dev/null 2>&1; then
  sudo -n offcputime-bpfcc -df -p "$pid" 30 > "$out/offcpu-$key.txt" || log "offcputime failed"
else
  sudo -n perf sched record -o "$raw/sched.data" -- sleep 20 &&
    sudo -n perf sched timehist -i "$raw/sched.data" -s > "$out/offcpu-$key.txt" || log "sched fallback failed"
fi
exit 0
)

# Diagnostics follow ALL normal repeats; one per GigglesDB target/client count.
if [ "$PROFILE" = 1 ] && { has giggles-sql || has giggles-native; }; then
  profile_ready=1
  if ! perf --version > "$out/logs/perf-version.txt" 2>&1; then
    sudo -n apt-get install -y "linux-tools-$(uname -r)" linux-tools-common > "$out/logs/perf-install.txt" 2>&1 || {
      progress "perf installation failed; skipping profiles"; profile_ready=0;
    }
  fi
  if [ "$profile_ready" = 1 ]; then
    perf_version=$(perf --version 2>/dev/null) || profile_ready=0
    sudo -n sysctl kernel.perf_event_paranoid=-1 kernel.kptr_restrict=0 > "$out/logs/perf-sysctl.txt" 2>&1 || progress "perf sysctl failed (captures may be unavailable)"
  fi
  if [ "$profile_ready" = 1 ]; then
    for c in "${C[@]}"; do for t in "${T[@]}"; do
      [ "$t" = pg ] && continue
      [ "$t" = giggles-native ] && [ "$native_support" = 0 ] && continue
      tag="profile-$t-c$c"
      if start_server "$t" "$tag"; then
        progress "$tag mix=${M[0]} protocol=${P[0]}"
        # Covers CPU, lock (including fallback), and off-CPU capture windows.
        loadtest "$t" "$out/logs/$tag.json" "$out/logs/$tag.log" 210 "$c" "$(mixarg "${M[0]}")" "$(protoarg "${P[0]}")" &
        load_pid=$!
        # First progress line is after schema setup and seeding.
        if wait_line "$out/logs/$tag.log" 'completed=' 60; then
          bench_profile "$gg_pid" "$out/profiles" "$t-c$c" > "$out/logs/$tag.capture.txt" 2>&1 || progress "$tag capture failed (nonfatal)"
        else progress "$tag load not ready; skipping captures"
        fi
        wait "$load_pid" || progress "$tag diagnostic load failed (nonfatal)"
      else progress "$tag server start failed (nonfatal)"
      fi
      stop_server
    done; done
    if compgen -G "$out/profiles/ran-*" >/dev/null; then profile_ran=1; fi
  fi
fi
python3 - "$out/meta.json" "$PROFILE" "$profile_ran" "$perf_version" <<'PYMETA'
import json, sys
path, requested, ran, version = sys.argv[1:]
meta = json.load(open(path))
meta.update(profile_requested=requested == "1", profiling_ran=ran == "1", perf_version=version or None)
json.dump(meta, open(path, "w"), indent=1)
PYMETA

# 7. Summary (median of repeats per config). Failed runs stay in the table.
python3 - "$out" "$build_s" <<'PY'
import json, sys, statistics as st
out, build_s = sys.argv[1], sys.argv[2]
meta = json.load(open(out + "/meta.json"))
rows = [json.loads(l) for l in open(out + "/results.jsonl")]
groups = {}
for r in rows:
    groups.setdefault((r["target"], r["clients"], r["mix"], r["protocol"]), []).append(r)
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
     f"GigglesDB: {meta['giggles']}; pgwire connections {meta['giggles_config']['postgresql']['connections']}, api sessions {meta['giggles_config']['api_limits']['sessions']}, service_limits {meta['giggles_config']['service_limits']}",
     "",
     f"**{len(rows) - len(failed)} of {len(rows)} runs passed" + (f"; {len(failed)} FAILED (see notes)**" if failed else "**"), "",
     "| target | clients | mix | protocol | ops/s | p50 ms | p99 ms | errors | repeats | ops/s per repeat | notes |",
     "|---|---|---|---|---|---|---|---|---|---|---|"]
for (t, c, m, p), g in groups.items():
    ok = [r for r in g if "skipped" not in r]
    notes = sorted({why(r) for r in g} - {""})
    errs = sum(r.get("errors") or 0 for r in ok)
    L.append(f"| {t} | {c} | {m} | {p} | {f(med([r.get('ops_per_s') for r in ok]))} | {f(med([r.get('p50_ms') for r in ok]),2)} | "
             f"{f(med([r.get('p99_ms') for r in ok]),2)} | {errs} | {len(ok)} | "
             f"{' / '.join(f(r.get('ops_per_s')) for r in ok)} | {'; '.join(notes)} |")
L += ["", "Medians across repeats. Latency is per app operation (all statements, retries, commit), in ms.",
      "Compare ratios between targets within one box; absolute numbers vary between cloud boxes."]
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
