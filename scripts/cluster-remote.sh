#!/usr/bin/env bash
# Coordinator-only multi-host cluster runner. Never used for worker iteration.
set -euo pipefail
usage() {
  echo 'Usage: cluster-remote.sh --tip COMMIT --who NAME --nodes N [--client-box] --suite FILE [--ttl S<=7200] [--slot-wait S] [--keep-going] [--kit KIT_FLAGS...]'
  echo '3 <= N <= 6; at most six boxes. Default TTL 7200, slot wait 1800; TTL must exceed 600.'
  echo 'Optional --colocate MS [--colocate-rounds N (default 3)] [--colocate-strict]; off by default.'
  echo 'Exit: 0 pass, 1 suite failure, 2 usage, 3 daily cap, 4 slots, 5 infrastructure.'
}
bad() { usage >&2; exit 2; }
tip='' who='' nodes='' suite='' ttl=7200 slot_wait=1800 separate=0 keep=0 colocate='' colocate_rounds=3 strict=0
kit=()
while [ $# -gt 0 ]; do
  case "$1" in
    --help|-h) usage; exit 0 ;;
    --tip|--who|--nodes|--suite|--ttl|--slot-wait|--colocate|--colocate-rounds)
      [ $# -ge 2 ] || bad
      case "$1" in --tip) tip=$2 ;; --who) who=$2 ;; --nodes) nodes=$2 ;; --suite) suite=$2 ;; --ttl) ttl=$2 ;; --slot-wait) slot_wait=$2 ;; --colocate) colocate=$2 ;; --colocate-rounds) colocate_rounds=$2 ;; esac
      shift 2 ;;
    --client-box) separate=1; shift ;;
    --keep-going) keep=1; shift ;;
    --colocate-strict) strict=1; shift ;;
    --kit) shift; kit=("$@"); break ;;
    *) bad ;;
  esac
done
[[ "$who" =~ ^[A-Za-z0-9_.-]+$ ]] && [ -n "$tip" ] && [ -f "$suite" ] || bad
[[ "$nodes" =~ ^[3-6]$ && "$ttl" =~ ^[0-9]{1,4}$ && "$slot_wait" =~ ^[0-9]{1,6}$ ]] || bad
[[ "$colocate_rounds" =~ ^[1-9][0-9]{0,2}$ ]] || bad
[ -z "$colocate" ] || [[ "$colocate" =~ ^[0-9]{1,6}([.][0-9]{1,6})?$ ]] || bad
ttl=$((10#$ttl)); slot_wait=$((10#$slot_wait)); count=$((nodes + separate))
[ "$count" -le 6 ] && [ "$ttl" -le 7200 ] && [ "$ttl" -gt 600 ] || bad
# Prevent pass-through flags from replacing runner-owned kit paths or topology.
for flag in "${kit[@]}"; do
  case "$flag" in --nodes|--nodes=*|--hosts|--hosts=*|--local|--root|--root=*|--binary|--binary=*) bad ;; esac
done
who=cluster-${who#cluster-}
here=$(cd "$(dirname "$0")" && pwd); repo=$(cd "$here/.." && pwd)
boatdir=J:/gigglesdb-work/boat; B=${BOAT:-$HOME/.ascii/bin/boat.exe}
g() { git -C "$repo" "$@"; }
tip=$(g rev-parse --verify "$tip^{commit}") || exit 2
short=${tip:0:12}; stamp=$(date -u +%Y%m%dT%H%M%SZ)
token="$who $$ $stamp"
run="$boatdir/cluster/$stamp-$short-$who-$$"
mkdir -p "$run/tmp" "$boatdir/slots"
export TEMP="$run/tmp" TMP="$run/tmp"
log() { echo "[cluster-remote $(date -u +%T)] $*" >&2; }
bj() { timeout --kill-after=10 660 "$B" --no-update "$@"; }
jfield() { python -I -c 'import json,sys
lines=[x for x in sys.stdin.read().splitlines() if x.strip().startswith("{")]
d=json.loads(lines[-1]) if lines else {}
if sys.argv[1] == "ip": d = dict(d, ip=d.get("sandbox", {}).get("ip") or d.get("ip", ""))
if sys.argv[1] == "stdout" and (d.get("exit_code", d.get("exitCode", 0)) != 0 or int(sys.argv[4])):
    message = "Remote step failed: " + sys.argv[2] + "\n" + str(d.get("stderr", "")) + "\nstdout (last 20 lines):\n" + "\n".join(str(d.get("stdout", "")).splitlines()[-20:]) + "\n"
    sys.stderr.write(message)
    with open(sys.argv[3], "a", encoding="utf-8") as f: f.write(message)
    sys.exit(1)
print(d.get(sys.argv[1], ""))' "$@"; }
bexec_t() { local sec=$1 box=$2 status=0 reply; shift 2
  reply=$(timeout --kill-after=10 "$((sec + 30))" "$B" --no-update exec "$box" --timeout "$sec" --json -- bash -lc "set -euo pipefail; $*") || status=$?
  printf '%s\n' "$reply" | jfield stdout "$box: $*" "$run/failure.log" "$status" || return 1
  return "$status"
}
bexec() { bexec_t 600 "$@"; }
quote() { [ "$#" -gt 0 ] || return 0; printf '%q ' "$@"; }
slots=() pids=() boxes=() ips=(); client=0; copied=0
ref="refs/cluster-remote/$stamp-$$"; started=$(date +%s)
slot_owned() { [ "$(head -n 1 "$boatdir/slots/$1/owner" 2>/dev/null)" = "$token" ]; }
release_slots() {
  local held=("${slots[@]}") i
  slots=() # A signal during release must not make cleanup revisit released slots.
  for i in "${held[@]}"; do
    slot_owned "$i" || continue
    rm -f "$boatdir/slots/$i/owner"; rmdir "$boatdir/slots/$i"
  done
}
check_cap() {
  local used
  used=$(grep -c "^$(date -u +%F).*| START" "$boatdir/usage.log" 2>/dev/null || true); used=${used:-0}
  [ "$((used + $1))" -le 200 ] || { log "daily cap: $used + $1 > 200"; return 3; }
}
delete_box() {
  local b=$1 rc=$2 mins
  [ ! -f "$run/deleted-$b" ] || return 0
  bj delete "$b" --yes >/dev/null 2>&1 || bj delete "$b" --yes >/dev/null 2>&1 || return 5
  mins=$(( ($(date +%s) - started + 59) / 60 ))
  echo "$(date -u +%FT%TZ) | $who | $b | large | $short | END cluster rc=$rc ${mins}min" >> "$boatdir/usage.log"
  touch "$run/deleted-$b"
}
copy_back() {
  [ "$copied" = 0 ] || return 0; copied=1
  local i b
  if [ "${#boxes[@]}" -gt "$client" ]; then
    b=${boxes[$client]}
    bexec_t 120 "$b" 'cd /home/user; test -d out; tar -czf out.tar.gz out' >/dev/null &&
      (cd "$run" && bj scp "$b:/home/user/out.tar.gz" out.tar.gz >/dev/null 2>&1) &&
      tar --force-local -xzf "$run/out.tar.gz" -C "$run" || log 'partial client results unavailable'
    [ ! -f "$run/out/summary.md" ] || cp "$run/out/summary.md" "$run/summary.md"
  fi
  for ((i=0; i<nodes && i<${#boxes[@]}; i++)); do
    b=${boxes[$i]}
    bexec_t 120 "$b" 'mkdir -p /home/user/cx-logs
[ ! -f /home/user/prepare.log ] || cp /home/user/prepare.log /home/user/cx-logs/
if [ -d /home/user/cx ]; then cd /home/user/cx
for f in node-*/stdout.log node-*/stderr.log agent-*.log; do
  [ -f "$f" ] || continue; mkdir -p "/home/user/cx-logs/$(dirname "$f")"
  tail -c 20971520 "$f" > "/home/user/cx-logs/$f"
done; fi
tar -czf /home/user/cx-logs.tar.gz -C /home/user/cx-logs .' >/dev/null &&
      (cd "$run" && bj scp "$b:/home/user/cx-logs.tar.gz" "node-$((i+1))-logs.tar.gz" >/dev/null 2>&1) || log "node $((i+1)) logs unavailable"
  done
  return 0
}
cleanup() {
  local rc=$? f b i failed=0 action='continued with final placement'
  trap - EXIT ERR; trap '' INT TERM HUP
  set +e
  # Drain parallel starts before discovering ids: a late successful new is owned too.
  for i in "${pids[@]}"; do wait "$i"; done
  copy_back
  shopt -s nullglob
  for f in "$run"/start-*.id; do
    b=$(cat "$f"); [ -n "$b" ] || continue
    # A successful id is recorded before START; repair a missing START on early failure.
    grep -q "| $b |.*| START cluster" "$boatdir/usage.log" 2>/dev/null ||
      echo "$(date -u +%FT%TZ) | $who | $b | large | $short | START cluster" >> "$boatdir/usage.log"
    log "deleting $b"
    delete_box "$b" "$rc" || { log "DELETE FAILED: $b (TTL remains active)"; failed=1; }
  done
  release_slots
  g update-ref -d "$ref" 2>/dev/null
  rm -f "$run/stack.bundle" "$run/cx.tar.gz"; rmdir "$run/tmp" 2>/dev/null
  if [ ! -f "$run/summary.md" ]; then printf 'Cluster run rc=%s; see collected logs and meta.json.\n' "$rc" > "$run/summary.md"; fi
  if [ -f "$run/colocate-incomplete" ]; then
    [ "$strict" = 0 ] || action='strict mode stopped run'
    printf '\nCo-location incomplete after %s rounds (pairwise RTT <= %s ms); %s; see colocate.json.\n' "$colocate_rounds" "$colocate" "$action" >> "$run/summary.md"
  fi
  cat "$run/summary.md"
  log "results: $run"
  [ "$failed" = 0 ] || rc=5
  exit "$rc"
}
trap cleanup EXIT
trap 'exit 5' INT TERM HUP
trap 'exit 5' ERR
[ -x "$B" ] || { log "missing boat CLI: $B"; exit 5; }
check_cap "$count" || exit 3
deadline=$(( $(date +%s) + slot_wait ))
while :; do
  for ((i=1; i<=10; i++)); do
    if mkdir "$boatdir/slots/slot-$i" 2>/dev/null; then
      slots+=("slot-$i"); printf '%s\n' "$token" > "$boatdir/slots/slot-$i/owner"
      [ "${#slots[@]}" -lt "$count" ] || break
    fi
  done
  [ "${#slots[@]}" -lt "$count" ] || break
  release_slots
  [ "$(date +%s)" -lt "$deadline" ] || exit 4
  sleep 5
done
base=$(cat "$boatdir/bench-base-commit.txt")
g update-ref "$ref" "$tip"
if g merge-base --is-ancestor "$base" "$tip" && [ "$base" != "$tip" ]; then
  g bundle create "$run/stack.bundle" "$ref" "^$base"
else g bundle create "$run/stack.bundle" "$ref"; fi
cp "$here/cluster-remote-box.sh" "$run/cluster-remote-box.sh"
cp "$suite" "$run/suite.sh"
# Each child persists its id before any operation that can fail. Parent joins ALL children.
start_box() {
  local k=$1 status=0 b
  check_cap 1 || return 3
  bj new --from gigglesdb-bench-base --type large --no-env --ttl "$ttl" --name "$who-$short-$((k+1))" --json > "$run/start-$k.json" 2>&1 || status=$?
  b=$(grep -o '"id":"bx_[a-z0-9]*"' "$run/start-$k.json" | head -1 | cut -d'"' -f4) || return 5
  [[ "$b" =~ ^bx_[a-z0-9]+$ ]] || return 5
  echo "$b" > "$run/start-$k.id"
  echo "$(date -u +%FT%TZ) | $who | $b | large | $short | START cluster" >> "$boatdir/usage.log"
  slot_owned "${slots[$k]}" || return 5
  printf '%s\n' "$token" "$b $short" > "$boatdir/slots/${slots[$k]}/owner"
  [ "$status" = 0 ] || return 5
}
join() { local p failed=0; for p in "${pids[@]}"; do wait "$p" || failed=1; done; pids=(); [ "$failed" = 0 ]; }
launch_box() {
  local status=0
  # Serialize cap checks, but let cleanup drain a start interrupted by a signal.
  start_box "$1" & pids=("$!")
  wait "${pids[0]}" || status=$?
  pids=()
  return "$status"
}
started=$(date +%s)
for ((i=0; i<count; i++)); do launch_box "$i" || exit $?; done
for ((i=0; i<count; i++)); do boxes+=("$(cat "$run/start-$i.id")"); done
[ "$separate" = 0 ] || client=$nodes
get_ips() {
  local b ip
  ips=()
  for b in "${boxes[@]}"; do
    ip=$(bj info "$b" --json | jfield ip) || return 5
    python -I -c 'import ipaddress,sys; ipaddress.IPv6Address(sys.argv[1])' "$ip" || return 5
    ips+=("$ip")
  done
}
colocate_round() {
  local round=$1 i
  for ((i=0; i<count; i++)); do
    bexec_t 120 "${boxes[$i]}" "python3 -I -c $(quote 'import json,re,subprocess,sys
row=[]
for host in sys.argv[1:]:
    p=subprocess.run(["ping","-6","-n","-c","5","-W","2",host],capture_output=True,text=True,timeout=15)
    m=re.search(r"= [\d.]+/([\d.]+)/",p.stdout)
    row.append(float(m[1]) if m and p.returncode == 0 else None)
print(json.dumps(row))') $(quote "${ips[@]}")" > "$run/colocate-row-$i.json" || return 5
  done
  python -I - "$run" "$colocate" "$round" "${boxes[@]}" -- "${ips[@]}" <<'PY'
import ipaddress, itertools, json, pathlib, sys
path=pathlib.Path(sys.argv[1]); limit=float(sys.argv[2]); args=sys.argv[4:]; split=args.index('--')
boxes,ips=args[:split],args[split+1:]; n=len(boxes)
matrix=[json.loads((path/f'colocate-row-{k}.json').read_text()) for k in range(n)]
group=next(g for size in range(n,0,-1) for g in itertools.combinations(range(n),size)
           if all(matrix[i][j] is not None and matrix[i][j]<=limit for i in g for j in g if i!=j))
file=path/'colocate.json'; data=json.loads(file.read_text()) if file.exists() else dict(limit_ms=limit,rounds=[])
data['complete']=len(group)==n
data['rounds'].append(dict(round=int(sys.argv[3]),boxes=[dict(id=b,ipv6=ips[k],
    prefix=str(ipaddress.IPv6Network(ips[k]+'/48',strict=False))) for k,b in enumerate(boxes)],
    matrix_ms=matrix,group=[boxes[k] for k in group]))
file.write_text(json.dumps(data,indent=2)+'\n',newline='\n')
(path/'meta.json').write_text(json.dumps(dict(colocate=data),indent=2)+'\n',newline='\n')
print(' '.join(str(k) for k in range(n) if k not in group))
PY
}
colocate_boxes() {
  local round outside k
  [ -n "$colocate" ] || return 0
  for ((round=1; round<=colocate_rounds; round++)); do
    outside=$(colocate_round "$round") || return 5
    [ -n "$outside" ] || return 0
    [ "$round" -lt "$colocate_rounds" ] || break
    for k in $outside; do
      delete_box "${boxes[$k]}" 0 || return 5
      rm -f "$run/start-$k.id"
      launch_box "$k" || return $?
      boxes[$k]=$(cat "$run/start-$k.id")
    done
    get_ips || return 5
  done
  touch "$run/colocate-incomplete"
  log 'co-location incomplete; continuing with the final placement unless strict'
  [ "$strict" = 0 ] || return 5
}
get_ips
colocate_boxes || exit $?
setup_box() {
  local k=$1 b=${boxes[$1]} head
  (cd "$run"; for f in stack.bundle cluster-remote-box.sh suite.sh; do bj scp "$f" "$b:/home/user/$f" >/dev/null || return 1; done)
  head=$(bexec "$b" "cd /home/user/gigglesdb; git fetch -q /home/user/stack.bundle '$ref:refs/cluster/tip'; git checkout -q --detach '$tip'; git rev-parse HEAD")
  [ "$(echo "$head" | tr -d '[:space:]')" = "$tip" ] || return 1
  local role=''; [ "$k" != "$client" ] || role=client
  bj exec "$b" --detach -- bash -lc "bash /home/user/cluster-remote-box.sh prepare $role > /home/user/prepare.log 2>&1; echo rc=\$? > /home/user/prepare.done" >/dev/null
}
for ((i=0; i<count; i++)); do setup_box "$i" > "$run/setup-$i.log" 2>&1 & pids+=("$!"); done
join || exit 5
wait_for() {
  local b=$1 donefile=$2 progress=$3 st rc previous=''
  while :; do
    [ "$(( $(date +%s) - started ))" -lt "$((ttl - 600))" ] || { log 'TTL safety deadline'; return 5; }
    st=$(bexec_t 60 "$b" "cat '$donefile' 2>/dev/null || :; tail -1 '$progress' 2>/dev/null || :") || return 5
    if [ "$st" != "$previous" ]; then log "$b: $st"; echo "$b: $st" >> "$run/progress.log"; previous=$st; fi
    rc=$(echo "$st" | sed -n 's/^rc=\([0-9]*\)$/\1/p' | head -1)
    if [ -n "$rc" ]; then [ "$rc" = 0 ]; return; fi
    sleep 15
  done
}
for b in "${boxes[@]}"; do wait_for "$b" /home/user/prepare.done /home/user/prepare.log & pids+=("$!"); done
join || exit 5
for ((i=0; i<count; i++)); do
  peers=(); for ((j=0; j<count; j++)); do [ "$i" = "$j" ] || peers+=("${ips[$j]}"); done
  bexec "${boxes[$i]}" "bash /home/user/cluster-remote-box.sh firewall $(quote "${peers[@]}")" >/dev/null
done
cb=${boxes[$client]}; hosts=$(IFS=,; echo "${ips[*]:0:$nodes}")
bexec "$cb" "source /home/user/bench.env; /home/user/cxvenv/bin/python /home/user/gigglesdb/scripts/cluster-exit/kit.py init --nodes '$nodes' --hosts '$hosts' --root /home/user/cx --binary \"\$CARGO_TARGET_DIR/release/giggles-server\" $(quote "${kit[@]}"); tar -czf /home/user/cx.tar.gz -C /home/user cx" >/dev/null
(cd "$run"; bj scp "$cb:/home/user/cx.tar.gz" cx.tar.gz >/dev/null)
for ((i=0; i<nodes; i++)); do
  b=${boxes[$i]}
  if [ "$i" != "$client" ]; then (cd "$run"; bj scp cx.tar.gz "$b:/home/user/cx.tar.gz" >/dev/null); bexec "$b" 'tar -xzf /home/user/cx.tar.gz -C /home/user' >/dev/null; fi
  bexec "$b" "bash /home/user/cluster-remote-box.sh agent '$((i+1))'" >/dev/null
done
bexec "$cb" "cd /home/user/gigglesdb/scripts/cluster-exit; /home/user/cxvenv/bin/python -c 'from lib import Cluster,wait_for; c=Cluster.load(\"/home/user/cx/cluster.json\"); [wait_for(lambda: c.agent(n[\"id\"]).status(), 30) for n in c.nodes]'" >/dev/null
for ((i=0; i<count; i++)); do
  peers=(); for ((j=0; j<count; j++)); do [ "$i" = "$j" ] || peers+=("${ips[$j]}"); done
  bexec "${boxes[$i]}" "bash /home/user/cluster-remote-box.sh rtt $(quote "${peers[@]}")" > "$run/meta-$i.json"
done
python -I - "$run" "$tip" "$nodes" "$client" "${boxes[@]}" -- "${ips[@]}" <<'PY'
import json, pathlib, sys
root, tip, nodes, client = sys.argv[1:5]; args = sys.argv[5:]; split = args.index('--')
boxes, ips = args[:split], args[split+1:]; path = pathlib.Path(root)
rows = [dict(id=b, ipv6=ips[k], **json.loads((path/f'meta-{k}.json').read_text())) for k,b in enumerate(boxes)]
colocate = json.loads((path/'colocate.json').read_text()) if (path/'colocate.json').exists() else None
(path/'meta.json').write_text(json.dumps(dict(tip=tip, nodes=int(nodes), client_box=boxes[int(client)],
    client_placement='shared node 1' if int(client)==0 else 'separate', boxes=rows, colocate=colocate), indent=2)+'\n', newline='\n')
PY
(cd "$run"; bj scp meta.json "$cb:/home/user/meta.json" >/dev/null)
bexec "$cb" 'mkdir -p /home/user/out; cp /home/user/meta.json /home/user/out/meta.json' >/dev/null
bj exec "$cb" --detach -- bash -lc "NODES=$nodes KEEP_GOING=$keep bash /home/user/cluster-remote-box.sh suite > /home/user/suite-launch.log 2>&1; echo rc=\$? > /home/user/suite.done" >/dev/null
suite_rc=0
wait_for "$cb" /home/user/suite.done /home/user/out/suite.log || suite_rc=$?
[ "$suite_rc" != 5 ] || exit 5
# A completed nonzero suite is a test failure; a missing done file is infrastructure.
status=$(bexec_t 60 "$cb" 'cat /home/user/suite.done 2>/dev/null || :') || exit 5
[[ "$status" =~ ^rc=[0-9]+$ ]] || exit 5
copy_back
[ "$status" = rc=0 ] && exit 0 || exit 1
