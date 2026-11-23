#!/usr/bin/env bash
set -euo pipefail
# D2 native Spanner scenarios. Own only the emulator started here and our server.
scenarios=()
for scenario in "$@"; do
  case "$scenario" in
    resume|stream|probe|commit) scenarios+=("$scenario") ;;
    all) scenarios+=(probe resume stream commit) ;;
    *) echo 'FAIL setup arguments expected-resume-stream-probe-commit-or-all'; exit 1 ;;
  esac
done
[[ ${#scenarios[@]} != 0 ]] || { echo 'FAIL setup arguments missing-scenario'; exit 1; }
export NUNYA_D2_MUX_RW=${NUNYA_D2_MUX_RW:-on}
case "$NUNYA_D2_MUX_RW" in
  on) export GOOGLE_CLOUD_SPANNER_MULTIPLEXED_SESSIONS_FOR_RW=true; echo 'INFO mux-rw=on' ;;
  off) export GOOGLE_CLOUD_SPANNER_MULTIPLEXED_SESSIONS_FOR_RW=false; echo 'INFO mux-rw=off' ;;
  *) echo 'FAIL setup invalid-mux-rw'; exit 1 ;;
esac
scenario=setup
umask 077
here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
work=${NUNYA_D2_WORK:-}
[[ -n $work ]] || { echo 'FAIL setup missing-NUNYA_D2_WORK'; exit 1; }
binary=${NUNYA_D2_NUNYA:-${CARGO_TARGET_DIR:-$here/../../target}/debug/nunya}
if [[ ! -f $binary && -f $binary.exe ]]; then binary=$binary.exe; fi
[[ -f $binary ]] || { echo 'FAIL setup missing-nunya-binary'; exit 1; }
export GOMODCACHE="$work/gomod" GOCACHE="$work/gocache" GOTOOLCHAIN=local GOFLAGS=-mod=mod
export TEMP="$work/tmp" TMP="$work/tmp" TMPDIR="$work/tmp"
mkdir -p "$TEMP" "${NUNYA_D2_ROOT:-$work/runs}"
run=$(mktemp -d "${NUNYA_D2_ROOT:-$work/runs}/d2-XXXXXXXX")
export NUNYA_HOME="$run/home"
mkdir -p "$NUNYA_HOME" "$run/state"
owned_emulator=false
server_pid=
stage=setup
cleanup() {
  local rc=$?
  trap - EXIT
  if [[ -f $run/server.pid ]]; then server_pid=$(cat "$run/server.pid"); fi
  if [[ -n $server_pid ]]; then
    kill "$server_pid" 2>/dev/null || true
    wait "$server_pid" 2>/dev/null || true
  fi
  if $owned_emulator; then
    docker rm -fv "$NUNYA_D2_CONTAINER" >"$run/docker-cleanup.log" 2>&1 || rc=1
  fi
  if [[ $rc != 0 ]]; then printf 'FAIL %s %s see-run-evidence\n' "$scenario" "$stage"; fi
  exit "$rc"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
wait_port() {
  python - "$1" <<'PY'
import socket, sys, time
host, port = sys.argv[1].removeprefix('http://').rsplit(':', 1)
for _ in range(300):
    try:
        with socket.create_connection((host, int(port)), timeout=.2):
            sys.exit(0)
    except OSError:
        time.sleep(.1)
sys.exit(1)
PY
}
if [[ -z ${NUNYA_D2_EMULATOR_GRPC:-} && -z ${NUNYA_D2_EMULATOR_REST:-} ]]; then
  [[ -n ${NUNYA_D2_CONTAINER:-} ]] || { echo 'FAIL setup missing-NUNYA_D2_CONTAINER'; exit 1; }
  label=nunya-d2=1
  if [[ $NUNYA_D2_CONTAINER == gd81-d2c-* ]]; then label=gd81-d2c=1; fi
  docker run -d --name "$NUNYA_D2_CONTAINER" --label "$label" \
    -p 127.0.0.1::9010 -p 127.0.0.1::9020 \
    gcr.io/cloud-spanner-emulator/emulator:1.5.41 >"$run/docker-start.log" 2>&1
  owned_emulator=true
  NUNYA_D2_EMULATOR_GRPC=$(docker port "$NUNYA_D2_CONTAINER" 9010/tcp | tr -d '\r')
  NUNYA_D2_EMULATOR_REST=http://$(docker port "$NUNYA_D2_CONTAINER" 9020/tcp | tr -d '\r')
  export NUNYA_D2_EMULATOR_GRPC NUNYA_D2_EMULATOR_REST
  wait_port "$NUNYA_D2_EMULATOR_REST"
  NUNYA_SPANNER_REST="$NUNYA_D2_EMULATOR_REST" bash "$here/setup.sh" >"$run/emulator-setup.log" 2>&1
elif [[ -z ${NUNYA_D2_EMULATOR_GRPC:-} || -z ${NUNYA_D2_EMULATOR_REST:-} ]]; then
  exit 1
fi
# Reserve three distinct ports together; release them immediately before startup.
read -r admin_port mgmt_port native_port < <(python - <<'PY' | tr -d '\r'
import socket
sockets = [socket.socket() for _ in range(3)]
for s in sockets: s.bind(('127.0.0.1', 0))
print(*(s.getsockname()[1] for s in sockets))
for s in sockets: s.close()
PY
)
python - "$run/bootstrap.secret" <<'PY'
import os, secrets, sys
with open(sys.argv[1], 'w', encoding='ascii') as f: f.write(secrets.token_hex(32)+'\n')
os.chmod(sys.argv[1], 0o600)
PY
"$binary" key generate >"$run/root.key" 2>"$run/key.log"
verifier=$("$binary" standing hash <"$run/bootstrap.secret" 2>"$run/hash.log" | tr -d '\r\n')
# This scenario must reach revocation rather than the worked-example token budget.
sed -e 's/tokenize_limit_per_session = 20$/tokenize_limit_per_session = 100000/' \
    -e 's/tokenize_limit_per_principal = 200$/tokenize_limit_per_principal = 1000000/' \
    "$here/policy_gsql.toml" >"$run/policy.toml"
cat >"$run/nunya.toml" <<EOF
admin_listen = '127.0.0.1:$admin_port'
policy = 'policy.toml'
state_dir = 'state'
root_key_file = 'root.key'
allow_plaintext = true
[management]
listen = '127.0.0.1:$mgmt_port'
[[management.admin]]
name = 'bootstrap'
verifier = '$verifier'
[spanner_native]
listen = '127.0.0.1:$native_port'
database = 'projects/test-project/instances/test-instance/databases/gsql'
emulator_endpoint = 'http://${NUNYA_D2_EMULATOR_GRPC#http://}'
[session]
max_lifetime = '1h'
idle_timeout = 'off'
EOF
unset verifier
"$binary" policy check --config "$run/nunya.toml" >"$run/policy-check.log" 2>&1
start_server() {
  "$binary" serve --config "$run/nunya.toml" >"$run/serve-$1.log" 2>&1 &
  server_pid=$!
  printf '%s\n' "$server_pid" >"$run/server.pid"
  wait_port "127.0.0.1:$mgmt_port"
  wait_port "127.0.0.1:$native_port"
  kill -0 "$server_pid"
}
# The hook is a separate process: retain the current pid on disk for cleanup.
export NUNYA_D2_RESTART="$run/restart.sh"
{
  printf '#!/usr/bin/env bash\nset -euo pipefail\n'
  declare -f wait_port start_server
  printf 'binary=%q\nrun=%q\nmgmt_port=%q\nnative_port=%q\n' "$binary" "$run" "$mgmt_port" "$native_port"
  printf 'kill "$(cat "$run/server.pid")"\n'
  # A stopped listener can briefly remain open; wait for the old process to exit.
  printf 'for ((i=0; i<100; i++)); do kill -0 "$(cat "$run/server.pid")" 2>/dev/null || break; sleep .1; done\n'
  printf 'start_server commit-restart\n'
} >"$NUNYA_D2_RESTART"
chmod 0700 "$NUNYA_D2_RESTART"
start_server before
server="http://127.0.0.1:$mgmt_port"
bootstrap=(--server "$server" --principal bootstrap --principal-secret-file "$run/bootstrap.secret")
"$binary" principal create "${bootstrap[@]}" --kind agent --generate-secret d2-agent >"$run/principal-issued" 2>"$run/principal.log"
python - "$run/principal-issued" "$run/agent.secret" <<'PY'
import os, sys
lines = open(sys.argv[1], encoding='utf-8').read().splitlines()
if len(lines) != 2 or len(lines[1]) != 64 or any(c not in '0123456789abcdef' for c in lines[1]):
    sys.exit('unexpected principal output format')
with open(sys.argv[2], 'w', encoding='ascii') as f: f.write(lines[1]+'\n')
os.chmod(sys.argv[2], 0o600)
os.remove(sys.argv[1])
PY
"$binary" principal grant "${bootstrap[@]}" d2-agent --route spanner-native=read-write >"$run/grant.log" 2>&1
agent=(--server "$server" --principal d2-agent --principal-secret-file "$run/agent.secret")
export NUNYA_D2_NATIVE="127.0.0.1:$native_port"
export NUNYA_D2_DATABASE=projects/test-project/instances/test-instance/databases/gsql
export SPANNER_DISABLE_BUILTIN_METRICS=true
stage=build
(cd "$here/client" && go build -o "$run/client.exe" .) >"$run/go-build.log" 2>&1
if [[ " ${scenarios[*]} " == *" stream "* ]]; then
  stage=upstream
  "$run/client.exe" upstream
fi
expanded=()
for scenario in "${scenarios[@]}"; do
  if [[ $scenario == commit ]]; then
    expanded+=(commit-control commit-stream-dml commit-revoked commit-expired commit-restart)
  else
    expanded+=("$scenario")
  fi
done
echo 'INFO ny012 covered-by-unit-tests-only (the emulator never returns an OK Commit without timestamp)'
commit_started=$SECONDS
for scenario in "${expanded[@]}"; do
  if [[ $scenario == commit-control ]]; then commit_started=$SECONDS; fi
  stage=acquire
  ttl=()
  if [[ $scenario == commit-expired ]]; then ttl=(--ttl 20s); fi
  "$binary" session acquire "${agent[@]}" --context "$scenario" --route spanner-native --mode read-write "${ttl[@]}" >"$run/acquire-$scenario.log" 2>&1
  if [[ $scenario == commit-expired ]]; then
    "$binary" session show "${agent[@]}" --context "$scenario" >"$run/expiry.log" 2>&1
    export NUNYA_D2_EXPIRES_MS=$(sed -n 's/^expires_ms: //p' "$run/expiry.log" | tr -d '\r')
    [[ $NUNYA_D2_EXPIRES_MS =~ ^[0-9]+$ ]]
  fi
  # The trusted CLI emits quoted POSIX exports. Keep exports and bearer private.
  eval "$("$binary" session env "${agent[@]}" --context "$scenario" --format sh 2>"$run/env-$scenario.log" | tr -d '\r')"
  [[ -z ${PGUSER:-} && $NUNYA_SPANNER_BEARER == nunya.session:*.* ]]
  export NUNYA_D2_BEARER="$NUNYA_SPANNER_BEARER"
  scenario_session=${NUNYA_SPANNER_BEARER#nunya.session:}; scenario_session=${scenario_session%%.*}
  unset PGUSER PGPASSWORD PGHOST PGPORT PGDATABASE NUNYA_SPANNER_BEARER
  export NUNYA_D2_REVOKE="$run/revoke-$scenario.sh"
  {
    printf '#!/usr/bin/env bash\nset -euo pipefail\n'
    printf '%q ' "$binary" session revoke "${agent[@]}" --context "$scenario"
    printf '> %q 2>&1\n' "$run/revoke-$scenario.log"
  } >"$NUNYA_D2_REVOKE"
  chmod 0700 "$NUNYA_D2_REVOKE"
  case "$scenario" in
    probe)
      stage=probe
      "$run/client.exe" probe
      ;;
    commit-*)
      stage=transaction
      "$run/client.exe" commit "${scenario#commit-}"
      [[ $((SECONDS - commit_started)) -lt 180 ]]
      ;;
    resume)
      stage=before
      "$run/client.exe" resume before --reference "$run/token-reference"
      # Resume's persistence check intentionally restarts the shared gateway.
      stage=restart
      kill "$server_pid"
      wait "$server_pid" 2>/dev/null || true
      server_pid=
      start_server after
      stage=after
      "$run/client.exe" resume after --reference "$run/token-reference"
      ;;
    stream)
      stage=query
      "$run/client.exe" stream
      stage=gateway-stopped
      python - "$run" "$scenario_session" <<'PY'
import json, pathlib, sys
run, session = pathlib.Path(sys.argv[1]), sys.argv[2]
ended, statement = None, None
for path in run.glob('serve-*.log'):
    with path.open(encoding='utf-8') as source:
        for line in source:
            try:
                record = json.loads(line)
            except (ValueError, TypeError):
                continue
            if record.get('session') != session:
                continue
            if record.get('kind') == 'lifecycle' and record.get('event') == 'session_ended':
                ended = record.get('ts')
            if record.get('kind') == 'statement' and isinstance(record.get('rows'), int):
                if statement is None or record['rows'] > statement['rows']:
                    statement = record
rows = statement['rows'] if statement is not None else 'missing'
print(f'INFO stream gateway-rows={rows}')
ok = (ended is not None and statement is not None
      and statement.get('ts', -1) >= ended and statement['rows'] < 20000)
print(f'{"PASS" if ok else "FAIL"} stream gateway-stopped')
sys.exit(0 if ok else 1)
PY
      ;;
  esac
done
