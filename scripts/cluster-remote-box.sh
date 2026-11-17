#!/usr/bin/env bash
# Disposable cluster host operations; only the coordinator invokes real runs.
set -euo pipefail
usage() {
  echo 'Usage: cluster-remote-box.sh [--dry-run] prepare [client] | firewall <ip6>... | agent <k> | rtt <ip6>... | suite'
}
java21=/usr/lib/jvm/java-21-openjdk-amd64/bin/java
dry=0
args=()
for arg in "$@"; do
  if [ "$arg" = --dry-run ]; then dry=1; else args+=("$arg"); fi
done
set -- "${args[@]}"
phase=${1:---help}; [ $# -eq 0 ] || shift
bad() { usage >&2; exit 2; }
run() { if [ "$dry" = 1 ]; then printf '%q ' "$@"; printf '\n'; else "$@"; fi; }
ipv6() { python3 -I -c 'import ipaddress,sys; [ipaddress.IPv6Address(x) for x in sys.argv[1:]]' "$@" || bad; }
case "$phase" in
  --help|-h) usage; exit 0 ;;
  prepare) [ $# -eq 0 ] || { [ $# -eq 1 ] && [ "$1" = client ]; } || bad ;;
  firewall|rtt) [ $# -gt 0 ] || bad; ipv6 "$@" ;;
  agent) [ $# -eq 1 ] && [[ "$1" =~ ^[1-6]$ ]] || bad ;;
  suite) [ $# -eq 0 ] || bad ;;
  *) bad ;;
esac
case "$phase" in
  prepare)
    if [ "$dry" = 1 ]; then
      run bash -lc 'source "$HOME/bench.env"; cargo build --release --manifest-path /home/user/gigglesdb/crates/giggles-server/Cargo.toml'
      if [ "${1:-}" = client ]; then
        run sudo -n env DEBIAN_FRONTEND=noninteractive apt-get install -y -q openjdk-21-jdk-headless
        run "$java21" -version
      fi
      exit 0
    fi
    source "$HOME/bench.env"
    : "${CARGO_TARGET_DIR:?gate.env must set CARGO_TARGET_DIR}"
    cargo build --release --manifest-path /home/user/gigglesdb/crates/giggles-server/Cargo.toml
    python3 -m venv /home/user/cxvenv
    /home/user/cxvenv/bin/pip install --disable-pip-version-check 'psycopg[binary]==3.2.13'
    if [ "${1:-}" = client ]; then
      sudo -n env DEBIAN_FRONTEND=noninteractive apt-get install -y -q openjdk-21-jdk-headless
      java_version=$("$java21" -version 2>&1)
      printf '%s\n' "$java_version"
      [[ "$java_version" =~ ^openjdk\ version\ \"21[.\"] ]] || { echo 'JDK 21 required' >&2; exit 1; }
      cargo build --release --manifest-path /home/user/gigglesdb/crates/giggles-history/Cargo.toml
      jar=/home/user/postgresql-42.7.3.jar
      curl --fail --location --retry 3 --max-time 120 -o "$jar" https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar
      echo "a2644cbfba1baa145ff7e8c8ef582a6eed7a7ec4ca792f7f054122bdec756268  $jar" | sha256sum -c -
    fi
    ;;
  firewall)
    for ip in "$@"; do
      if [ "$dry" = 1 ]; then run sudo -n ip6tables -I INPUT 1 -s "$ip" -j ACCEPT
      else sudo -n ip6tables -C INPUT -s "$ip" -j ACCEPT 2>/dev/null || sudo -n ip6tables -I INPUT 1 -s "$ip" -j ACCEPT; fi
    done
    ;;
  agent)
    k=$1
    if [ "$dry" = 1 ]; then run nohup /home/user/cxvenv/bin/python /home/user/gigglesdb/scripts/cluster-exit/agent.py --cluster /home/user/cx/cluster.json --node "$k"; exit 0; fi
    pidfile=/home/user/cx/agent-$k.pid
    if [ -f "$pidfile" ] && kill -0 "$(cat "$pidfile")" 2>/dev/null; then exit 0; fi
    nohup /home/user/cxvenv/bin/python /home/user/gigglesdb/scripts/cluster-exit/agent.py --cluster /home/user/cx/cluster.json --node "$k" > "/home/user/cx/agent-$k.log" 2>&1 < /dev/null &
    echo $! > "$pidfile"
    ;;
  rtt)
    if [ "$dry" = 1 ]; then for ip in "$@"; do run ping -6 -n -c 10 -W 2 "$ip"; done; exit 0; fi
    source "$HOME/bench.env"
    python3 -I - "$CARGO_TARGET_DIR/release/giggles-server" "$@" <<'PY'
import hashlib, json, os, platform, re, subprocess, sys
from pathlib import Path
def command(*args):
    return subprocess.check_output(args, text=True, timeout=40).strip()
rtt = {}
for host in sys.argv[2:]:
    output = command('ping', '-6', '-n', '-c', '10', '-W', '2', host)
    match = re.search(r'= [\d.]+/([\d.]+)/', output)
    if not match:
        raise RuntimeError('ping did not report average RTT')
    rtt[host] = float(match[1])
cpu = next(line.split(':', 1)[1].strip() for line in Path('/proc/cpuinfo').read_text().splitlines()
           if line.startswith('model name'))
print(json.dumps(dict(cpu=cpu, nproc=int(command('nproc')), memory=command('free', '-b'),
    kernel=platform.release(), rustc=command('rustc', '--version'),
    binary_sha256=hashlib.file_digest(open(sys.argv[1], 'rb'), 'sha256').hexdigest(), rtt_ms=rtt)))
PY
    ;;
  suite)
    if [ "$dry" = 1 ]; then
      run env "JAVA=$java21" CX=/home/user/cx/cluster.json OUT=/home/user/out REPO=/home/user/gigglesdb PY=/home/user/cxvenv/bin/python 'CHECKER=$CARGO_TARGET_DIR/release/giggles-history' JDBC_JAR=/home/user/postgresql-42.7.3.jar "NODES=${NODES:-3}" "KEEP_GOING=${KEEP_GOING:-0}" bash /home/user/suite.sh
      exit 0
    fi
    source "$HOME/bench.env"
    export CX=/home/user/cx/cluster.json OUT=/home/user/out REPO=/home/user/gigglesdb
    export PY=/home/user/cxvenv/bin/python CHECKER=$CARGO_TARGET_DIR/release/giggles-history
    export JAVA=$java21
    export JDBC_JAR=/home/user/postgresql-42.7.3.jar NODES=${NODES:?} KEEP_GOING=${KEEP_GOING:-0}
    mkdir -p "$OUT"
    # DEBUG records commands with line numbers, but never expands passwords.
    export BASH_ENV=/home/user/suite-env.sh
    cat > "$BASH_ENV" <<'ENV'
set -E -o pipefail
trap 'printf "line %s: %s\n" "$LINENO" "$BASH_COMMAND" >&2' DEBUG
trap 'code=$?; echo "suite failure line $LINENO rc=$code" >&2; echo 1 > "$OUT/suite.failed"; if [ "$KEEP_GOING" != 1 ]; then exit "$code"; fi' ERR
ENV
    rm -f "$OUT/suite.failed"
    rc=0
    bash /home/user/suite.sh > "$OUT/suite.log" 2>&1 || rc=$?
    [ ! -f "$OUT/suite.failed" ] || rc=1
    echo "rc=$rc" > "$OUT/suite.done"
    exit "$rc"
    ;;
esac
