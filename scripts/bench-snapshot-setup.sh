#!/usr/bin/env bash
# Box-side setup for the gigglesdb-bench-base snapshot (GD-0082 W2). Run on a
# boat.dev box started from gigglesdb-gate-base, with /home/user/gigglesdb
# already checked out at the commit to warm. Never run locally. Redo it with:
#   boat new --from gigglesdb-gate-base --type large --no-env --ttl 7200
#   (upload this script, check out main), bash bench-snapshot-setup.sh
#   boat snapshot <id> gigglesdb-bench-base; boat delete <id>
# Installs PostgreSQL 18 (PGDG apt; no default cluster, service disabled),
# Node 22 (official tarball, SHA-256 verified), npm ci for scripts/app-loadtest,
# and builds giggles-server --release so the release target dir is warm.
# Writes /home/user/bench.env and /home/user/bench-base.txt (versions).
set -euxo pipefail
# Probe-only deliberately needs neither gate.env nor package/build setup.
perf_diagnosis() {
  {
    echo "diagnosis UTC=$(date -u +%FT%TZ) kernel=$(uname -r)"
    echo "virt=$(systemd-detect-virt 2>/dev/null || true)"
    echo "container=$(systemd-detect-virt --container 2>/dev/null || true)"
    echo 'pid1 cgroup:'; cat /proc/1/cgroup
    for knob in kernel.perf_event_paranoid kernel.kptr_restrict kernel.unprivileged_bpf_disabled; do
      sysctl "$knob" || true
    done
    echo "lockdown=$(cat /sys/kernel/security/lockdown 2>/dev/null || echo unavailable)"
    echo "BTF=$(test -r /sys/kernel/btf/vmlinux && echo present || echo missing)"
    if [ -r "/boot/config-$(uname -r)" ]; then
      grep -E 'CONFIG_(BPF_SYSCALL|KPROBES|KPROBE_EVENTS)=' "/boot/config-$(uname -r)" || true
    elif [ -r /proc/config.gz ]; then
      zcat /proc/config.gz | grep -E 'CONFIG_(BPF_SYSCALL|KPROBES|KPROBE_EVENTS)=' || true
    else echo 'kernel config unavailable (BPF_SYSCALL/kprobes unknown)'; fi
  } | tee "$HOME/bench-perf-diagnosis.txt"
}
# Logs are deliberately written by the calling user, not root.
# shellcheck disable=SC2024
probe() {
  local name=$1; shift
  if ! sudo -n timeout --kill-after=10 45 "$@" > "$probe_dir/$name.log" 2>&1; then
    echo "PROBE FAILED: $name: command failed or exceeded 45 seconds" >&2
    cat "$probe_dir/$name.log" >&2
    # Includes active lockdown, container/cgroup and missing-BTF clues.
    cat "$HOME/bench-perf-diagnosis.txt" >&2
    probe_failures+=("$name")
    return 0
  fi
  echo "PROBE OK: $name"
}
# shellcheck disable=SC2024
perf_probes() {
  local probe_dir libc
  probe_failures=()
  perf_diagnosis
  probe_dir=$(mktemp -d)
  # Retain small probe logs for snapshot debugging.
  echo "probe logs: $probe_dir"
  probe btf test -r /sys/kernel/btf/vmlinux
  probe kprobe bpftrace -e 'kprobe:do_nanosleep { exit(); }' -c 'sleep 0.2'
  libc=$(ldconfig -p | awk '/libc.so.6 .*x86-64/ && !found {print $NF; found=1}')
  [ -n "$libc" ] || libc=/usr/lib/x86_64-linux-gnu/libc.so.6
  # Python os.getpid invokes libc getpid, unlike shell builtins.
  probe uprobe bpftrace -e "uprobe:$libc:getpid { exit(); }" -c '/usr/bin/python3 -c "import os; os.getpid()"'
  probe futex bpftrace -e 'tracepoint:syscalls:sys_enter_futex { @calls = count(); } interval:s:1 { exit(); }'
  probe offcputime offcputime-bpfcc -f -d 1
  probe wakeuptime wakeuptime-bpfcc -f 1
  probe biolatency biolatency-bpfcc -m 1 1
  probe syscount syscount-bpfcc -L -d 1
  probe perf-trace perf trace -- true
  probe perf-sched perf record -e sched:sched_switch -a -o "$probe_dir/sched.data" -- sleep 0.2
  probe pidstat pidstat -t -u -w -p $$ 1 1
  probe iostat iostat -x 1 1
  printf 'probe;leaf 1\n' > "$probe_dir/input.folded"
  # shellcheck disable=SC2016  # $1/$2 expand in the inner sh, by design.
  probe inferno sh -c 'inferno-flamegraph < "$1" > "$2"' sh "$probe_dir/input.folded" "$probe_dir/flame.svg"
  if sudo -n timeout --kill-after=10 15 perf lock contention -a -b -- sleep 0.2 > "$probe_dir/lock.log" 2>&1; then
    echo 'perf_lock_bpf available' | tee "$HOME/bench-perf-lock.txt"
  else echo 'perf_lock_bpf unavailable (optional)' | tee "$HOME/bench-perf-lock.txt"; fi
  # Run every probe first so one snapshot attempt reports all failures.
  if [ "${#probe_failures[@]}" -gt 0 ]; then
    echo "PROBES FAILED (${#probe_failures[@]}): ${probe_failures[*]}; see diagnosis above" >&2
    exit 1
  fi
  echo PROBES_OK
}
if [ "${1:-}" = --probe-only ]; then
  perf_probes
  exit 0
fi
[ $# = 0 ] || { echo "usage: $0 [--probe-only]" >&2; exit 2; }

export DEBIAN_FRONTEND=noninteractive
source "$HOME/gate.env"
repo=/home/user/gigglesdb

# PostgreSQL 18 from the PGDG apt repository.
sudo apt-get update -q
sudo apt-get install -yq curl ca-certificates gnupg lsb-release xz-utils util-linux jq
sudo install -d /usr/share/postgresql-common/pgdg
sudo curl -fsSL -o /usr/share/postgresql-common/pgdg/apt.postgresql.org.asc \
  https://www.postgresql.org/media/keys/ACCC4CF8.asc
codename=$(lsb_release -cs)
echo "deb [signed-by=/usr/share/postgresql-common/pgdg/apt.postgresql.org.asc] https://apt.postgresql.org/pub/repos/apt $codename-pgdg main" \
  | sudo tee /etc/apt/sources.list.d/pgdg.list >/dev/null
sudo install -d /etc/postgresql-common
echo "create_main_cluster = false" | sudo tee /etc/postgresql-common/createcluster.conf >/dev/null
sudo apt-get update -q
sudo apt-get install -yq postgresql-18 postgresql-client-18
sudo systemctl disable --now postgresql >/dev/null 2>&1 || true
pgbin=/usr/lib/postgresql/18/bin
"$pgbin/postgres" --version

# Node 22, official linux-x64 tarball checked against SHASUMS256.txt.
nd=$(mktemp -d)
curl -fsSL -o "$nd/SHASUMS256.txt" https://nodejs.org/dist/latest-v22.x/SHASUMS256.txt
tarball=$(grep -oE 'node-v22\.[0-9]+\.[0-9]+-linux-x64\.tar\.xz' "$nd/SHASUMS256.txt" | head -1)
curl -fsSL -o "$nd/$tarball" "https://nodejs.org/dist/latest-v22.x/$tarball"
( cd "$nd" && grep " $tarball\$" SHASUMS256.txt | sha256sum -c - )
sudo rm -rf /usr/local/lib/nodejs && sudo mkdir -p /usr/local/lib/nodejs
sudo tar -xJf "$nd/$tarball" -C /usr/local/lib/nodejs --strip-components=1
for b in node npm npx; do sudo ln -sfn "/usr/local/lib/nodejs/bin/$b" "/usr/local/bin/$b"; done
rm -rf "$nd"
node --version; npm --version

# app-loadtest dependencies; the stamp lets the runner skip npm ci when the
# lock file is unchanged.
( cd "$repo/scripts/app-loadtest" && npm ci --no-audit --no-fund )
sha256sum "$repo/scripts/app-loadtest/package-lock.json" | cut -d' ' -f1 > /home/user/bench-npm.stamp

# Release build of giggles-server; the snapshot keeps target/release warm.
cat > "$HOME/bench.env" <<'ENV'
source "$HOME/gate.env"
export CARGO_BUILD_JOBS=$(nproc)
export PATH=/usr/lib/postgresql/18/bin:/usr/local/bin:$PATH
ENV
source "$HOME/bench.env"
if command -v sccache >/dev/null 2>&1; then export RUSTC_WRAPPER=$(command -v sccache); fi
s=$(date +%s)
( cd "$repo/crates/giggles-server" && cargo build --release --bin giggles-server )
build_s=$(( $(date +%s) - s ))
"$CARGO_TARGET_DIR/release/giggles-server" 2>&1 | head -1 || true

# Profilers are snapshot dependencies, never downloaded during a benchmark.
sudo apt-get install -yq bpfcc-tools bpftrace sysstat linux-tools-common
if ! sudo apt-get install -yq "linux-tools-$(uname -r)" "linux-headers-$(uname -r)"; then
  sudo apt-get install -yq linux-tools-generic linux-headers-generic
  # The /usr/bin/perf wrapper otherwise insists on the running kernel package.
  real_perf=$(find /usr/lib/linux-tools -type f -name perf | sort -V | tail -1)
  [ -n "$real_perf" ] || { echo 'PROBE FAILED: perf-install: no real perf binary' >&2; exit 1; }
  sudo ln -sfn "$real_perf" /usr/local/bin/perf
fi
# Disable collectors without disabling the interactive iostat tool.
sudo systemctl disable --now sysstat.service sysstat-collect.timer sysstat-summary.timer >/dev/null 2>&1 || true
if [ -f /etc/default/sysstat ]; then sudo sed -i 's/^ENABLED=.*/ENABLED="false"/' /etc/default/sysstat; fi
if [ -f /etc/cron.d/sysstat ]; then sudo mv /etc/cron.d/sysstat /etc/cron.d/sysstat.disabled; fi
cargo install inferno --locked --version 0.12.8 --root "$HOME/.cargo"
for tool in inferno-flamegraph inferno-collapse-perf; do
  sudo ln -sfn "$HOME/.cargo/bin/$tool" "/usr/local/bin/$tool"
done
printf 'kernel.perf_event_paranoid=-1\nkernel.kptr_restrict=0\n' | sudo tee /etc/sysctl.d/99-bench-perf.conf >/dev/null
sudo sysctl --system
perf_probes

{
  echo "commit $(git -C "$repo" rev-parse HEAD)"
  echo "os $(lsb_release -ds)"
  echo "kernel $(uname -r)"
  echo "cpu $(lscpu | sed -n 's/^Model name:[[:space:]]*//p') nproc=$(nproc)"
  echo "postgres $("$pgbin/postgres" --version)"
  echo "node $(node --version) npm $(npm --version)"
  echo "rustc $(rustc --version)"
  echo "sccache $(command -v sccache || echo none)"
  echo "release_build_seconds $build_s"
  echo "perf_binary $(readlink -f "$(command -v perf)")"
  echo "perf $(perf --version)"
  echo "inferno $(inferno-flamegraph --version) pinned=0.12.8"
  cat "$HOME/bench-perf-diagnosis.txt" "$HOME/bench-perf-lock.txt"
  echo "created $(date -u +%FT%TZ)"
} > /home/user/bench-base.txt
cat /home/user/bench-base.txt
df -h /home/user | tail -1
echo BENCH_SETUP_OK
