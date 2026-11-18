#!/usr/bin/env bash
# Box-side full agent-load benchmark (GD-0061 R3-404). Never run locally.
set -uo pipefail
source "$HOME/bench.env"
repo=/home/user/gigglesdb
out=/home/user/bench
rm -rf "$out"; mkdir -p "$out/logs"
progress() { echo "$(date -u +%T) $*" >> "$out/progress"; }
fail_setup() { progress "SETUP FAILED: $*"; echo "AGENT-LOAD SETUP FAILED: $*" > "$out/summary.md"; echo "rc=2" > "$out/done"; exit 2; }
n=$(nproc)
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

progress "cargo build --release giggles-server"
command -v sccache >/dev/null 2>&1 && export RUSTC_WRAPPER=$(command -v sccache)
s=$(date +%s)
( cd "$repo/crates/giggles-server" && cargo build --release --bin giggles-server ) > "$out/logs/build.log" 2>&1 \
  || fail_setup "release build (see logs/build.log)"
build_s=$(( $(date +%s) - s ))
gs=$CARGO_TARGET_DIR/release/giggles-server
progress "build done in ${build_s}s"
progress "full agent-load run"
python3 -I "$repo/compat/agent_load.py" --server-bin "$gs" --work "$data/agent-load" --report "$out/agent-load.json" > "$out/logs/agent-load.out" 2>&1
rc=$?
case "$rc" in 0|1|2|3) ;; *) rc=1 ;; esac
mkdir -p "$out/logs/agent-load"
if [ -d "$data/agent-load" ]; then
  ( cd "$data/agent-load" && tar --exclude=data -cf - . ) | ( cd "$out/logs/agent-load" && tar -xf - ) || rc=1
fi
python3 - "$out" "$rc" "$repo" "$n" "$data" "$data_fstype" "$data_source" "$data_mount" "$data_note" "$build_s" <<'PY'
import json, pathlib, subprocess, sys
out = pathlib.Path(sys.argv[1])
rc = int(sys.argv[2])
lines = ['# Agent-load suite', '', f'Exit code: {rc}']
path = out / 'agent-load.json'
if path.exists():
    report = json.loads(path.read_text())
    check = report['check']
    for key in ('passed', 'max_ratio', 'application_errors'):
        lines.append(f'{key}: {check[key]}')
    lines.append('')
    for result in check['results']:
        lines.append(f"- {result['phase']} {result['operation']}: measured_ms={result['measured_ms']} baseline_ms={result['baseline_ms']} limit_ms={result['limit_ms']} ratio={result['ratio']} passed={result['passed']}")
else:
    lines.append('Harness failure: no report; see logs/agent-load.out.')
(out / 'summary.md').write_text('\n'.join(lines) + '\n')
meta = dict(tip=subprocess.check_output(['git', '-C', sys.argv[3], 'rev-parse', 'HEAD'], text=True).strip(),
            nproc=int(sys.argv[4]), data_dir=sys.argv[5], data_fstype=sys.argv[6],
            data_source=sys.argv[7], data_mount=sys.argv[8], data_note=sys.argv[9],
            build_seconds=int(sys.argv[10]), mode='agent-load')
(out / 'meta.json').write_text(json.dumps(meta, indent=2) + '\n')
PY
if [ "$?" != 0 ]; then
  rc=1
  echo 'Agent-load harness/report failure; see logs/agent-load.out and bench.out.' > "$out/summary.md"
fi
progress "agent-load finished rc=$rc"
echo "rc=$rc" > "$out/done"
exit "$rc"
