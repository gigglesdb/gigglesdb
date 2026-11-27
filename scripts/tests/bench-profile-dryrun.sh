#!/usr/bin/env bash
# Stubbed production helpers: no sudo, BPF, server, or remote machine required.
set -euo pipefail
here=$(cd "$(dirname "$0")/.." && pwd)
# RUNNER=<path> checks another runner copy (for example the unpatched one).
runner=${RUNNER:-$here/bench-remote-runner.sh}
case $(uname -s) in
  MINGW*|MSYS*) base=J:/gigglesdb-work/perf-b/tmp ;;
  *) base=/mnt/j/gigglesdb-work/perf-b/tmp ;;
esac
export TEMP=$base TMP=$base TMPDIR=$base
mkdir -p "$base"
work=$(mktemp -d "$base/profile-test.XXXXXX")
trap 'rm -rf "$work"' EXIT
mkdir -p "$work/bin" "$work/profiles"
cat > "$work/bin/stub" <<'STUB'
#!/usr/bin/env bash
set -eu
name=${0##*/}
case "$name" in
 sudo) [ "${1:-}" != -n ] || shift; exec "$@" ;;
 timeout) while [ "${1#-}" != "$1" ]; do shift; done; shift; exec "$@" ;;
 perf)
   case "$1" in
     record) while [ $# -gt 0 ]; do if [ "$1" = -o ]; then printf data > "$2"; break; fi; shift; done ;;
     script) printf '    worker 123/124 1.0: cycles:\n        ab leaf (server)\n        cd root (server)\n\n' ;;
     lock) echo 'not built-in: BUILD_BPF_SKEL=1'; exit 1 ;;
     *) echo 'tiny report' ;;
   esac ;;
 inferno-collapse-perf) cat >/dev/null; echo 'worker;root;leaf 1' ;;
 inferno-flamegraph) cat >/dev/null; echo '<svg>test</svg>' ;;
 offcputime-bpfcc|wakeuptime-bpfcc) echo 'worker;root;leaf 100' ;;
 *) echo 'tiny sample' ;;
esac
STUB
chmod +x "$work/bin/stub"
for tool in sudo timeout perf offcputime-bpfcc bpftrace wakeuptime-bpfcc biolatency-bpfcc syscount-bpfcc pidstat iostat inferno-collapse-perf inferno-flamegraph; do
  cp "$work/bin/stub" "$work/bin/$tool"
done
stub_bin=$work/bin
if command -v cygpath >/dev/null 2>&1; then stub_bin=$(cygpath -u "$stub_bin"); fi
export PATH="$stub_bin:$PATH"
# Extract only function declarations; none of the runner main body executes.
# Production functions are extracted dynamically, never execute runner setup.
# shellcheck disable=SC1090
source <(sed -n '/^cap() {/,/^}/p' "$runner")
# Production functions are extracted dynamically, never execute runner setup.
# shellcheck disable=SC1090
source <(sed -n '/^bench_profile() /,/^# Diagnostics follow/{ /^# Diagnostics follow/d; p; }' "$runner")
# Consumed by the dynamically loaded post-processing function.
# shellcheck disable=SC2034
repo=$(cd "$here/.." && pwd)
deadline=$(( $(date +%s) + 600 ))
bench_profile $$ "$work/profiles" fake "$deadline" fp "$work/progress"
data=$work/data
mkdir -p "$data/gg/nested"; echo 'rocks log' > "$data/gg/nested/LOG"
bench_profile_rocksdb "$work/profiles" fake
bench_profile_post "$work/profiles" fake "$deadline" "$work/progress"
for file in wait-report-fake.md manifest-fake.txt offcpu-fake.folded futex-fake.txt flame-cpu-fake.svg flame-offcpu-fake.svg flame-wakeup-fake.svg pidstat-fake.txt iostat-fake.txt biolatency-fake.txt fsync-fake.txt syscount-fake.txt threads0-fake.txt threads1-fake.txt threads2-fake.txt comm-fake.txt folded-fake.txt ran-fake ran-offcpu-fake rocksdb-fake/LOG-1; do
  test -f "$work/profiles/$file" || { echo "missing $file" >&2; exit 1; }
done
grep -q '## fake' "$work/profiles/wait-report-fake.md"
grep -q '^proc ' "$work/profiles/threads2-fake.txt"
# Per-thread lines need Linux /proc/<pid>/task (absent under Git Bash).
[ ! -d /proc/$$/task ] || grep -Eq '^[0-9]+ [0-9]+ [0-9]+ ' "$work/profiles/threads2-fake.txt"
grep -q 'capture=lock status=skipped' "$work/profiles/manifest-fake.txt"
grep -q 'capture=offcpu status=ok' "$work/profiles/manifest-fake.txt"
grep -q '^worker;' "$work/profiles/folded-fake.txt"
# Exercise awk fallback, including thread comm preservation.
rm "$work/bin/inferno-collapse-perf"
mkdir -p "$work/profiles/raw-fallback"; echo data > "$work/profiles/raw-fallback/perf.data"
bench_profile_post "$work/profiles" fallback "$deadline" "$work/progress"
grep -q '^worker;root;leaf 1$' "$work/profiles/folded-fallback.txt"
# Missing and failing collectors must not abort remaining captures.
rm "$work/bin/offcputime-bpfcc"
printf '#!/usr/bin/env bash\nexit 1\n' > "$work/bin/bpftrace"
bench_profile $$ "$work/profiles" partial "$deadline" dwarf "$work/progress"
grep -q 'capture=offcpu status=skipped' "$work/profiles/manifest-partial.txt"
grep -q 'capture=futex status=failed' "$work/profiles/manifest-partial.txt"
test -s "$work/profiles/wakeup-partial.folded"
# Profile build parity: base and tip are profiled with the same build type and
# call graph (both fp, or both measured release binaries with dwarf).
cat > "$work/bin/cargo" <<'CARGO'
#!/usr/bin/env bash
case "$PWD" in *"${CARGO_FAIL_IN:-/nowhere/}"*) exit 1 ;; esac
mkdir -p "$CARGO_TARGET_DIR/release" && : > "$CARGO_TARGET_DIR/release/giggles-server"
CARGO
chmod +x "$work/bin/cargo"
loop=$(sed -n '/^# Diagnostics follow/,/^PYMETA$/p' "$runner")
grep -q 'profile_binary "$t"' <<< "$loop" || { echo 'profile loop does not take its call graph from profile_binary' >&2; exit 1; }
! grep -q 'callgraph=dwarf' <<< "$loop" || { echo 'profile loop forces dwarf for a target' >&2; exit 1; }
# Production functions are extracted dynamically, never execute runner setup.
# shellcheck disable=SC1090
source <(sed -n '/^fp_build() {/,/^}/p; /^profile_builds() {/,/^}/p; /^profile_binary() {/,/^}/p' "$runner")
declare -F profile_builds profile_binary >/dev/null || { echo 'runner has no profile_builds/profile_binary (base/tip profile build parity)' >&2; exit 1; }
mkdir -p "$work/tip/crates/giggles-server" "$work/base/crates/giggles-server" "$work/out/logs"
# shellcheck disable=SC2030,SC2031,SC2034
parity() ( # parity <targets> <cargo-fail-dir|''> <tip callgraph> <base callgraph|''> <parity prefix>
  repo=$work/tip base_repo=$work/base out=$work/out CARGO_TARGET_DIR=$work/target
  gs=$CARGO_TARGET_DIR/release/giggles-server gs_base=$CARGO_TARGET_DIR-base/release/giggles-server
  PROFILE=1 TARGETS=$1 BASE_COMMIT=deadbeef
  progress() { echo "$*" >> "$work/progress"; }
  export CARGO_FAIL_IN=$2
  rm -rf "$work/target-profile" "$work/target-base-profile"
  profile_builds
  read -r tbin tcg < <(profile_binary giggles-sql)
  read -r bbin bcg < <(profile_binary giggles-sql-base)
  [ "$tcg" = "$3" ] && [ "${bcg:-}" = "$4" ] || { echo "callgraph tip=$tcg base=${bcg:-}, want $3/$4 ($1 fail=$2)" >&2; exit 1; }
  case "$3" in
    fp) [ "$tbin" = "$CARGO_TARGET_DIR-profile/release/giggles-server" ] ;;
    dwarf) [ "$tbin" = "$gs" ] ;;
  esac || { echo "tip profile binary $tbin for $3 ($1 fail=$2)" >&2; exit 1; }
  case "$4" in
    fp) [ "$bbin" = "$CARGO_TARGET_DIR-base-profile/release/giggles-server" ] ;;
    dwarf) [ "$bbin" = "$gs_base" ] ;;
  esac || { echo "base profile binary $bbin for $4 ($1 fail=$2)" >&2; exit 1; }
  # Measured runs keep the plain release binaries.
  [ "$gs" = "$CARGO_TARGET_DIR/release/giggles-server" ] && [ "$gs_base" = "$CARGO_TARGET_DIR-base/release/giggles-server" ] ||
    { echo "measured binaries changed: $gs $gs_base" >&2; exit 1; }
  [[ "$profile_build_parity" == "$5"* ]] || { echo "parity '$profile_build_parity', want '$5' ($1 fail=$2)" >&2; exit 1; }
)
parity giggles-sql,giggles-sql-base '' fp fp 'fp (base and tip)'
parity giggles-sql,giggles-sql-base /base/ dwarf dwarf 'release+dwarf: base fp build failed'
parity giggles-sql,giggles-sql-base /tip/ dwarf dwarf 'release+dwarf: tip fp build failed'
parity pg,giggles-sql '' fp '' 'fp (tip only)'
echo 'PASS profile dryrun (full, fallback, missing/failing collectors, base/tip build parity)'
