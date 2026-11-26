#!/usr/bin/env bash
# Stubbed production helpers: no sudo, BPF, server, or remote machine required.
set -euo pipefail
here=$(cd "$(dirname "$0")/.." && pwd)
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
source <(sed -n '/^cap() {/,/^}/p' "$here/bench-remote-runner.sh")
# Production functions are extracted dynamically, never execute runner setup.
# shellcheck disable=SC1090
source <(sed -n '/^bench_profile() /,/^# Diagnostics follow/{ /^# Diagnostics follow/d; p; }' "$here/bench-remote-runner.sh")
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
echo 'PASS profile dryrun (full, fallback, missing/failing collectors)'
