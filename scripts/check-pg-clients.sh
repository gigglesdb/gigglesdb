#!/usr/bin/env bash
set -euo pipefail

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$root"
java_home=''
jdbc_jar=''
while [ "$#" -gt 0 ]; do
    case "$1" in
        --java-home|-JavaHome)
            [ "$#" -ge 2 ] || { printf '%s\n' "Missing value for $1" >&2; exit 2; }
            java_home=$2; shift 2 ;;
        --jdbc-jar|-JdbcJar)
            [ "$#" -ge 2 ] || { printf '%s\n' "Missing value for $1" >&2; exit 2; }
            jdbc_jar=$2; shift 2 ;;
        *) printf '%s\n' "Unknown argument: $1" >&2; exit 2 ;;
    esac
done
python=''
for candidate in python3 python; do
    if command -v "$candidate" >/dev/null 2>&1 && "$candidate" -I -c 'import sys; sys.exit(sys.version_info.major != 3)' >/dev/null 2>&1; then
        python=$candidate
        break
    fi
done
[ -n "$python" ] || { printf '%s\n' 'Python 3 is required.' >&2; exit 1; }
platform=$(uname -s)
suffix=''
case "$platform" in MINGW*|MSYS*|CYGWIN*) suffix='.exe' ;; esac
# Native Windows Python needs native paths even when launched from Git Bash.
native_path() {
    if [ -n "$suffix" ] && command -v cygpath >/dev/null 2>&1 && [ -n "$1" ]; then
        cygpath -m -- "$1"
    else
        printf '%s\n' "$1"
    fi
}
py() { MSYS2_ARG_CONV_EXCL='*' "$python" -I "$@"; }
export CARGO_TARGET_DIR="$root/target-datagrip"
export CARGO_BUILD_JOBS=1
export CARGO_INCREMENTAL=0
output="$root/artifacts/pg-clients-$(py -c 'import sys, uuid; sys.stdout.buffer.write(uuid.uuid4().hex.encode("ascii") + b"\n")')"
mkdir -p "$output"
if ! (cd crates/giggles-server && cargo test --locked --test network executable_server_starts_and_native_client_connects -- --exact --nocapture) | tee "$output/fixture.log"; then
    printf '%s\n' 'Server fixture test failed.' >&2
    exit 1
fi
if [ -z "$java_home" ]; then java_home=${JAVA_HOME:-}; fi
if [ -z "$java_home" ] && [ "$platform" = Darwin ] && [ -x /usr/libexec/java_home ]; then
    java_home=$(/usr/libexec/java_home 2>/dev/null) || java_home=''
fi
py - "$(native_path "$root")" "$(native_path "$output")" "$(native_path "$java_home")" "$(native_path "$jdbc_jar")" "$platform" "$suffix" "$(native_path "${HOME:-}")" "$(native_path "${APPDATA:-}")" "$(native_path "${ProgramFiles:-}")" <<'PY'
import json
import pathlib
import sys

root, output, java_home, jdbc_jar, platform, suffix, home, appdata, programfiles = sys.argv[1:]
root, output = pathlib.Path(root), pathlib.Path(output)
lines = (output / 'fixture.log').read_text(encoding='utf-8').splitlines()
fixtures = [line[len('retained_fixture='):] for line in lines if line.startswith('retained_fixture=')]
if len(fixtures) != 1:
    raise SystemExit('Expected one fresh retained fixture.')
fixture = (root / 'crates/giggles-server' / fixtures[0]).resolve()
if not java_home and suffix and programfiles:
    jdk_root = pathlib.Path(programfiles) / 'Eclipse Adoptium'
    if jdk_root.is_dir():
        jdks = sorted((p for p in jdk_root.iterdir() if p.is_dir()), key=lambda p: p.name, reverse=True)
        java_home = next((str(p) for p in jdks if (p / 'bin/javac.exe').is_file()), '')
if not jdbc_jar:
    if suffix:
        drivers = [pathlib.Path(appdata) / 'JetBrains'] if appdata else []
    elif platform == 'Darwin':
        drivers = [pathlib.Path(home) / 'Library/Application Support/JetBrains'] if home else []
    else:
        drivers = [pathlib.Path(home) / p for p in ('.local/share/JetBrains', '.cache/JetBrains')] if home else []
    jars = sorted(str(p) for directory in drivers if directory.is_dir() for p in directory.rglob('postgresql-42.7.3.jar') if p.is_file())
    jdbc_jar = jars[0] if jars else ''
manifest = {
    'server': str(root / ('target-datagrip/debug/giggles-server' + suffix)),
    'fixture': str(fixture),
    'server_config': str(fixture / 'server.TEST-ONLY.json'),
    'config_base': str(root / 'crates/giggles-server'),
    'output': str(output / 'matrix'),
    'timeout_seconds': 90,
}
if java_home and jdbc_jar:
    manifest.update(java=str((pathlib.Path(java_home) / ('bin/java' + suffix)).resolve()),
                    javac=str((pathlib.Path(java_home) / ('bin/javac' + suffix)).resolve()),
                    jdbc_jar=str(pathlib.Path(jdbc_jar).resolve()))
else:
    print('JDK or pgJDBC 42.7.3 not found; JDBC cases will be UNTESTED. Supply -JavaHome and -JdbcJar.')
(output / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n', encoding='utf-8')
PY
result=0
cargo run --locked --manifest-path crates/giggles-pg-conformance/Cargo.toml --target-dir target-gd48-conformance -j1 -- "$output/manifest.json" || result=$?
printf 'Client report: %s/matrix/report.json\n' "$output"
printf '%s\n' 'FAIL and UNTESTED are capability results, not support claims. Full DataGrip GUI introspection remains unqualified.'
exit "$result"
