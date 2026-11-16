#!/usr/bin/env bash
set -euo pipefail

usage() {
    echo 'Usage: dev.sh [--task|-Task TASK] [--crate|-Crate NAME] [--release|-Release] [--work|-Work PATH] [--port|-Port PORT]' >&2
    echo 'Tasks: setup fresh-setup dev-server dev-server-v2 server ping server-query sql sql-query native-query capabilities backup export topology outbox-example failover distributed range-history check build test clippy fmt docs' >&2
    exit 2
}
die() { echo "$*" >&2; exit 1; }
task=setup
crate=all
release=false
work=
port=5432
while [ "$#" -gt 0 ]; do
    case "$1" in
        --task|-Task) [ "$#" -ge 2 ] || usage; task=$2; shift 2 ;;
        --crate|-Crate) [ "$#" -ge 2 ] || usage; crate=$2; shift 2 ;;
        --work|-Work) [ "$#" -ge 2 ] || usage; work=$2; shift 2 ;;
        --port|-Port) [ "$#" -ge 2 ] || usage; port=$2; shift 2 ;;
        --release|-Release) release=true; shift ;;
        *) usage ;;
    esac
done
case "$task" in
    setup|fresh-setup|dev-server|dev-server-v2|server|ping|server-query|sql|sql-query|native-query|capabilities|backup|export|topology|outbox-example|failover|distributed|range-history|check|build|test|clippy|fmt|docs) ;;
    *) usage ;;
esac
case "$port" in ''|*[!0-9]*) usage ;; esac
windows=false
suffix=
case "$(uname -s)" in MINGW*|MSYS*|CYGWIN*) windows=true; suffix=.exe ;; esac
root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)
cd "$root"
python=
for candidate in python3 python; do
    if command -v "$candidate" >/dev/null 2>&1 && "$candidate" -c 'import sys; sys.exit(0 if sys.version_info.major == 3 else 1)' >/dev/null 2>&1; then
        python=$candidate
        break
    fi
done
[ -n "$python" ] || die 'Python 3 is required (python3 or python).'
# Keep JSON handling and path resolution in Python; no values are interpolated
# into Python source. Native Windows paths also work with Git Bash commands.
py() {
    "$python" - "$@" <<'PY'
import io, json, os, sys, uuid
# Native Windows Python otherwise emits CRLF into shell command substitutions.
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', newline='\n')
op, *args = sys.argv[1:]
def shell_path(path):
    return path.replace('\\', '/') if os.name == 'nt' else path
def read(path):
    with open(path, encoding='utf-8-sig') as f:
        return json.load(f)
def write(path, value):
    with open(path, 'w', encoding='utf-8', newline='\n') as f:
        json.dump(value, f, indent=2, ensure_ascii=False)
        f.write('\n')
def absolute(path, base=None):
    return os.path.realpath(os.path.join(base or os.getcwd(), path))
try:
    if op == 'work':
        base, work, windows = args
        base, work = absolute(base), absolute(work)
        compare_base, compare_work = base, work
        if windows == 'true':
            compare_base, compare_work = base.casefold(), work.casefold()
        try:
            contained = os.path.commonpath([compare_base, compare_work]) == compare_base
        except ValueError:
            contained = False
        if compare_work == compare_base or not contained:
            raise ValueError('Work must be a task subdirectory of ' + base + '.')
        print(shell_path(work))
    elif op == 'new':
        path = os.path.join(args[0], args[1] + uuid.uuid4().hex)
        os.makedirs(path)
        print(shell_path(path))
    elif op == 'state':
        if not os.path.isfile(args[0]):
            raise ValueError('Run the Setup local demo configuration first.')
        state = read(args[0])
        if len(args) == 1:
            for key in ('config', 'database', 'fixture'):
                if not os.path.exists(state[key]):
                    raise ValueError('Setup state references a missing path: ' + state[key] + '. Use Fresh local demo to retain old state and create another.')
            print('Reusing local demo: ' + state['directory'])
        else:
            print(shell_path(state[args[1]]))
    elif op == 'artifact':
        executables = []
        with open(args[0], encoding='utf-8') as f:
            for line in f:
                message = json.loads(line)
                if message.get('reason') == 'compiler-artifact' and message.get('target', {}).get('name') == 'network' and message.get('executable'):
                    executables.append(message['executable'])
        if len(executables) != 1:
            raise ValueError('Expected exactly one network test executable.')
        print(shell_path(executables[0]))
    elif op == 'fixture':
        with open(args[0], encoding='utf-8') as f:
            lines = [line.rstrip('\r\n')[len('retained_fixture='):] for line in f if line.startswith('retained_fixture=')]
        if len(lines) != 1:
            raise ValueError('Expected exactly one retained server fixture.')
        print(shell_path(absolute(lines[0], args[1])))
    elif op == 'configure':
        fixture, fixture_root, directory = args
        config = read(os.path.join(fixture, 'server.TEST-ONLY.json'))
        for field in ('database', 'certificate', 'private_key', 'client_ca'):
            config[field] = absolute(config[field], fixture_root)
        write(os.path.join(directory, 'server.json'), config)
        with open(os.path.join(directory, 'init.sql'), 'w', encoding='utf-8', newline='\n') as f:
            f.write("CREATE TABLE notes (id BIGINT PRIMARY KEY, body TEXT);\nINSERT INTO notes VALUES (1, 'hello GigglesDB');")
    elif op == 'save':
        state_path, directory, fixture = args
        write(os.path.join(directory, 'ping.json'), {'kind': 'Ping'})
        write(os.path.join(directory, 'query.json'), {'kind': 'Sql', 'sql': 'SELECT id,body FROM app WHERE id=1', 'params': []})
        write(state_path, {'directory': directory, 'fixture': fixture, 'config': os.path.join(directory, 'server.json'), 'database': os.path.join(directory, 'embedded.db')})
    elif op == 'crates':
        for name in sorted(os.listdir(args[0])):
            if os.path.isfile(os.path.join(args[0], name, 'Cargo.toml')):
                print(name)
    else:
        raise ValueError('Unknown helper operation: ' + op)
except (OSError, ValueError, KeyError) as error:
    print(str(error), file=sys.stderr)
    sys.exit(1)
PY
}
if [ "${GIGGLES_WORK_ROOT+x}" = x ]; then
    work_root=$GIGGLES_WORK_ROOT
elif "$windows"; then
    work_root=J:/gigglesdb-work
else
    work_root=$HOME/gigglesdb-work
fi
[ -n "$work" ] || work=$work_root/ide
work=$(py work "$work_root" "$work" "$windows")
export CARGO_TARGET_DIR="$work/target" TEMP="$work/tmp" TMP="$work/tmp" TMPDIR="$work/tmp"
# RocksDB builds inherit LIBCLANG_PATH; point it at your LLVM libclang directory.
export CARGO_BUILD_JOBS="${CARGO_BUILD_JOBS-1}"
mkdir -p "$work" "$CARGO_TARGET_DIR" "$TEMP"
profile=debug
if "$release"; then profile=release; fi
state_path=$work/current.json
invoke_cargo() {
    local name=$1
    shift
    [ -f "$root/crates/$name/Cargo.toml" ] || die "Unknown crate: $name"
    local status=0
    (cd "$root/crates/$name" && cargo "$@") || status=$?
    [ "$status" -eq 0 ] || die "cargo $* failed in $name (exit $status)"
}
invoke_binary() {
    local name=$1 binary=$2
    shift 2
    local build=(build --locked --bin "$binary")
    if "$release"; then build+=(--release); fi
    invoke_cargo "$name" "${build[@]}"
    local status=0
    "$CARGO_TARGET_DIR/$profile/$binary$suffix" "$@" || status=$?
    [ "$status" -eq 0 ] || die "$binary failed (exit $status)"
}
new_output() { py new "$work" output-; }
state() { py state "$state_path" "$1"; }

case "$task" in
    setup|fresh-setup)
        if [ "$task" = setup ] && [ -f "$state_path" ]; then py state "$state_path"; exit 0; fi
        build=(test --locked --no-run --message-format=json --test network)
        if "$release"; then build+=(--release); fi
        messages=$(py new "$TEMP" cargo-)
        invoke_cargo giggles-server "${build[@]}" > "$messages/messages.jsonl"
        executable=$(py artifact "$messages/messages.jsonl")
        fixture_root=$(py new "$work/fixtures" '')
        status=0
        (cd "$fixture_root" && "$executable" --exact executable_server_starts_and_native_client_connects --nocapture) > "$messages/fixture.log" || status=$?
        cat "$messages/fixture.log"
        [ "$status" -eq 0 ] || die "Network fixture test failed (exit $status)"
        fixture=$(py fixture "$messages/fixture.log" "$fixture_root")
        directory=$(new_output)
        py configure "$fixture" "$fixture_root" "$directory"
        invoke_binary giggles-cli giggles-sql --create "$directory/embedded.db" --file "$directory/init.sql"
        py save "$state_path" "$directory" "$fixture"
        echo "Ready. Start Server, then Client Ping or Client SQL query. State: $state_path"
        ;;
    check|build|test|clippy|fmt|docs)
        names=("$crate")
        if [ "$crate" = all ]; then
            names=()
            while IFS= read -r name; do names+=("$name"); done < <(py crates "$root/crates")
        fi
        for name in "${names[@]}"; do
            echo "=== $task $name ($profile) ==="
            case "$task" in
                fmt) arguments=(fmt -- --check) ;;
                clippy) arguments=(clippy --locked --all-targets) ;;
                docs) arguments=(doc --locked --no-deps) ;;
                test) arguments=(test --locked) ;;
                *) arguments=("$task" --locked --all-targets) ;;
            esac
            if "$release" && [ "$task" != fmt ]; then arguments+=(--release); fi
            if [ "$task" = clippy ]; then arguments+=(-- -D warnings); fi
            if [ "$task" = test ]; then arguments+=(-- --test-threads=1); fi
            invoke_cargo "$name" "${arguments[@]}"
        done
        ;;
    dev-server) invoke_binary giggles-server giggles-server --dev --data "$work/dev-data" --port "$port" ;;
    dev-server-v2)
        # v2 (RocksDB) engine; needs libclang (Xcode Command Line Tools or brew llvm + LIBCLANG_PATH).
        # Separate data folder: an existing folder keeps the engine that created it.
        build=(build --locked --features engine-rocksdb --bin giggles-server)
        if "$release"; then build+=(--release); fi
        invoke_cargo giggles-server "${build[@]}"
        status=0
        "$CARGO_TARGET_DIR/$profile/giggles-server$suffix" --dev --engine v2 --data "$work/dev-data-v2" --port "$port" || status=$?
        [ "$status" -eq 0 ] || die "giggles-server failed (exit $status)" ;;
    capabilities) invoke_binary giggles-language giggles-language capability-manifest ;;
    topology) invoke_binary giggles-topology giggles-topology analyze "$root/crates/giggles-topology/tests/fixtures/three-providers.json" baseline ;;
    outbox-example) destination=$(new_output); invoke_cargo giggles-outbox run --locked --example orders -- "$destination/orders.db" ;;
    failover) invoke_cargo giggles-server test --locked --test all replication_process::three_process_failover -- --exact --nocapture ;;
    distributed) invoke_cargo giggles-distributed test --locked --test process -- --test-threads=1 --nocapture ;;
    range-history) invoke_cargo giggles-range-history test --locked --test process -- --test-threads=1 --nocapture ;;
    *)
        # Validate state before dispatch, as in dev.ps1.
        directory=$(state directory)
        case "$task" in
            server) config=$(state config); invoke_binary giggles-server giggles-server "$config" ;;
            ping|server-query)
                fixture=$(state fixture)
                request=ping.json
                if [ "$task" = server-query ]; then request=query.json; fi
                invoke_binary giggles-server giggles-client 127.0.0.1:7432 localhost "$fixture/app.pem" "$fixture/app.key.TEST-ONLY" "$fixture/ca.pem" "$directory/$request"
                ;;
            sql) database=$(state database); invoke_binary giggles-cli giggles-sql --open "$database" --repl ;;
            sql-query) database=$(state database); invoke_binary giggles-cli giggles-sql --open "$database" --sql 'SELECT id,body FROM notes WHERE id=1' --format json ;;
            native-query) database=$(state database); invoke_binary giggles-language giggles-language run "$database" 'notes where id=1 select id,body' ;;
            backup)
                database=$(state database); destination=$(new_output); mkdir "$destination/catalog"
                invoke_binary giggles-ops giggles-ops backup --root "$destination/catalog" --source "$database" --id demo
                invoke_binary giggles-ops giggles-ops verify --root "$destination/catalog" --id demo
                invoke_binary giggles-ops giggles-ops restore --root "$destination/catalog" --id demo --target "$destination/restored.db"
                ;;
            export)
                database=$(state database); destination=$(new_output)
                invoke_binary giggles-export giggles-export export "$database" "$destination/catalog.jsonl"
                invoke_binary giggles-export giggles-export verify "$destination/catalog.jsonl"
                invoke_binary giggles-export giggles-export import "$destination/catalog.jsonl" "$destination/imported.db"
                ;;
        esac
        ;;
esac
