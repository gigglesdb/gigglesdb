"""Counted long-running checks against a real two-route Nunya gateway."""
import argparse
import json
import hashlib
import http.client
import queue
import threading
import logging
import os
import re
from pathlib import Path
import secrets
import shlex
import shutil
import socket
import subprocess
import sys
import time

import psycopg
from psycopg_pool import ConnectionPool

logging.disable(logging.CRITICAL)
HERE = Path(__file__).resolve().parent
HARNESS = HERE.parent
EXPECTED = {"setup", "idle-negative", "duration", "pg-renewal", "sp-renewal",
            "pg-reconnect-token", "sp-reconnect-token", "restart", "pg-limit",
            "sp-limit", "pg-stream-revoke", "pg-refused", "sp-stream-revoke", "sp-refused", "cleanup", "jdbc-pool-start", "jdbc-reconnect-token", "jdbc-renewal",
            "jdbc-restart", "jdbc-revoked", "jdbc-exit", "reload-continuity", "reload-standing-removed"}


def private(path, value):
    """Write private material without printing it."""
    path.write_text(value, encoding="utf-8", newline="\n")
    path.chmod(0o600)


class Agent:
    """Own just this run's gateway, named containers, and private files."""
    def __init__(self, args):
        self.binary = args.nunya_exe
        self.run = Path(os.environ.get("NUNYA_AGENT_RUN_DIR",
                             str(Path(args.work_root).resolve() / ("agent-" + secrets.token_hex(6)))))
        self.run.mkdir(parents=True, exist_ok=True)
        self.sec = self.run / "secrets"
        self.sec.mkdir()
        (self.run / "tmp").mkdir()
        self.env = os.environ.copy()
        self.env.update({k: str(self.run / "tmp") for k in ("TEMP", "TMP", "TMPDIR")})
        cache_root = Path(args.work_root).resolve()
        self.env.update(NUNYA_HOME=str(self.sec / "home"), GOMODCACHE=str(cache_root / "gomod"),
                        GOCACHE=str(cache_root / "gocache"), GOTOOLCHAIN="local", GOFLAGS="-mod=readonly",
                        MSYS_NO_PATHCONV="1", SPANNER_DISABLE_BUILTIN_METRICS="true")
        self.proc = None
        self.jdbc = None
        self.prefix = os.environ.get("NUNYA_AGENT_PREFIX", "gd81-d5")
        if not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9_.-]*", self.prefix):
            raise RuntimeError("invalid-container-prefix")
        self.owned = []
        self.seen = {}
        self.stage = "setup"

    def check(self, name, ok, detail=""):
        """Record a unique assertion; stop on any failure."""
        if name in self.seen:
            raise RuntimeError("duplicate-check")
        self.seen[name] = bool(ok)
        print(f'{"PASS" if ok else "FAIL"} {name}{detail}', flush=True)
        if not ok:
            raise RuntimeError("assertion")

    def command(self, argv, *, data=None, env=None, cwd=None, timeout=120):
        """Capture all tool output privately; errors never include its contents."""
        result = subprocess.run([str(x) for x in argv], input=data, text=True,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                env=env or self.env, cwd=cwd, timeout=timeout)
        if result.returncode:
            codes = sorted(set(re.findall(r"\bNY[0-9]{3}\b", result.stdout)))
            raise RuntimeError(f"command-{Path(str(argv[0])).name}-exit-{result.returncode}" +
                               ("-" + "-".join(codes) if codes else ""))
        return result.stdout

    def cli(self, *argv):
        """Invoke the helper with standing credentials kept in a file."""
        return self.command([self.binary, *argv, *self.auth])

    def port(self, port):
        """Wait for a listener without sending credentials."""
        for _ in range(300):
            try:
                with socket.create_connection(("127.0.0.1", port), timeout=.2):
                    return
            except OSError:
                time.sleep(.1)
        raise RuntimeError("listener-timeout")

    def start(self, phase):
        """Start a gateway on the same private state directory."""
        with (self.run / f"serve-{phase}.log").open("w") as log:
            self.proc = subprocess.Popen([self.binary, "serve", "--config", str(self.sec / "nunya.toml")],
                                         env=self.env, stdout=log, stderr=log)
        for port in (self.mgmt, self.pg, self.sp):
            self.port(port)
        if self.proc.poll() is not None:
            raise RuntimeError("gateway-exited")

    def stop(self):
        """Stop and reap the owned native gateway."""
        if self.proc is not None:
            self.proc.terminate()
            self.proc.wait(timeout=30)
            self.proc = None

    def credentials(self, context):
        """Read trusted POSIX helper exports without evaluating shell code."""
        output = self.cli("session", "env", "--context", context, "--format", "sh")
        result = {}
        for line in output.splitlines():
            parts = shlex.split(line)
            if len(parts) != 2 or parts[0] != "export" or "=" not in parts[1]:
                raise RuntimeError("unexpected-env-format")
            key, value = parts[1].split("=", 1)
            result[key] = value
        return result

    def show(self, context):
        """Read public session metadata."""
        output = self.cli("session", "show", "--context", context)
        return dict(line.split(": ", 1) for line in output.splitlines() if ": " in line)

    def acquire(self, context, route):
        """Acquire once; subsequent work only renews the same session."""
        self.cli("session", "acquire", "--context", context, "--route", route,
                 "--mode", "read-write", "--pg-port", str(self.pg))
        return self.credentials(context)

    def pgconnect(self, creds):
        """Open a fresh PG connection using private helper credentials."""
        return psycopg.connect(host=creds["PGHOST"], port=creds["PGPORT"],
                               user=creds["PGUSER"], password=creds["PGPASSWORD"],
                               dbname="app", autocommit=True, connect_timeout=10)

    def pgcycle(self, creds, reference):
        """Reuse a bound blind token on a separately opened connection."""
        kwargs = dict(host=creds["PGHOST"], port=creds["PGPORT"], user=creds["PGUSER"],
                      password=creds["PGPASSWORD"], dbname="app", autocommit=True, connect_timeout=10)
        with ConnectionPool(min_size=1, max_size=1, timeout=15, kwargs=kwargs) as pool:
            with pool.connection() as conn:
                token = conn.execute("SELECT email FROM customers WHERE id = 3").fetchone()[0]
                first_statement = time.monotonic()
        assert token.startswith("tok_email_") and (reference is None or reference == token)
        with ConnectionPool(min_size=1, max_size=1, timeout=15, kwargs=kwargs) as pool:
            with pool.connection() as conn:
                assert conn.execute("SELECT id FROM customers WHERE email = %s", (token,)).fetchall() == [(3,)]
                assert conn.execute("SELECT email FROM customers WHERE id = 3").fetchone()[0] == token
        return token, first_statement

    def go(self, mode, creds=None):
        """Run a fresh native Go client with bearer only in its environment."""
        env = self.env.copy()
        if creds:
            env["AGENT_BEARER"] = creds["NUNYA_SPANNER_BEARER"]
        env.update(AGENT_ENDPOINT=f"127.0.0.1:{self.sp}", AGENT_REFERENCE=str(self.sec / "sp-token"),
                   AGENT_EMULATOR=self.emulator, AGENT_BINARY=self.binary,
                   AGENT_SERVER=f"http://127.0.0.1:{self.mgmt}",
                   AGENT_SECRET=str(self.sec / "agent.secret"))
        return self.command([self.run / "client.exe", mode], env=env, timeout=240).strip()

    def setup(self):
        """Provision only the pinned, explicitly named local containers."""
        # Never run any docker ... prune command (volume, system, image, container, network, builder).
        for name, image, ports, extra in (
            (self.prefix + "-pg", "postgres:18.6", [5432], ["--tmpfs", "/var/lib/postgresql", "-e", "POSTGRES_HOST_AUTH_METHOD=trust", "-e", "POSTGRES_DB=app"]),
            (self.prefix + "-emulator", "gcr.io/cloud-spanner-emulator/emulator:1.5.41", [9010, 9020], [])):
            args = ["docker", "run", "-d", "--pull=never", "--name", name, "--label", self.prefix + "=1"]
            for p in ports:
                args += ["-p", f"127.0.0.1::{p}"]
            self.command(args + extra + [image])
            self.owned.append(name)
            (self.run / ("owned-" + name)).touch()
        def mapped(name, port):
            return self.command(["docker", "port", name, f"{port}/tcp"]).strip()
        pg_up = mapped(self.prefix + "-pg", 5432)
        self.emulator = mapped(self.prefix + "-emulator", 9010)
        self.port(int(pg_up.rsplit(":", 1)[1]))
        for _ in range(120):
            ready = subprocess.run(["docker", "exec", self.prefix + "-pg", "pg_isready", "-U", "postgres"],
                                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env=self.env)
            if ready.returncode == 0:
                break
            time.sleep(1)
        else:
            raise RuntimeError("postgres-timeout")
        self.command(["docker", "exec", "-i", self.prefix + "-pg", "psql", "-U", "postgres", "-d", "app", "-v", "ON_ERROR_STOP=1"],
                     data=(HARNESS / "seed.sql").read_text() + "\nINSERT INTO notes SELECT i, repeat('C',16384) FROM generate_series(1,20000) i;\n")
        env = self.env.copy()
        env["NUNYA_SPANNER_REST"] = "http://" + mapped(self.prefix + "-emulator", 9020)
        # Existing setup compares Python stdout in Bash; normalize Windows CRLF.
        setup = 'python() { command python "$@" | tr -d "\\r"; }; export -f python; source "$1"'
        self.command([self.env.get("NUNYA_AGENT_BASH", "bash"), "-c", setup, "setup",
                      str(HARNESS / "spanner/setup.sh")], env=env, timeout=240)
        sockets = [socket.socket() for _ in range(4)]
        for s in sockets:
            s.bind(("127.0.0.1", 0))
        self.pg, self.mgmt, self.sp, self.admin = [s.getsockname()[1] for s in sockets]
        for s in sockets:
            s.close()
        private(self.sec / "bootstrap.secret", secrets.token_hex(32) + "\n")
        private(self.sec / "root.key", self.command([self.binary, "key", "generate"]))
        verifier = self.command([self.binary, "standing", "hash"], data=(self.sec / "bootstrap.secret").read_text()).strip()
        for name in ("agent", "agent-standing2"):
            private(self.sec / (name + ".secret"), secrets.token_hex(32) + "\n")
        standing = ""
        for name in ("agent", "agent-standing2"):
            hashed = self.command([self.binary, "standing", "hash"], data=(self.sec / (name + ".secret")).read_text()).strip()
            standing += f"\n[[standing]]\nname = '{name}'\nverifier = '{hashed}'\n"
        self.standing2 = standing[standing.index("\n[[standing]]", 1):]
        private(self.sec / "upstream.password", "nunya-harness-only\n")
        for source, target in (("policy.toml", "pg-policy.toml"), ("spanner/policy_gsql.toml", "sp-policy.toml")):
            shutil.copyfile(HARNESS / source, self.sec / target)
        private(self.sec / "nunya.toml", f"""listen = '127.0.0.1:{self.pg}'
admin_listen = '127.0.0.1:{self.admin}'
max_connections = 256
policy = 'pg-policy.toml'
state_dir = 'state'
root_key_file = 'root.key'
allow_plaintext = true
schemas = ['public']
search_path = ['public']
[upstream]
host = '127.0.0.1'
port = {pg_up.rsplit(':', 1)[1]}
user = 'nunya'
dbname = 'app'
tls = 'disable'
password_file = 'upstream.password'
[management]
listen = '127.0.0.1:{self.mgmt}'
[[management.admin]]
name = 'bootstrap'
verifier = '{verifier}'
[spanner_native]
listen = '127.0.0.1:{self.sp}'
database = 'projects/test-project/instances/test-instance/databases/gsql'
emulator_endpoint = 'http://{self.emulator}'
policy = 'sp-policy.toml'
[session]
max_lifetime = '30m'
idle_timeout = '60s'
{standing}
""")
        # Build in the run directory, using the existing pinned dependency set.
        build = self.run / "go-client"
        build.mkdir()
        for filename in ("go.mod", "go.sum"):
            shutil.copyfile(HARNESS / "spanner/client" / filename, build / filename)
        shutil.copyfile(HERE / "main.go", build / "main.go")
        self.command(["go", "vet", "."], cwd=build, timeout=600)
        self.command(["go", "build", "-o", str(self.run / "client.exe"), "."], cwd=build, timeout=600)
        self.go("seed")
        self.start("before")
        self.auth = ["--server", f"http://127.0.0.1:{self.mgmt}", "--principal", "bootstrap",
                     "--principal-secret-file", str(self.sec / "bootstrap.secret")]
        self.cli("principal", "grant", "agent", "--route", "postgres=read-write", "--route", "spanner-native=read-write")
        self.cli("principal", "grant", "agent-standing2", "--route", "postgres=read-write")
        self.auth = ["--server", f"http://127.0.0.1:{self.mgmt}", "--principal", "agent",
                     "--principal-secret-file", str(self.sec / "agent.secret")]
        self.check("setup", True)

    def exercise(self, minutes):
        """Run bursts across renewed idle gaps, preserving the original credentials."""
        self.stage = "work"
        pg = self.acquire("agent-pg", "postgres")
        self.start_jdbc(pg)
        sp = self.acquire("agent-sp", "spanner-native")
        idle = self.acquire("agent-idle", "postgres")
        initial = {c: self.show(c) for c in ("agent-pg", "agent-sp")}
        ids = {c: self.credentials(c)["PGUSER"] for c in initial}
        started = None
        token = None
        renewals = 0
        cycles = 0
        restarted = False
        idle_checked = False
        gaps = []
        previous_last = None
        jdbc_last = None
        jdbc_count = 0
        jdbc_statements = 0
        jdbc_gaps = []
        reload_pending = None
        reload_done = False
        removed = False
        restart_pending = False
        while True:
            self.stage = "connection-cycle"
            token, first_statement = self.pgcycle(pg, token)
            if previous_last is not None:
                gap = first_statement - previous_last
                gaps.append(gap)
                assert gap > 60
            assert self.go("cycle", sp) == "OK"
            reply = self.jdbc_command("cycle")
            match = re.fullmatch(r"OK ([0-9a-f]{8}) first=(\d+) last=(\d+) before=(\d+) after=(\d+) retries=(\d+)", reply)
            assert match and match[1] == hashlib.sha256(token.encode()).hexdigest()[:8]
            first, end, before, after, retries = map(int, match.groups()[1:])
            assert before == jdbc_statements
            if jdbc_last is not None:
                jdbc_gaps.append((first - jdbc_last) / 1000)
                assert jdbc_gaps[-1] > 60
            jdbc_last, jdbc_statements = end, after
            jdbc_count += 1
            print(f"JDBC cycle={jdbc_count} retries={retries} first_statement_ms={first}", flush=True)
            if restart_pending:
                self.check("jdbc-restart", True, f" retries={retries}")
                restart_pending = False
            if reload_pending is not None:
                held, old_revision, new_revision = reload_pending
                with held:
                    assert held.execute("SELECT email FROM customers WHERE id = 3").fetchone()[0] == token
                revisions = set()
                for line in (self.run / "serve-before.log").read_text().splitlines():
                    try:
                        record = json.loads(line)
                    except ValueError:
                        continue
                    if isinstance(record, dict) and record.get("route") == "postgres":
                        revisions.add(record.get("policy_revision"))
                assert old_revision in revisions and new_revision in revisions
                self.check("reload-continuity", True, f" policy={old_revision[:8]}->{new_revision[:8]}")
                reload_pending = None
                reload_done = True
            if cycles == 0:
                held = self.pgconnect(pg)
                assert held.execute("SELECT email FROM customers WHERE id = 3").fetchone()[0] == token
            last = time.monotonic()
            previous_last = last
            if started is None:
                started = last
            cycles += 1
            for c in initial:
                assert self.credentials(c)["PGUSER"] == ids[c]
            if last - started >= minutes * 60 and renewals >= 2 and cycles >= 5 and restarted:
                break
            time.sleep(40)
            if time.monotonic() - started >= minutes * 30 and not restarted:
                self.stage = "restart"
                self.stop()
                self.start("after")
                self.check("restart", all(self.credentials(c)["PGUSER"] == ids[c] for c in initial))
                restarted = True
                restart_pending = True
            if cycles == 1:
                self.stage = "reload-continuity"
                policy = self.sec / "pg-policy.toml"
                old_revision = hashlib.sha256(policy.read_bytes()).hexdigest()
                private(policy, policy.read_text() + "\n# agent reload continuity revision\n")
                new_revision = hashlib.sha256(policy.read_bytes()).hexdigest()
                assert old_revision != new_revision
                config = self.sec / "nunya.toml"
                private(config, config.read_text().replace("max_connections = 256", "max_connections = 200"))
                self.reload()
                for c in initial:
                    current = self.show(c)
                    assert current["expires_ms"] == initial[c]["expires_ms"]
                    assert self.credentials(c)["PGUSER"] == ids[c]
                reload_pending = held, old_revision, new_revision
            elif reload_done and not removed:
                self.remove_standing()
                removed = True
            self.stage = "renewal"
            for c in initial:
                self.cli("session", "renew", "--context", c)
                assert self.show(c)["expires_ms"] == initial[c]["expires_ms"]
                assert self.credentials(c)["PGUSER"] == ids[c]
            renewals += 1
            time.sleep(35)
            elapsed = time.monotonic() - started
            if elapsed >= 65 and not idle_checked:
                self.stage = "idle-negative"
                try:
                    with self.pgconnect(idle):
                        pass
                except psycopg.Error as e:
                    refused = e.sqlstate in ("NY003", "NY011") or "token not valid in this session" in str(e)
                else:
                    refused = False
                self.check("idle-negative", refused)
                idle_checked = True
        self.check("duration", last - started >= minutes * 60,
                   f" seconds={last-started:.1f} pg={ids['agent-pg'][8:]} sp={ids['agent-sp'][8:]}")
        for prefix in ("pg", "sp"):
            self.check(prefix + "-renewal", renewals >= 2 and len(gaps) == cycles - 1 and min(gaps) > 60,
                       f" count={renewals} min_gap={min(gaps):.3f}")
            self.check(prefix + "-reconnect-token", cycles >= 5, f" cycles={cycles}")
        self.check("jdbc-reconnect-token", jdbc_count == cycles and cycles >= 5, f" cycles={jdbc_count}")
        self.check("jdbc-renewal", renewals >= 2 and len(jdbc_gaps) == cycles - 1 and min(jdbc_gaps) > 60,
                   f" count={renewals} min_gap={min(jdbc_gaps):.3f}")
        self.stage = "pg-limit"
        successes = 0
        with self.pgconnect(pg) as conn:
            for _ in range(21):
                try:
                    value = conn.execute("SELECT nunya_tokenize('email', 'quota@agent.test')").fetchone()[0]
                    assert value.startswith("tok_email_")
                    successes += 1
                except psycopg.Error as e:
                    self.check("pg-limit", e.sqlstate == "NY007" and successes == 20,
                               f" successes={successes} code={e.sqlstate}")
                    break
            else:
                self.check("pg-limit", False)
        self.stage = "sp-limit"
        quota = self.go("quota", sp)
        self.check("sp-limit", quota == "OK successes=20 code=NY007", " " + quota)
        assert self.jdbc_command("hold") == "HELD"
        self.stage = "pg-stream-revoke"
        count = 0
        # libpq single-row mode keeps one Execute in flight; rows exceed socket buffers.
        with self.pgconnect(pg) as conn:
            try:
                with conn.cursor() as cur:
                    for row in cur.stream("SELECT id, body FROM notes ORDER BY id", size=1):
                        count += 1
                        if count == 100:
                            self.cli("session", "revoke", "--context", "agent-pg")
            except psycopg.Error as e:
                self.check("pg-stream-revoke", e.sqlstate == "NY011" and 100 <= count < 20000,
                           f" rows={count} code={e.sqlstate}")
            else:
                self.check("pg-stream-revoke", False, f" rows={count}")
        self.stage = "jdbc-revoked"
        reply = self.jdbc_command("refused")
        self.check("jdbc-revoked", reply == "REFUSED held=NY011 fresh=NY003", " " + reply if reply.startswith("REFUSED ") else "")
        self.stage = "pg-refused"
        try:
            with self.pgconnect(pg):
                pass
        except psycopg.Error as e:
            self.check("pg-refused", e.sqlstate == "NY003" or "token not valid in this session" in str(e))
        else:
            self.check("pg-refused", False)
        self.stage = "sp-stream-revoke"
        self.check("sp-stream-revoke", self.go("stream", sp) == "OK")
        self.stage = "sp-refused"
        self.check("sp-refused", self.go("refused", sp) == "OK")
        assert self.jdbc_command("quit") == "BYE"
        self.check("jdbc-exit", self.jdbc.wait(timeout=30) == 0)

    def jdbc_line(self):
        """Bound waits on child output; never surface arbitrary diagnostics."""
        try:
            line = self.jdbc_lines.get(timeout=90)
        except queue.Empty:
            raise RuntimeError("jdbc-timeout") from None
        if not line:
            raise RuntimeError("jdbc-eof")
        return line.strip()

    def jdbc_command(self, command):
        """Send a public control word to the persistent child."""
        self.jdbc.stdin.write(command + "\n")
        self.jdbc.stdin.flush()
        return self.jdbc_line()

    def start_jdbc(self, creds):
        """Validate pinned jars and start exactly one Hikari pool."""
        self.stage = "jdbc-pool-start"
        jars = Path(os.environ.get("NUNYA_AGENT_JARS", str(self.run / "jars")))
        self.command([self.env.get("NUNYA_AGENT_BASH", "bash"), "-c",
                      'export PATH="/usr/bin:$PATH"; bash "$1" "$2"', "fetch-jdbc",
                      (HARNESS / "drivers/fetch-jdbc.sh").as_posix(), jars.as_posix()])
        env = self.env.copy()
        env.update(creds)
        env["JDK_JAVA_OPTIONS"] = "-Djava.io.tmpdir=" + str(self.run / "tmp")
        with (self.run / "jdbc-diagnostics.log").open("w") as log:
            self.jdbc = subprocess.Popen(["java", "--class-path", str(jars / "*"), str(HERE / "AgentJdbc.java")],
                                         stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=log,
                                         text=True, env=env)
        self.jdbc_lines = queue.Queue()
        def read_lines():
            for line in self.jdbc.stdout:
                self.jdbc_lines.put(line)
            self.jdbc_lines.put("")
        threading.Thread(target=read_lines, daemon=True).start()
        ready = self.jdbc_line()
        self.check("jdbc-pool-start", ready == "READY keepalive=0 max=2 min=1 lifetime=0 idle=0",
                   " keepalive=0 max=2 min=1 lifetime=0 idle=0")

    def admin_request(self, method, path):
        """Call the local admin endpoint without logging its response."""
        conn = http.client.HTTPConnection("127.0.0.1", self.admin, timeout=5)
        try:
            conn.request(method, path, body=b"")
            response = conn.getresponse()
            assert response.status == (202 if method == "POST" else 200)
            return response.read().decode()
        finally:
            conn.close()

    def reload(self):
        """Wait for this config reload's applied revision/status."""
        def line():
            return next(x for x in self.admin_request("GET", "/status").splitlines() if x.startswith("config revision="))
        before = line()
        time.sleep(.01)
        self.admin_request("POST", "/reload")
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            after = line()
            if after != before and "last_reload=applied" in after.split():
                return
            time.sleep(.025)
        raise RuntimeError("reload-timeout")

    def remove_standing(self):
        """Remove a second config identity and assert shared scope revocation."""
        self.stage = "reload-standing-removed"
        auth = self.auth
        self.auth = ["--server", f"http://127.0.0.1:{self.mgmt}", "--principal", "agent-standing2",
                     "--principal-secret-file", str(self.sec / "agent-standing2.secret")]
        try:
            creds = self.acquire("agent-rm", "postgres")
            with self.pgconnect(creds) as held:
                held.execute("SELECT email FROM customers WHERE id = 3").fetchone()
                config = self.sec / "nunya.toml"
                if self.standing2 not in config.read_text():
                    raise RuntimeError("standing-entry-not-found")
                private(config, config.read_text().replace(self.standing2, ""))
                self.reload()
                try:
                    held.execute("SELECT email FROM customers WHERE id = 3")
                except psycopg.Error as e:
                    if e.sqlstate != "NY011":
                        code = e.sqlstate if e.sqlstate and re.fullmatch(r"[A-Z0-9]{5}", e.sqlstate) else "none"
                        raise RuntimeError("removed-standing-held-code-" + code) from None
                    held_detail = "held=NY011"
                else:
                    raise RuntimeError("removed-standing-held-accepted")
            try:
                with self.pgconnect(creds):
                    pass
            except psycopg.Error as e:
                if not (e.sqlstate == "NY003" or "token not valid in this session" in str(e)):
                    code = e.sqlstate if e.sqlstate and re.fullmatch(r"[A-Z0-9]{5}", e.sqlstate) else "none"
                    raise RuntimeError("removed-standing-fresh-code-" + code) from None
            else:
                raise RuntimeError("removed-standing-fresh-accepted")
            try:
                self.cli("session", "renew", "--context", "agent-rm")
            except RuntimeError:
                pass
            else:
                raise RuntimeError("removed-standing-renew-accepted")
        finally:
            self.auth = auth
        # The next normal cycle checks all three clients and both original tokens.
        self.check("reload-standing-removed", True, " " + held_detail + " fresh=NY003 renew=refused")

    def cleanup(self):
        """Delete only owned resources, including all credentials and stored tokens."""
        ok = True
        if self.jdbc is not None and self.jdbc.poll() is None:
            self.jdbc.terminate()
            self.jdbc.wait(timeout=30)
        try:
            self.stop()
        except Exception:
            ok = False
        for name in self.owned:
            try:
                self.command(["docker", "rm", "-fv", name])
                (self.run / ("owned-" + name)).unlink(missing_ok=True)
            except Exception:
                ok = False
        try:
            shutil.rmtree(self.sec)
            remaining = self.command(["docker", "ps", "-a", "--filter", "name=^/" + re.escape(self.prefix) + "-(pg|emulator)$",
                                      "--format", "{{.Names}}"])
            ok = ok and not remaining.strip()
        except Exception:
            ok = False
        self.check("cleanup", ok)


def main():
    """Fail if any required check did not run; always print counted totals."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--work-root", required=True)
    parser.add_argument("--nunya-exe", default=os.environ.get("NUNYA_EXE", "nunya"))
    args = parser.parse_args()
    agent = Agent(args)
    try:
        minutes = float(os.environ.get("NUNYA_AGENT_MINUTES", "15"))
        assert minutes >= 3
        agent.setup()
        agent.exercise(minutes)
    except Exception as error:
        if agent.stage not in agent.seen:
            agent.seen[agent.stage] = False
            detail = str(error) if type(error) is RuntimeError else type(error).__name__
            print(f"FAIL {agent.stage} category={detail}", flush=True)
    finally:
        try:
            agent.cleanup()
        except Exception:
            pass  # cleanup already emitted its counted failure
    for name in sorted(EXPECTED - agent.seen.keys()):
        agent.seen[name] = False
        print(f"FAIL {name} not-run", flush=True)
    passed = sum(agent.seen.values())
    failed = len(agent.seen) - passed
    print(f"{passed} passed; {failed} failed", flush=True)
    return int(failed != 0)


if __name__ == "__main__":
    sys.exit(main())
