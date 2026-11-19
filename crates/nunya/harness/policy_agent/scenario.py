"""Count every policy-default check; never print rows, credentials or diagnostics."""
import argparse
import base64
import collections
import http.client
import importlib.util
import json
import os
from pathlib import Path
import queue
import re
import secrets
import shutil
import subprocess
import sys
import threading
import time

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("agent_helpers", HERE.parent / "agent/agent.py")
helpers = importlib.util.module_from_spec(spec)
spec.loader.exec_module(helpers)
private = helpers.private
TOKEN = re.compile(r"tok_f[a-z2-7]{10}_[a-z2-7]+\Z")
ORIGINAL = {"status": "sent", "masked": {"old": "fixture-private"}}
CHANGED = {"status": {"unapproved": "fixture-private"}, "new_top": "fixture-top",
           "masked": {"old": "fixture-private", "new": "fixture-private"}}
CHECKS = ("session", "pre-refresh", "direct", "star", "reconnect", "json-original",
          "json-shape", "json-path", "stream-open", "fence", "tightened", "loosened")


class Child:
    """Keep one driver process alive and collect its private protocol safely."""
    def __init__(self, argv, env, log):
        self.log = log.open("w")
        self.proc = subprocess.Popen(argv, env=env, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                     stderr=self.log, text=True, bufsize=1)
        self.lines = queue.Queue()
        def reader():
            for line in self.proc.stdout:
                self.lines.put(line.rstrip("\r\n"))
            self.lines.put("CODE child-exit")
        self.reader = threading.Thread(target=reader, daemon=True)
        self.reader.start()
        if self.read() != "READY":
            raise RuntimeError("child-start")

    def read(self):
        """Bound every driver handshake."""
        try:
            return self.lines.get(timeout=60)
        except queue.Empty:
            raise RuntimeError("child-timeout") from None

    def send(self, command):
        """Send only to this owned child."""
        self.proc.stdin.write(command + "\n")
        self.proc.stdin.flush()

    def query(self, sql):
        """Decode rows only in memory, never in logs."""
        self.send(base64.b64encode(sql.encode()).decode())
        rows = []
        while True:
            line = self.read()
            if line == "OK":
                return rows, None
            if line.startswith("CODE "):
                return rows, line[5:]
            if not line.startswith("ROW "):
                raise RuntimeError("child-protocol")
            rows.append([None if cell == "~" else base64.b64decode(cell).decode()
                         for cell in line[4:].split(" ")])

    def close(self):
        """Reap the owned process even after a failed check."""
        if self.proc.poll() is None:
            try:
                self.send("quit")
                self.proc.wait(timeout=10)
            except Exception:
                self.proc.kill()
                self.proc.wait(timeout=10)
        self.log.close()


class Scenario(helpers.Agent):
    """Reuse provisioning helpers, with small fixtures and a separate scenario."""
    def __init__(self, args, mode):
        super().__init__(args)
        self.mode = mode
        self.children = []
        self.connections = []
        self.contexts = []
        self.results = {}
        self.current = "setup"
        self.prefix = "gd88-close-" + mode

    def command(self, argv, **kwargs):
        """Replace only the old scenario's large fixture and old Go build."""
        if str(argv[0]) == "go" and argv[1] in ("vet", "build") and str(kwargs.get("cwd", "")).endswith("go-client"):
            return ""
        if kwargs.get("data") and "generate_series(1,20000)" in kwargs["data"]:
            kwargs["data"] = kwargs["data"].split("\nINSERT INTO notes SELECT")[0]
        return super().command(argv, **kwargs)

    def go(self, mode, creds=None):
        """The custom native driver seeds a bounded dataset through REST instead."""
        if mode != "seed":
            raise RuntimeError("unexpected-old-driver")
        return "OK"

    def rest(self, path, body, method="POST"):
        """Call only this run's emulator REST listener."""
        conn = http.client.HTTPConnection(self.rest_host, timeout=60)
        try:
            conn.request(method, "/v1/" + path, json.dumps(body), {"Content-Type": "application/json"})
            response = conn.getresponse()
            value = json.loads(response.read())
            if response.status >= 300 or "error" in value:
                raise RuntimeError("emulator-rest")
            return value
        finally:
            conn.close()

    def ddl(self, statement):
        """Wait for DDL completion before issuing dependent requests."""
        result = self.rest(self.database + "/ddl", {"statements": [statement]}, "PATCH")
        for _ in range(120):
            if result.get("done"):
                return
            time.sleep(.1)
            result = self.rest(result["name"], None, "GET")
        raise RuntimeError("ddl-timeout")

    def mutate(self, table, columns, values):
        """Write fixture rows directly as an upstream producer."""
        session = self.rest(self.database + "/sessions", {})["name"]
        try:
            self.rest(session + ":commit", {"singleUseTransaction": {"readWrite": {}},
                      "mutations": [{"insertOrUpdate": {"table": table, "columns": columns, "values": values}}]})
        finally:
            self.rest(session, None, "DELETE")

    def pgadmin(self, sql):
        """Run direct administrative SQL privately."""
        self.command(["docker", "exec", "-i", self.prefix + "-pg", "psql", "-U", "postgres",
                      "-d", "app", "-v", "ON_ERROR_STOP=1"], data=sql)

    def policy(self, native, tight=False):
        """Build a v2 policy with explicit JSON leaf and masked subtree rules."""
        table, id_col, country, doc, notes, body = ((".Customers", "Id", "Country", "Doc", ".Notes", "Body")
            if native else ("public.customers", "id", "country", "doc", "public.notes", "body"))
        text = f"version = 2\ndefault = '{self.mode}'\n"
        for t, name, action in ((table, id_col, "show = true"),
                                (table, country, "masked = true" if tight else "show = true"),
                                (notes, id_col, "show = true"),
                                (notes, body, "masked = true" if tight else "show = true")):
            text += f"[[column]]\ntable = '{t}'\nname = '{name}'\n{action}\n"
        text += f"[[json_path]]\ntable = '{table}'\nname = '{doc}'\npath = '$.status'\nshow = true\ntype = 'string'\n"
        text += f"[[json_path]]\ntable = '{table}'\nname = '{doc}'\npath = '$.masked'\nmasked = true\n"
        return text

    def start(self, phase):
        """Prepare small fixtures and policies before the helper starts the gateway."""
        self.database = "projects/test-project/instances/test-instance/databases/gsql"
        self.rest_host = self.command(["docker", "port", self.prefix + "-emulator", "9020/tcp"]).strip()
        self.pgadmin("ALTER TABLE customers ADD COLUMN doc jsonb; UPDATE customers SET doc='" + json.dumps(ORIGINAL) + "';\n"
                     "INSERT INTO notes SELECT i, repeat('C',131072) FROM generate_series(1,64) i;\n")
        self.ddl("ALTER TABLE Customers ADD COLUMN Doc JSON")
        self.mutate("Customers", ["Id", "Doc"], [[str(i), json.dumps(ORIGINAL)] for i in range(1, 6)])
        self.mutate("Notes", ["Id", "Body"], [[str(i), "C" * 131072] for i in range(1, 65)])
        for native, file in ((False, "pg-policy.toml"), (True, "sp-policy.toml")):
            private(self.sec / file, self.policy(native))
        config = self.sec / "nunya.toml"
        text = config.read_text().replace("max_connections = 256", "max_connections = 256\nsnapshot_refresh_ms = 600000\nsnapshot_max_age_ms = 600000\n[readlog]\nsink = 'stdout'")
        text = text.replace("[spanner_native]", "[spanner_native]\nsnapshot_refresh_ms = 600000")
        # readlog must follow the root keys, rather than enclosing them.
        text = text.replace("\n[readlog]\nsink = 'stdout'", "")
        text = text.replace("[upstream]", "[readlog]\nsink = 'stdout'\n[upstream]")
        text = text.replace("idle_timeout = '60s'", "idle_timeout = 'off'")
        private(config, text)
        super().start(phase)

    def policy_mgmt(self, route, op="", body=None, key=None):
        """Use the documented admin bearer, keeping responses private."""
        conn = http.client.HTTPConnection("127.0.0.1", self.mgmt, timeout=60)
        headers = {"Authorization": "Bearer nunya.bootstrap." + (self.sec / "bootstrap.secret").read_text().strip(),
                   "Content-Type": "application/json"}
        if key:
            headers["Idempotency-Key"] = key
        try:
            conn.request("GET" if body is None else "POST", "/v1/routes/" + route + "/policy" + ("/" + op if op else ""),
                         None if body is None else json.dumps(body), headers)
            response = conn.getresponse()
            return response.status, json.loads(response.read())
        finally:
            conn.close()

    def record(self, driver, name, ok, detail=""):
        """Count named assertions exactly once, stopping on product failures."""
        key = (driver, name)
        if key in self.results:
            raise RuntimeError("duplicate-check")
        self.results[key] = bool(ok)
        print(f"{'PASS' if ok else 'FAIL'} {self.mode}/{driver}/{name}{detail}", flush=True)
        if not ok:
            raise RuntimeError("assertion-" + driver + "-" + name)

    def prepare_drivers(self, pg, sp):
        """Compile only harness drivers; retain one child per native protocol."""
        jars_env = os.environ.get("NUNYA_AGENT_JARS")
        if not jars_env:
            raise RuntimeError("NUNYA_AGENT_JARS-required")
        jars = Path(jars_env)
        self.command(["javac", "-cp", str(jars / "*"), "-d", str(self.run), str(HERE / "PolicyJdbc.java")])
        env = self.env.copy()
        env.update(pg)
        jdbc = Child(["java", "-cp", str(self.run) + os.pathsep + str(jars / "*"), "PolicyJdbc"], env, self.run / "jdbc.log")
        self.children.append(jdbc)
        build = self.run / "native"
        build.mkdir()
        for file in ("go.mod", "go.sum"):
            shutil.copyfile(HERE.parent / "spanner/client" / file, build / file)
        shutil.copyfile(HERE / "main.go", build / "main.go")
        self.command(["go", "build", "-o", str(self.run / "policy-native.exe"), "."], cwd=build, timeout=600)
        env = self.env.copy()
        env.update(AGENT_ENDPOINT=f"127.0.0.1:{self.sp}", AGENT_BEARER=sp["NUNYA_SPANNER_BEARER"])
        native = Child([str(self.run / "policy-native.exe")], env, self.run / "native.log")
        self.children.append(native)
        conn = self.pgconnect(pg)
        self.connections.append(conn)
        self.pgcreds = pg
        self.drivers = {"psycopg": conn, "pgJDBC": jdbc, "Go-native": native}

    def query(self, driver, sql):
        """Return a uniform string matrix and SQLSTATE without exception text."""
        if driver != "psycopg":
            return self.drivers[driver].query(sql)
        try:
            cur = self.drivers[driver].execute(sql, prepare=True)
            return [[json.dumps(v) if isinstance(v, (dict, list)) else None if v is None else str(v)
                     for v in row] for row in cur.fetchall()], None
        except helpers.psycopg.Error as error:
            return [], error.sqlstate

    def exercise(self):
        """Hold access sessions across migration, shape change and policy applies."""
        pg = self.acquire("policy-pg", "postgres")
        self.contexts.append("policy-pg")
        sp = self.acquire("policy-sp", "spanner-native")
        self.contexts.append("policy-sp")
        initial = {c: (self.credentials(c)["PGUSER"], self.show(c)["expires_ms"]) for c in self.contexts}
        self.prepare_drivers(pg, sp)
        for d in self.drivers:
            self.record(d, "session", self.query(d, "SELECT Id FROM Customers WHERE Id = 1")[0] == [["1"]])
        self.pgadmin("ALTER TABLE customers ADD COLUMN recovery_email text; UPDATE customers SET recovery_email='recovery@fixture.test';")
        self.ddl("ALTER TABLE Customers ADD COLUMN recovery_email STRING(MAX)")
        self.mutate("Customers", ["Id", "recovery_email"], [[str(i), "recovery@fixture.test"] for i in range(1, 6)])
        for d in self.drivers:
            _, code = self.query(d, "SELECT * FROM Customers WHERE Id = 1")
            self.record(d, "pre-refresh", code == "NY004", " code=" + str(code))
        config = self.sec / "nunya.toml"
        private(config, config.read_text().replace("snapshot_refresh_ms = 600000", "snapshot_refresh_ms = 100"))
        self.reload()
        time.sleep(2)
        # Schema-stale PG plans close their sockets; reconnect with the SAME session.
        self.drivers["psycopg"].close()
        self.drivers["psycopg"] = self.pgconnect(pg)
        self.connections.append(self.drivers["psycopg"])
        self.drivers["pgJDBC"].send("reconnect")
        if self.drivers["pgJDBC"].read() != "OK":
            raise RuntimeError("jdbc-refresh-reconnect")
        for d in self.drivers:
            rows, code = self.query(d, "SELECT recovery_email FROM Customers WHERE Id = 1")
            value = rows[0][0] if rows else None
            self.record(d, "direct", code is None and (bool(TOKEN.fullmatch(value or "")) if self.mode == "tokenize" else value == "recovery@fixture.test"))
            rows, code = self.query(d, "SELECT * FROM Customers WHERE Id = 1")
            self.record(d, "star", code is None and len(rows) == 1 and rows[0][-1] == value)
            if d == "psycopg":
                self.drivers[d].close()
                self.drivers[d] = self.pgconnect(pg)
                self.connections.append(self.drivers[d])
            else:
                self.drivers[d].send("reconnect")
                if self.drivers[d].read() != "OK":
                    raise RuntimeError("driver-reconnect")
            self.record(d, "reconnect", self.query(d, "SELECT recovery_email FROM Customers WHERE Id = 1") == ([[value]], None))
            rows, code = self.query(d, "SELECT Doc FROM Customers WHERE Id = 1")
            doc = json.loads(rows[0][0]) if rows else {}
            self.record(d, "json-original", code is None and doc == {"status": "sent"})
        self.pgadmin("UPDATE customers SET doc='" + json.dumps(CHANGED) + "';")
        self.mutate("Customers", ["Id", "Doc"], [[str(i), json.dumps(CHANGED)] for i in range(1, 6)])
        for d in self.drivers:
            rows, code = self.query(d, "SELECT Doc FROM Customers WHERE Id = 1")
            doc = json.loads(rows[0][0]) if rows else {}
            self.record(d, "json-shape", code is None and "masked" not in doc and
                        (("new_top" not in doc and "status" not in doc) if self.mode == "tokenize" else doc.get("new_top") == "fixture-top"))
            sql = ("SELECT JSON_VALUE(Doc, '$.new_top') FROM Customers WHERE Id = 1" if d == "Go-native"
                   else "SELECT doc->>'new_top' FROM customers WHERE id = 1")
            expected = "fixture-top" if self.mode == "show" else None
            self.record(d, "json-path", self.query(d, sql) == ([[expected]], None))
        for native, route, drivers in ((False, "postgres", ["psycopg", "pgJDBC"]), (True, "spanner-native", ["Go-native"])):
            status, imported = self.policy_mgmt(route, "import", {}, "import-" + route)
            if status != 200:
                raise RuntimeError("policy-import")
            for d in drivers:
                if d == "psycopg":
                    self.stream = self.drivers[d].cursor().stream("SELECT id, body FROM notes ORDER BY id", size=1)
                    next(self.stream)
                else:
                    self.drivers[d].send("stream")
                    if self.drivers[d].read() != "READY":
                        raise RuntimeError("stream-open")
                self.record(d, "stream-open", True)
            status, current = self.policy_mgmt(route)
            body = {"policy_toml": self.policy(native, True), "expected_revision": current["active_revision"]["number"],
                    "catalog_fingerprint": current["catalog_fingerprint"]}
            status, applied = self.policy_mgmt(route, "apply", body, "tight-" + route)
            self.record(route, "apply", status == 200)
            for d in drivers:
                if d == "psycopg":
                    code = None
                    try:
                        for _ in self.stream:
                            pass
                    except helpers.psycopg.Error as error:
                        code = error.sqlstate
                else:
                    self.drivers[d].send("drain")
                    code = self.drivers[d].read().removeprefix("CODE ")
                self.record(d, "fence", code == "NY008", " code=" + str(code))
                # PostgreSQL row-release fencing closes the socket. Keep the
                # access session and reconnect before testing the new policy.
                if d == "psycopg":
                    self.drivers[d].close()
                    self.drivers[d] = self.pgconnect(pg)
                    self.connections.append(self.drivers[d])
                elif d == "pgJDBC":
                    self.drivers[d].send("reconnect")
                    if self.drivers[d].read() != "OK":
                        raise RuntimeError("jdbc-fence-reconnect")
            status, replay = self.policy_mgmt(route, "apply", body, "tight-" + route)
            self.record(route, "idempotent", status == 200 and replay["revision"]["number"] == applied["revision"]["number"] and replay["replayed"])
            status, stale = self.policy_mgmt(route, "apply", body, "stale-" + route)
            self.record(route, "stale", status == 409 and stale.get("error") == "stale_revision", " code=" + stale.get("error", "none"))
            for d in drivers:
                rows, code = self.query(d, "SELECT Country FROM Customers WHERE Id = 1")
                self.record(d, "tightened", code is None and len(rows) == 1 and rows[0][0] is None,
                            " code=" + str(code) + " rows=" + str(len(rows)))
            status, current = self.policy_mgmt(route)
            body.update(policy_toml=self.policy(native), expected_revision=current["active_revision"]["number"], catalog_fingerprint=current["catalog_fingerprint"])
            status, _ = self.policy_mgmt(route, "apply", body, "loose-" + route)
            self.record(route, "loosen-apply", status == 200)
            for d in drivers:
                self.record(d, "loosened", self.query(d, "SELECT Country FROM Customers WHERE Id = 1 /* new plan after loosening */") == ([["US"]], None))
        self.record("lifecycle", "same-sessions", all(self.same_session(c, initial[c]) for c in self.contexts))

    def same_session(self, context, initial):
        """The original session is still the active, unrevoked one."""
        shown = self.show(context)
        return ((self.credentials(context)["PGUSER"], shown["expires_ms"]) == initial
                and shown.get("state") == "Active" and shown.get("revoke_reason") == "None")

    def finish(self):
        """Release sessions, stop all owned clients, then use named cleanup."""
        for child in self.children:
            child.close()
        for conn in self.connections:
            conn.close()
        released = True
        for context in self.contexts:
            try:
                self.cli("session", "revoke", "--context", context)
            except Exception:
                released = False
        # A failed release must never prevent gateway/container cleanup.
        self.results[("lifecycle", "released")] = released
        print(f"{'PASS' if released else 'FAIL'} {self.mode}/lifecycle/released", flush=True)
        try:
            super().cleanup()
        finally:
            self.record("lifecycle", "cleanup", self.seen.get("cleanup", False))


def main():
    """Run sequential Tokenize and Show passes; missing checks always fail."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--work-root", required=True)
    parser.add_argument("--nunya-exe", required=True)
    args = parser.parse_args()
    expected = {(d, c) for d in ("psycopg", "pgJDBC", "Go-native") for c in CHECKS}
    expected |= {(r, c) for r in ("postgres", "spanner-native") for c in ("apply", "idempotent", "stale", "loosen-apply")}
    expected |= {("lifecycle", c) for c in ("same-sessions", "released", "cleanup")}
    total = collections.Counter()
    for mode in ("tokenize", "show"):
        os.environ["NUNYA_AGENT_PREFIX"] = "gd88-close-" + mode
        scenario = Scenario(args, mode)
        try:
            scenario.setup()
            scenario.exercise()
        except Exception as error:
            # Only fixed categories/SQLSTATEs are exposed, never driver diagnostics.
            category = str(error) if type(error) is RuntimeError else type(error).__name__
            if not re.fullmatch(r"[A-Za-z0-9_.-]+", category):
                category = type(error).__name__
            print("FAIL " + mode + "/scenario category=" + category, flush=True)
            scenario.results[("lifecycle", "scenario")] = False
        finally:
            try:
                scenario.finish()
            except Exception:
                print("FAIL " + mode + "/cleanup", flush=True)
        for d, c in sorted(expected - scenario.results.keys()):
            scenario.results[(d, c)] = False
            print(f"FAIL {mode}/{d}/{c} not-run", flush=True)
        for d in ("psycopg", "pgJDBC", "Go-native", "postgres", "spanner-native", "lifecycle"):
            values = [ok for (driver, _), ok in scenario.results.items() if driver == d]
            passed, failed = sum(values), len(values) - sum(values)
            print(f"SUMMARY {mode}/{d} {passed} passed; {failed} failed", flush=True)
            total["passed"] += passed
            total["failed"] += failed
        # A product failure stops execution, as required by the bounded brief.
        if total["failed"]:
            if mode == "tokenize":
                for d, c in sorted(expected):
                    print(f"FAIL show/{d}/{c} not-run-after-blocker", flush=True)
                for d in ("psycopg", "pgJDBC", "Go-native", "postgres", "spanner-native", "lifecycle"):
                    count = sum(driver == d for driver, _ in expected)
                    print(f"SUMMARY show/{d} 0 passed; {count} failed", flush=True)
                total["failed"] += len(expected)
            break
    print(f"{total['passed']} passed; {total['failed']} failed", flush=True)
    return int(total["failed"] != 0)


if __name__ == "__main__":
    sys.exit(main())
