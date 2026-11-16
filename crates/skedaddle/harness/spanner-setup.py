"""Credential-free, bounded Spanner emulator setup and harness lifecycle.

run starts only the spanner-emulator service, provisions both dialects, optionally
runs a standalone Cargo spike, and always removes its own Compose resources.
setup can provision an already-running emulator via its loopback REST endpoint.
"""

import argparse
from contextlib import nullcontext
import json
import os
from pathlib import Path
import re
import shutil
import shlex
import subprocess
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen
import uuid


HARNESS = Path(__file__).resolve().parent
PROJECT = "skedaddle-test"
INSTANCE = "skedaddle"


def rest(endpoint, method, path, body=None):
    parsed = urlparse(endpoint)
    if parsed.scheme != "http" or parsed.hostname != "127.0.0.1":
        raise ValueError("setup only permits the credential-free loopback emulator")
    data = None if body is None else json.dumps(body).encode()
    request = Request(endpoint + "/v1/" + path, data=data, method=method,
                      headers={"Content-Type": "application/json"})
    try:
        with urlopen(request, timeout=15) as response:
            return json.load(response)
    except HTTPError as error:
        raise RuntimeError(f"{method} {path}: {error.code} {error.read().decode()}") from error


def operation(endpoint, value):
    deadline = time.monotonic() + 90
    while not value.get("done"):
        if time.monotonic() >= deadline:
            raise TimeoutError("Spanner setup operation exceeded 90 seconds")
        time.sleep(0.2)
        value = rest(endpoint, "GET", value["name"])
    if "error" in value:
        raise RuntimeError(json.dumps(value["error"]))
    return value.get("response", {})


def schema(dialect):
    if dialect == "GOOGLE_STANDARD_SQL":
        return [
            "CREATE TABLE parents (parent_id INT64 NOT NULL, payload STRING(MAX), "
            "changed_at TIMESTAMP OPTIONS (allow_commit_timestamp=true)) PRIMARY KEY(parent_id)",
            "CREATE TABLE children (parent_id INT64 NOT NULL, child_id INT64 NOT NULL, "
            "payload STRING(MAX)) PRIMARY KEY(parent_id, child_id), "
            "INTERLEAVE IN PARENT parents ON DELETE CASCADE",
            "CREATE CHANGE STREAM changes FOR ALL OPTIONS "
            "(value_capture_type='NEW_ROW_AND_OLD_VALUES', retention_period='1d')",
        ]
    return [
        "CREATE TABLE parents (parent_id bigint NOT NULL PRIMARY KEY, payload text, "
        "changed_at spanner.commit_timestamp)",
        "CREATE TABLE children (parent_id bigint NOT NULL, child_id bigint NOT NULL, "
        "payload text, PRIMARY KEY(parent_id, child_id)) "
        "INTERLEAVE IN PARENT parents ON DELETE CASCADE",
        "CREATE CHANGE STREAM changes FOR ALL WITH "
        "(value_capture_type='NEW_ROW_AND_OLD_VALUES', retention_period='1d')",
    ]


def setup(endpoint):
    project = f"projects/{PROJECT}"
    instance = f"{project}/instances/{INSTANCE}"
    operation(endpoint, rest(endpoint, "POST", project + "/instances", {
        "instanceId": INSTANCE,
        "instance": {"name": instance, "config": project + "/instanceConfigs/emulator-config",
                     "displayName": "Skedaddle S1", "nodeCount": 1},
    }))
    result = {}
    for db, dialect in (("s1-google", "GOOGLE_STANDARD_SQL"), ("s1-postgres", "POSTGRESQL")):
        operation(endpoint, rest(endpoint, "POST", instance + "/databases", {
            "createStatement": f"CREATE DATABASE `{db}`" if dialect == "GOOGLE_STANDARD_SQL"
                               else f'CREATE DATABASE "{db}"',
            "databaseDialect": dialect,
        }))
        database = instance + "/databases/" + db
        operation(endpoint, rest(endpoint, "PATCH", database + "/ddl", {"statements": schema(dialect)}))
        result[dialect] = {"database": database, "ddl": rest(endpoint, "GET", database + "/ddl")}
    return result


def command(args, env):
    return subprocess.check_output(args, env=env, stderr=subprocess.STDOUT,
                                   text=True, encoding="utf-8").strip()


def run(task_root, spike, cargo_test=""):
    task = Path(task_root).resolve()
    if not task.as_posix().lower().startswith("j:/gigglesdb-work/"):
        raise ValueError("task root must stay under J:/gigglesdb-work")
    task.mkdir(parents=True, exist_ok=True)
    if cargo_test:
        required = ("CARGO_TARGET_DIR", "RUSTC_WRAPPER", "SCCACHE_DIR", "SCCACHE_CACHE_SIZE",
                    "CARGO_INCREMENTAL", "CARGO_BUILD_JOBS", "TEMP", "TMP")
        missing = [key for key in required if not os.environ.get(key)]
        if missing:
            raise ValueError("cargo-test requires caller build environment: " + ", ".join(missing))
    if shutil.disk_usage(task).free < 2 * 1024**3:
        raise RuntimeError("at least 2 GiB free disk required before pulling the emulator")
    run_id = "s1-" + uuid.uuid4().hex[:12]
    (task / "spanner-run-id.txt").write_text(run_id, encoding="utf-8")
    env = os.environ.copy()
    env["RUN_ID"] = run_id
    # Compose interpolates all services, even though up names only this profile's service.
    for key, image in {"PG_IMAGE": "postgres:16-alpine", "MINIO_IMAGE": "minio/minio",
                       "GCS_IMAGE": "fsouza/fake-gcs-server", "TOXIPROXY_IMAGE": "ghcr.io/shopify/toxiproxy",
                       "FAULTS_IMAGE": "alpine:3"}.items():
        env.setdefault(key, image)
    compose = ["docker", "compose", "-f", str(HARNESS / "compose.yaml"),
               "-p", "skedaddle-" + run_id, "--profile", "spanner-emulator"]
    # This opt-in applies only to the repository reader test lane. Preserve the
    # normal emulator service and all of its labels, limits and loopback ports.
    if cargo_test and os.environ.get("SKEDADDLE_SPANNER_SHORT_PARTITIONS") == "1":
        override = task / "short-partitions.json"
        override.write_text(json.dumps({"services": {"spanner-emulator": {"command": [
            "./gateway_main", "--hostname", "0.0.0.0",
            "--override_change_stream_partition_token_alive_seconds=1"]}}}), encoding="utf-8")
        compose[4:4] = ["-f", str(override)]
    try:
        command(compose + ["config", "--format", "json"], env)
        subprocess.run(compose + ["up", "-d", "spanner-emulator"], env=env,
                       stderr=subprocess.STDOUT, check=True)
        def port(number):
            address = command(compose + ["port", "spanner-emulator", str(number)], env)
            if not re.fullmatch(r"127\.0\.0\.1:\d+", address):
                raise RuntimeError("emulator port must bind only to loopback: " + address)
            return address
        endpoint = "http://" + port(9020)
        deadline = time.monotonic() + 90
        while True:
            try:
                rest(endpoint, "GET", f"projects/{PROJECT}/instanceConfigs")
                break
            except (URLError, HTTPError, OSError, RuntimeError):
                if time.monotonic() >= deadline:
                    raise TimeoutError("Spanner emulator did not become ready")
                time.sleep(0.25)
        config = {"endpoint": endpoint, "grpc_endpoint": port(9010), "run_id": run_id,
                  "project": PROJECT, "instance": INSTANCE, "stream": "changes",
                  "databases": setup(endpoint)}
        config_path = task / "spanner-config.json"
        config_path.write_text(json.dumps(config, indent=2), encoding="utf-8")
        print("SPANNER_CONFIG=" + str(config_path), flush=True)
        if spike or cargo_test:
            env["SPANNER_EMULATOR_HOST"] = config["grpc_endpoint"]
            env["S1_CONFIG"] = str(config_path)
            env["S1_OUTPUT"] = str(task / "spike-results.json")
            # Windows' system bash.exe may be a WSL launcher without a distro.
            bash = shutil.which("bash", path=str(Path(os.environ.get("ProgramFiles", "C:/Program Files")) / "Git/bin")) or "bash"
            if cargo_test:
                env["SKEDADDLE_SPANNER_CONFIG"] = str(config_path)
                # A caller holding its lane lock may explicitly skip reacquisition.
                lock = Path(env.get("SKEDADDLE_LANE_LOCK") or task / "build.lock")
                held = env.get("SKEDADDLE_LANE_LOCK_HELD") == "1"
                with nullcontext() if held else lock.open("x", encoding="utf-8") as owner:
                    try:
                        if owner is not None:
                            owner.write(str(os.getpid()))
                            owner.flush()
                        subprocess.run([bash, "J:/gigglesdb-work/r2/buildslot2.sh", "cargo", "test",
                                        *shlex.split(cargo_test)], cwd=HARNESS.parent,
                                       env=env, stderr=subprocess.STDOUT, check=True)
                    finally:
                        if owner is not None:
                            owner.close()
                            lock.unlink()
            else:
                subprocess.run([bash, "J:/gigglesdb-work/r2/buildslot2.sh", "cargo", "run",
                                "--locked", "--manifest-path", str(Path(spike).resolve())],
                               env=env, stderr=subprocess.STDOUT, check=True)
    finally:
        logs = subprocess.run(compose + ["logs", "--no-color", "spanner-emulator"], env=env,
                              text=True, encoding="utf-8", capture_output=True)
        (task / "emulator.log").write_text(logs.stdout + logs.stderr, encoding="utf-8")
        subprocess.run(compose + ["down", "--volumes", "--remove-orphans"], env=env,
                       stderr=subprocess.STDOUT, check=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="action", required=True)
    runner = sub.add_parser("run")
    runner.add_argument("--task-root", required=True)
    runner.add_argument("--spike", default="")
    runner.add_argument("--cargo-test", default="")
    provision = sub.add_parser("setup")
    provision.add_argument("--endpoint", required=True)
    args = parser.parse_args()
    if args.action == "run":
        if args.spike and args.cargo_test:
            parser.error("choose --spike or --cargo-test")
        run(args.task_root, args.spike, args.cargo_test)
    else:
        print(json.dumps(setup(args.endpoint), indent=2))


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        # PowerShell 5 treats redirected native stderr as terminating errors.
        print(f"SPANNER_SETUP_FAILED: {error}", flush=True)
        raise SystemExit(1) from error
