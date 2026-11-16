"""GD-0064 M1: one real MySQL variant, with exact-label teardown in finally."""
import datetime
from contextlib import nullcontext
import json
import os
import pathlib
import re
import shlex
import shutil
import subprocess
import sys
import time


ROOT = pathlib.Path(__file__).resolve().parent


def run_cargo(args, env, task, *, total_timeout=5400, output_timeout=1800):
    # buildslot2 is silent while queued. Allow 90 minutes overall, but only
    # 30 minutes after cargo's first output (stdout OR stderr). File-backed
    # capture works on Windows, where communicate timeouts omit partial output.
    stdout_path, stderr_path = task / "cargo-stdout.log", task / "cargo-stderr.log"
    timed_out = False
    with stdout_path.open("w", encoding="utf8") as stdout_file, \
            stderr_path.open("w", encoding="utf8") as stderr_file:
        process = subprocess.Popen(args, env=env, cwd=ROOT.parent,
                                   stdout=stdout_file, stderr=stderr_file)
        deadline = time.monotonic() + total_timeout
        output_started = False
        while True:
            try:
                process.wait(timeout=max(0, min(1, deadline - time.monotonic())))
                break
            except subprocess.TimeoutExpired:
                now = time.monotonic()
                if now >= deadline:
                    # Killing only bash would leave cargo/test children using the lane.
                    subprocess.run(["taskkill", "/T", "/F", "/PID", str(process.pid)],
                                   env=env, capture_output=True, check=False)
                    process.wait()
                    timed_out = True
                    break
                if not output_started and (stdout_path.stat().st_size or stderr_path.stat().st_size):
                    output_started = True
                    deadline = min(deadline, now + output_timeout)
    stdout = stdout_path.read_text(encoding="utf8", errors="replace")
    stderr = stderr_path.read_text(encoding="utf8", errors="replace")
    (task / "cargo-test.log").write_text(stdout + stderr, encoding="utf8", newline="\n")
    if timed_out:
        raise RuntimeError(
            f"MySQL cargo lane exceeded {total_timeout}s overall or {output_timeout}s "
            "after first cargo output; killed process tree; see cargo-test.log")
    return subprocess.CompletedProcess(args, process.returncode, stdout, stderr)


def main():
    profile, task_arg, gtid = sys.argv[1:4]
    # Explicit prefixes preserve empty optional arguments in Windows PowerShell.
    spike_arg = sys.argv[4].removeprefix("--spike=") if len(sys.argv) > 4 else ""
    cargo_test = sys.argv[5].removeprefix("--cargo-test=") if len(sys.argv) > 5 else ""
    task = pathlib.Path(task_arg).resolve()
    if profile not in ("mysql80", "mysql84") or gtid not in ("ON", "OFF"):
        raise ValueError("expected mysql80/mysql84 and ON/OFF")
    if not task.as_posix().lower().startswith("j:/gigglesdb-work/"):
        raise ValueError("task must be below J:/gigglesdb-work")
    task.mkdir(parents=True, exist_ok=True)
    (task / "tmp").mkdir(exist_ok=True)
    run = "m1-" + datetime.datetime.now().strftime("%Y%m%d-%H%M%S") + "-" + str(os.getpid())
    assert re.fullmatch("[a-z0-9-]+", run)
    env = os.environ.copy()
    env.update(RUN_ID=run, MYSQL_GTID_MODE=gtid, TEMP=str(task / "tmp"),
               TMP=str(task / "tmp"), SKEDADDLE_TEST_ROOT=str(task / "scratch"),
               CARGO_TARGET_DIR=env.get("CARGO_TARGET_DIR") or str(task / "target"),
               RUSTC_WRAPPER="J:/gigglesdb-work/tools/bin/sccache.exe",
               SCCACHE_DIR="J:/gigglesdb-work/sccache-cache", SCCACHE_CACHE_SIZE="100G",
               CARGO_INCREMENTAL="0", CARGO_BUILD_JOBS="4")
    lock = json.loads((ROOT / "images.lock").read_text())["images"]
    for name in ("mysql80", "mysql84"):
        env[name.upper() + "_IMAGE"] = lock[name]["ref"]
    # Compose interpolates unselected legacy services too; they are never started.
    for name in ("PG", "MINIO", "GCS", "TOXIPROXY", "FAULTS"):
        env.setdefault(name + "_IMAGE", "unused:unselected")

    def command(args, check=True, timeout=300):
        p = subprocess.run([str(a) for a in args], env=env, cwd=ROOT,
                           capture_output=True, encoding="utf8", errors="replace", timeout=timeout)
        if check and p.returncode:
            raise RuntimeError(f"command failed ({p.returncode}): {args}\n{p.stdout}\n{p.stderr}")
        return p

    def docker(*args, **kwargs):
        return command(["docker", *args], **kwargs)

    def compose(*args, **kwargs):
        return docker("compose", "-f", ROOT / "compose.yaml", "-p", "skedaddle-" + run,
                      "--profile", profile, *args, **kwargs)

    def save(name, value):
        (task / name).write_text(value, encoding="utf8", newline="\n")

    for drive in ("C:/", task):
        free = shutil.disk_usage(drive).free
        if free < 8 * 1024**3:
            raise RuntimeError(f"disk guard: {drive} has less than 8 GiB free")
    config = compose("config")
    save("compose-config.yaml", config.stdout)
    save("owner.json", json.dumps({"run_id": run, "task_root": str(task), "pid": os.getpid()}))
    # An image-only guard has the same ownership labels as all other resources.
    guard_name = "skedaddle-" + run + "-guard"
    try:
        if docker("image", "inspect", lock[profile]["ref"], check=False).returncode:
            docker("pull", lock[profile]["ref"])
        guard = docker("run", "--name", guard_name, "--network", "none", "--label",
                       "skedaddle.test=" + run, "--label", "com.docker.compose.project=skedaddle-" + run,
                       "--entrypoint", "df", lock[profile]["ref"], "-Pk", "/")
        save("disk-guard.txt", guard.stdout)
        if int(guard.stdout.splitlines()[-1].split()[3]) * 1024 < 4 * 1024**3:
            raise RuntimeError("Docker VM lacks 4 GiB headroom")
        up = compose("up", "-d", "--wait", "--wait-timeout", "240", profile, check=False)
        save("startup.log", up.stdout + up.stderr)
        if up.returncode:
            raise RuntimeError("MySQL startup failed; see startup.log and server.log")
        cid = compose("ps", "-q", profile).stdout.strip()
        ca = docker("exec", cid, "cat", "/var/lib/mysql/harness/tls/ca.pem").stdout
        save("ca.pem", ca)
        address = compose("port", profile, "3306").stdout.strip()
        env.update(M1_PORT=address.rsplit(":", 1)[1], M1_CA=str(task / "ca.pem"),
                   M1_OUTPUT=str(task), M1_PROFILE=profile, M1_GTID=gtid,
                   M1_CONTAINER_ID=cid)
        config_path = task / "mysql-config.json"
        save("mysql-config.json", json.dumps(dict(host="localhost", port=int(env["M1_PORT"]),
             ca=env["M1_CA"], user="root", password="skedaddle", profile=profile, gtid=gtid,
             container_id=cid, max_decompressed_bytes=16 * 1024 * 1024)))
        env["SKEDADDLE_MYSQL_CONFIG"] = str(config_path)
        sql = ("SELECT @@version,@@gtid_mode,@@binlog_format,@@binlog_row_image,@@binlog_row_metadata; "
               "SELECT COUNT(*) FROM type_zoo.zoo; "
               "SELECT HEX(cv),HEX(JSON_UNQUOTE(JSON_EXTRACT(jv,'$.nested[3]'))),OCTET_LENGTH(wide_row) FROM type_zoo.zoo WHERE id=1;")
        probe = docker("exec", "-e", "MYSQL_PWD=skedaddle", cid, "mysql", "-h127.0.0.1", "-uroot",
                       "--ssl-mode=VERIFY_CA", "--ssl-ca=/var/lib/mysql/harness/tls/ca.pem",
                       "--default-character-set=utf8mb4", "-Nse", sql)
        save("setup-probe.txt", probe.stdout)
        if cargo_test:
            # A caller holding its lane lock may explicitly skip reacquisition.
            lane_lock = pathlib.Path(env.get("SKEDADDLE_LANE_LOCK") or task / "build.lock")
            held = env.get("SKEDADDLE_LANE_LOCK_HELD") == "1"
            with nullcontext() if held else lane_lock.open("x", encoding="utf8") as owner:
                try:
                    if owner is not None:
                        owner.write(str(os.getpid()))
                        owner.flush()
                    args = ["C:/Program Files/Git/bin/bash.exe",
                            "J:/gigglesdb-work/r2/buildslot2.sh", "cargo", "test", "-p", "skedaddle",
                            *shlex.split(cargo_test)]
                    result = run_cargo(args, env, task)
                    print(result.stdout, end="", flush=True)
                    print(result.stderr, file=sys.stderr, end="", flush=True)
                    return result.returncode
                finally:
                    if owner is not None:
                        owner.close()
                        lane_lock.unlink()
        spike = pathlib.Path(spike_arg) if spike_arg else None
        if not spike or not spike.is_file():
            print("SKIP Rust client spike: supply -MysqlSpike <built standalone spike.exe>", flush=True)
            return 2
        result = command([spike], check=False, timeout=180)
        save("spike.log", result.stdout + result.stderr)
        print(result.stdout, end="", flush=True)
        if result.stderr:
            print(result.stderr, file=sys.stderr, end="")
        return result.returncode
    finally:
        logs = compose("logs", "--no-color", profile, check=False)
        save("server.log", logs.stdout + logs.stderr)
        # Compose only removes this project. The guard was not created by compose.
        compose("down", "--volumes", "--remove-orphans")
        guard = docker("inspect", guard_name, check=False)
        if guard.returncode == 0:
            obj = json.loads(guard.stdout)[0]
            labels = obj["Config"]["Labels"]
            if labels.get("skedaddle.test") != run or labels.get("com.docker.compose.project") != "skedaddle-" + run:
                raise RuntimeError("guard ownership mismatch")
            docker("rm", "-f", guard_name)
        remaining = {}
        for kind, args in (("containers", ["ps", "-aq"]), ("volumes", ["volume", "ls", "-q"]),
                           ("networks", ["network", "ls", "-q"])):
            remaining[kind] = docker(*args, "--filter", "label=skedaddle.test=" + run).stdout.split()
        save("cleanup.json", json.dumps(remaining, indent=2))
        if any(remaining.values()):
            raise RuntimeError("own labelled resources remain")


if __name__ == "__main__":
    sys.exit(main())
