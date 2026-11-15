"""GD-0064 M1: one real MySQL variant, with exact-label teardown in finally."""
import datetime
import json
import os
import pathlib
import re
import shutil
import subprocess
import sys


ROOT = pathlib.Path(__file__).resolve().parent


def main():
    profile, task_arg, gtid = sys.argv[1:4]
    spike_arg = sys.argv[4] if len(sys.argv) > 4 else ""
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
               CARGO_TARGET_DIR="J:/gigglesdb-work/conn-target-m1")
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
        sql = ("SELECT @@version,@@gtid_mode,@@binlog_format,@@binlog_row_image,@@binlog_row_metadata; "
               "SELECT COUNT(*) FROM type_zoo.zoo; "
               "SELECT HEX(cv),HEX(JSON_UNQUOTE(JSON_EXTRACT(jv,'$.nested[3]'))),OCTET_LENGTH(wide_row) FROM type_zoo.zoo WHERE id=1;")
        probe = docker("exec", "-e", "MYSQL_PWD=skedaddle", cid, "mysql", "-h127.0.0.1", "-uroot",
                       "--ssl-mode=VERIFY_CA", "--ssl-ca=/var/lib/mysql/harness/tls/ca.pem",
                       "--default-character-set=utf8mb4", "-Nse", sql)
        save("setup-probe.txt", probe.stdout)
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
