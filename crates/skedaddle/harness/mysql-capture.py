"""Capture real server binlogs independently of either Rust candidate.

Heartbeat events only exist on the replication wire and are captured over verified TLS.
Run through run.ps1 -MysqlSpike mysql-capture.cmd; --output overrides the output directory.
"""
import argparse
import hashlib
import runpy
import json
import os
import pathlib
import struct
import subprocess
import socket
import ssl

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--output", type=pathlib.Path,
                    default=pathlib.Path(os.environ["M1_OUTPUT"]) / "disk-fixtures")
OUT = parser.parse_args().output.resolve()
if not OUT.as_posix().lower().startswith("j:/gigglesdb-work/"):
    parser.error("output must be below J:/gigglesdb-work; copy verified test data into the repository afterwards")
OUT.mkdir(parents=True, exist_ok=False)
checker = runpy.run_path(str(pathlib.Path(__file__).with_name("mysql-fixtures-verify.py")))
verify_event = checker["verify_event"]
query_text = checker["query_text"]
CID = os.environ["M1_CONTAINER_ID"]


def sql(statement):
    args = ["docker", "exec", "-i", "-e", "MYSQL_PWD=skedaddle", CID, "mysql",
            "-h127.0.0.1", "-uroot", "--ssl-mode=VERIFY_CA",
            "--ssl-ca=/var/lib/mysql/harness/tls/ca.pem", "--default-character-set=utf8mb4", "-NB"]
    p = subprocess.run(args, input=statement.encode(), capture_output=True, check=True, timeout=30)
    return p.stdout.decode("utf8")


def heartbeat_fixture(status):
    """Independent TLS wire capture: heartbeats are not stored in binlog files."""
    def packet(stream):
        def exact(n):
            data = bytearray()
            while len(data) < n:
                part = stream.recv(n - len(data))
                if not part:
                    raise EOFError("replication socket closed")
                data.extend(part)
            return bytes(data)
        header = exact(4)
        size = int.from_bytes(header[:3], "little")
        assert size < 32 * 1024**2
        return exact(size)

    def send(stream, seq, body):
        stream.sendall(len(body).to_bytes(3, "little") + bytes([seq]) + body)

    tcp = socket.create_connection(("127.0.0.1", int(os.environ["M1_PORT"])), timeout=5)
    hello = packet(tcp)
    assert hello[0] == 10
    at = hello.index(b"\0") + 1 + 4
    extra = at + 8 + 1 + 2 + 1 + 2 + 2 + 1 + 10
    salt = hello[at:at+8] + hello[extra:extra+12]
    assert hello[extra+13:].rstrip(b"\0") == b"caching_sha2_password"
    flags = 1 | 4 | 512 | 2048 | 8192 | 32768 | 131072 | 524288
    prefix = struct.pack("<IIB", flags, 32 * 1024**2, 255) + bytes(23)
    send(tcp, 1, prefix)
    tls = ssl.create_default_context(cafile=os.environ["M1_CA"]).wrap_socket(tcp, server_hostname="localhost")
    try:
        first = hashlib.sha256(b"skedaddle").digest()
        second = hashlib.sha256(first).digest()
        third = hashlib.sha256(second + salt).digest()
        token = bytes(a ^ b for a, b in zip(first, third))
        send(tls, 2, prefix + b"migration\0" + bytes([len(token)]) + token + b"caching_sha2_password\0")
        reply = packet(tls)
        if reply == b"\x01\x04":
            send(tls, 4, b"skedaddle\0")
            reply = packet(tls)
        elif reply == b"\x01\x03":
            reply = packet(tls)
        assert reply[0] == 0, reply
        for setting in ("SET @master_binlog_checksum='CRC32'", "SET @master_heartbeat_period=200000000"):
            send(tls, 0, b"\x03" + setting.encode())
            reply = packet(tls)
            assert reply[0] == 0, reply
        file, pos, *_ = sql(status).strip().split("\t")
        send(tls, 0, b"\x12" + struct.pack("<IHI", int(pos), 0, 6402) + file.encode())
        sql("FLUSH BINARY LOGS")
        for _ in range(50):
            reply = packet(tls)
            assert reply[0] == 0, reply
            event = reply[1:]
            if event[4] == 27:
                (OUT / "HEARTBEAT_EVENT.bin").write_bytes(event)
                (OUT / "heartbeat-tls.json").write_text(json.dumps({"cipher": tls.cipher(), "version": tls.version(), "verified_hostname": "localhost", "file": file, "start_pos": int(pos)}), encoding="utf8")
                return {"type": "HEARTBEAT_EVENT", "file": "replication-wire", "offset": None,
                        "size": len(event), "sha256": hashlib.sha256(event).hexdigest()}
        raise AssertionError("no heartbeat event")
    finally:
        tls.close()


# Historical fixtures use their original control-table layout, independently of installation.
sql("CREATE TABLE _skedaddle.watermark (id BIGINT PRIMARY KEY, marker VARCHAR(128) NOT NULL) ENGINE=InnoDB")
# Isolate the workload from entrypoint timezone/system-table initialization.
sql("FLUSH BINARY LOGS")
capture_file = sql("SHOW BINARY LOGS").splitlines()[-1].split("\t")[0]
columns = sql("SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA='type_zoo' AND TABLE_NAME='zoo' ORDER BY ORDINAL_POSITION").splitlines()
status = "SHOW BINARY LOG STATUS" if os.environ["M1_PROFILE"] == "mysql84" else "SHOW MASTER STATUS"
settings = sql("SELECT @@version,@@gtid_mode,@@binlog_format,@@binlog_row_image,@@binlog_row_metadata").strip().split("\t")
assert settings[1:] == [os.environ["M1_GTID"], "ROW", "FULL", "FULL"], settings
zoo = sql("SELECT COUNT(*),SUM(nullable_v IS NULL),MIN(OCTET_LENGTH(wide_row)),MAX(OCTET_LENGTH(wide_row)) FROM type_zoo.zoo").strip()
assert zoo == "1\t1\t20480\t20480", zoo
image = subprocess.run(["docker", "inspect", "--format", "{{.Config.Image}} {{.Image}}", CID],
                       capture_output=True, check=True, timeout=30).stdout.decode().strip()
metadata = ("profile=" + os.environ["M1_PROFILE"] + "\ngtid=" + os.environ["M1_GTID"] +
            "\nimage=" + image + "\nsettings(version,gtid,format,row_image,row_metadata)=" +
            "\t".join(settings) + "\nzoo(rows,nulls,min_wide_bytes,max_wide_bytes)=" + zoo + "\n" +
            sql("SELECT @@version,@@gtid_executed,@@gtid_purged,@@server_uuid; SHOW BINARY LOGS; " + status))
(OUT / "metadata.txt").write_text(metadata, encoding="utf8", newline="\n")
selection = ",".join("2" if c == "id" else "`" + c + "`" for c in columns)
workload = f"""
SET SESSION binlog_transaction_compression=OFF;
CREATE TABLE type_zoo.capture_ddl (id BIGINT PRIMARY KEY) ENGINE=InnoDB;
START TRANSACTION;
INSERT INTO type_zoo.zoo SELECT {selection} FROM type_zoo.zoo WHERE id=1;
UPDATE type_zoo.zoo SET vv=CONVERT(0x7570646174656420f09f9880 USING utf8mb4),nullable_v='nonnull' WHERE id=2;
DELETE FROM type_zoo.zoo WHERE id=2;
INSERT INTO _skedaddle.watermark VALUES(99,'capture-multi-statement');
COMMIT;
SET SESSION binlog_transaction_compression=ON;
START TRANSACTION;
INSERT INTO type_zoo.zoo SELECT {selection} FROM type_zoo.zoo WHERE id=1;
DELETE FROM type_zoo.zoo WHERE id=2;
COMMIT;
FLUSH BINARY LOGS;
"""
(OUT / "workload.sql").write_text(workload, encoding="utf8", newline="\n")
sql(workload)
seen = set()
manifest = []
for line in sql("SHOW BINARY LOGS").splitlines()[:-1]:
    name = line.split("\t")[0]
    if name != capture_file:
        continue
    raw = subprocess.run(["docker", "exec", CID, "cat", "/var/lib/mysql/harness/data/" + name], capture_output=True, check=True, timeout=30).stdout
    assert raw[:4] == b"\xfebin"
    (OUT / (name + ".binlog")).write_bytes(raw)
    at = 4
    while at < len(raw):
        assert at + 19 <= len(raw), "truncated event header"
        size = struct.unpack_from("<I", raw, at + 9)[0]
        assert size >= 19 and at + size <= len(raw)
        event = raw[at:at + size]
        kind = verify_event(event)
        capture = kind + ".bin"
        if kind == "QUERY_EVENT":
            query = query_text(event)
            if query == "BEGIN":
                capture = "QUERY_EVENT.bin"
            elif query.upper().startswith("CREATE TABLE"):
                capture = "QUERY_DDL_EVENT.bin"
            else:
                raise AssertionError("unexpected query: " + query)
        if capture not in seen:
            (OUT / capture).write_bytes(event)
            manifest.append({"type": kind, "capture": capture, "file": name, "offset": at, "size": size,
                             "sha256": hashlib.sha256(event).hexdigest()})
            seen.add(capture)
        at += size
manifest.append(heartbeat_fixture(status))
verify_event((OUT / "HEARTBEAT_EVENT.bin").read_bytes())
(OUT / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf8", newline="\n")
count, types = checker["verify_variant"](OUT, os.environ["M1_GTID"])
print(f"captured and verified {count} real event captures ({len(types)} types), including BEGIN, DDL and verified-TLS wire heartbeat")
