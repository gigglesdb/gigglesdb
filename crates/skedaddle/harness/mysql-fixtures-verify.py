"""Verify real MySQL binlog fixture bytes without a server or third-party packages."""
import argparse
import hashlib
import json
import pathlib
import struct
import sys
import zlib


EVENT_TYPES = {
    2: "QUERY_EVENT", 4: "ROTATE_EVENT", 15: "FORMAT_DESCRIPTION_EVENT",
    16: "XID_EVENT", 19: "TABLE_MAP_EVENT", 27: "HEARTBEAT_EVENT",
    30: "WRITE_ROWS_EVENT", 31: "UPDATE_ROWS_EVENT", 32: "DELETE_ROWS_EVENT",
    33: "GTID_EVENT", 34: "ANONYMOUS_GTID_EVENT", 35: "PREVIOUS_GTIDS_EVENT",
    40: "TRANSACTION_PAYLOAD_EVENT",
}
COMMON_TYPES = set(EVENT_TYPES.values()) - {"GTID_EVENT", "ANONYMOUS_GTID_EVENT"}


def require(condition, message):
    if not condition:
        raise ValueError(message)


def verify_event(raw):
    require(len(raw) >= 23, "event shorter than header and CRC32")
    require(struct.unpack_from("<I", raw, 9)[0] == len(raw), "header event size mismatch")
    require(zlib.crc32(raw[:-4]) == struct.unpack_from("<I", raw, len(raw) - 4)[0],
            "CRC32 mismatch")
    return EVENT_TYPES.get(raw[4], f"EVENT_{raw[4]}")


def query_text(raw):
    require(raw[4] == 2 and len(raw) >= 37, "invalid QUERY_EVENT")
    database_len = raw[27]
    status_len = struct.unpack_from("<H", raw, 30)[0]
    start = 32 + status_len + database_len + 1
    require(start <= len(raw) - 4, "query fields exceed event bounds")
    return raw[start:-4].decode("utf8")


def verify_variant(fixtures, mode):
    manifest = json.loads((fixtures / "manifest.json").read_text(encoding="utf8"))
    require(isinstance(manifest, list) and manifest, "empty manifest")
    types, captures, queries = set(), set(), set()
    binlogs = {}
    for path in fixtures.glob("*.binlog"):
        whole = path.read_bytes()
        require(whole[:4] == b"\xfebin", f"{path.name}: invalid binlog magic")
        offsets = set()
        at = 4
        while at < len(whole):
            require(at + 19 <= len(whole), f"{path.name}: truncated header at {at}")
            size = struct.unpack_from("<I", whole, at + 9)[0]
            require(size >= 23 and at + size <= len(whole), f"{path.name}: invalid size at {at}")
            verify_event(whole[at:at + size])
            offsets.add(at)
            at += size
        binlogs[path.name] = (whole, offsets)
    require(len(binlogs) == 1, "expected one isolated workload binlog")
    for entry in manifest:
        capture = entry.get("capture", entry["type"] + ".bin")
        require(pathlib.Path(capture).name == capture and capture.endswith(".bin"),
                "capture must be a local .bin file")
        require(capture not in captures, f"duplicate capture {capture}")
        captures.add(capture)
        raw = (fixtures / capture).read_bytes()
        require(len(raw) == entry["size"], f"{capture}: size mismatch")
        require(hashlib.sha256(raw).hexdigest() == entry["sha256"], f"{capture}: SHA-256 mismatch")
        require(verify_event(raw) == entry["type"], f"{capture}: event type mismatch")
        types.add(entry["type"])
        if entry["offset"] is not None:
            whole, offsets = binlogs[entry["file"] + ".binlog"]
            offset = entry["offset"]
            require(offset in offsets, f"{capture}: offset is not an event boundary")
            require(raw == whole[offset:offset + entry["size"]], f"{capture}: source slice mismatch")
        else:
            require(entry["type"] == "HEARTBEAT_EVENT" and entry["file"] == "replication-wire",
                    f"{capture}: only heartbeat may lack a disk offset")
        if entry["type"] == "QUERY_EVENT":
            queries.add(query_text(raw))
    required = COMMON_TYPES | {"GTID_EVENT" if mode == "ON" else "ANONYMOUS_GTID_EVENT"}
    require(types == required, f"event set mismatch: missing={required - types}, extra={types - required}")
    require("BEGIN" in queries, "missing BEGIN query capture")
    require(any(query.upper().startswith("CREATE TABLE") for query in queries), "missing DDL query capture")
    require({path.name for path in fixtures.glob("*.bin")} == captures, "unmanifested .bin capture")
    tls = json.loads((fixtures / "heartbeat-tls.json").read_text(encoding="utf8"))
    require(tls["verified_hostname"] == "localhost", "heartbeat hostname was not verified")
    require(tls["version"] in ("TLSv1.2", "TLSv1.3"), "unexpected TLS version")
    return len(manifest), sorted(types)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("fixtures_root", type=pathlib.Path)
    args = parser.parse_args()
    count = 0
    for profile in ("mysql80", "mysql84"):
        for mode in ("ON", "OFF"):
            name = f"{profile}-gtid-{mode.lower()}"
            captures, types = verify_variant(args.fixtures_root / name, mode)
            count += captures
            print(f"PASS {name}: {captures} captures; {', '.join(types)}")
    print(f"PASS: 4 variants, {count} captures checked (size, SHA-256, CRC32, source offsets)")


if __name__ == "__main__":
    try:
        main()
    except (ValueError, KeyError, OSError, struct.error) as error:
        print(f"FAIL: {error}", file=sys.stderr)
        sys.exit(1)
