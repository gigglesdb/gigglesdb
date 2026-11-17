# Cluster exit evidence kit

Use Python 3.11+ with psycopg 3 and OpenSSL on PATH. Generate a local cluster:

```bash
python scripts/cluster-exit/kit.py init --local --nodes 3 --root /absolute/path/cx --binary /absolute/path/giggles-server
python scripts/cluster-exit/agent.py --cluster /absolute/path/cx/cluster.json --node 1
# Run an agent for nodes 2 and 3 in two more terminals.
```

The agents start servers when a driver calls `Cluster.reset(mode)`. Modes are
`ranges`, `single_range`, and `replication`. Configs, private test PKI and logs
live under the cluster root; use disposable storage. Each remote node runs one
agent, using the same manifest and paths on every host.

Drivers use `--cluster /absolute/path/cx/cluster.json --out /absolute/path/out`.
`anomaly.py` checks scheduled transactions; `history.py` records transfer
histories for `giggles-history check`. Run each driver's `--help` for its
options. PostgreSQL self-checks use `--target pg --dsn DSN` where supported.

The coordinator writes a plain bash suite, for example:

```bash
"$PY" "$REPO/scripts/cluster-exit/anomaly.py" --cluster "$CX" --out "$OUT"
```

Then run `scripts/cluster-remote.sh --tip COMMIT --who r5-p12 --nodes 3
--client-box --suite suite.sh` (one command). Use a separate client for fewer
than six nodes; six nodes share node 1. `--kit --split-size-bytes 1048576`
passes optional kit settings. `--keep-going` records failed suite commands
and continues where bash permits; explicit `exit` and suite shell control
flow still apply. Suites must propagate driver failures and avoid printing
credentials. The runner provides CX, OUT, REPO, PY, CHECKER, JDBC_JAR and NODES.

Only coordinators run the remote runner. It reserves up to six of the ten
account slots, enforces the 200-start daily cap and a TTL of at most two hours,
collects metadata/RTTs and partial logs, then deletes its boxes. Results are
under `J:/gigglesdb-work/boat/cluster/<utc>-<tip12>-<who>-<pid>/`.
`cluster-remote-box.sh --dry-run firewall 2001:db8::1`, `--dry-run rtt
2001:db8::1`, and `--dry-run suite` exercise parsing without a box.
