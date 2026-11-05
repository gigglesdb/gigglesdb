# Single-host service preview (P2)

Rust server and native client over the existing giggles-core/redb database. This
is local implementation evidence, not production, cloud, ORM or general
PostgreSQL compatibility qualification. Designs: [GD-0007](https://app.archdocs.io/design-docs/bcc8c9b3-54d8-4f78-aeba-e64771af999c)
and [GD-0008](https://app.archdocs.io/design-docs/ea3a9a40-18d9-48a1-aa6b-67e2f19f2847).
See [Design documentation](https://app.archdocs.io/design-docs/5850ae99-0d51-403f-9201-d9735687a000), [COMPATIBILITY.md](COMPATIBILITY.md) and
[SECURITY.md](SECURITY.md).

## Run using generated local fixtures

From this crate directory:

```powershell
cargo test --test network executable_server_starts_and_native_client_connects -- --nocapture
```

The output names a unique retained `artifacts/network-fixture-*` directory. It
contains `db.redb`, `server.TEST-ONLY.json`, generated CA/client/server PEMs and
private keys named `*.key.TEST-ONLY`. These are disposable local demonstration
identities, never deployment credentials. Keep the artifact directory; no cleanup
is required. Tests start real TLS sockets and terminate their own subprocesses.
Paths in the fixture configuration are relative to this crate directory.

With the test finished, use its printed directory in these commands:

```powershell
cargo run --bin giggles-server -- artifacts/NETWORK-FIXTURE/server.TEST-ONLY.json
```

The fixture configuration binds native to `127.0.0.1:7432` and pgwire to
`127.0.0.1:7433`. Startup prints actual addresses. If those ports are occupied,
edit a copy of the configuration to unused loopback ports. The database must
already exist. The server never creates or silently replaces a missing database.
Ctrl-C stops listeners and drops staged transactions. A synchronous core decision
already started may finish before shutdown.

Create a local request file containing `{"kind":"Ping"}` and run:

```powershell
cargo run --bin giggles-client -- 127.0.0.1:7432 localhost artifacts/NETWORK-FIXTURE/app.pem artifacts/NETWORK-FIXTURE/app.key.TEST-ONLY artifacts/NETWORK-FIXTURE/ca.pem REQUEST.json
```

Native protocol: one big-endian u32 payload length followed by UTF8 JSON. Responses
are framed the same way. Request examples:

```json
{"kind":"Sql","sql":"INSERT INTO app (id,body) VALUES ($1,$2)","params":[{"Int":9},{"Text":"hello"}]}
```

```json
{"kind":"Status","operation":700}
```

```json
{"kind":"RepairSql","operation":701,"sql":"UPDATE deliveries SET status = 'queued' WHERE tenant = 42 AND id = 7","approval":null}
```

Finite repair SQL requires whitespace around operators; it is the core's existing
repair parser, not generic application SQL. Preview and Execute use the existing
typed repair Request. Approval IDs come from a separate trusted local approver.
Remote Preview returns only count/digest; an approver independently obtains the
exact full preview through trusted local APIs. Network clients cannot approve.

An optional `{"kind":"Deadline","budget_ms":0,"request":{...}}` cancels
before dispatch and returns 57014. Budgets are relative durations, maximum 10000ms,
starting after a complete frame is read; frame reading has its own 10s timeout.
One envelope only. The deadline is checked after parsing and waiting for the task
gate, immediately before entering the synchronous operation. It cannot interrupt
an operation that has entered core. Native clients do not automatically retry.

To use pgwire, use `tokio-postgres = "=0.7.15"` with
`tokio-postgres-rustls = "0.13"`, require TLS, load the generated CA and app client
certificate/private key, use TLS hostname `localhost`, startup user/database
`giggles`, and port7433. These startup names never select authority; the verified
leaf fingerprint does. An executable integration example is in `tests/network.rs`
(`Fixture::pg`). No password is needed or accepted. Server version14.0 is a wire
parameter for driver negotiation, not a PostgreSQL implementation claim.

Task wire uses simple-query `GIGGLES ` followed by exactly one native JSON request
for preview/execute/repair/status (optionally Deadline). It returns one TEXT JSON
column named `decision`. The same structured code/count/digest/operation response
is returned natively. Task SQL never obtains a generic SqlSession.

## Verification and retained evidence

`./verify.ps1 -ArtifactDirectory artifacts/UNIQUE-NAME` runs fmt, warning-free
Clippy, all debug/release targets and doctests, retaining commands, stdout/stderr
and SHA256 input manifests. Use a fresh artifact directory each time. Tests retain
their databases, generated test-only certificates, seeded histories and malformed
wire traces. `--nocapture` on a targeted run prints fixture paths. Existing core
and FFI verification scripts and actual C clients must also pass after core edits.

Official sources consulted: [PostgreSQL protocol flow](https://www.postgresql.org/docs/17/protocol-flow.html),
[message formats](https://www.postgresql.org/docs/17/protocol-message-formats.html),
[rustls client verification](https://docs.rs/rustls/0.23.46/rustls/server/struct.WebPkiClientVerifier.html),
and the pinned tokio-postgres source for START TRANSACTION and RowDescription.
