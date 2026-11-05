# GigglesDB consensus task SDK

This standalone Rust crate connects to the actual version-1 consensus native mTLS
task listener. It supports Preview, Execute and Status. It does not expose trusted
operator commands or the different single-host server protocol.

Persist an application-chosen business operation ID **and the exact request** before
the first execute. Storage durability is the application's responsibility. Never
replace that ID following a timeout. Request correlation IDs are separate from
business IDs. Preview is advisory and does not authorize a later execute.

```rust,no_run
use giggles_client::{Client, Config, Operation, Outcome};

async fn execute_persisted(config: Config, persisted: &[u8])
    -> Result<(), Box<dyn std::error::Error + Send + Sync>>
{
    // The caller durably stored these canonical bytes and their business ID.
    let request = giggles_consensus::native::parse(persisted)?;
    let operation = Operation::from_request(request)?;
    let client = Client::new(config)?;
    let outcome = client.send(&operation).await;
    if let Some(status) = outcome.resolution(900, 5000)? {
        match client.send(&status).await {
            Outcome::Recorded(record) if record.is_committed() => {
                // A committed, server-released result for the original operation.
                let _redacted_receipt = record.reply();
            }
            _ => {
                // Retain the ID and request. Absent status is not proof of rollback:
                // a previously submitted proposal can still decide later.
            }
        }
    }
    Ok(())
}
```

`Config` accepts one to three independent, distinct IP socket endpoints, a CA PEM,
client certificate PEM, private key PEM and the TLS server name. Every seed uses
that verified TLS identity. There are no redirects, trust-on-first-use, or insecure
fallbacks. Recreate the client to load replacement credentials. Sessions,
credential epochs, task revocation and all accounting remain server authority.
The SDK never caches a preview or result as permission.

`Limits` bounds connection/handshake time, each request, the whole operation
(including admission and backoff), attempt count and concurrent operations.
Defaults: 1s connect, 5s request, 10s whole operation, 25ms backoff, three attempts,
eight concurrent operations. Request wire deadlines must be 1..=5000ms. There is
one fresh connection per attempt; dropping the future drops the connection and
releases its shared permit without detached transport tasks. A sent operation may
still commit after cancellation.

Execute automatically tries another configured seed only following a transport
failure **before any application write**. It never retries a received error string,
including `unavailable` and `capacity`: the protocol does not provide a distinct
structured predecision result. Such execute errors, response loss, malformed or
oversized responses, and postcommit disclosure denial yield `Unknown` with the
original business ID. Status/preview availability refusals can advance to another
configured seed. No attempt generates a new business ID or changes request bytes.
`TimeoutBeforeSend` establishes that this call sent no application request bytes;
it says nothing about earlier calls with the same ID.

`Recorded` means a server-released result. Inspect its reply explicitly: it can
represent a preview, committed operation, recorded business rejection, or absent
status. `is_committed()` recognizes only a released committed result. Disclosure
exhaustion may hide a committed outcome; it cannot safely be interpreted as an
aborted write. Error/debug formatting hides credentials, request and result data.
The demo CLI prints a released result only when explicitly invoked:

```text
cargo run --manifest-path crates/giggles-client/Cargo.toml --bin giggles-client -- CONFIG.json PERSISTED-REQUEST.json
```

Configuration JSON uses Serde Duration objects (for example `{"secs":1,"nanos":0}`)
when overriding limits. Omit `limits` for defaults. Example shape:

```json
{"endpoints":["127.0.0.1:7443"],"ca":"task-ca.pem","certificate":"client.pem","key":"client.key.pem","server_name":"localhost"}
```

Local tests separate scripted transport models, real mTLS endpoints, and an actual
three-process consensus cluster. Build `giggles-node` into this crate's own target
directory before cluster tests; `GIGGLES_NODE_EXE` can select an explicit binary.
Fixtures retain unique temporary directories and process logs for investigation.
The cluster tests are local correctness evidence, with no cloud, performance,
security audit or production qualification claim. PostgreSQL comparison remains
deferred. The root project and all other crates remain outside this crate's scope.
