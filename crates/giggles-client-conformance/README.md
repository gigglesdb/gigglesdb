# GigglesDB public SDK local conformance harness

Runs real loopback mTLS fault scripts through the public `giggles-client` API.

```powershell
$env:CARGO_BUILD_JOBS=1
cargo run --manifest-path crates/giggles-client-conformance/Cargo.toml --target-dir crates/giggles-client-conformance/target -j1 -- baseline 42
cargo run --manifest-path crates/giggles-client-conformance/Cargo.toml --target-dir crates/giggles-client-conformance/target -j1 -- inner-id 42
```

The CLI accepts exactly a scenario and u64 seed. `--list` prints supported scripts.
It prints a retained JSON report path; fixtures and test certificates remain in a
unique system temp directory. No cleanup runs. Reports contain fixed outcome
classes, operation IDs and observed counters, never raw requests, peer payloads,
credentials or keys. Generated test keys remain in fixture files.

Validation checks each scenario's expected outcome reason and observed script
boundary as well as request counts. An incidental transport failure cannot stand
in for a protocol rejection or deadline; a failed peer response write fails the verdict.

`run(Scenario, seed)` is reusable from Rust. Call `Report::validate` for shared
at-most-once/identity invariants and `Report::retain` to write JSON exclusively.
Seed controls operation identity and fault offsets. Certificates, ports and task
scheduling vary. Unsupported scenario names or external targets are rejected.

```no_run
# fn main() -> Result<(), Box<dyn std::error::Error>> {
use giggles_client_conformance::{Scenario, run};
let runtime = tokio::runtime::Runtime::new()?;
let report = runtime.block_on(run(Scenario::LostReplyStatus, 42))?;
let path = report.retain()?;
report.validate()?;
println!("{}", path.display());
# Ok(())
# }
```

Retain before validating to preserve a falsifying transcript. The CLI follows
this order and returns exit code 2 on invalid arguments or a failed invariant.
The `lost-reply-status` script sends the public SDK resolution query and verifies
one Execute and one Status with the original ID against the scripted peer.

Always persist the original operation ID and exact request before execution.
Unknown retains that ID; construct status resolution without inventing a new ID.
An absent status never proves rollback or that a pending operation cannot commit.
Execute failover is permitted only before application write; a server availability
string does not prove a predecision refusal.

Partial request scripts report exactly how many bytes the peer consumed. Kernel
and TLS buffering can already hold later bytes. These observations cannot prove
the client failed during prefix/body write or flush. The write-pressure script
withholds reads from a bounded frame; its observed outcome is reported without
claiming a deterministic write timeout. No public SDK transport seam is available.
Scripted committed replies do not prove durable database commit. Existing SDK
three-process integration is separate. This is no cloud, security, performance or
production qualification, and no PostgreSQL comparator or source migration.
