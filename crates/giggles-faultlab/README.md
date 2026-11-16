# GigglesDB local TCP fault proxy

This standalone crate is a bounded, opaque byte-stream TCP proxy for local fault
rehearsals: delay, partition, disconnect and restore, per direction, with a
retained event trace. Put one in front of a listener to isolate or degrade a
connection without killing the process behind it.

It used to also drive a three-process rehearsal against the `giggles-node`
replication prototype through the `giggles-client` task SDK (`run`/`check`
binary, `lab`, `report` and node-process control). That prototype and its SDK
were retired after R4's exit evidence (GD-0074 Phase 8), so only the proxy
remains. Replicated `giggles-server` failover is exercised by
`crates/giggles-server/tests/replication_process.rs`.

All domains are **LOCAL simulations on one machine**. Proxy runs do not
establish cloud/AZ independence, availability qualification, production
readiness, security qualification or performance.

```rust,no_run
use giggles_faultlab::proxy::{Direction, Fault, Proxy, ProxyLimits};
# #[tokio::main]
# async fn main() -> giggles_faultlab::Result<()> {
let proxy = Proxy::start("127.0.0.1:9000".parse()?, ProxyLimits::default()).await?;
proxy.set(Direction::Downstream, Fault::Partition)?;
proxy.set(Direction::Downstream, Fault::Pass)?;
let trace = proxy.stop().await;
assert!(!trace.overflow);
# Ok(())
# }
```

The proxy never terminates TLS or reads application frames. It forwards arbitrary
bytes unchanged. `Upstream` means accepted socket → target; `Downstream` means target
→ accepted socket. `Delay` waits before each buffered chunk; a control change
interrupts that wait. `Partition` holds at most one chunk per direction and applies
backpressure until restored. `Disconnect` cuts the entire TCP connection when
either direction requests it and rejects new connections while selected. A
disconnect generation also cuts connections if a quick restore follows it.
Half-close propagates after buffered bytes. Reordering, packet-level loss,
bandwidth shaping, clock/storage faults, and multi-range history checking are
outside this byte-stream proxy profile.

The general proxy library defaults to 4096 retained events. Trace overflow is
reported in the returned trace.

Verification uses one dedicated target directory:

```powershell
cargo fmt --manifest-path crates/giggles-faultlab/Cargo.toml --check
cargo clippy --manifest-path crates/giggles-faultlab/Cargo.toml --all-targets -- -D warnings
cargo test --manifest-path crates/giggles-faultlab/Cargo.toml -- --test-threads=1
cargo test --doc --manifest-path crates/giggles-faultlab/Cargo.toml
```

Proxy tests use binary round trips, half-close, directional controls,
backpressure, admission limits, timeouts and fault sequences.
