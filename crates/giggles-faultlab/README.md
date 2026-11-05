# GigglesDB local fault laboratory

This standalone crate runs a bounded single-range rehearsal against three real
`giggles-node` processes using the public `giggles-client` SDK. The default run
isolates the elected leader's synthetic domain, observes a successor, removes
quorum, restores connectivity, reconciles every operation by status, deliberately
replays its original action, and checks durable receipts and actual task effects.
The library also supports follower-domain loss with `lab::run_with_loss(..., false)`.

All domains are **LOCAL simulations on one machine**. These runs do not establish
cloud/AZ independence, availability qualification, production readiness, security
qualification, performance, or sustained service RTO. PostgreSQL CONTROL and
external migrations are outside this tool's scope.

From the repository root in PowerShell:

```powershell
$env:CARGO_TARGET_DIR = "$PWD/target-faultlab"
$env:CARGO_BUILD_JOBS = '1'
cargo build --manifest-path crates/giggles-consensus/Cargo.toml --bin giggles-node -j1
cargo run --manifest-path crates/giggles-faultlab/Cargo.toml -- run "$PWD/target-faultlab/debug/giggles-node.exe" 2
# The preceding command prints an absolute retained report path.
cargo run --manifest-path crates/giggles-faultlab/Cargo.toml -- check 'C:/Users/you/AppData/Local/Temp/giggles-faultlab-run-.../report.json'
```

`run NODE_EXE [SAMPLES]` accepts 1–8 samples for each of four phases. `check REPORT`
returns nonzero for inconsistent summaries, unresolved ambiguity, mismatched
identities, changed replay results, wrong receipt digests/effect counts, missing
fault records, incomplete traces, or incomplete replica catch-up. It checks
internal evidence consistency; it does not authenticate a report's origin.

Each run exclusively creates a unique directory under `std::env::temp_dir()`.
It retains node configurations, locally generated credentials, databases, owned
process IDs, bounded stdin/stdout/stderr logs, immutable SDK operations, immediate
proxy JSONL events, all SDK observations, and the final JSON report. Failed runs
retain artifacts and an error where available. Files are never automatically
cleaned up. Teardown kills and waits only on spawned child handles. These are
synthetic fixture credentials and data; protect artifact access as appropriate.

`Recorded` retains the actual inner reply returned by the SDK. The SDK checks the
outer wire correlation ID internally but does not expose that envelope. The
report labels its request association `sdk_validated_correlation_id`; it is not
a raw packet capture. Unknown observations remain Unknown after a separate status
observation resolves them. An absent status is a point-in-time observation, not a
claim that a timed-out request can never apply. Explicit identical replays remain
visible and are checked against one durable receipt per business operation.

The proxy never terminates TLS or reads application frames. It forwards arbitrary
bytes unchanged. `Upstream` means accepted socket → node; `Downstream` means node
→ accepted socket. `Delay` waits before each buffered chunk; a control change
interrupts that wait. `Partition` holds at most one chunk per direction and applies
backpressure until restored. `Disconnect` cuts the entire TCP connection when
either direction requests it and rejects new connections while selected. A
disconnect generation also cuts connections if a quick restore follows it.
Half-close propagates after buffered bytes. Reordering, packet-level loss,
bandwidth shaping, clock/storage faults, and multi-range history checking are
outside this byte-stream proxy profile.

The scenario uses six source-specific peer proxies and three SDK proxies. Each
allows 16 connections, two 4096-byte buffers per connection, 16384 retained
events, 1 second connect timeout, 10 seconds idle timeout, and 30 seconds maximum
connection lifetime. Nine proxies therefore permit at most 144 active connections
and 1,179,648 application buffer bytes; OS/TLS/node buffers and event metadata are
additional. The general proxy library defaults to 4096 events; the scenario uses
16384 because the maximum 8-sample run includes substantial peer mTLS traffic.
Trace overflow or log capture failure prevents successful validation.
Node control lines are capped at 1 MiB, each input/output log at 16 MiB (stderr
retains at most one extra byte to detect overflow), the output channel at 128
messages, and encoded/parsed reports at 32 MiB. The scenario deadline is 180 seconds;
individual process control and readiness waits have independent bounded timeouts.
Actual node controls are small and trusted; this is not a sandbox for arbitrary
malicious child executables or descendants inheriting pipes.

Latencies use monotonic microseconds around each SDK call. Phase distributions
include every attempt and outcome, including status and replay calls during
Restored. They use nearest-rank p50/p95/p99 over small closed-loop samples, with
raw start/end values retained. Election waiting is visible as gaps between calls;
the distributions are not election-time or sustained-recovery metrics. Replicas
must all report applied indices at least the committed final inspection barrier.

Report format 2 uses one monotonic clock origin for proxy events and SDK calls.
Validation binds active faults to each call, raw catch-up diagnostics to the
inspection barrier, final rows to receipt after-images, and every original
operation to a restored replay. Format 1 reports remain historical artifacts;
`check` rejects them because their separate clock origins cannot establish this
timing contract. Capture a new campaign to obtain format 2 evidence.

Verification uses one dedicated target directory:

```powershell
$env:GIGGLES_NODE_EXE = "$PWD/target-faultlab/debug/giggles-node.exe"
cargo fmt --manifest-path crates/giggles-faultlab/Cargo.toml --check
cargo clippy --manifest-path crates/giggles-faultlab/Cargo.toml --all-targets -- -D warnings
cargo test --manifest-path crates/giggles-faultlab/Cargo.toml -- --test-threads=1
cargo test --release --manifest-path crates/giggles-faultlab/Cargo.toml -- --test-threads=1
cargo test --doc --manifest-path crates/giggles-faultlab/Cargo.toml
$env:RUSTDOCFLAGS = '-D warnings'
cargo doc --manifest-path crates/giggles-faultlab/Cargo.toml --no-deps
```

Real process tests require the node build above; missing binaries fail explicitly.
Fixtures retain all artifacts. Proxy tests use binary round trips, half-close,
directional controls, backpressure, admission limits, timeouts and fault sequences.
Report tests mutate real and synthetic evidence to establish rejection controls.
