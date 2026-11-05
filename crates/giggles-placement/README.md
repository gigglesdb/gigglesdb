# giggles-placement

Local Rust owner controller and JSON-line CLI for **one explicit replacement of
a voter by an already provisioned authenticated learner**. It does not move a
range, change its domain/ownership identity or transaction home, provision a
cloud, force promotion, or qualify cloud availability.

`Plan::create(Request, &mut Owner)` validates the declared base topology and final
replacement with `giggles-topology`, checks transient learner residency, then
binds a fresh leader quorum `current` observation. Request version 1 includes
the topology group, an exact numeric runtime-node mapping, database domain,
membership group/owner epoch, membership intent (id/epoch/learner/remove), and
an explicit `learner_capacity` measurement. The candidate must be a nonmember;
the base must have voters only. Every generated single zone/region/provider
loss and explicit scenario must pass declared voter, dependency and capacity
checks. Missing dependency/capacity knowledge has distinct error codes.
Locality, independence, reachability and supplied capacity remain declarations.

Plans contain canonical input and SHA256 digests, original voters, retained
range descriptor and the observed term/applied frontier. Compare the exact plan
digest when authorizing a change; the digest is an integrity binding, not a
signature or authorization system. Unsupported/missing policy is rejected.

`Controller::open(path, plan)` exclusively opens a durable redb journal bound
permanently to that plan. `step(&mut Owner)` makes a fresh quorum read and at most
one begin/resume call, recording its attempt before issuing it. `Pending` means
call again; `Complete` means a fresh matching replicated receipt and final voter
set were observed. Transport errors are indeterminate: reopen the same journal
and retry the same plan. Never create a new intent to work around a lost reply.
The runtime authenticates learner applied readiness and owns joint consensus.
This library never submits readiness. NoQuorum, StalePlan, IdentityConflict,
IneligibleResidency/Profile, DependencyUnknown/Failed, CapacityUnknown/Failed,
Journal, Runtime, Transport and Limit are distinct outcomes.

## Local CLI

Build the actual runtime separately into this crate's target:

```powershell
$env:CARGO_BUILD_JOBS='1'
$env:CARGO_TARGET_DIR='K:/Projects/gigglesdb/crates/giggles-placement/target'
cargo build --manifest-path crates/giggles-distributed/Cargo.toml --bin giggles-ranges -j1
cargo run --manifest-path crates/giggles-placement/Cargo.toml -- ROSTER.json
```

The owner-trusted roster is `{"processes":{"1":{"binary":"ABSOLUTE giggles-ranges.exe path","config":"ABSOLUTE runtime config path","artifacts":"NEW absolute directory","timeout_ms":12000},"2":{...},"3":{...},"4":{...}}}`.
Use real provisioned runtime Config documents, peer certificate fingerprints and
durable paths. The CLI starts these local processes (two Tokio worker threads
each), retaining stdout/stderr. It owns their lifetime; EOF stops children without
removing any data. Restart with the same runtime configs, new artifact directories,
the same exact plan and journal. Do not launch a second process for a live store.

After the ready/profile line, send JSON lines:

```json
{"command":"plan","request":{"version":1,"topology":{},"topology_group":"data","nodes":{"1":"n1","2":"n2","3":"n3","4":"n4"},"domain":42,"identity":{"group":2,"owner_epoch":1},"intent":{"id":4001,"epoch":1,"learner":4,"remove":3},"learner_capacity":{"node":"n4","available":1,"source":"supplied allocation"}}}
```

Replace `topology` with a complete valid GD23 topology. This illustrative empty
object is intentionally rejected. A successful reply contains the full plan.
Send `{"command":"step","plan":FULL_RETURNED_PLAN,"journal":"ABSOLUTE retained journal.redb"}`
until Complete. Replies have `ok/data` or `ok/error` with a code/message. Elections
can initially return NoQuorum; retry current planning after actual quorum returns.
After an indeterminate timeout, the adapter poisons and stops that process to
prevent an old reply satisfying a new request. Restart its retained config.

The adapter uses current/membership_begin/membership_resume over trusted local
stdin. There is no remote admin endpoint. Absolute config/binary paths, at most
31 processes, 100..30000ms per pipe operation, 2MiB documents/lines, 64MiB retained
output per stream/process, and 100000 journal events bound this profile. Leader
discovery tries the bounded roster sequentially; it never uses inspect as authority.
Journal exhaustion fails closed and needs explicit operator handling; there is
no journal pruning or implicit plan rebind.

## Validation

Run process suites serially: `cargo test --manifest-path crates/giggles-placement/Cargo.toml -j1 -- --test-threads=1`.
Set `GIGGLES_RANGES_BIN` to an absolute explicitly built runtime binary to override
the default owned `target/debug/giggles-ranges.exe`. For release runtime integration,
build the actual runtime with `--release` and set `GIGGLES_RANGES_BIN` to the owned
`target/release/giggles-ranges.exe` before `cargo test --release`. Evidence identifies
both binaries and their source closure. Fixtures use unique retained OS temp paths.
No filesystem cleanup runs, including after failures.

Four local mTLS processes exercise actual learner replacement and joint restart,
committed data/transaction receipt preservation, replay and quorum loss. Model
failure schedules are separate evidence. These tests do not qualify production,
security, cloud independence, physical durability, RTO/RPO or performance.
PostgreSQL comparative CONTROL and external deployment remain deferred.
