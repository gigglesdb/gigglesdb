# giggles-topology

A Rust library and CLI for contributors analyzing declared topology, placement and dependencies under failures.

**Status:** Supports static, bounded topology analysis; reports do not prove actual availability or safe membership transitions.

Part of [GigglesDB](../../README.md).

A standalone Rust library and CLI for bounded, versioned declarative topology analysis.
It evaluates static full-data memberships, placement policy, declared dependency paths,
and optional supplied additive capacity under domain failures. Reports do not prove
actual availability, independence, durability, recovery time, or safe membership transitions.

## Quick start

```powershell
cargo run --manifest-path crates/giggles-topology/Cargo.toml -- validate crates/giggles-topology/tests/fixtures/three-providers.json
cargo run --manifest-path crates/giggles-topology/Cargo.toml -- analyze crates/giggles-topology/tests/fixtures/three-providers.json loss:provider:a
```

Commands: `validate FILE`, `inspect FILE`, `export FILE`, `analyze FILE [SCENARIO-ID]`.
Inspect/export emit the same canonical, round-trippable JSON. Analyze without a scenario
emits the baseline, every declared zone/region/provider loss, and each explicit simultaneous
failure set. `baseline` and the `loss:` scenario prefix are reserved. Input/output is JSON;
errors go to stderr. Exit codes: `0` valid input (validate/export/inspect), or all analyzed
checks pass; `2` invalid input/usage/I/O; `3` at least one known scenario check fails;
`4` incomplete analysis when no known check fails. Analysis code 0 is a declarative result.

The product fixture illustrates the full version-1 schema. Its capacity values and
endpoint assumptions are synthetic examples solely for behavior tests, not benchmarks.
Every top-level collection is required. Unknown fields, enum values and versions are
rejected. IDs are case-sensitive ASCII letters/digits or `_ - . :`, 1–128 bytes, unique
within their typed namespace. All references must resolve. Provider/region/zone/node
hierarchies use unique global IDs within each type; missing locality is invalid.

Each group lists full-data voters and learners. Majority is `floor(voters/2)+1`; learners
have zero votes. A group needs at least one voter. Empty residency/provider allowlists
are rejected; allowlists apply to every replica, including learners. The generic
`majority` profile accepts arbitrary stable memberships. `three_independent_providers`
requires a 1/1/1 voter layout; `five_voters221` requires 2/2/1. Both require unique voter
zones, at least three regions, and explicit distinct nonempty provider independence
identities. Those identities assert independence; they do not verify corporate, IAM,
network or administrative isolation. Region labels alone establish no provider independence.
The profiles admit extra learners subject to the same residency policy. Witnesses and
joint configurations have no version-1 representation. Analyze role changes as separate
static snapshots; the tool does not authorize or validate a transition protocol.

Dependencies form a named DAG. Use `endpoint` with explicit failure-domain `bindings`
and `assumptions`, `all` with nonempty child IDs, `any` with nonempty child IDs, or
`unknown` with a reason. Endpoint bindings are AND conditions: any lost binding loses
the endpoint. Node binding includes its zone, region and provider losses; zone includes
region/provider, region includes provider. A provider-bound endpoint declares that
smaller domain losses do not affect it; bind specific nodes/zones where applicable.
Model shared networks, issuers and key roots as shared endpoint dependencies with all
relevant bindings. Different provider identities are distinct scenario domains even
if their declared independence identities coincide; such providers cannot satisfy the
independent profiles. Add a simultaneous failure set for known correlated providers.
No runtime connectivity, credential freshness, DNS caching or cold-start state is inferred.

Each group declares one dependency root per required category: `application`, `gateway`,
`bootstrap_discovery`, `auth`, `kms`, `dns`. Missing categories yield `unknown`. Unknown
references are invalid; unknown knowledge uses the explicit `unknown` expression.
AND fails if any child fails; otherwise unknown propagates. OR passes if any child
passes; otherwise unknown propagates. The scenario's dependency table exposes every
expression and child IDs without expanding shared graphs exponentially. Combine each
application path with the services it actually needs; an OR across endpoints alone
declares those endpoints independently usable.

Capacity is separate from quorum and dependencies. If supplied, it requires integer
units, demand, per-member available allocations with measurement provenance, and explicit
assumptions (including shared resource allocation, reserve and recovery work). All supplied
values use the single declared unit. Surviving node allocations are added; zero is explicit.
A missing surviving member measurement yields `not_evaluated`; absent capacity does too.
There is no inference of throughput, latency, bottlenecks, elastic scale or N−1 headroom
beyond those supplied values. The snapshot export retains measurement sources. Checked
totals reject overflow rather than wrap. Dependency failure does not suppress the
independent capacity or voter arithmetic results.

Bounds: 1 MiB JSON, 256 entries per top-level collection/membership/reference list,
2048 dependency edges, 32 dependency vertices per path, and 4096 bytes per nonblank
assumption/provenance/reason/unit. The parser also enforces its JSON recursion limit.
The total analysis-cost estimate must fit 64 MiB: scenario count times (eight times
canonical input bytes plus 128 KiB per group). This conservative work bound can reject
an individually valid large collection; it is not a measured output-size prediction.
Cycles are rejected even when unreachable from a group. Typed input goes through the
same validation and canonical serialized size bound. Reports and exports sort all
order-insensitive declarations; sorted JSON maps and explanations make output deterministic.

Adapters can use `Validated::from_json`, or construct public `Topology` types and call
`Validated::new`. Its immutable snapshot offers `topology`, `export_json`, `scenarios`,
`analyze`, and `analyze_scenario`. The latter accepts explicit unions of provider/region/
zone/node failures. Serializable report version 1 separates voter arithmetic, declared
dependency reachability, and supplied capacity for each group. No runtime adapters are included.
