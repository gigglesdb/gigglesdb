# giggles-transactions

Bounded rigorous locking and recoverable replicated transaction state machines
for database runtime implementers.

**Status:** Provides deterministic locking and committed-state transitions; it is an implementation dependency, not a consensus or storage engine.

Part of [GigglesDB](../../README.md).

## Usage

Create a `Machine` with a database/range/epoch `Domain` and `Limits`. Apply only
entries committed by the exact consensus group. Persist the resulting snapshot
atomically with the applied index and external effects before returning replies.
On persistence failure, discard tentative state and replay from durable state.
See [the embedding contract](src/lib.rs) for the complete requirements.

## Limits

The embedding runtime must authenticate snapshots, fence rollback, cover reads
and writes with the required locks, and resolve evidence through committed state.
Checksums detect corruption; they do not authenticate input. Prepared work cannot
time out or abort unilaterally. Terminal records remain until admission caps
reject new work; this library supplies no garbage collection or epoch migration.

The [distributed transaction release notes](../../docs/release-notes/R5.md)
describe the integrated server behavior. They are separate from this library's
locking and persistence contract.
