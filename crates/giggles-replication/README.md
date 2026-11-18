# giggles-replication

Raft replication, transport, snapshots and membership for the GigglesDB v2 engine.
Server embedders use this crate alongside storage owned by `giggles-core`.

**Status:** Supports three-voter replication and range-group infrastructure; deployment and transaction limits depend on the configured server mode.

Part of [GigglesDB](../../README.md).

## Usage

Configure replication through `giggles-server`. The [replication release notes](../../docs/release-notes/R4.md)
cover three-voter configuration, peer mTLS, leader connections, lost replies and
voter replacement. The [distributed transaction release notes](../../docs/release-notes/R5.md)
cover range-group clusters and current isolation behavior.

## Details

The library owns the Raft log, node driver, transport, snapshots and membership.
The [module list](src/lib.rs) also exposes routing, range split/move and cluster
administration components. Storage remains in `giggles-core`.

All voters must run the same build. The single-group mode serves SQL only on
the leader; range-group clusters have different routing behavior. Consult the
release notes for the selected mode before deployment.
