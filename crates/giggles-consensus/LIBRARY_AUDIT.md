# Pinned Raft integration audit

Selected `raft = =0.7.0`, `raft-proto =0.7.0`, protobuf2.28.0, Apache-2.0.
Cargo.lock fixes package checksums and all transitive versions. This crate uses the
library's consensus algorithm; no replacement majority protocol was written.

Primary sources examined before integration:

- [TiKV raft-rs upstream](https://github.com/tikv/raft-rs), including its description
  of the storage/state-machine/transport boundaries.
- [Pinned 0.7.0 API and Ready processing contract](https://docs.rs/raft/0.7.0/raft/).
- Local Cargo-registry `raft-0.7.0/src/raw_node.rs`, `storage.rs`, `raft.rs`,
  `config.rs` and the actual generated protobuf API used at compilation.
- [OpenRaft log storage contract](https://docs.rs/openraft/0.9.25/openraft/storage/trait.RaftLogStorage.html)
  and state-machine/snapshot contracts were evaluated as an alternative.

The upstream primary GitHub commits API, queried November5,2026, reported commit
[`ad13f3d90780f53aea2488c6a4b76c0d334bf136`](https://github.com/tikv/raft-rs/commit/ad13f3d90780f53aea2488c6a4b76c0d334bf136)
dated May13,2026 (unstable-entry buffer change). This establishes visible upstream
maintenance activity. It does **not** mean pinned0.7.0 includes that change or
that we completed a release/security/upgrade audit. ## Rationale and boundary

RawNode gives explicit tick, message and Ready inputs/outputs, making one driver
usable for deterministic scheduling and actual subprocesses. The storage contract
is small enough to inspect and exercise independently. OpenRaft's asynchronous
traits/suite are viable; no claim that raft-rs is generally better is made.

Ready hard state, entries and snapshots must reach stable storage before persisted
messages. This adapter conservatively saves before **all** messages. Commit index
must be persisted with/before application; Ready and LightReady paths publish it
with indexed kernel state. A second core disk WAL is intentionally absent.
ReadIndex uses `ReadOnlyOption::Safe`, with explicit applied-index verification;
no clock lease enables minority reads. Normal command entries only are accepted.
The library supports joint membership; this bounded profile exposes no change API,
rejects configuration entries and checks exact fixed membership in snapshots.

## Local source fingerprints (SHA256)

| raft0.7.0 source | SHA256 |
| --- | --- |
| raw_node.rs | 92CBCA1FA82288AD8CCECBC74066BB766EA38B5FFCE4ED89A0D1E0F174DDB057 |
| storage.rs | 670EBB2944449EAB9EEE8C1318F0885BAACA3AB103F9E82E6C88B451ED63588C |
| raft.rs | A595CC7420EB83F49546253D004EBC9657E255581DB467376A13CF02A383A8EB |
| config.rs | 4E2448D6B587B0D695011548A476CCF974C8E17C200213812ECB1C5FC0631DB9 |

## Conformance evidence and limits

Local tests call the actual Storage API for ordered ranges, unavailable/compacted
entries, size limits, snapshot term boundary, full restart and durable vote. They
compare file-backed and deterministic memory-backed stores through identical
library/kernel schedules. Process tests exercise actual snapshot transport and
crashes on both sides of the atomic application transaction. These are local
adapter conformance tests, **not** the upstream exhaustive harness, proof,
Byzantine protection, device power-loss testing or release qualification.

Pinned-version upgrades require another boundary audit and rerun of these tests.
