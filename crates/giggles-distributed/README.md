# Bounded range runtime

`giggles-ranges` hosts up to 16 independent, durable, fixed-three-voter Raft
groups in each process. Group 1 is the replicated directory; data group IDs and
stable range IDs are separate identities. Each group uses its own redb file and
mTLS peer port. Database domain and group are bound in peer frames, log commands
and snapshots. The peer listener accepts Raft messages only.

This is an owner-controlled typed row API, using GigglesDB core `Key`, `Row` and
kernel images. It does not provide distributed application SQL or protected task
transactions. Existing `giggles-node` native/pgwire TASK service remains separate.
There is no automatic migration or split of that service's task-bearing image.

## Start three voters

Build from this directory with `cargo build --bin giggles-ranges`. Start one
`target/debug/giggles-ranges.exe CONFIG.json` per voter. Keep stdin and database
files accessible only to the trusted owner. stdout contains one JSON response per
newline-delimited owner request; stderr contains fatal group diagnostics. No new
remote administrative endpoint is exposed.

A configuration has this shape (substitute real certificates, fingerprints,
ports and absolute paths):

```json
{
  "domain": 42,
  "node": 1,
  "groups": [{
    "peer": {
      "id": 1, "domain": 42, "group": 1,
      "database": "K:/data/voter1-directory.redb",
      "listen": "127.0.0.1:7101",
      "certificate": "K:/pki/voter1.pem",
      "key": "K:/pki/voter1.key.pem",
      "ca": "K:/pki/ca.pem",
      "peers": {
        "1": {"address":"127.0.0.1:7101","fingerprint":"64 lowercase hex characters"},
        "2": {"address":"127.0.0.1:7102","fingerprint":"64 lowercase hex characters"},
        "3": {"address":"127.0.0.1:7103","fingerprint":"64 lowercase hex characters"}
      }
    },
    "initial": {"Directory": {
      "range":10,"epoch":1,"group":2,
      "bounds":{"start":{"tenant":0,"row":0},"end":null},
      "voters":[1,2,3]
    }}
  }]
}
```

Add data group 2 using the same descriptor under `"initial":{"Data":...}`.
Preconfigure groups 3 and 4 with `"initial":"Vacant"` for a split. Use distinct
database paths and peer port triplets for every group. All voters must have the
same initial directory/data descriptors, domain and group set. Voter IDs are
exactly 1, 2, 3. Each CA-signed leaf must support server/client authentication,
verify as `localhost`, and match the configured SHA-256 DER fingerprint. Peer
leaves must be distinct. Initial state is used only for a new file; reopen checks
persisted node, domain and group identity. The implementation trusts the owner
and honest peer processes; it is not Byzantine consensus.

## Owner operations

```json
{"command":"inspect","group":1}
{"command":"current","group":1}
{"command":"campaign","group":1}
{"command":"transfer","group":1,"to":2}
{"command":"put","operation":100,"key":{"tenant":42,"row":1},"expected":null,"row":{"status":"Failed","failed_at":103,"attempt_count":0,"version":999}}
{"command":"observe","bounds":{"start":{"tenant":42,"row":0},"end":{"tenant":43,"row":0}}}
{"command":"checkpoint","group":2}
```

`inspect` is local diagnostic state, explicitly `quorum_checked:false`; it never
authorizes work. `current` requires leadership, ReadIndex quorum and applied-index
completion and returns `quorum_checked:true`. Followers report the known leader
in refusal messages. Each new group elects independently. `transfer` uses Raft
leadership transfer within the existing fixed membership; it does not move data.

`put` reads the current directory then submits an exact descriptor-fenced CAS to
the chosen local group leader. `expected:null` means absent; a number means exact
existing row version. Core assigns version 0 on insertion and increments it on
replacement; the supplied row version is not authority. Request IDs are retained
in the data range; repeating the same request/key/payload returns its prior
version. Preserve the original key and operation when resolving an ambiguous
response. There is no global application request-ID or task transaction service.

The controller needs local leadership of each involved group. Contact the
reported leader for individual typed steps, or colocate leadership using trusted
`transfer` before calling routed operations or `resume`. Peer messages travel
between processes; administrative requests are never forwarded over peer TLS.
`RouteCache` is a bounded-retry library helper; the runtime always reads the root
and uses no negative cache to authorize operations.

`observe` bounds output to 4096 rows. It pins the current directory revision,
checks exact descriptors and range fences after each group's current barrier,
and rechecks the directory before releasing any rows. Ownership change or a
fenced parent fails the whole observation. It is an ownership-coherent diagnostic
observation: concurrent data changes across groups do not have a common SQL
transaction snapshot. Strict cross-range reads require the future predicate-lock
and decision protocol. No partial result is returned as a complete observation.

## Split and recovery

Create a `Split` value with ID, complete `parent`, `key`, and `left`/`right`
descriptors. Child bounds must be exactly `[parent.start,key)` and
`[key,parent.end)`. Both epochs must equal parent epoch + 1. Child range/group IDs
must be new and never reused. Placement remains voters `[1,2,3]`.

```json
{"command":"apply","group":1,"action":{"Begin":{"id":77,"parent":{"range":10,"epoch":1,"group":2,"bounds":{"start":{"tenant":0,"row":0},"end":null},"voters":[1,2,3]},"key":{"tenant":42,"row":50},"left":{"range":11,"epoch":2,"group":3,"bounds":{"start":{"tenant":0,"row":0},"end":{"tenant":42,"row":50}},"voters":[1,2,3]},"right":{"range":12,"epoch":2,"group":4,"bounds":{"start":{"tenant":42,"row":50},"end":null},"voters":[1,2,3]}}}}
{"command":"resume","id":77}
```

`resume` commits the parent fence, reads that exact frozen image, prepares each
child through its own quorum, verifies current durable child state, atomically
publishes the directory, then activates children. Inactive children reject data
writes. Publication followed by a crash may leave temporary unavailability;
repeating `resume` activates remaining children. Parent state and log lineage are
retained. Prepared-work guard must be empty. Parent writes remain fenced forever.
Same lifecycle ID and exact content is idempotent; conflicting content fails.

The trusted controller derives readiness proofs from current group reads. Proofs
are not cryptographic certificates for untrusted controllers. Low-level
`apply` steps are trusted recovery/provisioning input; callers must follow the
controller protocol. Do not manually fabricate `Publish`, `Bootstrap` or
`Activate` input. A file owner can bypass these guarantees, as with embedded core.

Inspect directory intents, parent fence/index/source digest and child provenance
through `current`. Ambiguous timeout is not rollback: inspect and repeat the
original `resume` or operation. No automatic cleanup discards recovery state.
Immutable row versions survive projection. Grants, accounting, approvals, task
operations and relational/catalog state are excluded from child images. Images
with unsupported namespaces are refused by the range machine. `TaskWrite` and
`Move` always reject without user/system effects.

## Bounds and fault tests

Each group bounds proposals/current reads to 32 pending, actor/peer queues to
128, network concurrency to 8, uncommitted Raft data to 32 MiB, command to 4 MiB,
and durable record to 64 MiB. Data rows and retained requests each cap at 4096.
There is no lifecycle/request garbage collection; capacity exhaustion requires a
future retention protocol. Network exchange timeout is 2 seconds, owner request
wait is 5 seconds, tick interval is 100 ms. A timeout after submission is
indeterminate. Queues do not grant local writes without quorum.

The parent checks exact serialized child bootstrap sizes before committing its
fence. An unsupported child footprint rejects that fence while keeping the
parent active; the retained intent remains visible for inspection. The command
cap accommodates the declared maximum typed row/retry footprint. Chunked child
bootstrap and changing retained lifecycle intent are not implemented.

`cargo test --test lifecycle` runs persisted seeded transition schedules.
`cargo test --test process -- --test-threads=1` creates actual three-process mTLS
clusters under unique ignored `artifacts/multi-process-*` roots, retaining configs,
certificates, databases, input/output and independent data oracles. The fixtures
also demonstrate complete machine-generated configurations.

Trusted local fault controls `block`, `arm_crash`, `concurrent` and
`observe_resume` provide deterministic network partition, before/after apply-save,
concurrent proposal and publication-during-observation schedules. They are owner
stdin only. `block` takes `group` and a list of peer IDs; restarting clears it.
`arm_crash` takes `group` and `after` and exits the process with code 86 at the
next applied atomic save. Neither removes artifacts.

Membership moves, rigorous locks/prepared participants/replicated distributed
decisions, general distributed SQL, catalog partitioning, transactional multi-range
reads, CDC delivery/retention, dynamic placement and production/cloud/performance
qualification remain unavailable. Local tests imply no scaling or physical-time
guarantee.
