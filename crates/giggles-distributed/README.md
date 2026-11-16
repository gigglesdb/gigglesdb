# Bounded range runtime

`giggles-ranges` hosts up to 16 independent, durable Raft
groups in each process. Group 1 is the replicated directory; data group IDs and
stable range IDs are separate identities. Each group uses its own redb file and
mTLS peer port. Database domain and group are bound in peer frames, log commands
and snapshots. The peer listener accepts authenticated Raft messages and bounded internal learner durability probes.

This is an owner-controlled typed row API, using GigglesDB core `Key`, `Row` and
kernel images. It does not provide distributed application SQL or protected task
transactions.

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
same initial directory/data descriptors, domain and group set. New groups begin with voter IDs 1, 2, 3. Provisioned peer rosters may include
up to 31 distinct node identities, including staged replacements. Each CA-signed leaf must support server/client authentication,
verify as `localhost`, and match the configured SHA-256 DER fingerprint. Peer
leaves must be distinct. Initial state is used only for a new file; reopen checks
persisted node, domain, group and complete provisioned fingerprint roster. The implementation trusts the owner
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
leadership transfer within the current membership; it does not move data.

`put` reads the current directory then submits an exact descriptor-fenced CAS to
the chosen local group leader. `expected:null` means absent; a number means exact
existing row version. Core assigns version 0 on insertion and increments it on
replacement; the supplied row version is not authority. Request IDs are retained
in the data range; repeating the same request/key/payload returns its prior
version. Preserve the original key and operation when resolving an ambiguous
response. Use the transaction API below for a durable home-scoped operation identity. Default physical protected task transactions remain fenced; the separate trusted-owner logical profile is described below.

The controller needs local leadership of each involved group. Contact the
reported leader for individual typed steps, or colocate leadership using trusted
`transfer` before calling routed operations or `resume`. Peer messages travel
between processes; administrative requests are never forwarded over peer TLS.
`RouteCache` is a bounded-retry library helper; the runtime always reads the root
and uses no negative cache to authorize operations.

`observe` acquires shared predicate/gap locks on every intersecting current
range before releasing any rows. Locks survive prepare and restarts and resolve
through a durable home decision. Readers meeting prepared work consult that home
or fail without releasing a result. The typed observation bound is 32 rows per
participant and 4096 rows overall. There is no MVCC historical-read API.

## Typed transactions

The owner API accepts `transact` with a `plan` and `outcome` (`Commit` or `Abort`).
A plan contains `database` (domain), nonzero `nonce` (u128), exact `home` descriptor,
and `participants` sorted by group. The home must be one participant. Each entry
contains its exact `route`, `writes` and `reads`. A write is
`{"key":KEY,"before":null,"row":ROW}` for an absent-key CAS; replacement requires
the complete previous Row including version. A read is
`{"bounds":BOUNDS,"expected":null}` for a complete observation; `expected` can
instead be the complete ascending array of `[KEY,ROW]` pairs, including an empty
array to protect absence. Reads observe before the transaction's own writes.
Only typed row puts are supported. TaskWrite continues to reject.

For example, in the initial unsplit data group, substitute descriptor D above:

```text
{"command":"transact","plan":{"database":42,"nonce":8001,"home":D,"participants":[{"route":D,"writes":[{"key":{"tenant":42,"row":1},"before":null,"row":{"status":"Failed","failed_at":103,"attempt_count":10,"version":999}}],"reads":[]}]},"outcome":"Commit"}
{"command":"tx_status","home":D,"id":{"home":{"database":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,42],"range":10,"epoch":1},"nonce":8001}}
{"command":"tx_recover","home":D,"id":ID,"outcome":"Commit"}
```

The first request durably stores the immutable full plan, registers and seals all
participants, acquires all locks before any prepare, validates full before-images
and predicates, stages redo, records all prepare votes at the home, chooses one
durable outcome and finalizes every participant before success. Single-range work
uses the same conservative protocol. `tx_status` returns durable home outcome and
retained complete observations only after commit; no undecided read result is
released. Repeating the same nonce/body is idempotent, a changed body fails, and
an existing outcome always wins over a later requested outcome. There is no GC.
A failed or timed-out coordinator call is indeterminate: preserve its plan/ID,
inspect status and recover. Explicit owner-selected Abort is a replicated home
decision, never unilateral participant timeout. Partially prepared operations may
block until home quorum returns. `tx_recover` loads the stored plan after restart.

Automatic `transact`, `tx_recover` and `observe` currently require local leadership
of their groups. Independently led groups support the same real protocol through
owner-routed `tx_step` requests:

```text
{"command":"tx_step","request":{"route":D,"step":{"Open":PLAN},"evidence":[]}}
{"command":"tx_pin","route":D}
```

After Open, carry its home `tx_pin` response as evidence to `Begin {id}` on each
participant. `Lock {id}` each participant with no evidence. Pin all locked groups
and pass that vector to each `Prepare {id}`. Pin each prepared group and pass it
to the home's `Vote {id,participant:DESCRIPTOR}`. `Decide {id,outcome}` at home
uses no evidence. Finally carry a new home pin to every `Finalize {id}`. Step enum
objects use the same externally tagged JSON form as Open. A step response exposes
only group/applied/reply, with no partial observation results. An external owner
coordinator can reconstruct its next step from pinned immutable plan, phase and
home outcome after death; there is no cross-node administrative RPC forwarding.
The process tests exercise this routing with different leaders.

A pin is a committed-state locator and snapshot from a quorum-checked current
barrier. It is **trusted owner control input, not a cryptographic certificate**.
Copy only real responses from authenticated owner sessions; never accept pins
from an untrusted remote user. The peer listener never accepts owner commands. `inspect`,
`current`, and `tx_pin` expose full trusted diagnostics; these are not protected
application read endpoints. Apply validates exact database/range/epoch,
operation digest, sealed participants, engine position and applied-index binding
through the transaction library's private evidence methods. It restores pinned
state deterministically and never contacts the network during apply. Engine
positions encode `raft_index * 256 + subcommand` for atomic bounded batches.
Transaction state, applied index and core redo effects share one Immediate redb
commit; response loss cannot cause redo to increment row versions twice.

Each range retains at most 16 transaction records; a plan has at most 8
participants, 8 combined reads/writes per participant and a 4 KiB canonical body.
Each participant captures at most 32 rows in total. The transaction engine has a
64 KiB snapshot bound with reserved resolution space. Admission prevalidates core
writes and reserves row slots for admitted active/prepared inserts. Capacity
exhaustion rejects new work; it does not evict locks, redo or terminal decisions.
Direct Put conservatively rejects a range with any unresolved home/participant
work. Split fencing rejects active, queued and prepared work. Terminal homes
remain on retained parent groups; retry/status use their original descriptors.
Transaction identities never migrate to a different logical home; retained state is
replicated in full during voter replacement. No transaction deletion is implemented. The on-disk range
schema changed; automatic upgrade from older binaries is not provided.

## Durable voter replacement

Provision all four peer identities, endpoints and CA-signed certificates in each
node's group configuration **before creating its store**. Start the fourth process
with the same initial group descriptors; its Raft configuration initially excludes
it. Provisioned fingerprints are persisted and must match exactly on restart.
Restart cannot silently add, remove or rebind a certificate. This bounded interface
does not provide live certificate rotation or dynamic roster provisioning.

At the current data-group leader, submit a stable nonzero intent ID and current
membership epoch (available under `current.membership.epoch`):

```json
{"command":"membership_begin","group":2,"intent":{"id":9001,"epoch":1,"learner":4,"remove":1}}
{"command":"membership_resume","group":2,"id":9001}
{"command":"current","group":2}
```

Repeat `membership_resume` at the reported leader until that exact receipt is
`Complete`. Each call advances one durable stage: Planned ? learner configuration
? authenticated applied proof ? explicit Joint ? Complete. A timeout is ambiguous;
reuse the same intent and inspect status. Reusing an ID for a different payload
fails. Receipts retain up to 64 intents; there is no GC or concurrent membership
change. Directory/vacant/inactive/fenced groups reject replacement. TaskWrite
remains rejected.

The controller and ConfState are saved atomically with the complete Machine and
applied index. Snapshots carry actual kernel rows, transaction homes/decisions,
participant locks/redo, retries and split provenance. The learner's internal mTLS
probe response is produced by its actor from its durable applied frontier and
binds its certificate identity, database/group envelope, intent, epoch, leader
term and barrier. The leader also checks Raft replication progress. The owner
cannot supply a remote readiness assertion; transport enqueue acknowledgments
are never durability proof. A new term requires a new proof before proposing
joint consensus. During joint consensus both incoming and outgoing majorities
are required. A removed leader stops serving writes/current reads and cannot
continue leader heartbeats after its durable removal.

`Descriptor.epoch` is the **logical ownership epoch**, while `membership.epoch`
tracks Raft configuration. `Descriptor.voters` retains the original ownership
placement, not live membership; read live voter placement from `membership` and
ConfState. Replacing a voter does not alter a descriptor, directory revision,
transaction home or receipt identity. Continue using original descriptors for
status and exact retries. Independently led transaction groups still need trusted
owner routing of `tx_step`/`tx_pin`; replacement adds no administrative forwarding.

Splits are conservatively refused while membership is changing **and after any
replacement on that parent**. Replaying a split cannot reactivate removed voter
placement through a new child. A split fence that commits first prevents membership
admission. A rejected fence leaves its directory intent retained and unpublished;
inspect it and keep serving the original range. General online ownership movement,
placement metadata refresh and splitting a replaced range need a later protocol.
Changing a voter list alone does not move ownership or repoint the directory.

The membership-aware durable format and snapshot version differ from earlier
binaries. There is no automatic upgrade path; retain prior files and use newly
provisioned stores for this local bounded runtime.

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
retained. Actual unresolved transaction state must be drained; no prepared locks or intents may be omitted. Parent writes remain fenced forever.
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
`cargo test --test process -- --test-threads=1` creates actual three- and four-process mTLS
clusters under unique ignored `artifacts/multi-process-*` roots, retaining configs,
certificates, databases, input/output and independent data oracles. The fixtures
also demonstrate complete machine-generated configurations.

Trusted local fault controls `block`, `arm_crash`, `concurrent` and
`observe_resume` provide deterministic network partition, before/after apply-save,
concurrent proposal and publication-during-observation schedules. They are owner
stdin only. `block` takes `group` and a list of peer IDs; restarting clears it.
`arm_crash` takes `group` and `after` and exits the process with code 86 at the
next applied atomic save. Neither removes artifacts.

General online ownership movement, protected task authority/budget/approval/receipt integration,
general distributed SQL, catalog partitioning, CDC delivery/retention, dynamic
placement and production/cloud/performance
qualification remain unavailable. Local tests imply no scaling or physical-time
guarantee.


## Protected authority admission and physical-time fence

The unique protected authority lives in directory Raft group 1, in a row-free
core image persisted atomically with the directory/home records and applied
index. Data groups remain authority-free. Owner controls are privileged local
stdin input; neither pins nor staged material are safe external task responses.

`protected_step { group, step }` accepts typed `Install { image }`, restricted
`Admin { command }` (JSON bytes for Grant, Revoke, RevokeApproval or resource
provisioning), `Open(plan)`, `Lock { plan, home }`, `Stage { nonce, rows }`,
`Abort { nonce }`, `Unlock { plan, home }`, and `Complete { nonce, rows }`.
`protected_pin { group }` obtains a current quorum snapshot for trusted owner
routing between independent leaders. Do not construct pins from caller claims.

A plan contains database, nonce, directory revision, task, the core protected
Intent and the exact complete routes in keyspace order. Open reserves the whole
authority image. Lock reserves each whole row range, including every gap and
empty range. Stage needs all row pins in route order and invokes the actual core
protected evaluator, storing opaque canonical staging bytes at the home without
changing authority, rows, approvals, budget, receipts or disclosure counters.

`Commit { nonce }` ALWAYS returns an error: the runtime has no trusted apply-time
clock/pause fence satisfying core's decision freshness contract. A caller time
interval or pre-proposal wall-clock check is insufficient under delayed Raft
apply. Use only the explicitly installed trusted-owner logical profile below for
protected publication; it provides no physical-time expiry. The legacy unit
TaskWrite action continues to reject. Staging never grants publication permission.

An unknown response retains the original nonce. An explicit Abort records the
home outcome. Route Unlock using a fresh aborted home pin to every participant,
including ranges never locked, then Complete with all released row pins. Abort
tombstones prevent delayed old Lock replay. `protected_abort { nonce }` performs
this recovery when all relevant groups are led locally. Independently led groups
use the same owner-routed steps. There is no administrative forwarding or timeout
rollback. Status is visible in the privileged current home record, not a task API.

At most 8 retained identities per authority/range, 8 complete routes, 16 KiB plan
and 512 KiB authority or staged body are admitted. There is no garbage collection.
Whole-image serialization reserves shared generation/capacity by excluding every
other mutator. Ordinary Put, transaction operations (including strict Observe),
and lifecycle commands reject while a protected intent is active. Authority
revoke waits behind physical-profile intents; the logical profile serializes
revoke before or after its immutable decision. Retained row
identities conservatively block future splits; membership replacement retains
complete machine state and logical homes. Directory authority replacement is
unsupported. No authority ownership migration is provided.

Durable payloads have checksummed `GD44v005` magic; snapshots require version 5.
Earlier files/snapshots fail closed and are retained. No automatic migration or
old-reader compatibility is claimed. Logical existing receipts/retries remain in
the current machine representation. New test fixtures are retained beneath the
system temporary directory and are never automatically removed.


For row Lock/Unlock, project the root current pin with
`protected::Pin::for_home(nonce)`: `HomePin` carries only database/applied,
immutable plan, opened/aborted indices, commit/finalization and release flags. No grant, approval,
receipt or stage image enters a row-group log. For Complete, project each current
row pin with `Pin::for_release(nonce)`: `Released` binds database, applied index,
exact descriptor, nonce, canonical plan digest, acquired index and released flag.
Completion proof size is independent of row images and ordinary transaction
history, keeping abort resolution within proposal headroom. JSON owner callers
must perform these same projections from authentic current responses; they do
not turn caller assertions into certificates.


## Trusted owner logical protected decisions

`protected::Step::Logical` is a separately installed **trusted-owner logical**
profile. `logical::Step::Install { image, profile }` declares that authority grant
and approval bounds use logical ticks. A physical `Step::Install` authority cannot
be converted. Default `Step::Commit` continues to reject, including after restart
or term changes. This profile makes no physical expiry or production credential
claim. Only the trusted owner may advance replicated epoch/tick state; an Intent
must match that state and cannot advance it.

Owner protocol (all pins from current quorum leaders):

1. `Logical(Open(plan))`, then ordinary protected `Lock` on every exact full range.
2. `Stage { nonce, rows }` persists the canonical original authority and compact
   row before images. `ResumeStage { nonce }` needs no old pins; fresh barriers
   cannot change the original digest.
3. Project `Pin::logical_preparation` to each participant, apply `Logical(Prepare)`
   and collect `Pin::logical_prepared`. Whole-range locks reserve the fixed-width
   all-row rewrite footprint; this profile limits each image to 16 KiB and each
   retained home record to 384 KiB. Capacity rejection occurs before a decision.
4. `Logical(Commit { nonce, prepared })` performs real core validation at current
   replicated logical authority state, persists one outcome plus exact validated
   digest, complete prepare set, authority after bytes and participant after bytes.
   A revoke ordered before this decision denies it. A committed decision finishes
   after a later revoke without consuming budget or approval again.
5. Carry authority-free `Pin::logical_finalization` to each participant. Apply
   `Logical(Finalize)`, collect exact `Pin::logical_finalized`, then home
   `Logical(ConfirmFinalized)`. Locks remain during partial publication.
6. Use current `for_home` proof for `Unlock` everywhere, then `Complete` with all
   `for_release` proofs. Ordinary reads/writes fail closed behind local barriers.

`Runtime::protected_logical_recover` / owner control `protected_logical_recover`
continues an already staged logical identity when the local process leads all
involved groups. Independent leaders use the same steps through owner routing.
Recovery explicitly re-runs core redo verification through `Logical(VerifyDecision)`
against retained decision-time originals. Routine pin/shape checks use canonical
integrity bindings rather than reexecuting the evaluator. A timeout is unknown,
never an abort. All steps retain immutable retry identity.
There is no retained-record garbage collection. Membership snapshots carry the
same original home and data; splitting retained protected identities is fenced.
Durable format v5 rejects earlier files without migration. Protected stores
checkpoint an applied log prefix once retained log bytes exceed 16 MiB, preserving
all unapplied entries and immutable original/decision records for recovery.

`Logical(Output { nonce, principal })` is trusted owner control only. It requires
complete publication and uses actual core `NativeTask::Status` disclosure debit
and `ValidateRelease` separately for each attempt. It returns redacted output,
never authority images. No task executor listener is exposed; owner commit success
is distinct from delivering output to an executor. Full owner pins are privileged
administration, never task output or remotely authenticated certificates.

Existing exact approvals, if present in an installed canonical authority, remain
bound and consumed by the real core evaluator. New distributed approval issuance
is not provided: the current public core API cannot project an approved assembled
workspace back to a row-free authority while retaining its approval. This profile
therefore does not claim an end-to-end single-use approval provisioning campaign.
No security, cloud, performance, PostgreSQL comparator or physical-clock
qualification follows from these local tests.
