# Typed prepared task requests (GD-0011)

Local tested surface: pinned tokio-postgres 0.7.15 / tokio-postgres-rustls 0.13.0,
mandatory mTLS, certificate bound offline to one domain/task/principal. These are
finite task envelopes, with no generic SQL access or remote policy provisioning.
The existing simple `GIGGLES <native JSON>` envelope is unchanged.

Only these case-sensitive token sequences are accepted. ASCII whitespace may
separate tokens, including before/after the statement. Punctuation separates
tokens. No semicolon, comments, casts, literals, suffixes, alternate placeholders
or extra statements are accepted. Prepared envelope text is at most 4096 bytes.

```text
GIGGLES STATUS ($1)
GIGGLES PREVIEW KEY ($1,$2,$3,$4)
GIGGLES EXECUTE KEY ($1,$2,$3,$4,$5)
GIGGLES PREVIEW RANGE ($1,$2,$3,$4,$5,$6)
GIGGLES EXECUTE RANGE ($1,$2,$3,$4,$5,$6,$7)
```

| Shape | Parameters in order |
| --- | --- |
| STATUS | operation BYTEA |
| PREVIEW KEY | operation BYTEA, tenant BYTEA, row BYTEA, after TEXT |
| EXECUTE KEY | same four parameters, approval BYTEA or NULL |
| PREVIEW RANGE | operation BYTEA, tenant BYTEA, before TEXT, from BIGINT, until BIGINT, after TEXT |
| EXECUTE RANGE | same six parameters, approval BYTEA or NULL |

Identifiers are exactly eight bytes, big endian unsigned u64, including zero and
u64::MAX. They are not signed PostgreSQL integers. Text BYTEA uses `\x` plus
exactly 16 hexadecimal digits; binary BYTEA sends the eight raw bytes. BIGINT
bounds are signed i64, must be nonnegative, and require from < until. The interval
is half open. RANGE therefore supports times 0..=i64::MAX rather than all core
u64 timestamps. TEXT statuses are exactly `failed`, `queued`, `succeeded`.
Only the optional approval accepts NULL. Type OIDs are 17 (BYTEA), 25 (TEXT),
20 (BIGINT); omitted/zero OIDs infer these types, explicit OIDs must match.
Text and binary parameter/result formats are supported. Existing per-frame,
32 statement/32 portal, name and aggregate response limits apply.

Preparation and binding validate grammar, types, widths, counts and bounds with
no clock, policy, row, approval, operation status or budget read. Describe returns
only static parameters and one TEXT column named `decision`. No execute, preview,
approval consumption, clock advance or policy mutation happens before Execute.
Invalid Bind enters normal extended-protocol drain until Sync. Ordinary task SQL
and task transactions remain unavailable. Application SQL retains its existing
catalog/domain/transaction prepared fences and portal suspension behavior.

Each task Execute constructs a typed native Request directly, without text
substitution. KEY is exactly one core Key and Column::Status; RANGE is the core
complete predicate selection and Column::Status. Canonical request bytes and
exact approval comparison are the native core implementation's bytes. An
approval never broadens task authority. Wrong operation, changed range, revoked
approval or stale rows reject. Approval expiry currently equals task expiry, so
expiry is rejected by the current task gate before approval consumption.

Every Execute, including repeated Execute of the same completed portal, returns
one newly checked JSON decision row and `SELECT 1`. No task outcome or offset is
cached. max_rows=0 means all, any positive legal max_rows returns the single row;
task results never suspend. Idle Sync closes portals; Close and error drain retain
the ordinary protocol rules. A prepared statement may survive Sync and be rebound.

Durable dedupe belongs to (task, original operation ID, canonical request).
Identical original requests return the original decision without charging twice;
conflicting parameters return GD002. Failed durable decisions also keep their
original IDs: use a new operation ID for a newly approved attempt. Approval IDs
are execution authority, not part of the canonical business payload; a committed
original request can replay after approval consumption under fresh task authority.
Changing a portal, reconnecting or restarting never resets task budget or dedupe.

Execute obtains fresh domain/principal/task/time/revocation and approval checks
through the native service and atomic core decision. Before releasing protected
count/digest, prepared task responses check current authority/time once more,
ordered by the shared task gate. This is a result-release authorization point;
already queued/delivered bytes cannot be recalled. Task revoke/expiry denies
completed retries/status. Expiry after a durable commit can withhold its response
with 42501; this does not undo the commit. Trusted local forensic status resolves
the original operation when remote authority is no longer valid.

Cancellation contexts belong to the current frame, including portal reexecution.
A cancellation that wins before decision returns 57014 without that invocation
publishing business changes, consuming approval or charging mutation budget.
After decision wins, cancellation cannot undo it. A lost response/disconnect can
leave a committed operation; reconnect and query original STATUS, then retry the
same payload/operation if authorized. 57014 on a retry does not prove rollback of
an earlier invocation. Shared blocking32 ownership and clock cancellation rules
are unchanged.

Example after connecting with the task certificate:

```rust,ignore
let statement = client.prepare("GIGGLES EXECUTE KEY ($1,$2,$3,$4,$5)").await?;
let operation = 700u64.to_be_bytes().to_vec();
let tenant = 42u64.to_be_bytes().to_vec();
let row = 7u64.to_be_bytes().to_vec();
let approval: Option<Vec<u8>> = None;
let rows = client.query(&statement,
    &[&operation, &tenant, &row, &"queued", &approval]).await?;
let response: giggles_server::Response = serde_json::from_str(rows[0].get(0))?;
```

Tests in `tests/prepared_network.inc` use actual TLS sockets, driver metadata and
binary binding, raw text/binary wire cases, real durable decisions and deterministic
decision barriers. Their local evidence is not cloud, production, broad client
compatibility or independent security qualification. Durable cumulative disclosure
accounting now applies across native/simple/prepared task responses; see
[TASK_ACCOUNTING.md](TASK_ACCOUNTING.md). Certificate rotation/rate bounds and P3 are
not implemented here; PostgreSQL comparison CONTROL remains deferred.
