# giggles-ffi

The versioned C ABI for trusted in-process access to the GigglesDB Rust kernel.

**Status:** Provides opaque handles and stable numeric outcome codes; it is not a remote security boundary.

Part of [GigglesDB](../../README.md).

This is the embedded Rust kernel's trusted in-process binding, not a remote security
boundary. Build with `cargo build` or `cargo build --release`. MSVC outputs
`giggles_ffi.dll`, `giggles_ffi.dll.lib` and `giggles_ffi.lib`; link C consumers
against the import library and include `include/giggles.h`. Symbols use the C
calling convention; all exported names start with `gd_v1_`.

Shared authority is implemented exclusively by `giggles-core`; `giggles-model` is not a
runtime dependency.

## Ownership and ABI contracts

`gd_handle` is an opaque uint64_t registry ID, never a pointer. Zero is invalid;
IDs never repeat during a process lifetime. Admin, Executor and Buffer have
separate runtime kinds. Close every successful handle exactly once using
`gd_v1_close`, including response buffers returned on policy denial or errors.
Double close, wrong-kind and stale use return 18. Closing Admin does not revoke
Executor clones; task revocation is an explicit durable admin command. Executor
keeps the database lock alive until it too is closed. Buffer handles retain an
owned copy and survive closure of their source. No Rust allocation pointer or
borrowed view crosses the ABI. IDs are local references, not secrets, authentication
credentials or unforgeable capabilities against malicious code in the same process.

All pointers supplied by C must refer to valid, correctly aligned readable or
writable memory of the declared size. Input may not overlap out parameters;
copy destination may not overlap `needed`. Null input is allowed only at length0.
Null out parameters reject. Rust cannot validate arbitrary non-null C addresses.
No handles are valid across a process restart or fork. Fork support is unqualified.

Request bytes are UTF-8 JSON, at most 1MiB, no terminating NUL. Unknown command
fields reject. Replies have `{ "version":1, "code":0, "data":... }`. At most
64MiB per response, 128MiB retained response bytes and 1024 live handles per process.
Registry calls serialize. Response capacity is reserved before durable dispatch;
resource rejection does not execute an action. Buffers are copied without partial
writes. Query `gd_v1_buffer_copy(buffer,NULL,0,&needed)` returns 20 and exact size;
allocate that many bytes and copy. JSON bytes have no terminating NUL: C clients
may allocate one extra byte themselves. Close response after copying, or retain
it and copy repeatedly. No free function for Rust memory is required.

Every ABI entry that can panic uses catch_unwind. A contained panic returns19;
registry poison recovery keeps close available, and a poisoned kernel gate fences
operations. Close all related handles, reopen and resolve the original operation
ID. Aborting panics, allocator failure, process termination and invalid C memory
cannot be contained. Intended callers must obey memory and ownership contracts.

## Functions

- `gd_v1_abi_version()` returns1.
- `gd_v1_admin_open(json,len,&admin)` accepts `{ "mode":"create", "path":"..." }`,
  `open`, or `{ "mode":"restore", "path":"fresh", "export":"sealed-file" }`.
  Failed calls zero out the handle. Create/restore never overwrite paths.
- `gd_v1_executor(admin,task,principal,&executor)` is a **trusted admin rebind**.
  It checks the stored principal, but does not authenticate a supplied integer.
- `gd_v1_call(admin_or_executor,json,len,&buffer)` returns both the stable outcome
  code and an owned JSON response handle when dispatch succeeds. Policy denial
  still returns a buffer. Invalid handles/lengths/resource rejection may return
  no buffer (zero). Callers always check the handle before copying/closing.
- `gd_v1_buffer_copy(buffer,dst,capacity,&needed)` and `gd_v1_close(handle)`.

Admin call commands: seed, grant, approve, revoke, revoke_approval, inspect,
metadata and export. Executor commands: preview, execute, sql and status. Exact
JSON DTOs are in core `src/local.rs`; worked CLI examples are in core `LOCAL.md`.
Preview includes a hex `preview` token plus effects. The v1 token encodes the exact
Rust Preview (bounded fixed-int little-endian bincode1), not a bearer credential.
Only trusted Admin may turn it into an approval after independent review. It is
rechecked against current dependencies before approval and before execution.

## Stable outcome codes

| Code | Meaning |
| --- | --- |
| 0 | Success or committed receipt |
| 1 | Operation absent; no outcome was found |
| 10 | Storage error; protected commit outcome may be unknown |
| 11 | Corrupt data or token |
| 12 | Unsupported format/operation/token |
| 13 | Destination exists |
| 14 | Busy admission |
| 15 | Fenced handle |
| 16 | Resource/length limit |
| 17 | Invalid JSON or command |
| 18 | Invalid pointer/handle or wrong handle kind |
| 19 | Contained panic |
| 20 | Buffer too small; needed is populated |
| 100..117 | Policy denials listed below |

Policy codes, in order100..117: Authentication, InvalidGrant, TaskExists,
UnknownTask, Revoked, Expired, UncertainTime, TimeRegression, Scope, MissingRow,
DuplicateKey, InvalidPredicate, Budget, ApprovalRequired, InvalidApproval,
StalePreview, IdConflict, Overflow. Codes are explicit matches, independent of
Rust enum discriminants. Text diagnostics are not a stable parsing interface.
Status of a durable denial returns its original policy code. Receipt integer
domains are u128 JSON numbers: use a lossless JSON integer parser (not JS Number).
Metadata uses decimal domain strings. Snapshot maps are arrays of key/value pairs.

Storage errors can occur after a decision committed. Close **all** clones, reopen,
rebind under trusted authority and query status of the original operation ID.
Retry the same payload and ID; do not allocate a new ID after an unknown response.

## Trust and remote service integration

The embedding host, filesystem owner, admin, principal binding and certified
logical clock are trusted. Never provide admin handles or admin dispatch to an
untrusted task. Never use this registry as remote authentication or accept a
remote caller's principal/time claims. remote service must authenticate transport/identity,
map a session to an issued Executor, supply a trusted clock, restrict status/
receipt disclosure and call the same Executor kernel. See the [core API documentation](../giggles-core/README.md).
This profile makes no physical expiry, transport, compatibility or cloud claim.

`verify.ps1 -ArtifactDirectory artifacts/UNIQUE` retains fmt/clippy/debug/release/
doc logs. `verify-c.ps1 -ArtifactDirectory artifacts/UNIQUE -Profile debug` (or
release) builds the actual MSVC C client with /W4 /WX, runs it, retains its DLL,
executable, raw outputs/hashes and databases. Run from this crate. Never clean
artifacts. The C runner currently locates VS2022 Community; other toolchains and
ABI/platform pairs need their own qualification. No installation packaging or
header-generation compatibility campaign has been performed.

The relational API adds `gd_v1_sql_session(Admin,out)`. Its handle accepts {sql,params,expected_version} through gd_v1_call; close rolls back any staged transaction. Executor cannot create a SQL session. SQL codes200..216 and exact grammar/bounds/format2 migration are documented in [SQL.md](../giggles-core/SQL.md). Native Rust prepared/Scan APIs have no separate C opcode in this profile. Existing ABI version1 ownership and numeric policy codes remain stable.
