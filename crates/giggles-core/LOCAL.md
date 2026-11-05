# Trusted local CLI v1

`cargo run --bin giggles -- COMMAND ...` runs entirely locally. This tool is a
trusted Admin surface: filesystem access is authority. It does not authenticate
principal integers. It must not be offered as an untrusted RPC executable.
All time inputs are trusted monotonic logical decision time, not wall time.

Commands return one JSON line `{ "version":1, "code":N, "data":... }`.
Exit0 means success, exit2 means policy denial (code100..117), exit3 means
absent status or other error (code1,10..20). Errors do not become success merely
because dispatch completed. Code10 can mean committed-but-response-lost;
reopen/status/retry **the original operation ID**, never a replacement ID.
Numbers use exact JSON integers, including u128 domains in receipts. Use a
lossless parser; metadata represents domain as a decimal string.

```
giggles create PATH
giggles seed PATH JSON_OR_@FILE
giggles grant PATH JSON_OR_@FILE
giggles preview PATH TASK PRINCIPAL JSON_OR_@FILE
giggles approve PATH JSON_OR_@FILE
giggles revoke PATH JSON_OR_@FILE
giggles revoke_approval PATH JSON_OR_@FILE
giggles execute PATH TASK PRINCIPAL JSON_OR_@FILE
giggles status PATH TASK PRINCIPAL OP
giggles repair PATH TASK PRINCIPAL OP LOGICAL_TIME SQL
giggles inspect PATH
giggles export PATH FILE
giggles verify FILE
giggles restore FILE FRESH_PATH
```

`admin PATH JSON_OR_@FILE` and `executor PATH TASK PRINCIPAL JSON_OR_@FILE`
are generic equivalents. Named command JSON must contain its matching `command`
field. `@file` loads at most1MiB. JSON request input is bounded at1MiB; unsupported
commands/unknown command fields reject. Paths are local filesystem paths.

Seed (one row per invocation; row versions are assigned by the kernel):
```json
{"command":"seed","key":{"tenant":42,"row":0},"row":{"status":"Failed","failed_at":103,"attempt_count":0,"version":0}}
```
Grant (trusted issuer, executor and approver are distinct):
```json
{"command":"grant","grant":{"task":1,"issuer":1,"executor":2,"approver":3,"policy":{"tenant":42,"columns":["Status"],"transitions":[["Failed","Queued"]],"incident":[100,107],"distinct_limit":25,"transition_limit":25,"approval_above":5,"valid_from":1,"expires":1000}}}
```
Preview:
```json
{"command":"preview","request":{"operation":1,"selection":{"Predicate":{"tenant":42,"status":"Failed","from":100,"until":107}},"column":"Status","after":"Queued"},"time":{"Certified":10}}
```
Review `data.effects`; save the exact `data.preview` hex token. Approve it with a
separate trusted approver action:
```json
{"command":"approve","principal":3,"preview":"HEX_TOKEN_FROM_PREVIEW","time":{"Certified":10}}
```
The output `data` is an approval ID (zero is valid). Execute the **same** request:
```json
{"command":"execute","request":{"operation":1,"selection":{"Predicate":{"tenant":42,"status":"Failed","from":100,"until":107}},"column":"Status","after":"Queued"},"time":{"Certified":10},"approval":0}
```
For no approval use `"approval":null`. Preview is advisory and approval is exact:
changed row versions, predicate phantoms, policy/schema/domain mismatch or changed
request fail validation. A durable denial binds the operation ID; a corrected new
request needs a new ID after its denial is known. Retry after success returns the
original receipt without charging again, even after revoke.

Other request examples:
```json
{"command":"revoke","task":1}
{"command":"revoke_approval","approval":0}
{"command":"status","operation":1}
{"command":"metadata"}
{"command":"sql","operation":2,"sql":"UPDATE deliveries SET status = 'queued' WHERE tenant = 42 AND id = 0","time":{"Certified":11},"approval":null}
```
Only the finite SQL grammar in README.md is accepted. No SQL/pgwire compatibility.
Preview token codec is fixed by protocol v1; it is a bounded exact Preview encoding,
not a remote secret or authentication assertion. Full native details are in
`src/types.rs`, `src/local.rs` and the versioned C header/README.

Export retains `FILE.staging-RANDOM` and atomically creates FILE by a no-clobber
hard link only after file sync and readback verification. Never edit either hard
link: they name the same file. The local filesystem must support hard links and
honest file sync. Unsupported publication fails with artifacts retained; no
fallback that overwrites a destination is attempted. Parent-directory persistence
under actual power loss is not qualified. Verification is checksum/integrity,
not authenticated protection against malicious filesystem owners. Restore makes
a new database identity with old tasks and approvals revoked, preserves forensic
receipts/origin, and rejects every existing destination (even incomplete ones).
A failed install remains for inspection; retry into a different fresh path.
