# giggles-pg-json

Standalone UTF-8 PostgreSQL JSON/JSONB foundation. This crate has its own workspace,
lockfile and target directory. It is **not integrated** into GigglesDB values, SQL,
registry, catalog, wire server, persistence, native, HTTP or gRPC APIs.

`Json::parse` validates the complete JSON grammar and preserves input text, whitespace,
number spelling and duplicate keys. Raw text, binary send, typeof and array length
avoid Unicode unescaping, as PostgreSQL does. Access functions decode strings and can
therefore reject escaped NUL/unpaired surrogates accepted by JSON input.
`Jsonb::parse` decodes Unicode, rejects escaped NUL and invalid surrogate sequences,
normalizes numeric spelling with exact frozen `giggles-pg-types::Numeric`, preserves
display scale, orders keys by UTF-8 byte length then bytes, and retains the last value
for duplicate decoded keys. No float conversion or serde deserialization is involved.
`numeric()` recovers exact scalar Numeric and its scale; JSONB numeric text is an exact
coefficient/scale representation, not a float. JSON raw numbers are unrestricted by
Numeric's smaller limits until a caller explicitly requests JSONB.

Functional API correspondence (functions are not registered SQL routines):

| Methods | PostgreSQL family |
|---|---|
| parse/text/receive/send | pg_catalog.json_in/out/recv/send, jsonb counterparts |
| typeof_ | pg_catalog.json_typeof / jsonb_typeof |
| array_length | pg_catalog.json_array_length / jsonb_array_length |
| object_keys | pg_catalog.json_object_keys / jsonb_object_keys |
| field / field_text | pg_catalog.json_object_field / json_object_field_text, jsonb counterparts |
| element / element_text | pg_catalog.json_array_element / json_array_element_text, jsonb counterparts |

Negative array indexes count from the end. JSONB scalar wrappers support index 0 and
-1, matching PG18. Missing or wrong-shaped access returns `None` (SQL NULL); a selected
JSON null remains a typed JSON value. Text access to JSON null returns `None`. SQL NULL
inputs can be represented by `Option<Json>`/`Option<Jsonb>` outside this strict API.
JSONB selections borrow subtrees. JSON selection preserves the selected raw span.
JSON binary is raw UTF-8; JSONB binary is byte 1 plus canonical text, never the on-disk
JSONB representation. `send` charges the JSONB version byte against output limits.

Limits are explicit and deterministic; the library reads no host environment.
Defaults: 1 MiB input/output, 100,000 nodes (including object keys), depth 64;
hard recursion ceiling 128 even with a larger configured limit. Nodes are charged
before insertion; input bytes before parsing/copying; output bytes before appending,
copying raw selections or emitting key vectors. Temporary parse tree storage is bounded
by input/nodes/depth, separately from output. Numeric arithmetic scratch uses the frozen
Numeric bounds (16,384 coefficient digits, 16,383 scale and 32,800 input text bytes).
Resource errors are `Limit`, not successful truncation or PostgreSQL-compatible errors.

Error SQLSTATE categories supported here: Syntax 22P02, Encoding 22021, escaped-NUL
Unicode 22P05, wrong shape 22023. Diagnostic messages/positions and malformed binary
version diagnostics are not PostgreSQL-compatible. Rust input is UTF-8 only. No JSON
hash operators, casts beyond parsing, constructors,
aggregates, JSONPath, SQL/JSON query, record conversion, SQL binding or execution is
implemented. `Unsupported` is reserved for future explicit dispatch rejection;
there is no dispatcher that silently claims unimplemented routines succeed.

Qualification uses 3,546 independent PG18 oracle operation rows, deterministic
1,000 generated roundtrip/duplicate/scale properties, depth/input/node/output limits,
index extremes and wire roundtrips. Durable TSV/SQL under tests/fixtures are read at
runtime, with no dependency on target artifacts. Regeneration is real PostgreSQL,
not expected-output generation by this implementation. Negative cases compare exact
SQLSTATE categories; positive values/text/binary/order compare exact bytes.
Oracle is PostgreSQL 18.0 Debian x86_64 UTF8 en_US.utf8; coverage does not imply all PG
JSON functions or every malformed-input diagnostic is qualified.

From repository root, create the sole owned oracle container:

```powershell
docker run -d --name dd68-json-sol-20261106 --network none --tmpfs /var/lib/postgresql:rw --tmpfs /tmp:rw -e POSTGRES_HOST_AUTH_METHOD=trust postgres@sha256:41fc5342eefba6cc2ccda736aaf034bbbb7c3df0fdb81516eba1ba33f360162c
# Once pg_isready succeeds:
python crates/giggles-pg-json/tests/verify.py --live
docker rm -f dd68-json-sol-20261106
```

Offline checks: run verify.py without --live. Live regeneration needs Python, Docker
and the owned container; it fails if unavailable. The script writes exact exit codes
and logs to target-dd68-json. `cargo test --include-ignored` runs the live check.

Primary source study: upstream/json.c, jsonb.c, jsonfuncs.c, jsonapi.c and COPYRIGHT.
Exact commit, URLs and SHA256 are in upstream/provenance.json. Rust implementation was
independently written against these contracts and the live oracle; no LGPL source
was adapted. The reference source copies retain the PostgreSQL license.

Typed JSONB operator increment:

- `sql_eq`, `compare_c`: exact Numeric equality independent of scale; total ordering
  for PostgreSQL databases with C collation only. Object length precedes traversal,
  stored key order is length/bytes, comparison of keys is lexical bytes, and the
  historical root empty-array/raw-scalar ordering exception is retained.
- `contains`, `contained_by`, `exists`, `exists_any`, `exists_all`: nested container
  matching and top-level scalar-array exceptions; NULL existence keys ignored.
- `concat`: nonrecursive object last-key-wins merge or array concatenation with
  scalar/object wrapping. Results retain Numeric scale.
- `delete_key`, `delete_index`, `delete_path`, `path`, `path_text`, `strip_nulls`:
  explicit text[] paths, negative indexes, missing versus JSON null versus SQL NULL,
  and PG18 array-null stripping. NULL reached deletion path elements report 22004.

Paths are one-dimensional slices of optional text. A SQL NULL input array is handled
by the caller's Option; access with a NULL element returns None. Path elements and
bytes are capped by depth/nodes and input_bytes before traversal. Shrinking mutations
preflight the source canonical output before cloning, so a source exceeding that
output budget fails even if deletion would shrink it below the budget. Concat writes
bounded text before bounded parsing; canonical output is also checked before return.
All operations leave their operands unchanged on resource failure. No allocator-OOM
recovery is promised. Comparison uses C collation explicitly, never host locale.

The increment adds an independent PG18 C-collation crossproduct oracle in
`tests/fixtures/operators.{sql,tsv}`, generated by `tests/generate_operators.py`.
Run `python tests/verify_functions.py` (from this crate or repository root) for offline
checks, or `--live` with the owned README container and a C-locale `operator_oracle`
database (`docker exec dd68-json-sol-20261106 createdb -U postgres -T template0
--locale=C operator_oracle`). The verifier writes direct exit codes/logs under
`target-dd68-json-functions`; it never manages or stops containers. The generator's
container can be specified by PG_JSON_OPERATOR_CONTAINER. Always track successful
container creation and remove only that owned container afterward.

Non-C collation comparison remains unimplemented. This is a
standalone library increment, with no SQL/binder/storage/wire-server integration.


Typed growth mutations (`set`, `insert`, associated `Jsonb::set_lax`) implement the
PG18 jsonb_set/jsonb_insert/jsonb_set_lax contracts. Paths are one-dimensional
slices of optional strings. `set_lax` takes optional target, path array, replacement,
create flag and treatment string: SQL NULL replacement differs from typed JSON null.
A NULL target/path/create returns SQL NULL first; NULL treatment errors next even
with a non-NULL replacement. Non-NULL replacement ignores an invalid non-NULL
treatment. With NULL replacement the four exact modes are `raise_exception`,
`use_json_null`, `delete_key`, `return_target`. Return-target bypasses path semantic
validation; source resource admission still applies. Reached NULL elements error,
including at reached scalars; absent intermediate paths do not reach later elements.
Existing object keys error on insert; reached scalars and missing intermediates
otherwise leave the target unchanged. Root scalars error even with an empty path.
Out-of-range array insertion prepends for negative indexes and appends for positive
indexes, with no gap filling. NULL input arrays and multidimensional arrays are
outside the strict set/insert slice API; lax represents NULL input arrays explicitly.

Mutation builds a borrowed edit plan, streams node/depth/byte-admitted canonical
text, then parses a result only after admission. No result subtree is cloned before
admission. Both the result input budget and output budget apply. Concat now counts
expanded nodes/depth before rendering (including duplicate object keys, matching
parser accounting). Shrinking mutations preflight source output before cloning;
parse-time source node/depth limits already hold. Each operation is atomic on errors.
Limits bound allocated data and recursion, **not CPU work**. The existing infallible
`contains`/`contained_by` preserve exact boolean semantics and never convert resource
errors into false. Worst-case containment visits O(N*M) node/key candidates, with
key/string byte comparison and exact Numeric parsing/comparison costs per candidate;
Numeric cost is capped by the documented Numeric operand bounds. Nested arrays with
many repeated similar containers can therefore be expensive. Path walks scan object
siblings: a conservative O(P*N*K) bound for P path length, N operand nodes and K
maximum key/path byte length; rendering/parsing adds output byte and Numeric costs.
No unbounded-CPU guard is claimed. No allocator-OOM recovery is promised.

Mutation qualification adds 26,420 independent PG18 rows, including a crossproduct
of target/path/replacement/flags/treatments and 500 seeded generated nested cases.
`tests/fixtures/mutation.{sql,tsv}` are durable runtime fixtures; their manifest pins
the generator, source, image, and actual database version/collation. Tests also
exercise exact/over-limit output and nodes, adversarial depth/wide expansion,
atomic operands, and 500 mutation properties.

Run `python crates/giggles-pg-json/tests/verify_mutation.py` from the repository root
for offline checks, or `--live` after creating `dd68-json-mutation-sol-20261106`
with the same pinned image/network/tmpfs settings above and creating its
`operator_oracle` database using `createdb -U postgres -T template0 --locale=C`.
The verifier records direct exit codes, raw logs, stable before/after source hashes,
and fixture stability under `target-dd68-json-mutation`. It does not manage containers.
Only remove a container after tracking its successful creation in your own task.
This increment has no SQL/native/storage/wire integration or JSONPath claim.
`jsonpath` provides separately qualified standalone navigation, three-valued predicates, filters, ranges and terminal bounded recursive descent; see [JSONPATH.md](JSONPATH.md) for exact grammar and limits. Full grammar, descent continuations and codecs remain deferred. Predicate qualification adds65,280 actual PG18 rows (124,376 total preserved/new oracle rows). Run `python crates/giggles-pg-json/tests/verify_predicates.py [--live]`; live uses the owned `dd68-jsonpath-predicates-sol-20261106` container under the same image/network/tmpfs/C-database procedure. Logs and before/after hashes are in target-dd68-jsonpath-predicates.
