# PostgreSQL function qualification

New generated outputs must stay outside the repository. `GIGGLESDB_WORK_ROOT`
overrides the default `J:/gigglesdb-work`; automatic directories use
`dd68-pgfunc-<script>-<UTC timestamp>-<pid>`. Defaults and explicit output paths
are checked before writes, including resolved paths. `.runs` contains legacy
evidence only; new runs there are prohibited. Keep existing evidence in place.
Use `python -B` (as below) or `PYTHONDONTWRITEBYTECODE=1` to avoid repository
bytecode caches. Set `TEMP`, `TMP`, and `CARGO_TARGET_DIR` under a unique work
folder for child commands.


[DD-0068](https://app.archdocs.io/design-docs/51078bfa-4e9b-4169-b2b5-6058774394c2)
extends GigglesDB's engine with hosted compatible runtimes and explicit tested
GigglesDB equivalents for PostgreSQL physical functions.

This is the initial inventory and differential-test foundation, **not function
implementation or full qualification**. It is separate from the concurrently
developed JDBC/catalog conformance runner.

## Reference

`postgres-18.0.json.gz` contains a deterministic JSON snapshot of a fresh
PostgreSQL **18.0** catalog: 3,413 routine identities, types, casts, operators,
aggregates, namespaces and installed extensions (default `plpgsql` only).
`reference.json` pins its canonical SHA-256 and Docker image digest. Compression
uses a zero timestamp. `inventory.sql` regenerates the data without selecting
only the functions GigglesDB happens to implement. Routine identities distinguish
schema, argument signature and kind. The snapshot includes internal routines;
their presence does not imply they are legal direct SQL calls.
The snapshot is derived from PostgreSQL; `POSTGRESQL-LICENSE` retains the
upstream `REL_18_0/COPYRIGHT` notice.

Python 3 and Docker are required. The runner creates its own container, with no
network, no host mounts, ephemeral tmpfs data and local-only trust authentication.
It never connects to a user's database. It stops/removes only the container it
created. Output paths must be new; artifacts and embedded fixture databases are
retained for inspection. Cases are trusted repository SQL.

From the repository root:

```powershell
$env:GIGGLESDB_WORK_ROOT='J:/gigglesdb-work/dd68-example'
$env:TEMP="$env:GIGGLESDB_WORK_ROOT/tmp"; $env:TMP=$env:TEMP
$env:CARGO_TARGET_DIR="$env:GIGGLESDB_WORK_ROOT/target"
New-Item -ItemType Directory -Force $env:TEMP | Out-Null
python -B -m unittest discover -s scripts/postgres-functions -v
$env:DD68_REFERENCE_TESTS='1' # also run live binding regressions against pinned Docker reference
python -B -m unittest discover -s scripts/postgres-functions -v
cargo build --locked --manifest-path crates/giggles-core/Cargo.toml --example function_probe --target-dir "$env:CARGO_TARGET_DIR" -j 1
python -B scripts/postgres-functions/conformance.py check --output "$env:GIGGLESDB_WORK_ROOT/reference-check"
python -B scripts/postgres-functions/conformance.py run --output "$env:GIGGLESDB_WORK_ROOT/functions-run" --probe "$env:CARGO_TARGET_DIR/debug/examples/function_probe.exe"
```

On Unix, omit `.exe`. Use a new output directory for every run.
`check` exits zero only when a freshly captured catalog exactly matches the
snapshot. `capture` writes a candidate inventory/reference without updating the
checked-in baseline. Review differences before intentionally changing a baseline.

## Results and limits

The 66-case corpus retains every initial case and compares values, column names,
types, NULLs and row counts. It adds core prepared bindings, NULL/type/arity,
integer overflow, Unicode, substring boundaries, nesting and COALESCE short-circuit
cases. COALESCE has a separate special-form ledger; the denominator remains 3,413.

Negative cases pin both PostgreSQL SQLSTATE and an independent exact core error
variant. Verbose psql diagnostics supply SQLSTATE only after a successful setup
marker. Setup and transport failures cannot pass negative cases. A passing negative
case proves the declared core variant alongside the observed PostgreSQL error;
**it is not observed pgwire SQLSTATE evidence**.

Core parameters use execute_prepared bindings. The psql adapter expands explicitly
typed trusted literals for its reference view; PostgreSQL extended protocol remains
untested. UTF-8/locale C determine Unicode case expectations. Decimal JSON numbers
are preserved as tagged exact decimal text, never rounded through float; broader
decimal and float qualification remains open.

Reference binding scans SQL lexical tokens: strings (including E-string escapes
and newline continuation), quoted identifiers, dollar quotes and comments remain
unchanged. Only real `$n` tokens are expanded; every supplied parameter must be
used and all referenced slots must exist. Typed integers must fit their declared
width; text rejects actual NUL. Reference sessions explicitly set
`standard_conforming_strings=on`, so ordinary-string backslashes remain literal.
This scanner validates lexical boundaries, not complete SQL grammar. Fixtures
remain trusted SQL; typed literal expansion is not extended-protocol testing.

`run` currently exits **1 even if all seed cases pass**, because this seed harness
cannot establish full qualification. `report.json` records each oracle/candidate
result, verdict, executable/corpus/inventory hashes and the complete routine
denominator. `untested` means no case exists; it does not claim the implementation
is absent. `implemented-unverified` means the supplied cases passed, not that all
overloads, contexts or behaviors work. No entry can become `verified` in this
initial harness.

The initial real run passed COUNT on populated/empty input and MIN/MAX on BIGINT;
scalar ABS/LENGTH/LOWER/UPPER/SUBSTRING calls failed in the application SQL parser.
SUM(BIGINT) returned BIGINT instead of PostgreSQL's NUMERIC. These gaps must not be
hidden by running only catalog expressions or dropping type comparisons.

Remaining M1 work includes SQL special-form inventory, wire SQLSTATE/extended-protocol
parameter cases, exact decimal/float/type codecs, multiple-row ordering,
stateful/permission contexts, extension packages, physical-equivalence contracts,
wire/native adapters and CI integration. Full qualification also needs the later
type/execution/runtime/recovery implementation phases. This runner does not claim
to test those surfaces, or to exhaustively test any routine family.

Supplemental string oracle corpus: `--cases scripts/postgres-functions/cases-strings.json`
selects the additional corpus; repeat `--cases` to combine files. Omitting it preserves
the original 66. `observe_strings.py` reobserves every stored expectation on the
owned pinned PG18.0 reference; `strings-reference.json` pins the canonical corpus hash.
The supplemental corpus includes catalog overloads beyond current core types; failures
remain visible. All results remain partial, including matching negative cases.

The legacy untyped `abs($1)` binding case compares different binding paths: reference
literal expansion forces its declared integer overload while core infers its own
parameter type. Its failure is retained honestly, not pgwire equivalence evidence.
The supplement adds an explicit SQL CAST counterpart fixing the overload on both sides.
Negative supplemental cases intend `Sql(Type)` for binding/domain errors. A current
`Sql(Syntax)` parser rejection is a failure; matching PostgreSQL SQLSTATE does not
establish core/wire SQLSTATE equivalence. Reports explicitly mark executable/source
correspondence unverified; source hashes captured at run time do not prove build inputs.

Supplemental binary oracle corpus: `--cases scripts/postgres-functions/cases-binary.json`
selects 255 additional cases (repeat `--cases` to combine all three corpora).
The default remains the original 66; the string supplement remains 250 and the
inventory remains 3,413. `observe_binary.py` regenerates the binary corpus by
observing the pinned PostgreSQL reference, rejecting unexpected success/error
categories. `binary-reference.json` pins its canonical hash. Run the enabled
unittest command above to reobserve every string and binary expectation.

Binary fixtures exercise catalog bytea overloads, SHA digests, get/set byte and
bit indices, trim sets, substring boundaries, encoding errors, hex whitespace,
base64 padding/whitespace, escape octets, NULLs and empty inputs. Primitive text
parameters use explicit SQL casts to bytea; this is not binary wire evidence.
`length(bytea,name)` is included as a catalog obligation beyond the parallel
bytea implementation. Invalid arity/type/domain cases keep the intended exact
core `Sql(Type)` mapping independent of the observed PostgreSQL SQLSTATE.
Sized decode/nested octet_length cases include 32,768 and 65,536 bytes; a core
output-cap rejection where PostgreSQL succeeds is a visible mismatch, not a
PostgreSQL negative expectation. Full qualification remains false.

The `.runs/binary-stale-probe` and `.runs/binary-stale-probe-final` reports
are reference-valid but **stale/unverified for candidate binary support**.
The shared probe changed between runs; no stable source/build or per-case
executable identity was established. Final diagnostic counts are 199 PASS,
56 FAIL, zero oracle errors; they do not validate the parallel implementation.
Rebuild and rerun under a stable probe before evaluating that implementation.
`binary-results.json` records exact test results, corpus and file hashes, both
probe hashes and this limitation. Full qualification remains false.

### Exact numeric/float family

`python -B scripts/postgres-functions/observe_numeric.py` regenerates 783 independent
PG18-observed cases in `cases-numeric.json`. `numeric-reference.json` pins the
canonical corpus hash. Enable `DD68_REFERENCE_TESTS=1` for live regeneration tests.
Add `--cases scripts/postgres-functions/cases-numeric.json` to the differential
runner; repeated `--cases` combines it with the unchanged 66/250/255 families.

This family opts into `exact_numeric`: numeric results are decimal text retaining
scale, real/double results are hexadecimal network-order IEEE bits obtained from
PG `float4send`/`float8send` and the core Value. Typed numeric parameters use decimal
strings; typed float parameters use 8/16 hexadecimal digits. The reference converts
IEEE bits to an exact round-trip decimal input then casts to the declared type.
No numeric parameter travels through f64, and no float travels as a JSON number.
NaN payload preservation is not claimed: PostgreSQL textual NaN input is canonical.
Typed nulls use SQL casts because core Value::Null carries no standalone type.

The core prepares and executes each query with Values. The reference uses the
lexically checked literal adapter, so parameter-inference failures remain visible
and these are not extended-protocol or real-wire SQLSTATE tests. Successful output
checks include actual result names/types; numeric typmods may expose core metadata
gaps. Error expectations retain `Sql(Type)` independently of PostgreSQL SQLSTATE;
Syntax or incidental failures cannot satisfy them. This family covers math overloads
in the pinned inventory, including routines the product does not implement. All
3,413 routines remain in the denominator and full qualification stays false.

Corpus observers (`observe_strings.py`, `observe_binary.py`, `observe_numeric.py`,
and `observe_temporal.py`) accept `--output DIRECTORY` and write generated corpus
and reference candidates there, leaving the checked-in corpus unchanged.
