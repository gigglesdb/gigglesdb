# Prepared-wire oracle

New generated outputs must stay outside the repository. `GIGGLESDB_WORK_ROOT`
overrides the default `J:/gigglesdb-work`; automatic directories use
`dd68-pgfunc-<script>-<UTC timestamp>-<pid>`. Defaults and explicit output paths
are checked before writes, including resolved paths. `.runs` contains legacy
evidence only; new runs there are prohibited. Keep existing evidence in place.
Use `python -B` (as below) or `PYTHONDONTWRITEBYTECODE=1` to avoid repository
bytecode caches. Set `TEMP`, `TMP`, and `CARGO_TARGET_DIR` under a unique work
folder for child commands.


`prepared_wire.py` sends PostgreSQL protocol-v3 Parse with zero parameter OIDs,
statement Describe, Sync, Bind (text format), Execute and Sync. It records exact
frames, ParameterDescription OIDs, RowDescription OIDs and UTF-8 DataRow values.
Docker exec/bash only carries bytes to loopback in its own network-none container.
It performs no literal substitution. Existing literal fixture files remain unchanged.

`run_prepared.py` copies source/manifests/locks to the owned artifact directory,
builds with --locked in a fresh target, and retains input hashes, command, native
exit code and executable hash. It runs the 2163 old cases separately. Metadata
provenance qualifies that source copy; check before/after for concurrent changes
before attributing it to the latest working source. No routine is verified (0/3413).

The core probe accepts optional `describe_metadata: true`. Description reports
`parameterTypes` and `resultTypes`; execution failures retain successfully described
metadata. Without this flag, legacy JSON output is unchanged. `exact_numeric` keeps
numeric text and IEEE float bits lossless.

PG Bind text is not a core typed Value. In particular, bytea Bind text `abc` is
parsed by PostgreSQL's bytea input function; core Text `abc` cannot be presumed a
bytea Value. Binary fixture execution comparisons are withheld absent an explicit
input encoding contract. Metadata and observed errors remain visible independently.
Core errors are not pgwire SQLSTATE observations.

Primary references:
- https://www.postgresql.org/docs/18/protocol-message-formats.html
- https://www.postgresql.org/docs/18/protocol-flow.html
- https://www.postgresql.org/docs/18/app-psql.html
- https://raw.githubusercontent.com/postgres/postgres/REL_18_0/src/bin/psql/common.c

The psql18 `\parse`/`\bind_named` smoke script is retained separately from the raw
frame traces. PREPARE/EXECUTE literal SQL is never used as extended evidence.

Run from the repository root:

```powershell
python -B scripts/postgres-functions/run_prepared.py
python -B scripts/postgres-functions/observe_prepared.py --output J:/gigglesdb-work/dd68-prepared-example/reobserved.json
```

`cases-prepared.json` and `prepared-reference.json` hold 169 direct PG observations:
22 new context/NULL/reuse cases and 147 parameterized legacy SQL cases reobserved
without their literal expectations. This is bounded coverage, not every overload.
The regenerator exits nonzero on any reference drift. Core bytea execution
comparisons remain withheld pending an explicit typed Bytes input codec; raw
candidate errors remain visible and metadata comparisons still apply.

Provenance correction (E387): `python -B run_prepared.py` now creates a fresh
`dd68-pgfunc-provenance-<UTC timestamp>-<pid>` campaign outside the repository and refuses to reuse it. `capture_provenance.py
correct-history` preserves the historical latest-source record and writes a
superseding nonqualified report and corrected latest-source record in the new output directory; it does not rebuild or reinterpret that binary.
The empty compiler set in E383 was invalid evidence. Windows path keys are
normalized before comparison, aliases/escapes rejected, mandatory core/query/
functions/pg-types/model inputs required, and the entire captured/copy/live union
is compared. Conservative copied inventories include all file types; compiler
membership is separately recovered from Rust dep-info. Cargo metadata and
external package inventories, toolchain/cfg, build command/native exit, and
build-script outputs are retained. Build scripts with unproven external inputs
remain explicitly nonqualified; their output logs are not a claim to have
observed arbitrary I/O. Source drift never qualifies current source. The probe
hash and frozen inventory are checked throughout the campaign. Prepared results
retain separate root and parameter dimension denominators, and bytea Text
execution is withheld without a conversion contract. Full qualification stays
false. Regression tests are in `test_source_closure.py`.

The provenance scripts accept `--output DIRECTORY`. Use the same explicit
directory for `capture_provenance.py build`, `run_provenance_campaign.py`, and
`observe_prepared_parameters.py` when running those steps separately.
`run_prepared.py` shares one directory across its build and campaign.
