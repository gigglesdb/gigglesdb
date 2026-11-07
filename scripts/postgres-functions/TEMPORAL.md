# Temporal reference and SQL boundary

New generated outputs must stay outside the repository. `GIGGLESDB_WORK_ROOT`
overrides the default `J:/gigglesdb-work`; automatic directories use
`dd68-pgfunc-<script>-<UTC timestamp>-<pid>`. Defaults and explicit output paths
are checked before writes, including resolved paths. `.runs` contains legacy
evidence only; new runs there are prohibited. Keep existing evidence in place.
Use `python -B` (as below) or `PYTHONDONTWRITEBYTECODE=1` to avoid repository
bytecode caches. Set `TEMP`, `TMP`, and `CARGO_TARGET_DIR` under a unique work
folder for child commands.


`python -B scripts/postgres-functions/observe_temporal.py` regenerates this family's
expectation candidates in a new external output directory (`--output DIRECTORY`) from the exact PG18 image in conformance.py, in an owned network-none,
tmpfs container. It verifies the unchanged inventory pin, C/UTF8 and tzdata 2025b.
The optional `--reuse-observed` retains previously observed expectations only when
every fixture input matches, and observes additions; the default observes all.

Run integrity checks with `python -B -m unittest discover -s scripts/postgres-functions
-p test_temporal.py`. Run the complete combined corpus with `python -B
scripts/postgres-functions/run_temporal.py PATH_TO_EXISTING_FUNCTION_PROBE_EXE`.
That driver copies the executable, records before/after source/corpus hashes,
calls the CLI through subprocess.run and preserves its actual exit code. Its
artifact directory (optional second positional argument) must be new. Existing executable/source correspondence remains
unverified: a source snapshot does not establish the inputs of an earlier build.

Candidate setup is empty, allowing actual SQL literal casts to be rejected by the
current probe. `reference_setup` records explicit UTC, ISO YMD, postgres interval,
C lc_time and extra_float_digits=3 for each independent oracle session. Constructor
and timezone arguments select other zones explicitly. Candidate session-state,
wire and persistence qualification remain absent.

The main expectations use existing exact_numeric transport: numeric_text retains
PostgreSQL dscale, float4_bits/float8_bits retain send bytes, temporal values retain
PostgreSQL text, and columns retain format_type. SQL NULL is JSON null. Errors
retain actual SQLSTATE; temporal errors cannot pass solely by matching an engine
error. `expectedUnsupported` is a baseline limitation marker, not permission to
skip or synthesize result cells. Every non-ok candidate for these cases is FAIL.

`temporal-reference.json` separately records actual PG send-function hex for each
successful supported result type, including numeric_send's scale bytes. These
are reference-only binary contracts, never claims that the SQL probe implements
temporal binary transport. Its binary NULL stays SQL NULL, not empty bytes.
Casts link the actual catalog input routine; EXTRACT syntax and callable
pg_catalog."extract" have distinct case IDs. Cast and EXTRACT special_form labels
are outside the immutable 3413 pg_proc denominator.

Current-date age overloads, one-argument session-zone timezone overloads, and
named-zone timetz conversions depending on today's date are deliberately outside
this deterministic family. Their inventory entries remain untested. This corpus
is a foundation sample, not full temporal qualification or an exhaustive routine
catalog. Standalone pg-types results do not qualify SQL execution. verified stays
0 and complete/full stays false. Original 66, string 250, binary 255 and numeric
783 cases, inventory and their existing reference pins are unchanged.
