# Project memory

## Implementation delegation

All implementation work must be performed by SOL subagents. Use the SOL model
family (currently `gpt-6.1-sol`) when assigning implementation tasks. The primary
agent coordinates work, investigates, reviews changes, and reports results.

This is a standing project instruction from the user, recorded on 2026-11-06.

## Keeping SOL worker context small

Claude coordinators give each SOL worker (gpt-6.1-sol) a bounded task with:
- a clear deliverable;
- pointers to the relevant files and sections;
- acceptance checks;
- a stop condition.

Give each worker only the context its task needs. Don't pass full coordinator
history or whole specs, and don't ask for broad exploration or open-ended
continuation. Workers return compact findings or results, then stop and wait
for Claude's next bounded assignment. Required tests and project constraints
still apply. A worker brief that may write to Archdocs must name spaceId
5714972c-8f62-4f17-8ebc-b62fbf4420b9 explicitly and require checking the saved
spaceId afterwards.

Standing user instruction, recorded on 2026-11-07.

Never use `codex exec resume --last`. Several agents run Codex on this machine,
so `--last` can pick up another agent's session. Resume only by explicit
session id, and prefer a fresh bounded brief.

## Archdocs space

All GigglesDB design docs and related architecture documents must be bound to
the GigglesDB space: 5714972c-8f62-4f17-8ebc-b62fbf4420b9.
Pass this spaceId explicitly when creating documents and verify the saved
spaceId afterward. Never leave GigglesDB documents in My Docs. Include this
requirement and spaceId in every coordinator and worker brief that may write
to Archdocs. This applies to documents created by any agent or thread.

Standing user instruction, recorded on 2026-11-06.
## Generated files and task storage

All future GigglesDB build outputs, target directories, source qualification
captures, fixtures, logs, and task TEMP/TMP must live under unique task
subfolders of J:/gigglesdb-work. Set CARGO_TARGET_DIR, TEMP, and TMP explicitly
for child commands. Do not write generated files to the user's TEMP folder or
the K: source workspace. This supersedes previous C-only output guidance.
Existing evidence stays in place; do not move or delete it unless separately
requested.

Standing user instruction, recorded on 2026-11-06.

## Committing in the shared checkout

Several agents share this checkout, so commit only through a private
GIT_INDEX_FILE seeded from HEAD, then
`git update-ref refs/heads/main <new> <expected-old>` as a compare-and-swap.

Before committing, run the full `cargo test` for every crate the commit touches,
plus `cargo clippy --all-targets -- -D warnings`. Pass `--no-fail-fast` to
`cargo test` and report the totals from every test binary; without it, cargo
stops at the first failing binary and hides the rest. Run them on an exact archive of
HEAD with the staged changes applied. `cargo check` alone is not enough.

Committed blobs must be LF. K: copies are CRLF (core.autocrlf=true), so never
`git hash-object -w` a K: file with autocrlf disabled. Hash from an LF worktree
or strip CR first. Before the compare-and-swap, check
`git diff --cached --numstat <expected-old>`: a small edit that shows as a
whole-file rewrite means CRLF blobs, so stop and fix them.

Before reporting a landing, and before any index reset or K: sync, confirm
that `git -C K:/Projects/gigglesdb rev-parse refs/heads/main` equals your new
commit. A failed step earlier in the chain (for example an index.lock error)
can skip the update-ref without stopping the script.

After a successful compare-and-swap, run `git reset -q -- <committed paths>` so
the shared index matches HEAD for those paths. Otherwise the shared index keeps
the old content staged, and anyone's plain `git commit` reverts your work. Never
reset or stage another owner's paths.

Then bring the K: working copies of your committed paths up to date. A path
whose K: content still equals its pre-commit blob gets
`git -c core.autocrlf=false checkout HEAD -- <path>`. A path that holds someone
else's uncommitted edits gets your hunks merged in, or is left alone and
reported. A stale K: copy reads as a revert of your commit. End every commit message with the
required Co-Authored-By trailer, and never push.

## Ownership of giggles-core SQL files

Agreed by DD-0068 on 2026-11-07, at the user's request, to speed up feature work.

- `relational.rs` and `relational/query.rs` (the parser, planning and execution
  dispatch) are co-owned by E1 (GD-0059) and E3 (GD-0058). They commit changes
  there without a DD-0068 pre-release.
- DD-0068 keeps pre-release authority over:
  - its scalar/function layer: `relational/query/functions*.rs`,
    `query/functions/**`, the scalar types and coercion, and the closed-set
    error goldens;
  - persisted-format surfaces, wherever they live: Relational's serialized
    fields and its namespace-5 encoders and decoders (`LegacyNamespace5`,
    `StatementNamespace5`, the format-first decode, `catalog_ext.rs` wire
    encoding and `ext_version`), the `validate()` format and ext rules, the
    index codec revision rules, `format_policy.rs` and the feature-bit mapping.
    Any change to the bytes written or accepted needs pre-release; refactors
    that only change in-memory representation don't.
  - DD-0068's test files, which are behaviour tripwires: the
    `crates/giggles-core/tests/sql_*.rs` files DD-0068 published,
    `quoted_routines.rs`, `table_free_classification.rs`,
    `sql_character_adversarial.rs`, `relational/query/aggregate_tests.rs`, the
    DD-0068 server `*_network.inc` files (including
    `routing_rejection_network.inc`), `qualified_routine_network.rs`,
    `declared_parameter_network.rs`, `tests/fixtures/dd68/*` and
    `scripts/postgres-functions/`. Any edit to these files, including changing
    an expected result, needs pre-release.
- A co-owned change that breaks a DD-0068 test either gets fixed, or goes to
  DD-0068 before landing with the expectation change and its PG18 basis.
- Co-owned changes that alter error codes or scalar semantics, and any change
  that touches the order of authorization before classification (E939, even
  with no test change), get a same-day heads-up to DD-0068 after landing. The
  heads-up gives the commit SHA, the diff path and the SQLSTATEs or semantics
  affected, and is copied into DD-0068's work log. A DD-0068 follow-up fix goes
  to the front of the commit queue.

## Shared compile cache (sccache)

Every cargo command sets these environment variables:

```
RUSTC_WRAPPER=J:/gigglesdb-work/tools/bin/sccache.exe
SCCACHE_DIR=J:/gigglesdb-work/sccache-cache
SCCACHE_CACHE_SIZE=100G
CARGO_INCREMENTAL=0
```

sccache 0.8.2 is installed from the official release, with its checksum
verified. It returns cache hits only when the source path and the target
directory are both the same as in the earlier build. So each track works from
one stable source location (a git worktree under J:/gigglesdb-work/wt/<track>)
and keeps one stable CARGO_TARGET_DIR. Reuse both across proofs rather than
creating a new archive or target directory per proof. A target directory can
then be deleted at any time to free disk, because rebuilding it comes from the
cache. Two concurrent builds must never share one CARGO_TARGET_DIR.

Builds share one machine (32 threads, limited Windows commit charge). Too many
parallel links fail with LNK1171 or OOM, and that wastes a whole gate run. So:
- every cargo command also sets `CARGO_BUILD_JOBS=4`;
- each coordinator, together with its own Codex workers, runs at most one heavy
  cargo build or test at a time. Hold a per-coordinator lock file under
  J:/gigglesdb-work/<track>/ while it runs;
- a gate that fails with LNK1171, an OOM or a lost connection in a timing test
  is rerun alone before anyone treats it as a code failure;
- stopping a task or wrapper does not stop its children. Kill the whole
  process tree (`taskkill /T /F /PID <pid>`), and confirm that no cargo,
  rustc, codex or test process is still using that worktree or target dir
  before you start another run there.

The shared repository sets core.autocrlf=true, so a plain worktree checkout
gives files CRLF line endings, and LF-sensitive fixtures (tzdata, typmod
oracles) then show as modified or fail. Create and move worktrees with
`git -c core.autocrlf=false worktree add --detach <path> <ref>` and
`git -c core.autocrlf=false -C <path> checkout --detach <ref>`, and compute
proof hashes from committed blobs (`git hash-object` / `git rev-parse <ref>:<path>`),
never from CRLF working copies.

## Integration test binaries

giggles-core and giggles-language build their `tests/*.rs` files as a single
test binary, `tests/all.rs` (`autotests = false`), because linking one binary
per file dominated gate time. Build plus tests went from 620s to 249s for
giggles-core and from 115s to 67s for giggles-language; target directories
shrank from 21G to 1.7G and 4.6G to 1.3G (GD-0001 E135-E137).

- Register every new test file. Add it to `tests/all.rs` as
  `#[path = "x.rs"] mod x;`, or, if it re-runs its own test binary by exact
  test name (`current_exe`), as its own `[[test]]` in `Cargo.toml`. The
  `every_test_file_is_registered` test fails for an unregistered file.
- Test names in the merged binary carry the file as a module prefix, so run
  one file with `cargo test --test all <file>::` and use
  `<file>::<test>` with `--exact`.

## Cleaning up J:/gigglesdb-work

Agents must clean up after themselves in J:/gigglesdb-work. When a work item
is committed, abandoned or superseded, the agent that created its folders
deletes the regenerable bulk:
- target directories;
- git-archive exports and proof overlays;
- task TEMP/TMP;
- dev-server and test data directories;
- benchmark datasets;
- stopped containers and volumes it created.

Keep only small evidence that a commit, journal entry or report references,
such as reports, qualification JSON, patches and short logs. Delete only
folders you created, never another thread's, and never while a run is still
using them. A long-lived per-track CARGO_TARGET_DIR that is reused across
proofs is fine, and is preferred over a new target directory per proof.

Standing user instruction, recorded on 2026-11-06.
