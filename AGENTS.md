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
Which crates and modes the gate covers is set by "Tiered verification" below;
the gate runs once per landed stack, not once per commit or per worker.

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

The repository's `.cargo/config.toml` links with `rust-lld` and builds the dev
profile with `debug = "line-tables-only"`. On giggles-core that cut the test
build from 197s to 138s and the target directory from 21G to 11G, with
backtraces keeping file and line numbers. Builds of any crate under the
repository pick it up; do not override it per track, because a changed linker
or debug level also misses the shared sccache.

## Integration test binaries

giggles-core, giggles-language, giggles-server, giggles-pg-catalog and
giggles-pg-types build their `tests/*.rs` files as a single test binary,
`tests/all.rs` (`autotests = false`), because linking one binary per file
dominated gate time. Build plus tests went from 620s to 249s for
giggles-core and from 115s to 67s for giggles-language; target directories
shrank from 21G to 1.7G and 4.6G to 1.3G (GD-0001 E135-E137).

- Register every new test file. Add it to `tests/all.rs` as
  `#[path = "x.rs"] mod x;`, or, if it re-runs its own test binary by exact
  test name (`current_exe`), as its own `[[test]]` in `Cargo.toml`. The
  `every_test_file_is_registered` test fails for an unregistered file.
- Test names in the merged binary carry the file as a module prefix, so run
  one file with `cargo test --test all <file>::` and use
  `<file>::<test>` with `--exact`.

## Tiered verification

Agreed with the user on 2026-11-14 to raise throughput. Compile time, not
test run time, dominates, so verification is split into lanes and each
expensive pass runs once.

Worker lanes. SOL workers do not run the landing gate.
- On every change: `cargo check` and only the tests for the code they
  changed (`cargo test --test all <file>::` or `cargo test --lib <module>::`).
  Never a crate's full suite or clippy per change or per sub-task (user,
  2026-11-15).
- Once, as a sanity check when the whole feature is done (the coordinator
  marks that brief FINAL): `cargo test --no-fail-fast` and
  `cargo clippy --all-targets -- -D warnings`, default features only, in each
  crate whose `src/` or `tests/` they changed. Add the `--no-default-features`
  pair only when the change touches `cfg(feature = ...)` code.
- Workers report totals and failing test names, not logs.
- Workers reuse a fixed lane (one stable worktree plus one stable
  CARGO_TARGET_DIR per lane, reset between tasks), never a fresh per-task
  worktree: sccache misses on every new source path, so per-task worktrees
  build cold every time.

Landing gate. The coordinator runs it once on the tip of the stack it lands
(see batching below), sized by what the stack changes:
- Code in a crate's `src/`: full `cargo test --no-fail-fast` plus clippy for
  that crate, in default mode and, for crates with optional features
  (giggles-core, giggles-server, giggles-language), also in
  `--no-default-features` mode. Add dependent crates only where behaviour
  crosses: SQL parsing or semantics in giggles-core also gates
  giggles-language and giggles-server; public API changes gate the crates
  that call the changed items.
- Only new or changed `tests/` files (registered in `tests/all.rs`): build
  that crate's test binary, run `cargo test --test all <file>::` for each
  changed file, plus clippy `--all-targets` for that crate. No dependent sweep.
- Docs, `compat/` Python, `scripts/` and other non-Rust files only: no cargo
  gate; run that tooling's own tests if it has any (for example
  `python -I -m pytest compat/tests`).
- Clippy runs once per crate per mode, and clippy with `--no-default-features`
  only for crates whose change touches feature-gated code.

Rebase after main moves. The disjoint-rebase rule applies to every track:
if the commits that moved main touch none of the stack's paths and nothing
they depend on, run `git apply --check`, `cargo check` of the affected
crates and any test binary the new commits added, then land without a new
gate. Otherwise rebase and re-gate.

Build slots. Every cargo command in a worker or gate is wrapped in
`bash J:/gigglesdb-work/r2/buildslot2.sh cargo ...`. That gives two shared
heavy slots, plus a third slot for check-only `cargo check` and
`cargo clippy`, which link nothing. CARGO_BUILD_JOBS stays 4.

## When to gate

Agreed with the user on 2026-11-15. A landing gate is for finished work only.
- Gate when a feature, or a stack of features, is complete: the worker
  lanes pass, the review is done, and it would land if the gate passed.
  Never gate to explore, to see how far along something is, or to check a
  partial feature.
- Batch. If another feature in the same track will be ready within about an
  hour, wait and gate the two together as one stack.
- A failed gate gets a fix in a worker lane, checked in that lane. Rerun the
  gate only once the fix is done, not after each attempt.

## Remote gates on boat.dev

Landing gates may run on boat.dev Linux boxes (user account, paid per start
and per machine hour). The rules below cap what anyone starts.
- Only coordinators start or delete boxes. SOL workers never run `boat`.
- Boxes run landing gates only (see "When to gate"). No worker iteration,
  focused tests, exploration or agent prompts on boxes.
- At most 2 gate boxes run at once, account-wide, across all coordinators.
  Take a slot with an atomic
  `mkdir J:/gigglesdb-work/boat/slots/slot-<1|2>`, release it after
  `boat delete`, and write the box id into the slot's `owner` file.
- At most 15 box starts per day, all agents together. Append one line per start to
  `J:/gigglesdb-work/boat/usage.log` (UTC time, coordinator, box id, type,
  stack tip, and later the result and minutes). Check the day's count before
  starting. If the limit is reached, gate locally and tell the user.
- Use `scripts/gate-remote.sh`, which enforces these rules (cap, slot, TTL,
  bundle, expected Linux failures, delete, log). For example:
  `scripts/gate-remote.sh --tip <stack tip> --who <track> -- giggles-core
  giggles-core:nodef giggles-language`. Exit 3 (cap reached) or 4 (no slot)
  means run the gate locally. Tests that fail on Linux by design are listed,
  with reasons, in `scripts/gate-linux-expected-failures.txt`; any other
  failure fails the gate.
- Every box starts from the saved snapshot `gigglesdb-gate-base` with
  `--no-env` and a `--ttl` of 2 hours at most. Never use `--no-auto-stop` or
  `--count`.
- One box per gate. A flake rerun (out-of-memory, lost connection) reuses the
  same box, without a new start.
- Delete the box (`boat delete <id>`) as soon as the results are copied back.
  Never leave a box running or stopped. Once a day, check `boat list` and
  delete any box with no slot owner.
- Size: `default` unless the user approves `large` for gates. `xlarge` is
  not allowed.
- Code goes up only as a git bundle of the stack's commits. Never push
  secrets, `.env` files or K: working copies.
- Only one coordinator, named by the user, rebuilds and re-saves
  `gigglesdb-gate-base`, and at most once a day, or when the toolchain or
  native dependencies change.

## Landing with scripts/land-commit.sh

`scripts/land-commit.sh` performs the steps of "Committing in the shared
checkout" after a passing gate: private index seeded from the expected main,
blobs hashed from an LF worktree (it refuses CR bytes in text files), the
numstat printout, the `update-ref` compare-and-swap and its verification, the
shared-index reset of the committed paths, and the K: sync, which leaves any
K: copy with foreign edits alone and names it. It never pushes.

```
scripts/land-commit.sh --base <expected-old-main> --from <LF worktree>     --message-file <msg> <repo-relative path>...
```

Batch landings when several finished changes are ready. Stack them as
separate commits in one worktree on the current main, run one full gate on the
tip, then land them in order, passing each printed commit as the next
`--base`. One gate then covers the whole stack instead of one gate per commit.
If main moves first, rebase the stack and re-gate (or apply the disjoint-rebase
rule).

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
