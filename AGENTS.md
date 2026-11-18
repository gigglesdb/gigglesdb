# Agent guide

Rules for AI agents working in this repository.

**Read `AGENTS_LOCAL.md` too, if it exists.** It is gitignored and holds this
machine's values: the work directory, compile-cache settings, build limits,
the build-slot wrapper, remote-gate account limits and worker model names.
Wherever this file says "see local settings", take the value from there. A
template is in [`AGENTS_LOCAL.example.md`](AGENTS_LOCAL.example.md).

Contents:
[Delegation](#delegation) ·
[Sizing leads and workers](#sizing-leads-and-workers) ·
[Archdocs space](#archdocs-space) ·
[Generated files](#generated-files) ·
[Committing in the shared checkout](#committing-in-the-shared-checkout) ·
[Ownership of giggles-core SQL files](#ownership-of-giggles-core-sql-files) ·
[Compile cache and build limits](#compile-cache-and-build-limits) ·
[Integration test binaries](#integration-test-binaries) ·
[Tiered verification](#tiered-verification) ·
[When to gate](#when-to-gate) ·
[Remote gates](#remote-gates) ·
[Landing with land-commit.sh](#landing-with-scriptsland-commitsh) ·
[Cleaning up](#cleaning-up)

## Delegation

Delegated implementation workers do all implementation work. The worker model
is named in local settings. The primary agent coordinates, investigates,
reviews changes and reports results.

Give each worker a bounded task:
- a clear deliverable;
- pointers to the relevant files and sections;
- acceptance checks;
- a stop condition.

Give each worker only the context its task needs. Don't pass full coordinator
history or whole specs, and don't ask for broad exploration or open-ended
continuation. Workers return compact findings or results, then stop and wait
for the next bounded assignment. Required tests and project constraints still
apply. A brief for a worker that may write to Archdocs names the space ID
(see [Archdocs space](#archdocs-space)) and requires checking the saved space
afterwards.

Never use `codex exec resume --last`. Several agents may run Codex on the same
machine, so `--last` can pick up another agent's session. Resume only by
explicit session id, and prefer a fresh bounded brief.

## Sizing leads and workers

An agent that nears its context limit loses detail when it compacts, and a
long session costs more per step than a fresh one, because every later turn
re-reads the whole history. Split the work to fit each agent:
- One lead session covers one stack: brief the workers, review, gate once,
  land, write the handoff, then stop. A lead that has landed its stack is
  finished, even if its phase has more items.
- Never give a lead a whole plan phase or an open-ended list. Never add new
  items, rebases or follow-ups to a lead that is already running. Queue them
  in the journal handoff, and start a fresh lead for each.
- A lead that is blocked or waiting on a decision writes its handoff and
  stops. It does not stay idle.
- The coordinator starts each new lead from the journal handoff with a short
  brief: the one item, the lane, the acceptance checks and the stop condition.
  It never passes coordinator history.
- Leads keep their reads small: report files, greps and summary lines, not
  whole logs, diffs or other threads' transcripts.
- A lead that has run past about 15 turns, or past two gate rounds on one
  stack, writes a handoff and stops at its next boundary. The coordinator
  continues the work with a fresh lead.
- The coordinator follows the same rule. When its own thread grows long, it
  writes a handoff and asks to continue in a new thread.

## Archdocs space

All GigglesDB design docs and related architecture documents belong to the
GigglesDB space: `5714972c-8f62-4f17-8ebc-b62fbf4420b9`. Pass this spaceId
explicitly when creating documents and verify the saved spaceId afterward.
Never leave GigglesDB documents in My Docs. Include this requirement and the
spaceId in every coordinator and worker brief that may write to Archdocs. This
applies to documents created by any agent or thread.

## Generated files

Build outputs, target directories, qualification captures, fixtures, logs and
task TEMP/TMP live in unique task subfolders of the work directory (see local
settings), never in the source checkout or the system TEMP folder. Set
`CARGO_TARGET_DIR`, `TEMP` and `TMP` explicitly for child commands. Existing
evidence stays where it is; do not move or delete it unless asked.

## Committing in the shared checkout

Several agents share one checkout, so commit only through a private
`GIT_INDEX_FILE` seeded from HEAD, then
`git update-ref refs/heads/main <new> <expected-old>` as a compare-and-swap.

Before committing, run the full `cargo test` for every crate the commit
touches, plus `cargo clippy --all-targets -- -D warnings`. Pass
`--no-fail-fast` to `cargo test` and report the totals from every test binary;
without it, cargo stops at the first failing binary and hides the rest. Run
them on an exact archive of HEAD with the staged changes applied.
`cargo check` alone is not enough. Which crates and modes the gate covers is
set by [Tiered verification](#tiered-verification); the gate runs once per
landed stack, not once per commit or per worker.

Committed blobs must be LF. The shared checkout uses `core.autocrlf=true`, so
its working copies are CRLF; never `git hash-object -w` one with autocrlf
disabled. Hash from an LF worktree or strip CR first. Before the
compare-and-swap, check `git diff --cached --numstat <expected-old>`: a small
edit that shows as a whole-file rewrite means CRLF blobs, so stop and fix them.

Before reporting a landing, and before any index reset or checkout sync,
confirm that `git -C <shared checkout> rev-parse refs/heads/main` equals your
new commit. A failed step earlier in the chain (for example an index.lock
error) can skip the update-ref without stopping the script.

After a successful compare-and-swap, run `git reset -q -- <committed paths>` so
the shared index matches HEAD for those paths. Otherwise the shared index keeps
the old content staged, and anyone's plain `git commit` reverts your work.
Never reset or stage another owner's paths.

Then bring the shared checkout's working copies of your committed paths up to
date. A path whose content still equals its pre-commit blob gets
`git -c core.autocrlf=false checkout HEAD -- <path>`. A path that holds someone
else's uncommitted edits gets your hunks merged in, or is left alone and
reported. A stale working copy reads as a revert of your commit. End every
commit message with the required Co-Authored-By trailer, and never push.

## Ownership of giggles-core SQL files

- `relational.rs` and `relational/query.rs` (the parser, planning and execution
  dispatch) are co-owned by E1 (GD-0059) and E3 (GD-0058). They commit changes
  there without a DD-0068 review.
- DD-0068 owns:
  - its scalar/function layer: `relational/query/functions*.rs`,
    `query/functions/**`, the scalar types and coercion, and the closed-set
    error goldens;
  - persisted-format surfaces, wherever they live: Relational's serialized
    fields and its namespace-5 encoders and decoders (`LegacyNamespace5`,
    `StatementNamespace5`, the format-first decode, `catalog_ext.rs` wire
    encoding and `ext_version`), the `validate()` format and ext rules, the
    index codec revision rules, `format_policy.rs` and the feature-bit mapping.
    Any change to the bytes written or accepted needs review; refactors that
    only change in-memory representation don't.
  - DD-0068's test files, which are behaviour tripwires: the
    `crates/giggles-core/tests/sql_*.rs` files DD-0068 published,
    `quoted_routines.rs`, `table_free_classification.rs`,
    `sql_character_adversarial.rs`, `relational/query/aggregate_tests.rs`, the
    DD-0068 server `*_network.inc` files (including
    `routing_rejection_network.inc`), `qualified_routine_network.rs`,
    `declared_parameter_network.rs`, `tests/fixtures/dd68/*` and
    `scripts/postgres-functions/`. Any edit to these files, including changing
    an expected result, needs review.
- A co-owned change that breaks a DD-0068 test either gets fixed, or goes to
  review before landing with the expectation change and its PG18 basis.

DD-0068 is inactive, so nobody answers a pre-release request; do not wait for
one. Wherever a change above needs review, the landing lead gets one
independent review before the landing gate:
- The reviewer is a fresh agent that did not write the change (for example a
  delegated worker with role review).
- For persisted-format changes it checks that databases written before the
  change still open and read byte for byte the same, and that new bytes,
  feature bits or `ext_version` values are written only when the new feature
  is used. It also checks that the numbers taken do not collide with
  anything already on main.
- For scalar and function semantics, error codes and changed test
  expectations, it checks each change against PostgreSQL 18.6 output from the
  oracle image and records that output as the basis.
- Fix what the review finds. Then record the review and its verdict in the
  design doc's journal, with a short note in DD-0068's work log so the
  history stays in one place.
- Co-owned changes that alter error codes or scalar semantics, or touch the
  order of authorization before classification (E939, even with no test
  change), get the same journal note after landing: commit SHA, diff path and
  the SQLSTATEs or semantics affected.

## Compile cache and build limits

Every cargo command uses sccache with incremental compilation off. The exact
`RUSTC_WRAPPER`, `SCCACHE_DIR` and `SCCACHE_CACHE_SIZE` values are in local
settings; always set `CARGO_INCREMENTAL=0`.

sccache returns hits only when the source path and the target directory are
both the same as in the earlier build. So each track works from one stable
source location (a git worktree under the work directory) and keeps one
stable `CARGO_TARGET_DIR`. Reuse both across proofs rather than creating a new
archive or target directory per proof. A target directory can then be deleted
at any time to free disk, because rebuilding it comes from the cache. Two
concurrent builds must never share one `CARGO_TARGET_DIR`.

Builds share one machine, and too many parallel links fail with LNK1171 or
OOM, wasting a whole gate run. So:
- every cargo command sets `CARGO_BUILD_JOBS` to the local value;
- each coordinator, together with its own workers, runs at most one heavy
  cargo build or test at a time, holding a per-coordinator lock file under the
  work directory while it runs;
- a gate that fails with LNK1171, an OOM or a lost connection in a timing test
  is rerun alone before anyone treats it as a code failure;
- stopping a task or wrapper does not stop its children. Kill the whole
  process tree (on Windows, `taskkill /T /F /PID <pid>`), and confirm that no
  cargo, rustc, codex or test process is still using that worktree or target
  dir before you start another run there.

Every cargo command in a worker or gate is wrapped in the build-slot script
named in local settings. It provides two shared heavy slots, plus a third slot
for `cargo check` and `cargo clippy`, which link nothing.

The repository sets `core.autocrlf=true`, so a plain worktree checkout gives
files CRLF line endings, and LF-sensitive fixtures (tzdata, typmod oracles)
then show as modified or fail. Create and move worktrees with
`git -c core.autocrlf=false worktree add --detach <path> <ref>` and
`git -c core.autocrlf=false -C <path> checkout --detach <ref>`, and compute
proof hashes from committed blobs (`git hash-object` /
`git rev-parse <ref>:<path>`), never from CRLF working copies.

The repository's `.cargo/config.toml` links with `rust-lld` and builds the dev
profile with `debug = "line-tables-only"`. That cut the giggles-core test
build from 197s to 138s and its target directory from 21G to 11G, with
backtraces keeping file and line numbers. Do not override it per track: a
changed linker or debug level also misses the shared cache.

## Integration test binaries

giggles-core, giggles-language, giggles-server, giggles-pg-catalog and
giggles-pg-types build their `tests/*.rs` files as a single test binary,
`tests/all.rs` (`autotests = false`), because linking one binary per file
dominated gate time. Build plus tests went from 620s to 249s for giggles-core
and from 115s to 67s for giggles-language.

- Register every new test file. Add it to `tests/all.rs` as
  `#[path = "x.rs"] mod x;`, or, if it re-runs its own test binary by exact
  test name (`current_exe`), as its own `[[test]]` in `Cargo.toml`. The
  `every_test_file_is_registered` test fails for an unregistered file.
- Test names in the merged binary carry the file as a module prefix, so run
  one file with `cargo test --test all <file>::` and use `<file>::<test>`
  with `--exact`.

## Tiered verification

Compile time, not test run time, dominates, so verification is split into
lanes and each expensive pass runs once.

Worker lanes. Workers do not run the landing gate.
- On every change: `cargo check` and only the tests for the code they changed
  (`cargo test --test all <file>::` or `cargo test --lib <module>::`). Never a
  crate's full suite or clippy per change or per sub-task.
- Workers never run a crate's full test suite locally, not even at the end of
  a feature. Local machine time is for scoped runs; the full run is the
  coordinator's remote landing gate once the whole feature is done.
- On the final brief, workers run `cargo clippy --all-targets -- -D warnings`
  once, on each crate they changed. Clippy doesn't link, so it is cheap, and
  it keeps lint failures out of the remote gate.
- Workers report totals and failing test names, not logs.
- Workers reuse a fixed lane (one stable worktree plus one stable
  `CARGO_TARGET_DIR` per lane, reset between tasks), never a fresh per-task
  worktree: the cache misses on every new source path, so per-task worktrees
  build cold every time.

Landing gate. The coordinator runs it once on the tip of the stack it lands,
remotely with `scripts/gate-remote.sh` by default, and locally only when the
script exits 3 (daily cap) or 4 (no slot). It is sized by what the stack
changes:
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

Rebase after main moves. If the commits that moved main touch none of the
stack's paths and nothing they depend on, run `git apply --check`,
`cargo check` of the affected crates and any test binary the new commits
added, then land without a new gate. Otherwise rebase and re-gate.

## When to gate

A landing gate is for finished work only.
- Gate when a feature, or a stack of features, is complete: the worker lanes
  pass, the review is done, and it would land if the gate passed. Never gate
  to explore, to see how far along something is, or to check a partial
  feature.
- Batch. If another feature in the same track will be ready within about an
  hour, wait and gate the two together as one stack.
- A failed gate gets a fix in a worker lane, checked in that lane. Rerun the
  gate only once the fix is done, not after each attempt.

## Remote gates

Landing gates may run on remote Linux boxes through `scripts/gate-remote.sh`.
Boxes cost money per start and per hour, so the account limits in local
settings (concurrent boxes, daily starts, box size, snapshot names) are hard
caps, and the script enforces them.
- Only coordinators start or delete boxes. Workers never do.
- Boxes run landing gates and planned benchmarks only. No worker iteration,
  focused tests, exploration or agent prompts on boxes.
- Take a slot before starting a box and release it after deleting the box, as
  the script does. Log every start in the usage log. If the daily limit is
  reached, gate locally and say so.
- Tests that fail on Linux by design are listed, with reasons, in
  `scripts/gate-linux-expected-failures.txt`; any other failure fails the gate.
- Every box starts from the saved gate snapshot with no environment and a
  short TTL. Never disable auto-stop or start several boxes in one command.
- One box per gate. A flake rerun (out-of-memory, lost connection) reuses the
  same box, without a new start.
- Delete the box as soon as the results are copied back. Never leave a box
  running or stopped. Once a day, list boxes and delete any with no slot owner.
- Code goes up only as a git bundle of the stack's commits. Never upload
  secrets, `.env` files or working copies.
- Only one designated coordinator rebuilds and re-saves the gate snapshot, at
  most once a day, or when the toolchain or native dependencies change.

Benchmarks use `scripts/bench-remote.sh` under the same rules, from the bench
snapshot, marked `bench` in the usage log. Run one only for a planned
measurement (a baseline or a before/after comparison), never to iterate on
code.

## Landing with scripts/land-commit.sh

`scripts/land-commit.sh` performs the steps of
[Committing in the shared checkout](#committing-in-the-shared-checkout) after a
passing gate: private index seeded from the expected main, blobs hashed from
an LF worktree (it refuses CR bytes in text files), the numstat printout, the
`update-ref` compare-and-swap and its verification, the shared-index reset of
the committed paths, and the working-copy sync, which leaves any copy with
foreign edits alone and names it. It never pushes.

```
scripts/land-commit.sh --base <expected-old-main> --from <LF worktree> \
    --message-file <msg> <repo-relative path>...
```

Batch landings when several finished changes are ready. Stack them as
separate commits in one worktree on the current main, run one full gate on the
tip, then land them in order, passing each printed commit as the next
`--base`. If main moves first, rebase the stack and re-gate (or apply the
disjoint-rebase rule).

## Cleaning up

Clean up after yourself in the work directory. When a work item is committed,
abandoned or superseded, the agent that created its folders deletes the
regenerable bulk:
- target directories;
- git-archive exports and proof overlays;
- task TEMP/TMP;
- dev-server and test data directories;
- benchmark datasets;
- stopped containers and volumes it created.

Keep only small evidence that a commit, journal entry or report references,
such as reports, qualification JSON, patches and short logs. Delete only
folders you created, never another thread's, and never while a run is still
using them. A long-lived per-track `CARGO_TARGET_DIR` reused across proofs is
fine, and preferred over a new target directory per proof.
