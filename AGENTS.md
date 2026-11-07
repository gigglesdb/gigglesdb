# Project memory

## Implementation delegation

All implementation work must be performed by SOL subagents. Use the SOL model
family (currently `gpt-6.1-sol`) when assigning implementation tasks. The primary
agent coordinates work, investigates, reviews changes, and reports results.

This is a standing project instruction from the user, recorded on 2026-11-06.

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

After a successful compare-and-swap, run `git reset -q -- <committed paths>` so
the shared index matches HEAD for those paths. Otherwise the shared index keeps
the old content staged, and anyone's plain `git commit` reverts your work. Never
reset or stage another owner's paths. End every commit message with the
required Co-Authored-By trailer, and never push.

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
