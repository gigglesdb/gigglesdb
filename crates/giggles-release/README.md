# giggles-release

A bounded local Rust packaging library and CLI. It creates deterministic `giggles-release-json-v1` archives (`.ggr`), verifies embedded file sizes and SHA256 hashes, and inspects the verified manifest. Hashes establish integrity against an expected digest; they do not authenticate a publisher or provide signatures.

Build and check on the currently exercised host, Windows x86_64 MSVC:

```powershell
$env:CARGO_TARGET_DIR = "$PWD/crates/giggles-release/target-release-worker"
$env:CARGO_BUILD_JOBS = '1'
cargo build --manifest-path crates/giggles-release/Cargo.toml --locked -j1
cargo test --manifest-path crates/giggles-release/Cargo.toml --locked -j1
cargo run --manifest-path crates/giggles-release/Cargo.toml -- create request.json new-release.ggr
cargo run --manifest-path crates/giggles-release/Cargo.toml -- verify new-release.ggr
cargo run --manifest-path crates/giggles-release/Cargo.toml -- inspect new-release.ggr
```

`request.json` is a regular UTF8 JSON file, at most 1MiB. Stdin is intentionally unsupported. Example (replace the absolute workspace and explicit identity with your inputs):

```json
{
  "workspace": "K:/work/source",
  "manifest": "crates/giggles-core/Cargo.toml",
  "release": "0.1.0-local",
  "target": "x86_64-pc-windows-msvc",
  "profile": "release",
  "source_revision": "explicit-source-revision-or-snapshot-label",
  "dirty": true,
  "build_receipt": null,
  "files": [
    {"source":"K:/work/build/release/giggles.exe","path":"bin/giggles.exe","role":"Binary"},
    {"source":"crates/giggles-core/README.md","path":"README.md","role":"Readme"}
  ]
}
```

Revision and dirty state are explicit caller assertions; the tool never replaces uncommitted contents with HEAD. Exact sorted source fingerprints identify content independently. No tested/production flags exist. Files are opt-in only: single-level `bin/` binaries, product `README.md`, and `LICENSE`/`LICENSE.*`. Select only trusted product documentation and binaries; a role cannot determine whether arbitrary bytes contain secrets. No database, logs or process records are automatically included. No license is invented; missing product/dependency license metadata appears in `readiness_issues`.

The archive is one JSON object containing `manifest` and a unique map of portable relative file names to lowercase hexadecimal bytes. It is our own versioned inventory/archive format, not ZIP, tar, CycloneDX or another SBOM standard. Files, sources, dependencies and issues are sorted on creation. No timestamps or staging names enter the archive. Identical explicit inputs (including toolchain versions and any optional build receipt) produce identical bytes. A receipt records exact absolute command paths/output, so relocating inputs changes those explicit inputs. Deterministic packaging does not establish compiler bit reproducibility.

Each artifact and total artifact bytes are capped at64MiB, archive at160MiB, manifest at4MiB; limits include128 files,8192 source records and2048 dependency records. Unsafe/absolute/traversing paths, Windows device names, alternate streams, trailing dots/spaces, non-ASCII paths, case collisions, unknown fields/formats, symlinks and Windows reparse points fail. Hard links are ordinary files and permitted. Verification reads only the archive and never launches its contents; it requires no Cargo or network.

Creation uses offline `cargo metadata --locked` plus the actual `Cargo.lock`; all lock entries remain inventoried, including optional/platform entries absent from metadata. Such missing metadata and missing licenses are explicit issues. Local package manifests must be included in the source fingerprint. Rust sources, Cargo manifests/locks and simple literal `include_str!`/`include_bytes!` inputs are fingerprinted within the explicit workspace. Direct string literals retain spaces, raw-string contents and Rust escapes. Dynamic includes/generated `include!`, explicit module `#[path]` redirects (including conditional attributes), non-`.rs` Cargo targets and targets outside the scanned package tree fail closed. Source-only inventory excludes targets/artifacts/.git. This is a bounded Rust source profile, not a capture of arbitrary compiler environment or native/system dependencies. Metadata may fail when the local Cargo cache is incomplete. The explicit workspace must contain every local dependency, including dev dependencies and workspace manifests. Unknown path dependencies fail.

Optional strict local build uses a request with `request` (the create request), `package`, `binary`, a new absolute `target_dir` and `timeout_seconds` (1..3600):

```powershell
cargo run --manifest-path crates/giggles-release/Cargo.toml -- build build-request.json build-receipt.json
```

It invokes Cargo through argument arrays with `--locked --offline --manifest-path ... --package ... --bin ... --profile ... --target ... -j1`, owns `CARGO_TARGET_DIR`, records rustc/Cargo versions, command, status and exact executable digest, and compares pre/post source fingerprints. Local custom build scripts require additional input/environment qualification and are rejected. Registry dependencies may run their normal build scripts; those are represented by locked versions/checksums, not a claim about native toolchain reproducibility. Failed Cargo invocations retain a failure receipt; preflight errors print the reason before a build is launched. A supplied successful receipt must match the packaged executable, target/profile/toolchain and source fingerprints. Builds use a deadline and capped retained diagnostic output; killing Cargo does not guarantee termination of all external compiler descendants.

Only an explicit action executes a trusted **locally built** executable, separately from verification:

```powershell
cargo run --manifest-path crates/giggles-release/Cargo.toml -- smoke --local-built build-receipt.json K:/work/build/x86_64-pc-windows-msvc/release/giggles.exe K:/work/smoke
```

This checks the local build receipt's digest, then creates a uniquely retained scratch DB, creates/inserts a SQL row and queries it in fresh processes, then inspects the reopened DB. Do not point this action at untrusted downloaded executables or receipts. Receipts and hashes are not proof of publisher identity.

Publication uses a newly created unique staging directory beside the output. Files and complete archive are synced and verified before one no-clobber filesystem hard-link publishes the archive. Existing outputs never change; a failed publication leaves staging for inspection. The complete archive is the completion marker. Interrupted prepublication work is incomplete; a published archive must independently verify. Staging/temp fixtures and failure logs are retained without automatic cleanup. This requires a trusted local filesystem with same-volume hard links. Power-loss directory durability, malicious privileged host races, network filesystems and other platforms are not qualified.

Signing, independent audit, exact product license selection, cross-platform qualification, production/security/cloud/performance gates and the PostgreSQL control remain open. This tool and local demo do not constitute a release approval.
