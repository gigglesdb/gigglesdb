# CI and container releases

CI runs every top-level crate on pull requests and manual runs; main pushes
test giggles-server (default and no-default-features), nunya and skedaddle.
Each test lane runs locked tests and clippy. Core, language and server have
both feature modes. Other crates depending on core have isolated lanes;
engine-bench (122 locked packages) and pg-conformance (139) also get isolated
lanes; small crates are split into three groups (scripts/ci-matrix.json). RocksDB
and independently linked binaries make target caches too large for the
repository's 10 GB Actions cache allowance, so Rust targets are not cached.
BuildKit uses one cache scope per product, saves only on main, and exports
minimal final-stage cache. PR builds consume caches but do not save them.

Main publishes all three linux/amd64 images to ghcr.io/<lowercase-owner>:
giggles, nunya and skedaddle, with sha-<7-character-commit> and main tags.
It never updates latest. Fork PRs build and smoke-test without credentials
or publishing. Manual runs test everything and build without publishing.
The remote landing gate remains required and unchanged.

After each package's first publication, an owner must open the package's
GitHub settings, choose Change visibility, and make it Public. Do this once
for each of giggles, nunya and skedaddle; public repository visibility does
not automatically make GHCR packages public.

To release, wait for successful main CI for the chosen commit, then push,
for example, nunya-v0.2.0, giggles-v1.2.3 or skedaddle-v0.1.0-rc.1.
Run git tag nunya-v0.2.0 <tested-main-sha>, then git push origin nunya-v0.2.0.
Only MAJOR.MINOR.PATCH with an optional SemVer prerelease is accepted.
The commit must belong to main's history. Release automation checks the
source image's full revision label and promotes its existing digest without
rebuilding. Stable releases update X.Y.Z, X.Y and latest; prereleases update
only X.Y.Z-prerelease. Every destination digest is verified, and the GitHub
Release includes the immutable image reference. If publication has not
finished, rerun the failed release workflow after main CI succeeds.

## Running images

Giggles uses ENTRYPOINT giggles-server and CMD --no-stdin-control /etc/giggles/CONFIG.json.
Mount a complete configuration read-only there and persistent storage at
/var/lib/giggles (writable by UID/GID 10001). Set database in the configuration
to a path under /var/lib/giggles; mount the TLS certificate, private key and
client CA at the paths specified in that configuration. Configure the desired
PostgreSQL listener to bind 0.0.0.0:5432, then publish port 5432. See the
server's configuration examples and README for authentication and bindings.
The --dev listener is hard-coded to loopback, so it is unsuitable for the
container's default. Run with docker run -d; stdin need not remain open.
The default --no-stdin-control flag disables stdin commands. docker stop sends
SIGTERM, which gracefully drains listeners and API runtimes (SIGINT also
uses this shutdown path). Reload requires stdin control: omit the flag and
keep stdin open to send reload/shutdown commands.

Nunya keeps ENTRYPOINT nunya and CMD serve --config /etc/nunya/nunya.toml.
Mount its configuration there and publish its configured port (default 6432).
Skedaddle uses ENTRYPOINT skedaddle and CMD --help: pass the migration
subcommand and mount its configuration/data as described in its README.
All images run as UID 10001. For a quick smoke check, run an image with
--help; Giggles prints usage and exits 2, which CI accepts only with usage text.

## Service-dependent tests

CI runs ordinary cargo tests, leaving existing ignored tests ignored.
Nunya's live PostgreSQL, Spanner native/PGAdapter, linked-domain and upstream
TLS tests require the opt-in NUNYA_TEST_* environment and harness services.
Skedaddle's ignored real-PG apply, campaign/replication, MinIO/fake-GCS,
Toxiproxy and sizing tests require its PowerShell harness and explicit
configuration; ordinary tests use mocks or local fixtures. Windows-only
filesystem/security tests are compiled out on Linux. Server network suites
start loopback listeners/processes and need no externally hosted database.

PostgreSQL oracle fixtures are checked in; regeneration and full compat
oracle campaigns need Docker/PostgreSQL 18.6 and are separate from the compat
Python unit tests (pytest only; service calls are mocked). ICU 76.1 is
provisioned and the glibm shim built before Rust tests.

CI sources the remote gate's log checker, including exact reruns and aborted
binary detection. Only the existing documented Linux failure
deferred_routine_role_network::deferred_routines_nonsuperuser_four_protocols
is allowed. No additional platform failures are masked. New service or
platform failures must be investigated rather than blanket-allowlisted.

Baseline verification at bd25245c4: compat unit tests currently fail the
repository corpus inventory and primary-key policy checks. CI keeps these
failures visible. Optional agent-load smoke needs GIGGLES_SERVER_BIN. Server
implicit_block_oracle_network and pipeline_oracle_network generator tests
require disposable PostgreSQL 18.6 and GD0084_PG_*; they stay ignored.

Skedaddle currently pins a historical core commit using a machine-local file
URL. CI and its Dockerfile rewrite that URL at the Git transport layer to
https://github.com/gigglesdb/gigglesdb, keeping the locked revision unchanged.
This requires network access to that public Git history during builds.
