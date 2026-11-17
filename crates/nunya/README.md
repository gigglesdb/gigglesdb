**Documentation:** [Nunya site](site/index.html) | [Quickstart](site/docs/quickstart.html) | [Reference](site/docs/index.html)

# Nunya

Nunya is a separate database entry point for AI agents. Applications keep their existing direct database connections, connection strings and drivers. Agents use their own endpoint and access-session credentials; Nunya connects upstream with its own least-privilege role (`[upstream] user`) and replaces protected values with session-scoped tokens. Agents can carry tokens between approved columns without seeing the underlying values. Masked columns return a placeholder or SQL NULL. Unanalysable operations fail closed.

`nunya serve --config <path>` starts the configured routes and snapshot refresh; the service also starts the admin endpoint. Upstream TLS supports `disable`, `require` (encrypted, accepts any certificate), and `verify-full` (hostname verification against `ca_file` as the only trust root). Set `upstream.dialect` to `postgres` (default) or `spanner-pg` for Google PGAdapter. `max_connections` defaults to 256 and permits 1 through 10000 concurrent agent connections. Admin `POST /reload`, Unix SIGHUP and optional `policy_watch_ms` (100 through 3600000) re-read the config first, then attempt policy files independently; file watches wait for two identical consecutive changed reads, and a policy watch also re-reads config. Invalid config reloads apply nothing; restart-only changes are ignored with warnings while reloadable settings apply, preserving tokens, counters and valid sessions. Check the config and per-route policy outcomes in `GET /status`, including `restart_pending`, rather than relying on the reload response; see [reloadable and restart-only settings](site/docs/configuration.html#reload). Failed or unsafe snapshot refreshes retain the old snapshot until its age causes NY008 refusals. Ctrl-C or SIGTERM stops accepting and allows sessions up to 30 seconds to drain.

## Threat model

Agents must have no direct route to the database. Enforce this with network isolation and keep upstream credentials away from agents. Nunya does not protect against someone holding the database's own credentials. Unmarked columns are not protected. Counts of shared values remain visible, including equality relationships within a token domain and scope. Protect principal and session secrets, root keys, state files and logs using filesystem permissions. Plaintext mode is only appropriate on a trusted local network.

## Quickstart

Deploy once, provision a principal, then connect through an access session. The [site quickstart](site/docs/quickstart.html) has the complete local PostgreSQL configuration and Windows instructions. [Concepts](site/docs/concepts.html), [configuration](site/docs/configuration.html) and [CLI reference](site/docs/cli.html) explain scope continuity, lifetime settings and commands.

### 1. Deploy once

Build from this crate with `cargo build --release`. Put the binary on PATH. Copy `examples/nunya.toml` and `examples/policy.toml` into a private configuration directory, set the upstream host, port and database, and mark every sensitive column before exposing the gateway. Create a private local `state_dir` and preserve it across restarts. A local harness database is also available below.

Add an authenticated management listener to the configuration (separate from the private admin listener):

```toml
[management]
listen = "127.0.0.1:6481"
```

From the private directory, create credential files and set `root_key_file = "root.key"` at the top level and `password_file = "upstream.password"` under `[upstream]`:

```sh
umask 077
nunya key generate > root.key
printf '%s\n' 'your-upstream-password' > upstream.password
nunya policy check --config nunya.toml
nunya serve --config nunya.toml
```

Keep serve running. Use verified TLS for remote database traffic; management requires TLS off loopback. Instead of files, the service accepts `NUNYA_ROOT_KEY` (64 hex characters) and `NUNYA_UPSTREAM_PASSWORD`. Never commit secret files.

### 2. Provision an identity

On Unix, use the administrator socket as the service user; substitute the absolute state directory:

```sh
nunya principal create reporting-agent --kind agent --server unix:/absolute/private/state/nunya.sock
nunya principal grant reporting-agent --route postgres=read-only --server unix:/absolute/private/state/nunya.sock
nunya principal credential add reporting-agent --generate-secret --server unix:/absolute/private/state/nunya.sock
```

The credential command prints an ID and then the secret once. Save only the secret line in private `principal.secret`.

Windows has no socket. Before starting serve, run `nunya standing hash`, provide an administrator password on stdin, and add its verifier to the configuration:

```toml
[[management.admin]]
name = "operator"
verifier = "REPLACE_WITH_GENERATED_SCRAM_VERIFIER"
```

Protect the verifier and save the original password as the first line of private `operator.secret`. Replace the Unix provisioning commands with:

```sh
nunya principal create reporting-agent --kind agent --server http://127.0.0.1:6481 --principal operator --principal-secret-file operator.secret
nunya principal grant reporting-agent --route postgres=read-only --server http://127.0.0.1:6481 --principal operator --principal-secret-file operator.secret
nunya principal credential add reporting-agent --generate-secret --server http://127.0.0.1:6481 --principal operator --principal-secret-file operator.secret
```

### 3. Connect

Use the agent's management credential to acquire an access session, then run psql with that session's PostgreSQL environment:

```sh
nunya session acquire --server http://127.0.0.1:6481 --principal reporting-agent --principal-secret-file principal.secret --route postgres --context reporting --ttl 1h
nunya exec --server http://127.0.0.1:6481 --principal reporting-agent --principal-secret-file principal.secret --context reporting -- psql -d app -c "SELECT 1"
```

Database ports accept only session credentials: PostgreSQL user `session:REPLACE_WITH_SESSION_ID` (the ID is 32 lowercase hex characters) and the session secret. Principal credentials work only on management. `nunya session env --server http://127.0.0.1:6481 --principal reporting-agent --principal-secret-file principal.secret --context reporting --format sh` prints credentials for other clients; choose `--format powershell` on Windows and treat the output as secret. Protect the helper's private `sessions.json` in the per-user state directory, or the directory selected by `NUNYA_HOME`. The default route is `postgres`, with PostgreSQL at `127.0.0.1:6432`.

Reconnects and renewals preserve the persisted token scope, so equal values in the same domain keep equal tokens across connections. `nunya session show` displays public details; `nunya session renew` renews access, and `--rotate` rotates the credential while keeping the scope. Use `nunya session acquire --isolated` to create a separate scope in a new context, then `nunya exec --context <that context>` to use it. If a stored session has ended, the helper requires `--new-scope` before replacement; obtain fresh tokens for the new scope. Default lifetimes are 8 hours absolute and 1 hour idle. Shorten them with `[session] max_lifetime`, `idle_timeout`, or a shorter acquisition `--ttl`.

Revoke while the service is still running, then stop it with Ctrl-C:

```sh
nunya session revoke --server http://127.0.0.1:6481 --principal reporting-agent --principal-secret-file principal.secret --context reporting
```

Revocation or expiry ends active work and closes PostgreSQL connections. NY011 reports that access ended; NY012 reports a forwarded write whose outcome was not observed. Reconcile an uncertain write before retrying; Nunya never replays it. See [operations](site/docs/operations.html) for revocation, private-network reachability and recovery, and the [reload settings table](site/docs/configuration.html#reload) before changing a running service. A state restore can bring back revoked sessions: revoke all sessions or disable affected principals before serving traffic after any restore.

## Per-route policies

Each route accepts its own `policy` path; relative paths resolve against the configuration file's directory. The optional top-level `policy` is the fallback for routes without their own key:

```toml
policy = "policy.toml"          # optional fallback for routes without their own

[upstream]
policy = "pg-policy.toml"       # PostgreSQL route validates only against the PG catalog
# ...

[spanner_native]
policy = "spanner-policy.toml"  # native Spanner route validates only against its catalog
# ...
```

These are configuration fragments; add the remaining route settings. A configured route without an effective policy fails startup with `upstream.policy: required path missing (no service-level policy)` or `spanner_native.policy: required path missing (no service-level policy)`. Unknown keys are refused. To use the old single-policy behavior, point both routes to the same file, directly or through the fallback. Files with the same canonical path form one reload unit. `policy_watch_ms` stays top-level and applies to every file. Each route validates its policy only against its own catalog, including during `nunya policy check`, so a column present on only one route can be protected there.

| Per route | Shared by the service |
| --- | --- |
| Column treatments, domain definitions and tokenize limits, `snapshot_max_age_ms`, validation, reload outcome and policy revision | Read log (must match), state store, sessions/principals and explicit-tokenize counters |

Principal tokenize counters are keyed by principal and domain name: the same domain name on two routes shares a count. Each route enforces its own limit against that count, and whichever route opens a principal window sets its length. Per-session counters belong to one session, which belongs to one route. Use different domain names to keep route counters independent.

Every file must name the same `[readlog]` sink for statement, lifecycle and management records; startup refuses a mismatch with `policy files name different read logs`. With multiple policy files, a reload that changes the sink is rejected for that file. To move the log, update all files and restart. A single shared file may still change the sink on reload.

`policy_revision` is the SHA-256 of the route's own policy file. Statement records and management session views carry the session route's revision. Management `GET /v1/status` returns `policy_revisions`, an object mapping route IDs to revisions (null if unavailable), with the legacy `policy_revision` field present when one route exists or all routes share one revision, for older clients; `config_revision` identifies the last applied config file independently of policy revisions. Admin status fields and route-labeled metrics are documented in [operations](site/docs/operations.html#reload).

## Policy reference

```toml
snapshot_max_age_ms = 60000
[readlog]
sink = "stdout"
[domains.email]
tokenize = "on"
tokenize_limit_per_session = 20
tokenize_limit_per_principal = 200
tokenize_principal_window = "24h"
[[column]]
table = "public.customers"
name = "email"
blind = "email"
[[column]]
table = "public.customers"
name = "phone"
masked = true
placeholder = "[hidden]"
```

Each column has exactly one treatment: `blind` names a declared domain; `masked = true` replaces output with `placeholder`, or SQL NULL when omitted. Placeholders require textual columns. Domain names start with a lowercase letter and contain lowercase letters, digits or underscores (maximum 31 characters). Tokenization defaults to `off`; `on` requires per-session and per-principal limits and a principal window; the principal limit must be at least the session limit. The principal window persists across reconnects, new sessions and restarts. See the [policy reference](site/docs/policy.html#limits) for bounds and reload behavior. Domains can declare `range = { min = 0, max = 1048575 }` for supported bounded types. A range must contain at least 2^20 values and fit every participating column's type limits. Columns sharing a domain must have compatible types and ranges. Policies are validated against the actual upstream schema, including unsafe dependent objects.

The read-log sink is required. Use `sink = "file"`, `path = "read.log"` and optional positive `max_bytes` (default 100 MiB) for a rotating file. Relative log paths resolve against the policy directory. Snapshot maximum age defaults to 60000 ms. Unknown policy keys are errors. Read logs may contain query text and tokens; restrict access.

## Error codes

| SQLSTATE | Message |
| --- | --- |
| NY001 | token used outside its domain |
| NY002 | operation not allowed on a blind column |
| NY003 | token not valid in this session |
| NY004 | statement could not be fully analysed |
| NY005 | masked column used in an expression |
| NY006 | statement kind not allowed through the gateway |
| NY007 | tokenize limit reached |
| NY008 | schema snapshot is stale |
| NY009 | read log unavailable |
| NY010 | protocol feature not allowed through the gateway |
| NY011 | access session ended |
| NY012 | write outcome unknown |

See the [error reference](site/docs/errors.html) for recovery, including interrupted work and uncertain writes.

## Container image

Run the **Nunya image** workflow manually in GitHub Actions to build and push
`ghcr.io/gigglesdb/nunya:sha-<short sha>` for `linux/amd64`. Default-branch builds
also receive `latest`; the optional `tag` input adds a release tag such as
`0.1.0`. Set `push` to false for a dry run that loads and smoke-tests the image
without publishing it. The workflow summary records the image reference and
digest, and every build runs the image with `--help`.

## Container or service

Build from the repository root using `docker build -f crates/nunya/Dockerfile -t nunya .`. The image runs as uid 10001 and exposes port 6432. Mount a read-only configuration directory at `/etc/nunya` and a writable state directory owned by uid 10001 at the path configured by `state_dir`. Mount secret and TLS files read-only with permissions that let that uid read them. Set `[tls]` `cert_file` and `key_file` for agent TLS; without TLS, configuration requires explicit `allow_plaintext = true`. The admin address defaults to loopback port 6480; keep it private. Its `GET /health`, `GET /metrics`, `GET /status` and `POST /reload` routes have no authentication.

For a service, use a dedicated unprivileged account, a private configuration directory, and a writable private state directory. Run `nunya serve --config /etc/nunya/nunya.toml`. Exit 0 means clean shutdown; exit 1 means a configuration or service error.

## Local harness

`bash harness/run.sh --work-root /path/to/private/work` starts PostgreSQL 16, 17 and 18, waits for health checks, and removes containers and volumes on exit. Press Enter to stop. Host ports are 16432, 17432 and 18432; change the config port to select a version. Use `NUNYA_UPSTREAM_PASSWORD=nunya-harness-only` and a freshly generated `NUNYA_ROOT_KEY`. Seed credentials are for isolated local tests only. It uses the official `postgres:16-alpine`, `postgres:17-alpine` and `postgres:18-alpine` images. Each run creates a unique subdirectory under the work root for logs, data and temporary files.

`bash harness/run.sh private --work-root /path/to/private/work` runs the private-network, session, driver, restart, revocation and expiry checks against PostgreSQL 18.6. The database stays on the private container network; the gateway publishes loopback PostgreSQL port 26432 and management port 26481, overridable with `NUNYA_PG_PORT` and `NUNYA_MGMT_PORT`. Set `NUNYA_EXE` (or `--nunya-exe`) to the host binary, `NUNYA_PYTHON` to the Python executable and `NUNYA_JDBC_JARS` to the JDBC dependency directory. The private runner uses `build-linux.sh` unless `--linux-bin` supplies a Linux binary; that build helper accepts `NUNYA_BUILD_WRAPPER` as an optional command prefix for its Docker build invocation. The top-level runner delegates these settings to its helpers. Private mode exits after its scenarios and removes its Compose resources.

`bash harness/run.sh spanner --work-root /path/to/private/work` starts the Spanner emulator and PGAdapter harness and waits for Enter before cleanup. See the [driver guide](site/docs/drivers.html#harness) for private-harness prerequisites and the [Spanner guide](site/docs/spanner.html) for route setup.

## Spanner

Use the PostgreSQL route through PGAdapter with `upstream.dialect = "spanner-pg"`,
or expose the native GoogleSQL gRPC route:

```toml
[spanner_native]
listen = "127.0.0.1:6433"
database = "projects/my-project/instances/my-instance/databases/my-database"
# For a trusted local emulator:
# emulator_endpoint = "http://127.0.0.1:9010"
# snapshot_refresh_ms = 30000
```

The native route can run alone (omit `[upstream]` and top-level `listen`) or beside
the PostgreSQL route. At least one route is required. Both share the root key,
durable session store, principals and inbound TLS certificate/key files; each
route may select its own policy. `policy check` validates each route's policy
against its own catalog. Admin reload and Unix SIGHUP attempt each distinct file
independently; routes sharing one file reload together after validation against
all their catalogs. Native catalog snapshots still refresh periodically.

Agents supply `Authorization: Bearer nunya.session:REPLACE_WITH_SESSION_ID.REPLACE_WITH_SESSION_SECRET` metadata
using access-session credentials. Principal credentials authenticate only to the
management API. Pass GoogleSQL tokens as query parameters; literals are never
decrypted. `PartitionQuery` and `PartitionRead` RPCs are refused; read-only
sessions also refuse DML, Commit with mutations and BatchWrite.
For default-schema policy tables, use the leading-dot form `table = ".Customers"`.

Production native upstreams use Google credentials and HTTPS (the default endpoint
is `https://spanner.googleapis.com`; `endpoint` can select a trusted HTTPS endpoint).
Set `emulator_endpoint` explicitly to the emulator's `http://host:port` address to
use plaintext without Google credentials. The database must still use the full
resource path above. `allow_plaintext = true` permits plaintext agent connections
for trusted local development; otherwise supply `[tls]` certificate and key files.
