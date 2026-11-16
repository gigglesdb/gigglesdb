**Documentation:** [Nunya site](site/index.html) | [Quickstart](site/docs/quickstart.html) | [Reference](site/docs/index.html)

# Nunya

Nunya is a separate database entry point for AI agents. Applications keep their existing direct database connections, connection strings and drivers. Agents use their own endpoint and per-task credentials; Nunya connects upstream with its own least-privilege role (`[upstream] user`) and replaces protected values with task-scoped tokens. Agents can carry tokens between approved columns without seeing the underlying values. Masked columns return a placeholder or SQL NULL. Unanalysable operations fail closed.

`nunya serve --config <path>` starts the configured routes and snapshot refresh; the service also starts the admin endpoint. Upstream TLS supports `disable`, `require` (encrypted, accepts any certificate), and `verify-full` (hostname verification against `ca_file` as the only trust root). Set `upstream.dialect` to `postgres` (default) or `spanner-pg` for Google PGAdapter. `max_connections` defaults to 256 and permits 1 through 10000 concurrent agent connections. Policies reload across both routes through the admin endpoint or Unix SIGHUP; invalid policies retain the current policy. Optional `policy_watch_ms` (100 through 3600000) polls for changes and waits for two identical consecutive reads before attempting a reload. `GET /status` reports the applied policy hash, timestamps and last reload outcome. Failed or unsafe snapshot refreshes retain the old snapshot until its age causes NY008 refusals. Ctrl-C or SIGTERM stops accepting and allows sessions up to 30 seconds to drain.

## Threat model

Agents must have no direct route to the database. Enforce this with network isolation and keep upstream credentials away from agents. Nunya does not protect against someone holding the database's own credentials. Unmarked columns are not protected. Counts of shared values remain visible, including equality relationships within a token domain and scope. Protect task secrets, root keys, state files and logs using filesystem permissions. Plaintext mode is only appropriate on a trusted local network.

## Quickstart

Build from this crate with `cargo build --release`. Copy `examples/nunya.toml` and `examples/policy.toml` into a private configuration directory. Set the upstream host, port and database to your server. Mark every sensitive column in the policy before exposing the gateway. The example uses plaintext local connections explicitly.

```sh
umask 077
nunya key generate > root.key
# Add root_key_file = "root.key" at the top level of nunya.toml.
printf '%s\n' 'your-upstream-password' > upstream.password
# Add password_file = "upstream.password" under [upstream].
nunya policy check --config nunya.toml
nunya task open --config nunya.toml --ttl 30m --label reporting
nunya serve --config nunya.toml
# Use the printed user and secret:
psql 'host=127.0.0.1 port=6432 dbname=app user=task:<id> sslmode=disable'
```

Enter the one-time task secret at psql's password prompt. `task list --config nunya.toml` lists live tasks without secrets; `task close --config nunya.toml <id>` revokes one. TTL units are `s`, `m` and `h`. Expiries are Unix milliseconds. Never commit secret files. Instead of files, supply `NUNYA_ROOT_KEY` (64 hex characters) and `NUNYA_UPSTREAM_PASSWORD`. `standing hash` reads one password line from stdin and emits a SCRAM verifier for a `[[standing]]` entry. Its output is a credential and should be protected.

## Policy reference

```toml
snapshot_max_age_ms = 60000
[readlog]
sink = "stdout"
[domains.email]
tokenize = "on"
tokenize_limit_per_task = 20
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

Each column has exactly one treatment: `blind` names a declared domain; `masked = true` replaces output with `placeholder`, or SQL NULL when omitted. Placeholders require textual columns. Domain names start with a lowercase letter and contain lowercase letters, digits or underscores (maximum 31 characters). Tokenization defaults to `off`; `on` requires `tokenize_limit_per_task` between 1 and 100000. Domains can declare `range = { min = 0, max = 1048575 }` for supported bounded types. A range must contain at least 2^20 values and fit every participating column's type limits. Columns sharing a domain must have compatible types and ranges. Policies are validated against the actual upstream schema, including unsafe dependent objects.

The read-log sink is required. Use `sink = "file"`, `path = "read.log"` and optional positive `max_bytes` (default 100 MiB) for a rotating file. Relative log paths resolve against the policy directory. Snapshot maximum age defaults to 60000 ms. Unknown policy keys are errors. Read logs may contain query text and tokens; restrict access.

## Error codes

| SQLSTATE | Message |
| --- | --- |
| NY001 | token used outside its domain |
| NY002 | operation not allowed on a blind column |
| NY003 | token not valid in this task |
| NY004 | statement could not be fully analysed |
| NY005 | masked column used in an expression |
| NY006 | statement kind not allowed through the gateway |
| NY007 | tokenize limit reached for this task |
| NY008 | schema snapshot is stale |
| NY009 | read log unavailable |
| NY010 | protocol feature not allowed through the gateway |

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

## Local PostgreSQL harness

`bash harness/run.sh` starts PostgreSQL 16, 17 and 18, waits for health checks, and removes containers and volumes on exit. Press Enter to stop. Host ports are 16432, 17432 and 18432; change the config port to select a version. Use `NUNYA_UPSTREAM_PASSWORD=nunya-harness-only` and a freshly generated `NUNYA_ROOT_KEY`. Seed credentials are for isolated local tests only. It uses the official `postgres:16-alpine`, `postgres:17-alpine` and `postgres:18-alpine` images. The script prints a fresh temporary task root for run logs and temporary files; optionally choose any directory with `--task-root /path/to/runs`.

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
the PostgreSQL route. At least one route is required. Both use the same policy,
root key, task store, standing identities and inbound TLS certificate/key files.
`policy check` validates the policy against every configured route's catalog.
Admin reload and Unix SIGHUP reload both routes together after validation against
every current catalog. Native catalog snapshots still refresh periodically.

Agents supply `Authorization: Bearer nunya.<user>.<secret>` metadata, using the
user and secret returned by `task open` (user `task:<id>`), or a configured standing
identity. Pass GoogleSQL tokens as query parameters; literals are never decrypted.
`PartitionQuery`, `PartitionRead` and `BatchWrite` RPCs are refused.
For default-schema policy tables, use the leading-dot form `table = ".Customers"`.

Production native upstreams use Google credentials and HTTPS (the default endpoint
is `https://spanner.googleapis.com`; `endpoint` can select a trusted HTTPS endpoint).
Set `emulator_endpoint` explicitly to the emulator's `http://host:port` address to
use plaintext without Google credentials. The database must still use the full
resource path above. `allow_plaintext = true` permits plaintext agent connections
for trusted local development; otherwise supply `[tls]` certificate and key files.
