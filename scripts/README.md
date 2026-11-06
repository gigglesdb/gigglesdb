# IntelliJ runs

Open the repository root in IntelliJ IDEA. The shared `.run` configurations appear
in **Run > Edit Configurations** and the run selector, grouped by purpose. Enable
the bundled **Shell Script** plugin if the configurations appear as unknown.
These configurations use Windows PowerShell 5 and Cargo from your PATH; they do
not require a Rust IDE plugin. Install the Rust toolchain and Windows native build
tools first. Restart the IDE after changing PATH.

## Start and query a server

1. Run **01 Setup local demo**. The first run builds the server smoke test, creates
   development TLS credentials and an application database, and creates a separate
   embedded database containing a `notes` table and one example row.
2. Run **02 Server**. Wait for `native=127.0.0.1:7432 postgres=127.0.0.1:7433`.
3. While it stays running, run **03 Client Ping**, then **04 Client SQL query**.
   The query reads the initially empty `app` table over authenticated native JSON.

Type `shutdown` and Enter in the server console to stop gracefully. The IDE Stop
button terminates the process. Only one server can bind these ports. If startup
reports a lock or occupied port, stop the previous instance first.

Setup is persistent: rerunning it reuses the same database and certificates.
**Fresh local demo (retains previous)** creates another isolated setup and updates
the current pointer; stop the existing server before using it. Nothing deletes
previous databases or artifacts. All generated credentials are local development
fixtures, not deployment credentials.

The current setup is recorded in `artifacts/ide/current.json`. Its `config` field
points to the server configuration; its `fixture` field contains the client PEM
files. The generated `query.json` can be edited to send a different native SQL
request. Authorization is limited to the fixture's `app` table. Server startup
does not support network DDL. See the root README for request examples.

## Other useful runs

| Group | Runs |
| --- | --- |
| Daily | Interactive SQL shell, example SQL/native queries, native capability inventory |
| Tools | Backup + verify + fresh restore; logical export + verify + fresh import; baseline topology analysis; outbox example |
| Distributed | Real three-node failover, distributed process suite, multi-range history suite, network fault campaign |
| Verification | All-crate check, build, debug/release tests, strict Clippy, formatting check, documentation |
| Crate tests | A separate test configuration for each crate |

SQL and native example runs share the separate embedded `notes` database, not the
running server database. Close the SQL shell before running another embedded tool:
embedded writers hold an exclusive lock. Backup/export runs retain their output in
a fresh directory each time and print the resulting paths.

Distributed runs launch their own local processes and retain their fixtures. They
can take many minutes. They exercise local fault scenarios; they are not production
availability qualification. All-crate tests are similarly expensive and stop at
the first failure. Tests run serially within each crate to reduce port/process
contention. Build concurrency defaults to one job; Cargo output and failures appear
directly in the run console. Build artifacts use `target-ide`.

## Command-line equivalents and customization

From the repository root:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/dev.ps1 -Task setup
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/dev.ps1 -Task server
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/dev.ps1 -Task ping
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/dev.ps1 -Task test -Crate giggles-core -Release
```

Duplicate a configuration in the IDE to customize its task or crate. `-Release`
selects optimized binaries for daily tools and optimized builds/tests for the
generic verification tasks. The named distributed suites and outbox example use
their default debug profile. The shared configurations target Windows; on another
host the PowerShell interpreter path and `.exe` launcher assumptions need adapting.
Rust breakpoint debugging requires a Rust-capable IDE/plugin and its own Cargo
debug configuration; these shell runs provide execution and console interaction.

The XML uses JetBrains' [Shell Script run configuration](https://www.jetbrains.com/help/idea/run-debug-configuration-shell-script.html)
format. Personal `.idea/workspace.xml` settings are not changed.
