# scripts

Development and verification scripts for GigglesDB contributors, including shared IntelliJ run configurations.

**Status:** Provides Windows and macOS workflows; local rehearsals do not qualify production availability.

Part of [GigglesDB](../README.md).

Open the repository root in IntelliJ IDEA. The shared `.run` configurations appear
in **Run > Edit Configurations** and the run selector, grouped by purpose. Enable
the bundled **Shell Script** plugin if the configurations appear as unknown.
The original configurations use Windows PowerShell 5 and Cargo from your PATH;
they do not require a Rust IDE plugin. Install the Rust toolchain and Windows native
build tools first. Restart the IDE after changing PATH.

## macOS setup

On macOS, choose the matching **(macOS)** configurations in the **macOS - Daily**,
**macOS - Tools**, **macOS - Distributed**, **macOS - Verification**, and
**macOS - Crate tests** folders. They use `/bin/bash` and the sibling `.sh` scripts.
Install Xcode Command Line Tools (`xcode-select --install`), Rust through rustup,
and Python 3 (`python3`). A JDK is optional for PostgreSQL JDBC compatibility runs.
Restart the IDE after installing tools or changing PATH.

The macOS work root defaults to `~/gigglesdb-work`; set `GIGGLES_WORK_ROOT` to
override it. The demo uses its `ide` subdirectory by default. Pass `--work` to
choose another task subdirectory of that root.

## Start and query a server

1. Run **01 Setup local demo**. The first run builds the server smoke test, creates
   development TLS credentials and an application database, and creates a separate
   embedded database containing a `notes` table and one example row.
2. Run **02 Server**. Wait for `wire=127.0.0.1:7432 postgres=127.0.0.1:7433`.
3. While it stays running, run **03 Client Ping**, then **04 Client SQL query**.
   The query reads the initially empty `app` table over authenticated native wire.

Type `shutdown` and Enter in the server console to stop gracefully. The IDE Stop
button terminates the process. Only one server can bind these ports. If startup
reports a lock or occupied port, stop the previous instance first.

Setup is persistent: rerunning it reuses the same database and certificates.
**Fresh local demo (retains previous)** creates another isolated setup and updates
the current pointer; stop the existing server before using it. Nothing deletes
previous databases or artifacts. All generated credentials are local development
fixtures, not deployment credentials.

The current setup is recorded in `<work>/current.json` (Windows defaults to
the script's fixed Windows work directory, selected with `-Work`; macOS defaults to `~/gigglesdb-work/ide`). Its `config` field
points to the server configuration; its `fixture` field contains the client PEM
files. The generated `query.json` can be edited to send a different native SQL
request. Authorization is limited to the fixture's `app` table. Server startup
does not support network DDL. See the root README for request examples.

## Other useful runs

**PostgreSQL client compatibility** creates a fresh isolated test database and
runs actual pgJDBC 42.7.3 and the pinned Rust driver. It does not use the live demo.
It prints a JSON report with PASS/FAIL/UNTESTED for each case; known catalog gaps
currently make the overall run fail. It discovers a JDK from `JAVA_HOME` or
Eclipse Adoptium and the pinned JAR from JetBrains' driver cache. Override with
`powershell -NoProfile -File scripts/check-pg-clients.ps1 -JavaHome PATH -JdbcJar PATH`.
Missing drivers remain UNTESTED. Build targets are separate from the live IDE server.
On macOS, use `bash scripts/check-pg-clients.sh --java-home PATH --jdbc-jar PATH`;
the script also discovers a JDK using `JAVA_HOME` or `/usr/libexec/java_home`.

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
directly in the run console. Demo and verification build artifacts use
`<work>/target`; generated demo data and temporary files also live under `<work>`.

## Command-line equivalents and customization

From the repository root:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/dev.ps1 -Task setup
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/dev.ps1 -Task server
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/dev.ps1 -Task ping
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/dev.ps1 -Task test -Crate giggles-core -Release
```

On macOS:

```bash
bash scripts/dev.sh --task setup
bash scripts/dev.sh --task server
bash scripts/dev.sh --task ping
bash scripts/dev.sh --task test --crate giggles-core --release
```

The other equivalents are `--work`, `--port`, `--java-home`, and `--jdbc-jar`.
Regenerate the macOS configurations after changing a Windows configuration:

```bash
python3 scripts/gen-macos-run-configs.py
python3 scripts/gen-macos-run-configs.py --check
```

The check reports missing, stale, or orphan macOS files without writing them.

Duplicate a configuration in the IDE to customize its task or crate. `-Release`
selects optimized binaries for daily tools and optimized builds/tests for the
generic verification tasks. The named distributed suites and outbox example use
their default debug profile. On macOS the equivalent switch is `--release`.
Rust breakpoint debugging requires a Rust-capable IDE/plugin and its own Cargo
debug configuration; these shell runs provide execution and console interaction.

The XML uses JetBrains' [Shell Script run configuration](https://www.jetbrains.com/help/idea/run-debug-configuration-shell-script.html)
format. Personal `.idea/workspace.xml` settings are not changed.
