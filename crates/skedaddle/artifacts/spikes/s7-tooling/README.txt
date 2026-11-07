S7 / GD-0049 test tooling. No external writes, commits, pushes or subagents.
Run from K:\Projects\gigglesdb\crates\skedaddle in PowerShell:
  . .\artifacts\spikes\s7-tooling\env.ps1
  cargo install cargo-mutants --version 27.1.0 --locked --root $env:CARGO_HOME
  python -B artifacts\spikes\s7-tooling\mutations.py <fresh-run-id>
  python -B artifacts\spikes\s7-tooling\network.py <fresh-run-id>
  python -B artifacts\spikes\s7-tooling\collect.py <fresh-run-id>
Create artifacts\s7-<fresh-run-id> first. Never reuse an output directory.
mutation-commands.json records exact executed commands, exit statuses and times.
env.ps1 sets jobs=2, target/s7, shared S1 cargo-home and TEMP/TMP=target/s7/temp.
--leak-dirs retains source scratch copies. --gitignore=true excludes S1 cargo-home.
--jobs 1 serializes mutants, --jobserver-tasks 2 caps compiler parallelism.
--file src/lib.rs scopes a file; --re 'checkpoint::' scopes its module.
S1 uses --file src/main.rs --re ' in (lsn|fmt)\b' to scope two helpers.
--output <path> creates <path>\mutants.out (do not point at a reused directory).
Standalone [workspace] Rust 2024 crates work on Windows MSVC.
Exit 2 means surviving mutants, expected for this probe. S1 | -> ^ on disjoint
LSN halves is an equivalent survivor; it does not establish a missing test.
Toxiproxy 2.13.0: API POST /proxies; POST /proxies/{name}/toxics; DELETE toxic.
PG upstream pg:5432, fake-gcs upstream gcs:4443; listens 0.0.0.0:5433 / :4444.
Downstream toxics: latency 250ms jitter0; timeout400ms; reset_peer timeout0;
bandwidth8 KiB/s; slicer average_size128 variation0 delay10000 microseconds.
Clients: Python socket PG-wire/HTTP on Windows, psql in this run's PG container.
PG uses trust ONLY in this isolated disposable test network. Queries and object
reads verify an intact 32768-byte payload. Each toxic is removed before next test.
Fake-gcs object is uploaded/read through the proxy; MinIO runs but is not probed.
Container DNS uses toxiproxy and pg, avoiding a trip through Windows published ports.
Host uses ephemeral loopback ports discovered with docker compose port; no fixed range.
host.docker.internal is unnecessary here (it is for a container reaching a host service).
Docker Desktop surfaced reset_peer as EOF/closed connection rather than Winsock RST.
Slicer does not guarantee TCP recv boundaries; intact payload and added delay are checked.
Network adapter calls S4 guard/up/export/down/check(RUN); never sweep or cleanup.ps1.
Only the run's labeled resources are removed; all host files and scratch are retained.
Sources: https://github.com/sourcefrog/cargo-mutants/releases/tag/v27.1.0
         https://github.com/Shopify/toxiproxy/blob/main/README.md
