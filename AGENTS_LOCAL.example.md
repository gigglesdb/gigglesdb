# Local agent settings (template)

Copy to `AGENTS_LOCAL.md` (gitignored) and fill in values for your machine.
`AGENTS.md` refers here for every machine- and account-specific value.

## Checkout and work directory

- Shared checkout: `<path to this checkout>`
- Work directory: `<work-dir>` — all build outputs, target directories,
  captures, logs and task TEMP/TMP go in unique task subfolders here, outside
  the source checkout.
- Track worktrees: `<work-dir>/wt/<track>`
- Per-coordinator build lock files: `<work-dir>/<track>/`

## Worker model

- Implementation workers: `<provider>`, model `<model id>`
- Maximum concurrent delegated workers: `<n>`

## Compile cache

```
RUSTC_WRAPPER=<path to sccache>
SCCACHE_DIR=<work-dir>/sccache-cache
SCCACHE_CACHE_SIZE=<size, e.g. 100G>
CARGO_INCREMENTAL=0
CARGO_BUILD_JOBS=<jobs, e.g. 4>
```

## Build limits

- Machine: `<threads>`, `<memory notes>`
- Build-slot wrapper: `<command prefix that wraps cargo>`

## Remote gates

- Provider and account: `<provider>`
- Maximum concurrent gate boxes: `<n>`
- Maximum box starts per day: `<n>`
- Slot directory: `<work-dir>/boat/slots/`
- Usage log: `<work-dir>/boat/usage.log`
- Gate snapshot: `<name>`; bench snapshot: `<name>`
- Box size: `<size>`; maximum TTL: `<hours>`
- Snapshot rebuilds: `<who may rebuild, how often>`
