[CmdletBinding()]
param(
    [ValidateSet('p2','p2-backends','p3','linux-fs','guard','mutants','pg-matrix','spanner-emulator','mysql80','mysql84')][string]$Phase='p2',
    [string]$TaskRoot=('J:/gigglesdb-work/gd49-p2-' + (Get-Date -Format yyyyMMdd-HHmmss) + '-' + $PID),
    [string]$MutationFilter='',
    [ValidateSet('all','state','epoch','journal-chain','journal-io','gcs')][string]$MutationSuite='all',
    [ValidateSet('16','17','18')][string]$PgVersion='16',
    # Optional standalone S1 spike; never adds a dependency to skedaddle.
    [string]$SpannerSpike='',
    [string]$SpannerCargoTest='',
    [ValidateSet('ON','OFF')][string]$MysqlGtid='ON',
    [string]$MysqlSpike='',
    [string]$MysqlCargoTest=''
)
$ErrorActionPreference='Stop'
$resolved=[IO.Path]::GetFullPath($TaskRoot).Replace('\','/')
if (-not $resolved.StartsWith('J:/gigglesdb-work/',[StringComparison]::OrdinalIgnoreCase)) { throw 'TaskRoot must be a unique J:/gigglesdb-work subfolder' }
if (-not $env:CARGO_TARGET_DIR) { $env:CARGO_TARGET_DIR="$resolved/target" }
$env:RUSTC_WRAPPER='J:/gigglesdb-work/tools/bin/sccache.exe'
$env:SCCACHE_DIR='J:/gigglesdb-work/sccache-cache'
$env:SCCACHE_CACHE_SIZE='100G'
$env:CARGO_INCREMENTAL='0'
$env:TEMP="$resolved/tmp"
$env:TMP=$env:TEMP
$env:CARGO_BUILD_JOBS='4'
$env:SKEDADDLE_TEST_ROOT="$resolved/scratch"
$spannerCallerCargoHome=$env:CARGO_HOME
$env:CARGO_HOME="$resolved/cargo-home"
$env:PYTHONDONTWRITEBYTECODE='1'
$env:SKEDADDLE_MUTANTS_FILTER=$MutationFilter
$env:SKEDADDLE_MUTATION_SUITE=$MutationSuite
$env:SKEDADDLE_PG_VERSION=$PgVersion
New-Item -ItemType Directory -Force -Path $env:TEMP | Out-Null
if ($Phase -eq 'spanner-emulator') {
    # Repository tests reuse the caller's Cargo cache rather than an empty cache.
    if ($SpannerCargoTest) { $env:CARGO_HOME=$spannerCallerCargoHome }
    $spannerResult=1
    try {
        & python -I -X "pycache_prefix=$resolved/pycache" (Join-Path $PSScriptRoot 'spanner-setup.py') run --task-root $resolved "--spike=$SpannerSpike" "--cargo-test=$SpannerCargoTest"
        $spannerResult=$LASTEXITCODE
    }
    finally {
        # The Python runner tears down only its exact labelled Compose project.
        $spannerRunId=Get-Content -Raw (Join-Path $resolved 'spanner-run-id.txt') -ErrorAction SilentlyContinue
        if ($spannerRunId) {
            & (Join-Path $PSScriptRoot 'cleanup.ps1') -Check -TaskRoot $resolved -RunId $spannerRunId.Trim()
            if ($LASTEXITCODE -ne 0) { $spannerResult=1 }
        }
    }
    exit $spannerResult
}
# GD-0064 M1: a separate runner keeps the PostgreSQL matrix untouched.
if ($Phase -in @('mysql80','mysql84')) {
    if ($MysqlCargoTest) { $env:CARGO_HOME=$spannerCallerCargoHome }
    $mysqlResult=1
    $savedErrorPreference=$ErrorActionPreference
    try {
        # Windows PowerShell treats redirected native stderr as an error record.
        # Preserve the process exit code and always reach teardown verification.
        $ErrorActionPreference='Continue'
        & python -I -X "pycache_prefix=$resolved/pycache" (Join-Path $PSScriptRoot 'mysql-run.py') $Phase $resolved $MysqlGtid "--spike=$MysqlSpike" "--cargo-test=$MysqlCargoTest"
        $mysqlResult=$LASTEXITCODE
    } finally {
        & (Join-Path $PSScriptRoot 'cleanup.ps1') -Check -TaskRoot $resolved
        if ($LASTEXITCODE -ne 0) { $mysqlResult=1 }
        $ErrorActionPreference=$savedErrorPreference
    }
    exit $mysqlResult
}
if ($Phase -eq 'pg-matrix') {
    $clientLock=Get-Content -Raw (Join-Path $PSScriptRoot 'pg-client.lock') | ConvertFrom-Json
    $env:SKEDADDLE_PSQL=$clientLock.executable
}
$result=1
try {
    & python -B (Join-Path $PSScriptRoot 'harness.py') $Phase $resolved
    $result=$LASTEXITCODE
}
finally {
    if ($Phase -eq 'pg-matrix') {
        & (Join-Path $PSScriptRoot 'cleanup.ps1') -Check -TaskRoot $resolved
        if ($LASTEXITCODE -ne 0) { if ($Phase -eq 'pg-matrix') {
    $clientLock=Get-Content -Raw (Join-Path $PSScriptRoot 'pg-client.lock') | ConvertFrom-Json
    $env:SKEDADDLE_PSQL=$clientLock.executable
}
$result=1 }
    }
}
exit $result
