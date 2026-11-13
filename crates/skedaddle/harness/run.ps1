[CmdletBinding()]
param(
    [ValidateSet('p2','p2-backends','p3','linux-fs','guard','mutants','pg-matrix')][string]$Phase='p2',
    [string]$TaskRoot=('J:/gigglesdb-work/gd49-p2-' + (Get-Date -Format yyyyMMdd-HHmmss) + '-' + $PID),
    [string]$MutationFilter='',
    [ValidateSet('all','state','epoch','journal-chain','journal-io','gcs')][string]$MutationSuite='all',
    [ValidateSet('16','17','18')][string]$PgVersion='16'
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
$env:CARGO_HOME="$resolved/cargo-home"
$env:PYTHONDONTWRITEBYTECODE='1'
$env:SKEDADDLE_MUTANTS_FILTER=$MutationFilter
$env:SKEDADDLE_MUTATION_SUITE=$MutationSuite
$env:SKEDADDLE_PG_VERSION=$PgVersion
New-Item -ItemType Directory -Force -Path $env:TEMP | Out-Null
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
