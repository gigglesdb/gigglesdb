[CmdletBinding()]
param(
    [ValidateSet('p2','p2-backends','guard','mutants')][string]$Phase='p2',
    [string]$TaskRoot=('J:/gigglesdb-work/gd49-p2-' + (Get-Date -Format yyyyMMdd-HHmmss) + '-' + $PID),
    [string]$MutationFilter='',
    [ValidateSet('all','state','epoch','journal-chain','journal-io','gcs')][string]$MutationSuite='all'
)
$ErrorActionPreference='Stop'
$resolved=[IO.Path]::GetFullPath($TaskRoot).Replace('\','/')
if (-not $resolved.StartsWith('J:/gigglesdb-work/',[StringComparison]::OrdinalIgnoreCase)) { throw 'TaskRoot must be a unique J:/gigglesdb-work subfolder' }
$env:CARGO_TARGET_DIR="$resolved/target"
$env:TEMP="$resolved/tmp"
$env:TMP=$env:TEMP
$env:CARGO_BUILD_JOBS='2'
$env:SKEDADDLE_TEST_ROOT="$resolved/scratch"
$env:CARGO_HOME="$resolved/cargo-home"
$env:PYTHONDONTWRITEBYTECODE='1'
$env:SKEDADDLE_MUTANTS_FILTER=$MutationFilter
$env:SKEDADDLE_MUTATION_SUITE=$MutationSuite
New-Item -ItemType Directory -Force -Path $env:TEMP | Out-Null
& python -B (Join-Path $PSScriptRoot 'harness.py') $Phase $resolved
exit $LASTEXITCODE
