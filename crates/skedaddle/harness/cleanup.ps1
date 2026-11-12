[CmdletBinding()]
param([switch]$Check,[string]$RunId,[string]$TaskRoot)
$ErrorActionPreference='Stop'
if (-not $Check -and -not $RunId) { throw 'Teardown requires this task''s exact RunId; unscoped cleanup is forbidden' }
if (-not $TaskRoot) { $TaskRoot='J:/gigglesdb-work/gd49-p2-cleanup-' + (Get-Date -Format yyyyMMdd-HHmmss) + '-' + $PID }
$TaskRoot=[IO.Path]::GetFullPath($TaskRoot).Replace('\','/')
if (-not $TaskRoot.StartsWith('J:/gigglesdb-work/',[StringComparison]::OrdinalIgnoreCase)) {throw 'TaskRoot must stay below J:/gigglesdb-work'}
$env:CARGO_TARGET_DIR="$TaskRoot/target"
$env:TEMP="$TaskRoot/tmp"
$env:TMP=$env:TEMP
$env:CARGO_BUILD_JOBS='4'
$env:SKEDADDLE_TEST_ROOT="$TaskRoot/scratch"
$env:CARGO_HOME="$TaskRoot/cargo-home"
$env:PYTHONDONTWRITEBYTECODE='1'
New-Item -ItemType Directory -Force -Path $env:TEMP | Out-Null
$action=if ($Check) {'check'} else {'down'}
& python -B (Join-Path $PSScriptRoot 'harness.py') $action $TaskRoot $RunId
exit $LASTEXITCODE
