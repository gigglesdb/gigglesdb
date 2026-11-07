[CmdletBinding()]
param(
    [string]$RunId = ('ps-' + (Get-Date -Format yyyyMMdd-HHmmss) + '-' + $PID),
    [ValidateSet(16,17,18)][int]$PgMajor = 18,
    [double]$BudgetGiB = 0,
    [scriptblock]$Workload,
    [ValidateSet('smoke','privileges','large','hold','fail')][string]$Mode = 'smoke'
)
$ErrorActionPreference = 'Stop'
$engine = Join-Path $PSScriptRoot 'harness.py'
$env:PYTHONDONTWRITEBYTECODE = '1'
$env:SKEDADDLE_RUN_ID = $RunId
$env:SKEDADDLE_PG_MAJOR = "$PgMajor"
$env:SKEDADDLE_OWNER_PID = "$PID"
$env:SKEDADDLE_BUDGET_GIB = "$BudgetGiB"
function Invoke-Engine([string]$Action) {
    & python -B $engine $Action
    if ($LASTEXITCODE -ne 0) { throw "Harness $Action failed (exit $LASTEXITCODE)" }
}
$status = 0
$started = $false
try {
    # Guard precedes the sweeper, directory creation, image pulls, and compose up.
    Invoke-Engine 'guard'
    Invoke-Engine 'sweep'
    $started = $true
    Invoke-Engine 'up'
    if ($Workload) { & $Workload } else { Invoke-Engine $Mode }
} catch {
    $status = 1
    Write-Host "RUN FAILED: $_"
} finally {
    if ($started) {
        # Independent steps: export failure must never prevent Docker teardown.
        try { Invoke-Engine 'export' } catch { $status = 1; Write-Host $_ }
        try { Invoke-Engine 'down' } catch { $status = 1; Write-Host $_ }
        try { Invoke-Engine 'sweep' } catch { $status = 1; Write-Host $_ }
        try { Invoke-Engine 'check-run' } catch { $status = 1; Write-Host $_ }
    }
}
exit $status
