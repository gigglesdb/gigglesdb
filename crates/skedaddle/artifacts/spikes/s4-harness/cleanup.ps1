[CmdletBinding(SupportsShouldProcess=$true)]
param([switch]$Images, [switch]$Check)
$env:PYTHONDONTWRITEBYTECODE = '1'
$engine = Join-Path $PSScriptRoot 'harness.py'
if ($Check) { & python -B $engine check; exit $LASTEXITCODE }
if ($Images) {
    # Exact manifest references only; never a prune, tag glob, or force removal.
    $lock = Get-Content -LiteralPath (Join-Path $PSScriptRoot 'images.lock') -Raw | ConvertFrom-Json
    foreach ($item in $lock.images.PSObject.Properties.Value) {
        if ($PSCmdlet.ShouldProcess($item.ref, 'docker image rm (exact manifest reference)')) {
            & docker image rm $item.ref
            if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
        }
    }
    exit 0
}
if ($WhatIfPreference) { & python -B $engine cleanup-dry } else { & python -B $engine cleanup }
exit $LASTEXITCODE
