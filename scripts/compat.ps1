$ErrorActionPreference = 'Stop'
Push-Location (Split-Path -Parent $PSScriptRoot)
try {
    python -I compat/harness.py @args
    $code = $LASTEXITCODE
} finally {
    Pop-Location
}
exit $code
