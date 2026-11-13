[CmdletBinding()]
param([Parameter(Mandatory=$true)][string]$TaskRoot,
      [string]$Archive='')
$ErrorActionPreference='Stop'
$task=[IO.Path]::GetFullPath($TaskRoot).Replace('\','/')
if (-not $task.StartsWith('J:/gigglesdb-work/',[StringComparison]::OrdinalIgnoreCase)) { throw 'TaskRoot must be under J:/gigglesdb-work' }
$env:TEMP="$task/tmp"; $env:TMP=$env:TEMP
New-Item -ItemType Directory -Force $env:TEMP | Out-Null
$lock=Get-Content -Raw (Join-Path $PSScriptRoot 'pg-client.lock') | ConvertFrom-Json
if (-not $Archive) {
    $Archive="$task/pg-client.zip"
    Invoke-WebRequest -UseBasicParsing -Uri $lock.url -OutFile $Archive
}
$hash=(Get-FileHash -Algorithm SHA256 -LiteralPath $Archive).Hash.ToLowerInvariant()
if ($hash -ne $lock.sha256) { throw "PostgreSQL archive checksum mismatch: expected $($lock.sha256), got $hash; nothing extracted" }
$dest=Split-Path (Split-Path $lock.executable)
if (Test-Path -LiteralPath $dest) { throw "Destination exists: $dest; preserve it and inspect before reprovisioning" }
# Extract only psql and the DLLs distributed beside it. No server or PATH changes.
Add-Type -AssemblyName System.IO.Compression.FileSystem
$zip=[IO.Compression.ZipFile]::OpenRead($Archive)
try {
    New-Item -ItemType Directory -Path "$dest/bin" -Force | Out-Null
    foreach ($entry in $zip.Entries) {
        if ($entry.FullName -match '^pgsql/bin/(psql\.exe|[^/]+\.dll)$') {
            [IO.Compression.ZipFileExtensions]::ExtractToFile($entry,"$dest/bin/$($entry.Name)")
        }
    }
} finally { $zip.Dispose() }
$version=& $lock.executable --version
if ($LASTEXITCODE -ne 0 -or $version -ne $lock.psql_version) { throw "Client startup/version check failed: $version" }
Write-Output "$version; archive SHA256=$hash; installed at $dest"
