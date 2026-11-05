param([string]$ArtifactName = ("verify-" + [guid]::NewGuid().ToString('N')))
$ErrorActionPreference = 'Stop'
$crateRoot = $PSScriptRoot
$env:CARGO_TARGET_DIR = Join-Path $crateRoot 'target-ops'
$env:CARGO_BUILD_JOBS = '2'
$env:RUSTDOCFLAGS = '-D warnings'
$artifactRoot = Join-Path (Join-Path $crateRoot 'artifacts') $ArtifactName
New-Item -ItemType Directory -Path $artifactRoot | Out-Null
Push-Location $crateRoot
try {
    function Capture-Hashes([string]$Label) {
        $metadataText = & cargo metadata --locked --format-version 1
        if ($LASTEXITCODE -ne 0) { throw 'cargo metadata failed' }
        $metadataText | Set-Content -LiteralPath (Join-Path $artifactRoot "$Label-metadata.json")
        $metadata = $metadataText | ConvertFrom-Json
        $rows = foreach ($package in ($metadata.packages | Sort-Object name,version)) {
            $packageRoot = Split-Path $package.manifest_path
            $sourceFiles = & rg --files --hidden $packageRoot
            foreach ($sourceFile in ($sourceFiles | Sort-Object)) {
                $relative = $sourceFile.Substring($packageRoot.Length).TrimStart([char[]]'\/').Replace([char]92,[char]47)
                if ($relative -match '^(artifacts|target[^/]*|\.git)/') { continue }
                if ($sourceFile -match '\.(rs|toml|lock|c|h|json)$') {
                    $hash = (Get-FileHash -Algorithm SHA256 -LiteralPath $sourceFile).Hash.ToLower()
                    "$hash  $($package.name)@$($package.version)/$relative"
                }
            }
        }
        $rows | Set-Content -LiteralPath (Join-Path $artifactRoot "$Label-sources.sha256")
        Get-FileHash -Algorithm SHA256 -LiteralPath (Join-Path $artifactRoot "$Label-sources.sha256") | Format-List | Out-File (Join-Path $artifactRoot "$Label-digest.txt")
    }
    Capture-Hashes 'before'
    $checks = @(
        @{ Name='fmt'; Args=@('fmt','--check') },
        @{ Name='clippy'; Args=@('clippy','--locked','--all-targets','--','-D','warnings') },
        @{ Name='debug'; Args=@('test','--locked','-j2') },
        @{ Name='release'; Args=@('test','--locked','--release','-j2') },
        @{ Name='doctest'; Args=@('test','--locked','--doc','-j2') },
        @{ Name='docs'; Args=@('doc','--locked','--no-deps','-j2') }
    )
    $results = foreach ($check in $checks) {
        $arguments = $check.Args
        # cmd preserves native output without PowerShell converting stderr into errors.
        $command = 'cargo ' + ($arguments -join ' ')
        & cmd /d /c "$command > `"$(Join-Path $artifactRoot ($check.Name + '.log'))`" 2>&1"
        $code = $LASTEXITCODE
        $entry = [pscustomobject]@{ Check=$check.Name; Command=$command; Exit=$code }
        $entry | ConvertTo-Json -Compress | Add-Content -LiteralPath (Join-Path $artifactRoot 'results.jsonl')
        $entry
        if ($code -ne 0) { break }
    }
    Capture-Hashes 'after'
    $results | Format-Table
    Write-Output "Artifacts: $artifactRoot"
    if ($results.Count -ne 6 -or ($results | Where-Object Exit -ne 0)) { exit 1 }
} finally { Pop-Location }
