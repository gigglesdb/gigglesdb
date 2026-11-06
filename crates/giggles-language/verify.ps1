param(
    [Parameter(Mandatory=$true)][string]$ArtifactDirectory,
    [string]$TargetDirectory
)

# Run from any directory. Keep outputs and databases for review; never remove files.
$ErrorActionPreference = 'Stop'
$repository = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '../..')).Path
if (Test-Path -LiteralPath $ArtifactDirectory) {
    throw 'Artifact directory exists; choose a new path.'
}
New-Item -ItemType Directory -Path $ArtifactDirectory | Out-Null
$artifacts = (Resolve-Path -LiteralPath $ArtifactDirectory).Path
if (-not $TargetDirectory) {
    $TargetDirectory = Join-Path $repository 'target-gd0042'
}
$target = [System.IO.Path]::GetFullPath($TargetDirectory)
$previousTarget = $env:CARGO_TARGET_DIR
$previousJobs = $env:CARGO_BUILD_JOBS

function InputHashes {
    # Include cross-package source references checked by the SQL inventory.
    $paths = @(& rg --files (Join-Path $repository 'crates/giggles-core') (Join-Path $repository 'crates/giggles-model') $PSScriptRoot -g '*.rs' -g '*.json' -g '*.md' -g '*.ps1' -g 'Cargo.toml' -g 'Cargo.lock')
    if ($LASTEXITCODE -ne 0) { throw 'Could not enumerate verification inputs.' }
    $inventory = Get-Content -LiteralPath (Join-Path $PSScriptRoot 'tests/fixtures/sql_interfaces.json') -Raw | ConvertFrom-Json
    foreach ($interface in $inventory.interfaces) {
        $relativePaths = @($interface.docs) + @($interface.sources.path) + @($interface.evidence.path)
        foreach ($relative in $relativePaths) { $paths += Join-Path $repository $relative }
    }
    foreach ($limit in $inventory.limits) { $paths += Join-Path $repository $limit.source.path }
    @($paths | Sort-Object -Unique | ForEach-Object {
        $file = Get-FileHash -LiteralPath $_ -Algorithm SHA256
        [pscustomobject]@{ Path = $file.Path; SHA256 = $file.Hash }
    })
}

function QuoteArgument([string]$value) {
    # Windows command-line quoting for Start-Process; no shell evaluation occurs.
    '"' + [regex]::Replace([regex]::Replace($value, '(\\*)"', '$1$1\"'), '(\\+)$', '$1$1') + '"'
}

$results = [System.Collections.Generic.List[object]]::new()
try {
    $env:CARGO_TARGET_DIR = $target
    $env:CARGO_BUILD_JOBS = '1'
    $before = InputHashes
    $before | Export-Csv -LiteralPath (Join-Path $artifacts 'inputs-before.csv') -NoTypeInformation
    $steps = @(
        @{ Name='rustc-version'; Exe='rustc'; Arguments=@('--version') },
        @{ Name='cargo-version'; Exe='cargo'; Arguments=@('--version') }
    )
    foreach ($crate in @('core', 'language')) {
        $manifest = Join-Path $repository "crates/giggles-$crate/Cargo.toml"
        $steps += @(
            @{ Name="$crate-format"; Exe='cargo'; Arguments=@('fmt','--manifest-path',$manifest,'--','--check') },
            @{ Name="$crate-clippy"; Exe='cargo'; Arguments=@('clippy','--manifest-path',$manifest,'--locked','--all-targets','-j1','--','-D','warnings') },
            @{ Name="$crate-debug"; Exe='cargo'; Arguments=@('test','--manifest-path',$manifest,'--locked','--all-targets','-j1') },
            @{ Name="$crate-release"; Exe='cargo'; Arguments=@('test','--manifest-path',$manifest,'--locked','--all-targets','--release','-j1') },
            @{ Name="$crate-doc"; Exe='cargo'; Arguments=@('test','--manifest-path',$manifest,'--locked','--doc','-j1') }
        )
    }
    foreach ($step in $steps) {
        $arguments = ($step.Arguments | ForEach-Object { QuoteArgument $_ }) -join ' '
        $started = [DateTime]::UtcNow
        Write-Output "Running $($step.Name)"
        $process = Start-Process -FilePath $step.Exe -ArgumentList $arguments -WorkingDirectory $repository -WindowStyle Hidden -Wait -PassThru -RedirectStandardOutput (Join-Path $artifacts "$($step.Name).stdout.txt") -RedirectStandardError (Join-Path $artifacts "$($step.Name).stderr.txt")
        $result = [pscustomobject]@{
            Name = $step.Name
            Executable = $step.Exe
            Arguments = $step.Arguments
            ExitCode = $process.ExitCode
            StartedUtc = $started.ToString('o')
            FinishedUtc = [DateTime]::UtcNow.ToString('o')
        }
        $results.Add($result)
        $results | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath (Join-Path $artifacts 'results.json')
        Write-Output "$($step.Name): exit $($process.ExitCode)"
    }
    $after = InputHashes
    $after | Export-Csv -LiteralPath (Join-Path $artifacts 'inputs-after.csv') -NoTypeInformation
    $changed = @(Compare-Object $before $after -Property Path,SHA256)
    ConvertTo-Json -InputObject $changed -Depth 4 | Set-Content -LiteralPath (Join-Path $artifacts 'changed-inputs.json')
    if ($changed.Count -ne 0) {
        Write-Output 'Inputs changed during verification; results do not qualify one stable source snapshot.'
    }
    if (($results | Where-Object ExitCode -ne 0) -or $changed.Count -ne 0) { exit 1 }
} finally {
    $env:CARGO_TARGET_DIR = $previousTarget
    $env:CARGO_BUILD_JOBS = $previousJobs
}
