param(
    [Parameter(Mandatory=$true)]
    [ValidateSet('hashes','fmt','clippy','debug','release','doctest','doc','example')]
    [string]$Check,
    [string]$Label = ([guid]::NewGuid().ToString())
)
# Retained raw artifacts only. No fixture or artifact cleanup is performed.
$ErrorActionPreference = 'Continue'
$crate = $PSScriptRoot
$artifact = Join-Path $crate ('artifacts/' + $Label)
if (Test-Path -LiteralPath $artifact) { throw "Use a unique artifact label: $artifact" }
New-Item -ItemType Directory -Path $artifact | Out-Null
$env:CARGO_BUILD_JOBS = '2'
$env:CARGO_TARGET_DIR = Join-Path $crate 'target'
$manifest = Join-Path $crate 'Cargo.toml'
$commandArguments = @()
switch ($Check) {
    'hashes' {
        & cargo metadata --locked --format-version 1 --manifest-path $manifest 1> (Join-Path $artifact 'metadata.json') 2> (Join-Path $artifact 'metadata.stderr')
        if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
        $metadata = Get-Content -Raw (Join-Path $artifact 'metadata.json') | ConvertFrom-Json
        $rows = foreach ($package in ($metadata.packages | Sort-Object id)) {
            $packageRoot = Split-Path $package.manifest_path
            foreach ($file in (Get-ChildItem -LiteralPath $packageRoot -File -Recurse | Where-Object { $_.FullName -notmatch '\\(target[^\\]*|artifacts|\.git)\\' } | Sort-Object FullName)) {
                '{0}  {1}  {2}' -f $package.id, (Get-FileHash -LiteralPath $file.FullName -Algorithm SHA256).Hash, $file.FullName.Substring($packageRoot.Length)
            }
        }
        $rows | Set-Content (Join-Path $artifact 'sources.sha256')
        Get-FileHash -Algorithm SHA256 (Join-Path $artifact 'sources.sha256') | Format-List | Out-File (Join-Path $artifact 'aggregate.sha256')
        & rustc --version --verbose *> (Join-Path $artifact 'rustc.log')
        & cargo --version *> (Join-Path $artifact 'cargo.log')
        Write-Output $artifact
        exit 0
    }
    'fmt' { $commandArguments = @('fmt','--manifest-path',$manifest,'--','--check') }
    'clippy' { $commandArguments = @('clippy','--locked','--manifest-path',$manifest,'--all-targets','-j2','--','-D','warnings') }
    'debug' { $commandArguments = @('test','--locked','--manifest-path',$manifest,'-j2') }
    'release' { $commandArguments = @('test','--locked','--manifest-path',$manifest,'-j2','--release') }
    'doctest' { $commandArguments = @('test','--locked','--manifest-path',$manifest,'--doc','-j2') }
    'doc' { $env:RUSTDOCFLAGS = '-D warnings'; $commandArguments = @('doc','--locked','--manifest-path',$manifest,'--no-deps','-j2') }
    'example' {
        $db = Join-Path ([System.IO.Path]::GetTempPath()) ('giggles-migrate-example-' + [guid]::NewGuid().ToString())
        $m = Join-Path $crate 'examples/manifest.json'
        $c = Join-Path $crate 'examples/chunks.json'
        $commands = @(
            @('manifest','validate',$m),
            @('reconcile',$m,$c),
            @('import',$m,$c,$db),
            @('verify',$m,$c,$db),
            @('status',$m,$db)
        )
        $results = @()
        foreach ($arguments in $commands) {
            $name = $arguments[0]
            & cargo run --locked --manifest-path $manifest -- @arguments 1> (Join-Path $artifact ($name+'.stdout')) 2> (Join-Path $artifact ($name+'.stderr'))
            $resultCode = $LASTEXITCODE
            $results += @{ command = ('cargo run --locked --manifest-path ' + $manifest + ' -- ' + ($arguments -join ' ')); exit = $resultCode }
            if ($resultCode -ne 0) { break }
        }
        @{ database = $db; checks = $results } | ConvertTo-Json -Depth 6 | Set-Content (Join-Path $artifact 'result.json')
        Write-Output $artifact
        exit $resultCode
    }
}
$commandText = 'cargo ' + ($commandArguments -join ' ')
& cargo @commandArguments *> (Join-Path $artifact 'output.log')
$resultCode = $LASTEXITCODE
@{ command = $commandText; exit = $resultCode; target = $env:CARGO_TARGET_DIR; jobs = 2 } | ConvertTo-Json | Set-Content (Join-Path $artifact 'result.json')
Get-Content (Join-Path $artifact 'output.log') -Tail 45
Write-Output $artifact
exit $resultCode
