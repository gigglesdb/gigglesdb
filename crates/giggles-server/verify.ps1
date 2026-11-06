param([Parameter(Mandatory=$true)][string]$ArtifactDirectory)
$ErrorActionPreference = 'Stop'
if (Test-Path -LiteralPath $ArtifactDirectory) { throw 'Artifact directory exists; choose a unique path.' }
New-Item -ItemType Directory -Path $ArtifactDirectory | Out-Null
$artifactPath = (Resolve-Path -LiteralPath $ArtifactDirectory).Path
$commands = @(
 @{Name='rustc';Args=@('--version')},
 @{Name='cargo-version';Args=@('--version')},
 @{Name='format';Args=@('fmt','--all','--','--check')},
 @{Name='clippy';Args=@('clippy','--locked','--all-targets','--','-D','warnings')},
 @{Name='debug';Args=@('test','--locked','--all-targets','--','--test-threads=1')},
 @{Name='release';Args=@('test','--locked','--all-targets','--release','--','--test-threads=1')},
 @{Name='doc';Args=@('test','--locked','--doc')}
)
$failed = $false
foreach ($entry in $commands) {
 $exe = if ($entry.Name -eq 'rustc') {'rustc'} else {'cargo'}
 $argList = $entry.Args
 Add-Content -LiteralPath (Join-Path $artifactPath 'commands.txt') -Value "$exe $($argList -join ' ')"
 $process = Start-Process -FilePath $exe -ArgumentList $argList -WindowStyle Hidden -Wait -PassThru -RedirectStandardOutput (Join-Path $artifactPath "$($entry.Name).stdout.txt") -RedirectStandardError (Join-Path $artifactPath "$($entry.Name).stderr.txt")
 $code=$process.ExitCode
 Add-Content -LiteralPath (Join-Path $artifactPath 'commands.txt') -Value "exit=$code"
 Write-Output "$($entry.Name): exit $code"
 if ($code -ne 0) {$failed=$true}
}
$inputPaths=@('src','tests','examples','proto','docs','include','build.rs','Cargo.toml','Cargo.lock','README.md','API.md','CREDENTIALS_ADMISSION.md','HANDOFF.md','LOCAL.md','SQL.md','verify.ps1','verify-c.ps1','../giggles-core/src','../giggles-core/Cargo.toml','../giggles-pg-catalog/src','../giggles-pg-catalog/Cargo.toml') | Where-Object {Test-Path -LiteralPath $_}
rg --files @inputPaths | ForEach-Object { Get-FileHash -Algorithm SHA256 -LiteralPath $_ } | Format-Table -AutoSize | Out-String -Width 4096 | Set-Content -LiteralPath (Join-Path $artifactPath 'inputs-sha256.txt')
if ($failed) {exit 1}
