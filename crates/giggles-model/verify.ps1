param([string]$ArtifactDirectory)
$ErrorActionPreference = 'Stop'
if (-not $ArtifactDirectory) { throw 'Supply a unique artifact directory name.' }
$root = [IO.Path]::GetFullPath($PSScriptRoot)
$artifact = [IO.Path]::GetFullPath((Join-Path $root $ArtifactDirectory))
if (-not $artifact.StartsWith($root + [IO.Path]::DirectorySeparatorChar)) { throw 'Artifacts must stay inside crate.' }
if (Test-Path -LiteralPath $artifact) { throw 'Use a fresh artifact directory; existing logs are retained.' }
New-Item -ItemType Directory -Path $artifact | Out-Null
$commands = @(
    @{ Name = 'rustc-version'; Executable = 'rustc'; Arguments = '--version' },
    @{ Name = 'cargo-version'; Executable = 'cargo'; Arguments = '--version' },
    @{ Name = 'format'; Executable = 'cargo'; Arguments = 'fmt --all -- --check' },
    @{ Name = 'clippy'; Executable = 'cargo'; Arguments = 'clippy --all-targets -- -D warnings' },
    @{ Name = 'test-debug'; Executable = 'cargo'; Arguments = 'test --all-targets' },
    @{ Name = 'test-release'; Executable = 'cargo'; Arguments = 'test --all-targets --release' },
    @{ Name = 'doc-tests'; Executable = 'cargo'; Arguments = 'test --doc' },
    @{ Name = 'golden-transcript'; Executable = 'cargo'; Arguments = 'run --example p0' },
    @{ Name = 'golden-histories'; Executable = 'cargo'; Arguments = 'run --example p0 -- --histories' }
)
$results = @()
foreach ($command in $commands) {
    $start = New-Object Diagnostics.ProcessStartInfo
    $start.FileName = $command.Executable
    $start.Arguments = $command.Arguments
    $start.WorkingDirectory = $root
    $start.UseShellExecute = $false
    $start.CreateNoWindow = $true
    $start.RedirectStandardOutput = $true
    $start.RedirectStandardError = $true
    $process = New-Object Diagnostics.Process
    $process.StartInfo = $start
    [void]$process.Start()
    $stdout = $process.StandardOutput.ReadToEndAsync()
    $stderr = $process.StandardError.ReadToEndAsync()
    $process.WaitForExit()
    [IO.File]::WriteAllText((Join-Path $artifact ($command.Name + '.stdout.txt')), $stdout.Result)
    [IO.File]::WriteAllText((Join-Path $artifact ($command.Name + '.stderr.txt')), $stderr.Result)
    $record = '{0} {1}: exit_code={2}' -f $command.Executable, $command.Arguments, $process.ExitCode
    $results += $record
    [IO.File]::WriteAllLines((Join-Path $artifact 'commands.txt'), $results)
    Write-Output $record
    if ($process.ExitCode -ne 0) { exit $process.ExitCode }
}
# Hash source and fixture inputs only, without changing or cleaning any outputs.
$inputs = @('Cargo.toml','Cargo.lock','verify.ps1','README.md')
$files = @(Get-ChildItem -LiteralPath (Join-Path $root 'src'),(Join-Path $root 'tests'),(Join-Path $root 'examples') -Recurse -File)
foreach ($inputFile in $inputs) { $inputPath = Join-Path $root $inputFile; if (Test-Path -LiteralPath $inputPath) { $files += Get-Item -LiteralPath $inputPath } }
$hashes = foreach ($file in ($files | Sort-Object FullName)) { $hash = Get-FileHash -LiteralPath $file.FullName -Algorithm SHA256; '{0}  {1}' -f $hash.Hash,$file.FullName.Substring($root.Length+1) }
[IO.File]::WriteAllLines((Join-Path $artifact 'inputs-sha256.txt'), $hashes)
