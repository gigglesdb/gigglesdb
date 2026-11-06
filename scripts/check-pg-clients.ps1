[CmdletBinding()]
param([string]$JavaHome = '', [string]$JdbcJar = '')
$ErrorActionPreference = 'Stop'
$root = Split-Path $PSScriptRoot -Parent
Set-Location $root
# This target is separate from the IDE's live server executable.
$env:CARGO_TARGET_DIR = Join-Path $root 'target-datagrip'
$env:CARGO_BUILD_JOBS = '1'
$env:CARGO_INCREMENTAL = '0'
$output = Join-Path $root ('artifacts/pg-clients-' + [Guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path $output | Out-Null
$utf8 = New-Object System.Text.UTF8Encoding($false)
Push-Location (Join-Path $root 'crates/giggles-server')
try {
    $fixtureOutput = @(& cargo test --locked --test network executable_server_starts_and_native_client_connects -- --exact --nocapture)
    if ($LASTEXITCODE) { throw 'Server fixture test failed.' }
    $fixtureOutput | Out-Host
    [IO.File]::WriteAllLines((Join-Path $output 'fixture.log'), [string[]]$fixtureOutput, $utf8)
    $lines = @($fixtureOutput | Where-Object { "$_" -match '^retained_fixture=' })
    if ($lines.Count -ne 1) { throw 'Expected one fresh retained fixture.' }
    $fixture = [IO.Path]::GetFullPath((Join-Path (Get-Location).Path ($lines[0] -replace '^retained_fixture=','')))
} finally { Pop-Location }
if (!$JavaHome -and $env:JAVA_HOME) { $JavaHome = $env:JAVA_HOME }
if (!$JavaHome) {
    $jdkRoot = Join-Path $env:ProgramFiles 'Eclipse Adoptium'
    if (Test-Path $jdkRoot) {
        $jdk = Get-ChildItem -LiteralPath $jdkRoot -Directory | Sort-Object Name -Descending | Where-Object { Test-Path (Join-Path $_.FullName 'bin/javac.exe') } | Select-Object -First 1
        if ($jdk) { $JavaHome = $jdk.FullName }
    }
}
if (!$JdbcJar) {
    $drivers = Join-Path $env:APPDATA 'JetBrains'
    if (Test-Path $drivers) {
        $jar = Get-ChildItem -LiteralPath $drivers -Filter 'postgresql-42.7.3.jar' -File -Recurse | Sort-Object FullName | Select-Object -First 1
        if ($jar) { $JdbcJar = $jar.FullName }
    }
}
$manifest = @{
    server = Join-Path $env:CARGO_TARGET_DIR 'debug/giggles-server.exe'
    fixture = $fixture
    server_config = Join-Path $fixture 'server.TEST-ONLY.json'
    config_base = Join-Path $root 'crates/giggles-server'
    output = Join-Path $output 'matrix'
    timeout_seconds = 90
}
if ($JavaHome -and $JdbcJar) {
    $manifest.java = [IO.Path]::GetFullPath((Join-Path $JavaHome 'bin/java.exe'))
    $manifest.javac = [IO.Path]::GetFullPath((Join-Path $JavaHome 'bin/javac.exe'))
    $manifest.jdbc_jar = [IO.Path]::GetFullPath($JdbcJar)
} else {
    Write-Host 'JDK or pgJDBC 42.7.3 not found; JDBC cases will be UNTESTED. Supply -JavaHome and -JdbcJar.'
}
$manifestPath = Join-Path $output 'manifest.json'
[IO.File]::WriteAllText($manifestPath, ($manifest | ConvertTo-Json -Depth 5), $utf8)
& cargo run --locked --manifest-path crates/giggles-pg-conformance/Cargo.toml --target-dir target-gd48-conformance -j1 -- $manifestPath
$result = $LASTEXITCODE
Write-Host "Client report: $(Join-Path $output 'matrix/report.json')"
Write-Host 'FAIL and UNTESTED are capability results, not support claims. Full DataGrip GUI introspection remains unqualified.'
exit $result
