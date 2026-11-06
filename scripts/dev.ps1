[CmdletBinding()]
param(
    [ValidateSet('setup','fresh-setup','server','ping','server-query','sql','sql-query','native-query','capabilities','backup','export','topology','outbox-example','failover','distributed','faultlab','range-history','check','build','test','clippy','fmt','docs')]
    [string]$Task = 'setup',
    [string]$Crate = 'all',
    [switch]$Release
)
$ErrorActionPreference = 'Stop'
$root = Split-Path $PSScriptRoot -Parent
Set-Location $root
$env:CARGO_TARGET_DIR = Join-Path $root 'target-ide'
$env:CARGO_BUILD_JOBS = '1'
$profile = 'debug'
if ($Release) { $profile = 'release' }
$statePath = Join-Path $root 'artifacts/ide/current.json'
$utf8 = New-Object System.Text.UTF8Encoding($false)
function Write-Json($Path, $Value) {
    [IO.File]::WriteAllText($Path, ($Value | ConvertTo-Json -Depth 30), $utf8)
}
function Invoke-Cargo([string]$Name, [string[]]$Arguments) {
    $directory = Join-Path $root "crates/$Name"
    if (!(Test-Path (Join-Path $directory 'Cargo.toml'))) { throw "Unknown crate: $Name" }
    Push-Location $directory
    try {
        & cargo @Arguments
        if ($LASTEXITCODE -ne 0) { throw "cargo $($Arguments -join ' ') failed in $Name (exit $LASTEXITCODE)" }
    } finally { Pop-Location }
}
function Invoke-Binary([string]$Name, [string]$Binary, [string[]]$Arguments) {
    $build = @('build','--locked','--bin',$Binary)
    if ($Release) { $build += '--release' }
    Invoke-Cargo $Name $build | Out-Host
    & (Join-Path $env:CARGO_TARGET_DIR "$profile/$Binary.exe") @Arguments
    if ($LASTEXITCODE -ne 0) { throw "$Binary failed (exit $LASTEXITCODE)" }
}
function Read-State {
    if (!(Test-Path $statePath)) { throw 'Run the Setup local demo configuration first.' }
    Get-Content -Raw $statePath | ConvertFrom-Json
}
function New-OutputDirectory {
    $path = Join-Path $root ('artifacts/ide/output-' + [Guid]::NewGuid().ToString('N'))
    New-Item -ItemType Directory -Path $path | Out-Null
    $path
}
try {
    if ($Task -in @('setup','fresh-setup')) {
        if ($Task -eq 'setup' -and (Test-Path $statePath)) {
            $state = Read-State
            foreach ($path in @($state.config,$state.database,$state.fixture)) {
                if (!(Test-Path $path)) { throw "Setup state references a missing path: $path. Use Fresh local demo to retain old state and create another." }
            }
            Write-Host "Reusing local demo: $($state.directory)"
            exit 0
        }
        $output = @(Invoke-Cargo 'giggles-server' @('test','--locked','--test','network','executable_server_starts_and_native_client_connects','--','--exact','--nocapture'))
        $output | Out-Host
        $fixtureLine = @($output | Where-Object { "$_" -match '^retained_fixture=' })
        if ($fixtureLine.Count -ne 1) { throw 'Expected exactly one retained server fixture.' }
        $fixture = [IO.Path]::GetFullPath((Join-Path "$root/crates/giggles-server" ($fixtureLine[0] -replace '^retained_fixture=','')))
        $directory = New-OutputDirectory
        $config = Get-Content -Raw (Join-Path $fixture 'server.TEST-ONLY.json') | ConvertFrom-Json
        foreach ($field in @('database','certificate','private_key','client_ca')) {
            $config.$field = [IO.Path]::GetFullPath((Join-Path "$root/crates/giggles-server" $config.$field))
        }
        $configPath = Join-Path $directory 'server.json'
        Write-Json $configPath $config
        $database = Join-Path $directory 'embedded.db'
        $schema = Join-Path $directory 'init.sql'
        [IO.File]::WriteAllText($schema, "CREATE TABLE notes (id BIGINT PRIMARY KEY, body TEXT);`nINSERT INTO notes VALUES (1, 'hello GigglesDB');", $utf8)
        Invoke-Binary 'giggles-cli' 'giggles-sql' @('--create',$database,'--file',$schema)
        Write-Json (Join-Path $directory 'ping.json') @{kind='Ping'}
        Write-Json (Join-Path $directory 'query.json') @{kind='Sql';sql='SELECT id,body FROM app WHERE id=1';params=@()}
        Write-Json $statePath @{directory=$directory;fixture=$fixture;config=$configPath;database=$database}
        Write-Host "Ready. Start Server, then Client Ping or Client SQL query. State: $statePath"
        exit 0
    }
    if ($Task -in @('check','build','test','clippy','fmt','docs')) {
        $names = @($Crate)
        if ($Crate -eq 'all') { $names = @(Get-ChildItem "$root/crates" -Directory | Where-Object { Test-Path (Join-Path $_.FullName 'Cargo.toml') } | Sort-Object Name | ForEach-Object Name) }
        foreach ($name in $names) {
            Write-Host "=== $Task $name ($profile) ==="
            if ($Task -eq 'test' -and $name -in @('giggles-client','giggles-faultlab')) {
                $nodeBuild = @('build','--locked','--bin','giggles-node')
                if ($Release) { $nodeBuild += '--release' }
                Invoke-Cargo 'giggles-consensus' $nodeBuild
                $env:GIGGLES_NODE_EXE = Join-Path $env:CARGO_TARGET_DIR "$profile/giggles-node.exe"
            }
            $arguments = switch ($Task) {
                'fmt' { @('fmt','--','--check') }
                'clippy' { @('clippy','--locked','--all-targets') }
                'docs' { @('doc','--locked','--no-deps') }
                'test' { @('test','--locked') }
                default { @($Task,'--locked','--all-targets') }
            }
            if ($Release -and $Task -ne 'fmt') { $arguments += '--release' }
            if ($Task -eq 'clippy') { $arguments += @('--','-D','warnings') }
            if ($Task -eq 'test') { $arguments += @('--','--test-threads=1') }
            Invoke-Cargo $name $arguments
        }
        exit 0
    }
    switch ($Task) {
        'capabilities' { Invoke-Binary 'giggles-language' 'giggles-language' @('capability-manifest') }
        'topology' { Invoke-Binary 'giggles-topology' 'giggles-topology' @('analyze',"$root/crates/giggles-topology/tests/fixtures/three-providers.json",'baseline') }
        'outbox-example' {
            $destination = New-OutputDirectory
            Invoke-Cargo 'giggles-outbox' @('run','--locked','--example','orders','--',"$destination/orders.db")
        }
        'failover' { Invoke-Cargo 'giggles-consensus' @('test','--locked','--test','process','real_three_process_mtls_failover_snapshot_and_lost_ack','--','--exact','--nocapture') }
        'distributed' { Invoke-Cargo 'giggles-distributed' @('test','--locked','--test','process','--','--test-threads=1','--nocapture') }
        'range-history' { Invoke-Cargo 'giggles-range-history' @('test','--locked','--test','process','--','--test-threads=1','--nocapture') }
        'faultlab' {
            Invoke-Cargo 'giggles-consensus' @('build','--locked','--bin','giggles-node')
            Invoke-Binary 'giggles-faultlab' 'giggles-faultlab' @('run',"$env:CARGO_TARGET_DIR/debug/giggles-node.exe",'1')
        }
        default {
            $state = Read-State
            switch ($Task) {
                'server' { Invoke-Binary 'giggles-server' 'giggles-server' @($state.config) }
                { $_ -in @('ping','server-query') } {
                    $request = 'ping.json'
                    if ($Task -eq 'server-query') { $request = 'query.json' }
                    Invoke-Binary 'giggles-server' 'giggles-client' @('127.0.0.1:7432','localhost',"$($state.fixture)/app.pem","$($state.fixture)/app.key.TEST-ONLY","$($state.fixture)/ca.pem","$($state.directory)/$request")
                }
                'sql' { Invoke-Binary 'giggles-cli' 'giggles-sql' @('--open',$state.database,'--repl') }
                'sql-query' { Invoke-Binary 'giggles-cli' 'giggles-sql' @('--open',$state.database,'--sql','SELECT id,body FROM notes WHERE id=1','--format','json') }
                'native-query' { Invoke-Binary 'giggles-language' 'giggles-language' @('run',$state.database,'notes where id=1 select id,body') }
                'backup' {
                    $destination = New-OutputDirectory
                    New-Item -ItemType Directory -Path "$destination/catalog" | Out-Null
                    Invoke-Binary 'giggles-ops' 'giggles-ops' @('backup','--root',"$destination/catalog",'--source',$state.database,'--id','demo')
                    Invoke-Binary 'giggles-ops' 'giggles-ops' @('verify','--root',"$destination/catalog",'--id','demo')
                    Invoke-Binary 'giggles-ops' 'giggles-ops' @('restore','--root',"$destination/catalog",'--id','demo','--target',"$destination/restored.db")
                }
                'export' {
                    $destination = New-OutputDirectory
                    Invoke-Binary 'giggles-export' 'giggles-export' @('export',$state.database,"$destination/catalog.jsonl")
                    Invoke-Binary 'giggles-export' 'giggles-export' @('verify',"$destination/catalog.jsonl")
                    Invoke-Binary 'giggles-export' 'giggles-export' @('import',"$destination/catalog.jsonl","$destination/imported.db")
                }
            }
        }
    }
} catch {
    Write-Error $_ -ErrorAction Continue
    exit 1
}
