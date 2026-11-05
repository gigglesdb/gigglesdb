param([Parameter(Mandatory=$true)][string]$OutputPrefix)
$ErrorActionPreference = 'Stop'
$repoRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '../..'))
$sha256 = [Security.Cryptography.SHA256]::Create()
function HexHash([byte[]]$Bytes) {
    return ([BitConverter]::ToString($sha256.ComputeHash($Bytes))).Replace('-', '').ToLowerInvariant()
}
function WriteExclusive([string]$Path, [string]$Text) {
    $stream = [IO.File]::Open([IO.Path]::GetFullPath($Path), [IO.FileMode]::CreateNew, [IO.FileAccess]::Write)
    try {
        $bytes = (New-Object Text.UTF8Encoding($false)).GetBytes($Text)
        $stream.Write($bytes, 0, $bytes.Length)
    } finally { $stream.Dispose() }
}
$allLines = New-Object 'System.Collections.Generic.List[string]'
$trees = New-Object 'System.Collections.Generic.List[string]'
foreach ($crate in @('giggles-core','giggles-server','giggles-consensus','giggles-client','giggles-faultlab')) {
    $cratePath = Join-Path $repoRoot "crates/$crate"
    $files = New-Object 'System.Collections.Generic.List[string]'
    foreach ($directory in @('src','tests')) {
        $path = Join-Path $cratePath $directory
        if (Test-Path -LiteralPath $path) {
            Get-ChildItem -LiteralPath $path -Recurse -File | ForEach-Object { $files.Add($_.FullName) }
        }
    }
    foreach ($name in @('Cargo.toml','Cargo.lock')) {
        $path = Join-Path $cratePath $name
        if (Test-Path -LiteralPath $path) { $files.Add($path) }
    }
    if ($crate -eq 'giggles-faultlab') {
        foreach ($name in @('README.md','.gitignore','source-hashes.ps1')) { $files.Add((Join-Path $cratePath $name)) }
    }
    $crateLines = New-Object 'System.Collections.Generic.List[string]'
    foreach ($path in ($files | Sort-Object -Unique)) {
        $relative = $path.Substring($repoRoot.Length + 1).Replace('\','/')
        $line = (HexHash ([IO.File]::ReadAllBytes($path))) + '  ' + $relative
        $crateLines.Add($line)
        $allLines.Add($line)
    }
    $canonical = ($crateLines -join "`n") + "`n"
    $tree = HexHash ((New-Object Text.UTF8Encoding($false)).GetBytes($canonical))
    $trees.Add("$tree  $crate files=$($crateLines.Count)")
}
WriteExclusive ($OutputPrefix + '-files.log') (($allLines -join "`n") + "`n")
WriteExclusive ($OutputPrefix + '-trees.log') (($trees -join "`n") + "`n")
$trees
$sha256.Dispose()
