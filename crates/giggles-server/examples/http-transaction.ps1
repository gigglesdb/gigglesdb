# Requires PowerShell 5.1+ and curl with PEM TLS support.
param(
    [Parameter(Mandatory)][string]$Endpoint,
    [Parameter(Mandatory)][string]$CaCertificate,
    [Parameter(Mandatory)][string]$ClientCertificate,
    [Parameter(Mandatory)][string]$ClientKey,
    [switch]$Insert,
    [long]$Id = 9,
    [string]$Text = 'hello',
    [string]$CurlPath = 'curl.exe'
)
$ErrorActionPreference = 'Stop'
$session = $null
$curlApplication = (Get-Command $CurlPath -CommandType Application -ErrorAction Stop).Source
function Invoke-Giggles([string]$Method, [string]$Path, $Body) {
    $curlArgs = @('--silent', '--show-error', '--fail-with-body', '--max-time', '25',
        '--cacert', $CaCertificate, '--cert', $ClientCertificate, '--key', $ClientKey,
        '--request', $Method, '--header', 'content-type: application/json',
        '--write-out', '%{http_code}', ($Endpoint.TrimEnd('/') + $Path))
    if ($session) { $curlArgs += @('--header', "x-giggles-session: $session") }
    $requestFile = $null
    $responseFile = [IO.Path]::GetTempFileName()
    try {
        $curlArgs += @('--output', $responseFile)
        if ($null -ne $Body) {
            $requestFile = [IO.Path]::GetTempFileName()
            [IO.File]::WriteAllText($requestFile, ($Body | ConvertTo-Json -Depth 16 -Compress), [Text.UTF8Encoding]::new($false))
            $curlArgs += @('--data-binary', "@$requestFile")
        }
        $statusOutput = & $curlApplication @curlArgs
        if ($LASTEXITCODE -ne 0) { throw 'HTTP request failed. Its outcome may be unknown; no retry was attempted.' }
        $payload = [IO.File]::ReadAllText($responseFile, [Text.Encoding]::UTF8)
    } finally {
        if ($requestFile) { Remove-Item -LiteralPath $requestFile }
        Remove-Item -LiteralPath $responseFile
    }
    $status = [int]($statusOutput -join '')
    if ($status -eq 204) { return $null }
    $reply = $payload | ConvertFrom-Json
    if ($reply.code -and $reply.code -ne 'ok') { throw "Giggles returned $($reply.code)" }
    return $reply
}
function Invoke-Sql([string]$Sql, [array]$Parameters = @()) {
    Invoke-Giggles 'POST' '/v1/session/execute' @{sql=@{sql=$Sql;params=$Parameters};budgetMs=10000}
}
try {
    $session = (Invoke-Giggles 'POST' '/v1/sessions' @{}).sessionId
    $null = Invoke-Sql 'BEGIN'
    if ($Insert) {
        $null = Invoke-Sql 'INSERT INTO app (id,body) VALUES ($1,$2)' @(
            @{intValue=$Id.ToString([Globalization.CultureInfo]::InvariantCulture)}, @{textValue=$Text})
    }
    (Invoke-Sql 'SELECT id,body FROM app ORDER BY id LIMIT 10').sql | ConvertTo-Json -Depth 16
    $null = Invoke-Sql 'COMMIT'
} catch {
    if ($session) { try { $null = Invoke-Sql 'ROLLBACK' } catch { } }
    throw
} finally {
    if ($session) { $null = Invoke-Giggles 'DELETE' '/v1/session' $null }
}
