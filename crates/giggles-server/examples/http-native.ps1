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
            [IO.File]::WriteAllText($requestFile, ($Body | ConvertTo-Json -Depth 32 -Compress), [Text.UTF8Encoding]::new($false))
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
    if ($reply.code -and $reply.code -ne 'ok') {
        throw "Giggles returned $($reply.code): $($reply.nativeError.kind)"
    }
    return $reply
}
function Invoke-Native($Body) {
    $Body.budgetMs = 10000
    Invoke-Giggles 'POST' '/v1/session/execute' $Body
}
try {
    $session = (Invoke-Giggles 'POST' '/v1/sessions' @{}).sessionId
    $null = Invoke-Native @{begin=@{}}
    if ($Insert) {
        $null = Invoke-Native @{operation=@{
            request=@{insert=@{table='app';values=@(
                @{column='id';value=@{parameter='id'}},
                @{column='body';value=@{parameter='body'}})}}
            bindings=@(
                @{name='id';value=@{intValue=$Id.ToString([Globalization.CultureInfo]::InvariantCulture)}},
                @{name='body';value=@{textValue=$Text}})
        }}
    }
    $read = @{read=@{table='app';order=@(@{field='id';descending=$false});
        limit=@{literal=@{intValue='10'}};projection=@('id','body')}}
    $operation = Invoke-Native @{operation=@{request=$read;bindings=@()}}
    # A read-only block joins the explicit transaction and sees its staged writes.
    $block = Invoke-Native @{transaction=@{transaction=@{
        readOnly=$true
        steps=@(@{letBinding=@{name='rows';request=$read}})
        emit=@{values=@(@{rows='rows'})}
    };bindings=@()}}
    $null = Invoke-Native @{commit=@{}}
    @{operation=$operation.operationOutcome;block=$block.transactionOutcome} | ConvertTo-Json -Depth 32
} catch {
    if ($session) { try { $null = Invoke-Native @{rollback=@{}} } catch { } }
    throw
} finally {
    if ($session) { $null = Invoke-Giggles 'DELETE' '/v1/session' $null }
}
