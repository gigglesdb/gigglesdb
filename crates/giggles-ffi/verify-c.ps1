param([Parameter(Mandatory=$true)][string]$ArtifactDirectory,[ValidateSet('debug','release')][string]$Profile='debug')
$ErrorActionPreference='Stop'
if(Test-Path -LiteralPath $ArtifactDirectory){throw 'Choose unique artifact directory'}
New-Item -ItemType Directory -Path $ArtifactDirectory | Out-Null
$artifactPath=(Resolve-Path -LiteralPath $ArtifactDirectory).Path
$cratePath=$PSScriptRoot
$buildArgs=@('build');if($Profile -eq 'release'){$buildArgs+='--release'}
$p=Start-Process cargo -ArgumentList $buildArgs -WindowStyle Hidden -Wait -PassThru -RedirectStandardOutput "$artifactPath/rust.stdout" -RedirectStandardError "$artifactPath/rust.stderr"
Add-Content "$artifactPath/commands.txt" "cargo $buildArgs exit=$($p.ExitCode)"
if($p.ExitCode -ne 0){exit 1}
$vc='C:/Program Files/Microsoft Visual Studio/2022/Community/VC/Auxiliary/Build/vcvars64.bat'
$buildText=@"
@call "$vc"
@cl /nologo /W4 /WX /I"$cratePath/include" "$cratePath/examples/client.c" /Fo"$artifactPath/client.obj" /Fe"$artifactPath/client.exe" /link /LIBPATH:"$cratePath/target/$Profile" giggles_ffi.dll.lib
@exit /b %errorlevel%
"@
Set-Content -LiteralPath "$artifactPath/build.cmd" -Value $buildText
$p=Start-Process cmd.exe -ArgumentList @('/c',"`"$artifactPath/build.cmd`"") -WindowStyle Hidden -Wait -PassThru -RedirectStandardOutput "$artifactPath/compile.stdout" -RedirectStandardError "$artifactPath/compile.stderr"
Add-Content "$artifactPath/commands.txt" "cmd /c build.cmd exit=$($p.ExitCode)"
if($p.ExitCode -ne 0){exit 1}
Copy-Item -LiteralPath "$cratePath/target/$Profile/giggles_ffi.dll" -Destination "$artifactPath/giggles_ffi.dll"
$clientArgs=@("$artifactPath/db","$artifactPath/export","$artifactPath/restored")|ForEach-Object{'"'+($_ -replace '\\','/')+'"'}
$p=Start-Process "$artifactPath/client.exe" -ArgumentList $clientArgs -WindowStyle Hidden -Wait -PassThru -RedirectStandardOutput "$artifactPath/client.stdout" -RedirectStandardError "$artifactPath/client.stderr"
Add-Content "$artifactPath/commands.txt" "client.exe $clientArgs exit=$($p.ExitCode)"
Write-Output "C client ($Profile): exit $($p.ExitCode)"
Get-FileHash -Algorithm SHA256 -LiteralPath "$artifactPath/client.exe","$artifactPath/giggles_ffi.dll","$cratePath/examples/client.c","$cratePath/include/giggles.h" | Format-Table -AutoSize | Out-String -Width 4096 | Set-Content "$artifactPath/sha256.txt"
if($p.ExitCode -ne 0){exit 1}
