#Requires -Version 5.1
<#
.SYNOPSIS
Runs the multi-client AMQP conformance suites against a freshly started RedisMQ broker.

.DESCRIPTION
Publishes samples/RedisMQ.Host, starts one memory-only broker per selected client on a
free port, runs the client-only suite, and verifies that every scenario id in
scenarios.json was reported as a whole-line "PASS <id>".

.PARAMETER Client
One or more clients to run: pika, node, java, go, or all (default).

.PARAMETER Port
Fixed AMQP port. Defaults to a free port per client.

.PARAMETER Configuration
Build configuration for the published host. Defaults to Release.

.PARAMETER SkipPrepare
Do not install client dependencies or build client artifacts.

.PARAMETER SkipMissing
Warn about and drop clients whose toolchain is missing instead of failing.

.PARAMETER KeepBroker
Leave the broker running after the run. Requires exactly one client.
#>
param(
    [ValidateSet('pika', 'node', 'java', 'go', 'all')]
    [string[]]$Client = @('all'),
    [int]$Port = 0,
    [ValidateSet('Debug', 'Release')]
    [string]$Configuration = 'Release',
    [switch]$SkipPrepare,
    [switch]$SkipMissing,
    [switch]$KeepBroker
)

$ErrorActionPreference = 'Stop'

$isWindowsHost = [Environment]::OSVersion.Platform -eq [PlatformID]::Win32NT

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path
$clientsRoot = Join-Path $repoRoot 'tests\clients'
$logDir = Join-Path $clientsRoot '.logs'
$scenarioFile = Join-Path $clientsRoot 'scenarios.json'
$hostProject = Join-Path $repoRoot 'samples\RedisMQ.Host\RedisMQ.Host.csproj'

$required = (Get-Content $scenarioFile -Raw | ConvertFrom-Json).required

$clients = if ($Client -contains 'all') { @('pika', 'node', 'java', 'go') } else { $Client }
$clients = @($clients | Select-Object -Unique)

if ($KeepBroker -and $clients.Count -ne 1) {
    throw '-KeepBroker requires exactly one -Client value.'
}

function Write-Step([string]$Message) {
    [Console]::Error.WriteLine("==> $Message")
}

function Resolve-Python {
    foreach ($name in @('python', 'py')) {
        $command = Get-Command $name -ErrorAction SilentlyContinue
        if ($command) { return $command.Source }
    }

    return $null
}

$pythonExe = Resolve-Python

function Get-ClientSpec([string]$Name) {
    switch ($Name) {
        'pika' {
            return [pscustomobject]@{
                Name        = 'pika'
                Directory   = Join-Path $clientsRoot 'pika'
                Command     = $pythonExe
                Arguments   = @('conformance.py')
                Prepare     = if ($pythonExe) { @('-m', 'pip', 'install', '-r', 'requirements.txt') } else { @() }
                Requirements = @(
                    [pscustomobject]@{ Label = 'python (or py)'; Present = [bool]$pythonExe },
                    [pscustomobject]@{ Label = 'tests/clients/pika/conformance.py'; Present = (Test-Path (Join-Path $clientsRoot 'pika\conformance.py')) }
                )
            }
        }
        'node' {
            return [pscustomobject]@{
                Name        = 'node'
                Directory   = Join-Path $clientsRoot 'node'
                Command     = 'node'
                Arguments   = @('conformance.js')
                Prepare     = @('ci')
                Requirements = @(
                    [pscustomobject]@{ Label = 'node'; Present = [bool](Get-Command node -ErrorAction SilentlyContinue) },
                    [pscustomobject]@{ Label = 'npm'; Present = [bool](Get-Command npm -ErrorAction SilentlyContinue) },
                    [pscustomobject]@{ Label = 'tests/clients/node/conformance.js'; Present = (Test-Path (Join-Path $clientsRoot 'node\conformance.js')) }
                )
            }
        }
        'java' {
            return [pscustomobject]@{
                Name        = 'java'
                Directory   = Join-Path $clientsRoot 'java'
                Command     = 'java'
                Arguments   = @('-jar', 'target/redismq-client-conformance.jar')
                Prepare     = @('-q', '-DskipTests', 'package')
                Requirements = @(
                    [pscustomobject]@{ Label = 'java'; Present = [bool](Get-Command java -ErrorAction SilentlyContinue) },
                    [pscustomobject]@{ Label = 'mvn'; Present = [bool](Get-Command mvn -ErrorAction SilentlyContinue) },
                    [pscustomobject]@{ Label = 'tests/clients/java/pom.xml'; Present = (Test-Path (Join-Path $clientsRoot 'java\pom.xml')) }
                )
            }
        }
        'go' {
            return [pscustomobject]@{
                Name        = 'go'
                Directory   = Join-Path $clientsRoot 'go'
                Command     = 'go'
                Arguments   = @('run', '.')
                Prepare     = @('mod', 'download')
                Requirements = @(
                    [pscustomobject]@{ Label = 'go'; Present = [bool](Get-Command go -ErrorAction SilentlyContinue) },
                    [pscustomobject]@{ Label = 'tests/clients/go/go.mod'; Present = (Test-Path (Join-Path $clientsRoot 'go\go.mod')) }
                )
            }
        }
        default { throw "Unknown client '$Name'." }
    }
}

function Invoke-Logged([string]$Exe, [string[]]$Arguments, [string]$WorkingDirectory, [string]$OutFile, [string]$ErrFile) {
    $process = Start-Process -FilePath $Exe -ArgumentList $Arguments -WorkingDirectory $WorkingDirectory `
        -PassThru -Wait -NoNewWindow -RedirectStandardOutput $OutFile -RedirectStandardError $ErrFile
    return $process.ExitCode
}

function Get-FreePort {
    $listener = [System.Net.Sockets.TcpListener]::new([System.Net.IPAddress]::Loopback, 0)
    try {
        $listener.Start()
        return ([System.Net.IPEndPoint]$listener.LocalEndpoint).Port
    }
    finally {
        $listener.Stop()
    }
}

function Wait-ForPort([int]$PortNumber, [System.Diagnostics.Process]$Process, [int]$TimeoutSeconds = 20) {
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        if ($Process.HasExited) { return $false }

        $tcp = [System.Net.Sockets.TcpClient]::new()
        try {
            $connect = $tcp.ConnectAsync('127.0.0.1', $PortNumber)
            if ($connect.Wait(250) -and $tcp.Connected) { return $true }
        }
        catch {
            # Broker not listening yet.
        }
        finally {
            $tcp.Dispose()
        }

        Start-Sleep -Milliseconds 150
    }

    return $false
}

function Show-LogTail([string]$Label, [string[]]$Paths, [int]$Lines = 40) {
    foreach ($path in $Paths) {
        if (Test-Path $path) {
            [Console]::Error.WriteLine("--- ${Label}: $path (tail) ---")
            Get-Content $path -Tail $Lines | ForEach-Object { [Console]::Error.WriteLine($_) }
        }
    }
}

$specs = @()
$missing = @()
foreach ($name in $clients) {
    $spec = Get-ClientSpec $name
    $missingHere = @($spec.Requirements | Where-Object { -not $_.Present } | ForEach-Object { "$name`: $($_.Label)" })
    if ($missingHere.Count -gt 0) {
        $missing += $missingHere
        continue
    }

    $specs += $spec
}

if ($missing.Count -gt 0) {
    $message = "Missing client prerequisites:`n  " + ($missing -join "`n  ")
    if (-not $SkipMissing) { throw "$message`nInstall them, or pass -SkipMissing to run the remaining clients." }

    [Console]::Error.WriteLine("WARNING: skipping clients with missing prerequisites:")
    $missing | ForEach-Object { [Console]::Error.WriteLine("  $_") }
}

if ($specs.Count -eq 0) {
    throw 'No runnable clients remain.'
}

New-Item -ItemType Directory -Path $logDir -Force | Out-Null

if (-not $SkipPrepare) {
    foreach ($spec in $specs) {
        Write-Step "preparing $($spec.Name)"
        $out = Join-Path $logDir "$($spec.Name).prepare.out.log"
        $err = Join-Path $logDir "$($spec.Name).prepare.err.log"
        $exe = switch ($spec.Name) {
            'pika' { $pythonExe }
            'node' { if ($isWindowsHost) { (Get-Command npm.cmd).Source } else { 'npm' } }
            'java' { if ($isWindowsHost) { (Get-Command mvn.cmd).Source } else { 'mvn' } }
            'go' { 'go' }
        }
        $code = Invoke-Logged $exe $spec.Prepare $spec.Directory $out $err

        if ($code -ne 0) {
            Show-LogTail "prepare $($spec.Name)" @($out, $err)
            throw "Preparing $($spec.Name) failed with exit code $code."
        }
    }
}

$publishDir = Join-Path ([System.IO.Path]::GetTempPath()) ("redismq-conformance-" + [guid]::NewGuid().ToString('N'))
$broker = $null
$keepBroker = $false
$exitCode = 0

try {
    Write-Step "publishing $hostProject to $publishDir"
    & dotnet publish $hostProject -c $Configuration -o $publishDir
    if ($LASTEXITCODE -ne 0) { throw "dotnet publish failed with exit code $LASTEXITCODE." }

    $brokerDll = Join-Path $publishDir 'RedisMQ.Host.dll'
    if (-not (Test-Path $brokerDll)) { throw "Published host not found at $brokerDll." }

    foreach ($spec in $specs) {
        $amqpPort = if ($Port -gt 0) { $Port } else { Get-FreePort }
        $stdoutLog = Join-Path $logDir "$($spec.Name).log"
        $stderrLog = Join-Path $logDir "$($spec.Name).err.log"
        $clientOut = Join-Path $logDir "$($spec.Name).client.out.log"
        $clientErr = Join-Path $logDir "$($spec.Name).client.err.log"

        Write-Step "starting broker for $($spec.Name) on 127.0.0.1:$amqpPort"
        $brokerEnv = @{
            'RedisMQ__Broker__Port'              = "$amqpPort"
            'RedisMQ__Broker__Vhosts__0'         = '/'
            'RedisMQ__Broker__Users__0__Name'    = 'guest'
            'RedisMQ__Broker__Users__0__Password' = 'guest'
            'RedisMQ__Broker__Users__0__Permissions__0__Vhost'    = '/'
            'RedisMQ__Broker__Users__0__Permissions__0__Configure' = '.*'
            'RedisMQ__Broker__Users__0__Permissions__0__Write'     = '.*'
            'RedisMQ__Broker__Users__0__Permissions__0__Read'      = '.*'
            'RedisMQ__Redis__ConnectionString'   = ''
            'ASPNETCORE_URLS'                    = 'http://127.0.0.1:0'
        }

        $savedEnv = @{}
        foreach ($key in $brokerEnv.Keys) {
            $savedEnv[$key] = [Environment]::GetEnvironmentVariable($key, 'Process')
            [Environment]::SetEnvironmentVariable($key, $brokerEnv[$key], 'Process')
        }

        try {
            $broker = Start-Process -FilePath 'dotnet' -ArgumentList $brokerDll -WorkingDirectory $publishDir `
                -PassThru -NoNewWindow -RedirectStandardOutput $stdoutLog -RedirectStandardError $stderrLog
        }
        finally {
            foreach ($key in $savedEnv.Keys) {
                [Environment]::SetEnvironmentVariable($key, $savedEnv[$key], 'Process')
            }
        }

        if (-not (Wait-ForPort $amqpPort $broker)) {
            Show-LogTail "broker $($spec.Name)" @($stdoutLog, $stderrLog)
            throw "Broker for $($spec.Name) did not listen on port $amqpPort."
        }

        $env:REDISMQ_HOST = '127.0.0.1'
        $env:REDISMQ_PORT = "$amqpPort"
        $env:REDISMQ_USER = 'guest'
        $env:REDISMQ_PASSWORD = 'guest'
        $env:REDISMQ_VHOST = '/'

        Write-Step "running $($spec.Name) suite"
        $clientCode = Invoke-Logged $spec.Command $spec.Arguments $spec.Directory $clientOut $clientErr

        if (Test-Path $clientOut) { Get-Content $clientOut | ForEach-Object { Write-Output $_ } }
        if (Test-Path $clientErr) { Get-Content $clientErr | ForEach-Object { [Console]::Error.WriteLine($_) } }

        $reported = @()
        $failed = @()
        if (Test-Path $clientOut) {
            foreach ($line in Get-Content $clientOut) {
                $trimmed = $line.Trim()
                if ($trimmed -match '^PASS\s+(\S+)$') { $reported += $Matches[1] }
                elseif ($trimmed -match '^FAIL\s+(\S+)') { $failed += $trimmed }
            }
        }

        $notReported = @($required | Where-Object { $reported -notcontains $_ })

        if ($clientCode -ne 0 -or $failed.Count -gt 0 -or $notReported.Count -gt 0) {
            Show-LogTail "broker $($spec.Name)" @($stdoutLog, $stderrLog)
            if ($failed.Count -gt 0) { throw "$($spec.Name) reported failures: $($failed -join '; ')" }
            if ($notReported.Count -gt 0) { throw "$($spec.Name) did not report PASS for: $($notReported -join ', ')" }
            throw "$($spec.Name) suite exited with code $clientCode."
        }

        Write-Step "$($spec.Name) passed all $($required.Count) scenarios"

        if ($KeepBroker) {
            $keepBroker = $true
            [Console]::Error.WriteLine("Broker left running on 127.0.0.1:$amqpPort (PID $($broker.Id)).")
            [Console]::Error.WriteLine("REDISMQ_HOST=127.0.0.1 REDISMQ_PORT=$amqpPort REDISMQ_USER=guest REDISMQ_PASSWORD=guest REDISMQ_VHOST=/")
        }
        else {
            Stop-Process -Id $broker.Id -Force -ErrorAction SilentlyContinue
            $broker.WaitForExit(5000) | Out-Null
            $broker = $null
        }
    }
}
catch {
    [Console]::Error.WriteLine($_.Exception.Message)
    $exitCode = 1
}
finally {
    if ($broker -and -not $keepBroker -and -not $broker.HasExited) {
        Stop-Process -Id $broker.Id -Force -ErrorAction SilentlyContinue
    }

    if (-not $keepBroker -and (Test-Path $publishDir)) {
        Remove-Item $publishDir -Recurse -Force -ErrorAction SilentlyContinue
    }
}

exit $exitCode
