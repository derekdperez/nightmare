<#
.SYNOPSIS
  Runs commands from .ai-debug/debug_commands.ps1 and writes debug_results.json

.DESCRIPTION
  Execute from the DotNetSolution folder: .\debug.ps1
  Blank lines and lines starting with # are ignored.
  Each non-comment line runs in a separate child PowerShell process with output captured.
#>
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Continue'

$RepoRoot = if ($null -ne $PSScriptRoot -and '' -ne $PSScriptRoot) {
    $PSScriptRoot
} else {
    Split-Path -Parent $MyInvocation.MyCommand.Path
}
Set-Location -LiteralPath $RepoRoot

$commandsPath = Join-Path $RepoRoot '.ai-debug\debug_commands.ps1'
$outputPath = Join-Path $RepoRoot 'debug_results.json'

$results = New-Object System.Collections.Generic.List[hashtable]

$lines = @()
if (Test-Path -LiteralPath $commandsPath) {
    $lines = Get-Content -LiteralPath $commandsPath -Encoding UTF8
}

foreach ($raw in $lines) {
    $trimmed = $raw.Trim()
    if ([string]::IsNullOrWhiteSpace($trimmed)) {
        continue
    }
    if ($trimmed.StartsWith('#')) {
        continue
    }

    $cmd = $raw.TrimEnd("`r", "`n")
    $started = [DateTime]::UtcNow
    $tmpScript = [System.IO.Path]::ChangeExtension(
        [System.IO.Path]::GetTempFileName(),
        '.ps1'
    )
    $scriptTail = @'
if ($null -ne $LASTEXITCODE) { exit $LASTEXITCODE }
exit 0
'@
    try {
        Set-Content -LiteralPath $tmpScript -Encoding UTF8 -Value ($cmd + "`n" + $scriptTail)

        $psi = New-Object System.Diagnostics.ProcessStartInfo
        $psi.FileName = 'powershell.exe'
        $psi.WorkingDirectory = $RepoRoot
        $psi.UseShellExecute = $false
        $psi.RedirectStandardOutput = $true
        $psi.RedirectStandardError = $true
        $psi.CreateNoWindow = $true
        $psi.Arguments = "-NoProfile -NoLogo -NonInteractive -File `"$tmpScript`""

        $proc = New-Object System.Diagnostics.Process
        $proc.StartInfo = $psi
        [void]$proc.Start()
        $stdout = $proc.StandardOutput.ReadToEnd()
        $stderr = $proc.StandardError.ReadToEnd()
        $proc.WaitForExit()
        $exitCode = $proc.ExitCode
    } catch {
        $stdout = ''
        $stderr = "[debug.ps1] failed to run command: $($_.Exception.Message)`n"
        $exitCode = 125
    } finally {
        if (Test-Path -LiteralPath $tmpScript) {
            Remove-Item -LiteralPath $tmpScript -Force -ErrorAction SilentlyContinue
        }
    }

    $finished = [DateTime]::UtcNow
    $durationMs = [int]([TimeSpan]($finished - $started)).TotalMilliseconds

    $results.Add(@{
        command     = $cmd
        exit_code   = [int]$exitCode
        stdout      = $stdout
        stderr      = $stderr
        started_at  = $started.ToString('o')
        finished_at = $finished.ToString('o')
        duration_ms = $durationMs
    })
}

$utf8NoBom = New-Object System.Text.UTF8Encoding $false
if ($results.Count -eq 0) {
    [System.IO.File]::WriteAllText($outputPath, '[]', $utf8NoBom)
} else {
    # ConvertTo-Json serializes Generic.List as an object; use a real array for a JSON array root.
    $objects = @($results | ForEach-Object { [PSCustomObject]$_ })
    $json = ConvertTo-Json -InputObject $objects -Depth 10
    [System.IO.File]::WriteAllText($outputPath, $json, $utf8NoBom)
}
