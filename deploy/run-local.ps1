#Requires -Version 5.1
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$DeployDir = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $DeployDir

$EnvFile   = Join-Path $DeployDir '.env'
$TlsDir    = Join-Path $DeployDir 'tls'
$CertFile  = Join-Path $TlsDir 'server.crt'
$KeyFile   = Join-Path $TlsDir 'server.key'

function Get-RandomHex {
    param(
        [Parameter(Mandatory = $true)]
        [int]$ByteCount
    )

    $bytes = New-Object byte[] $ByteCount
    [System.Security.Cryptography.RandomNumberGenerator]::Create().GetBytes($bytes)
    return ([System.BitConverter]::ToString($bytes)).Replace('-', '').ToLowerInvariant()
}

function Test-CommandExists {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Name
    )

    return $null -ne (Get-Command $Name -ErrorAction SilentlyContinue)
}

function Get-ExistingEnvValue {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path,
        [Parameter(Mandatory = $true)]
        [string]$Key
    )

    if (-not (Test-Path -LiteralPath $Path)) {
        return $null
    }
    $prefix = "$Key="
    foreach ($line in [System.IO.File]::ReadAllLines($Path)) {
        if ($line.StartsWith($prefix, [System.StringComparison]::Ordinal)) {
            $value = $line.Substring($prefix.Length).Trim()
            if ($value.Length -gt 0) {
                return $value
            }
            return $null
        }
    }
    return $null
}

if (-not (Test-CommandExists 'openssl')) {
    throw "openssl was not found in PATH. Install OpenSSL for Windows and ensure 'openssl' is available from PowerShell."
}

if (-not (Test-CommandExists 'docker')) {
    throw "docker was not found in PATH. Install Docker Desktop and ensure 'docker' is available from PowerShell."
}

function Test-DockerDaemonAvailable {
    try {
        # Redirect all streams so docker stderr does not surface as a PowerShell
        # NativeCommandError when the daemon is down.
        $null = & docker info *> $null
        return $LASTEXITCODE -eq 0
    }
    catch {
        return $false
    }
}

if (-not (Test-DockerDaemonAvailable)) {
    throw @"
Docker CLI was found, but the Docker daemon is not reachable.

Windows fix:
  1) Start Docker Desktop and wait until it reports 'Engine running'
  2) Re-run: .\deploy\run-local.ps1

If Docker Desktop is already open, restart it and verify:
  docker info
"@
}

# Reuse secrets from existing .env so persisted Postgres volumes remain valid.
$ExistingPostgresPassword = Get-ExistingEnvValue -Path $EnvFile -Key 'POSTGRES_PASSWORD'
$ExistingCoordinatorToken = Get-ExistingEnvValue -Path $EnvFile -Key 'COORDINATOR_API_TOKEN'

$PostgresPassword = if ($ExistingPostgresPassword) { $ExistingPostgresPassword } else { Get-RandomHex -ByteCount 32 }
$CoordinatorApiToken = if ($ExistingCoordinatorToken) { $ExistingCoordinatorToken } else { Get-RandomHex -ByteCount 64 }

# Create TLS directory if needed
if (-not (Test-Path -LiteralPath $TlsDir)) {
    New-Item -ItemType Directory -Path $TlsDir | Out-Null
}

# Generate self-signed certificate
& openssl req -x509 -nodes -newkey rsa:2048 `
    -keyout $KeyFile `
    -out $CertFile `
    -days 365 `
    -subj '/CN=server'

if ($LASTEXITCODE -ne 0) {
    throw "OpenSSL certificate generation failed."
}

# Write .env file
$envContent = @"
POSTGRES_DB=nightmare
POSTGRES_USER=nightmare
POSTGRES_PASSWORD=$PostgresPassword
COORDINATOR_API_TOKEN=$CoordinatorApiToken
TLS_CERT_FILE=$CertFile
TLS_KEY_FILE=$KeyFile
COORDINATOR_BASE_URL=https://server:443
"@

[System.IO.File]::WriteAllText($EnvFile, $envContent, [System.Text.UTF8Encoding]::new($false))

Write-Host "Generated configuration:"
Write-Host "  .env file: $EnvFile"
Write-Host "  TLS cert: $CertFile"
Write-Host "  TLS key: $KeyFile"
Write-Host "  Coordinator API Token: $CoordinatorApiToken"
Write-Host ""
Write-Host "Starting local Nightmare cluster (1 central server + 4 workers)..."

# Run docker compose
& docker compose -f 'docker-compose.local.yml' --env-file '.env' up -d --build

if ($LASTEXITCODE -ne 0) {
    throw @"
docker compose up failed.

If you see 'failed to extract layer' / overlayfs / 'input/output error', that is usually Docker Desktop
disk or cache corruption, or the host disk is full. Try in order:
  1) Free disk space on the drive that holds Docker data (often C:).
  2) Docker Desktop -> Troubleshoot -> Clean / Purge data (or Reset to factory defaults) — this removes images/containers.
  3) Exit Docker, run: wsl --shutdown  then start Docker Desktop again.
  4) In an elevated prompt:  docker system prune -af --volumes  (only if you can lose unused Docker data)
  5) If it persists, reinstall Docker Desktop or run Windows disk check (chkdsk) on the volume.

Then re-run: .\run-local.ps1
"@
}

Write-Host ""
Write-Host "Nightmare cluster started successfully!"
Write-Host "  Central server: https://localhost (self-signed cert)"
Write-Host "  Dashboard: http://localhost/dashboard"
Write-Host ""
Write-Host "To stop: docker compose -f docker-compose.local.yml --env-file .env down"
Write-Host "To view logs: docker compose -f docker-compose.local.yml --env-file .env logs -f"
