# Dot-sourced by run-local.ps1. Expects $Root = DotNetSolution directory.
# Re-runnable deploy: export BUILD_SOURCE_STAMP, then build --pull and up --force-recreate.

function Export-NightmareBuildStamp {
  param([Parameter(Mandatory = $true)][string] $Root)
  $env:BUILD_SOURCE_STAMP = "nogit-$(Get-Date -Format 'yyyyMMddTHHmmss')Z"
  $gitDir = Join-Path $Root ".git"
  if (Test-Path $gitDir) {
    $head = (& git -C $Root rev-parse HEAD 2>$null)
    if (-not $head) { $head = "unknown" }
    & git -C $Root diff --quiet 2>$null
    $wd = $LASTEXITCODE
    & git -C $Root diff --cached --quiet 2>$null
    $ix = $LASTEXITCODE
    if ($wd -eq 0 -and $ix -eq 0) {
      $env:BUILD_SOURCE_STAMP = $head
    }
    else {
      $env:BUILD_SOURCE_STAMP = "$head-dirty"
    }
  }
  Write-Host "BUILD_SOURCE_STAMP=$($env:BUILD_SOURCE_STAMP)"
}

function Invoke-NightmareGitPullIfRequested {
  param([Parameter(Mandatory = $true)][string] $Root)
  if ($env:NIGHTMARE_GIT_PULL -ne "1") { return }
  $gitDir = Join-Path $Root ".git"
  if (-not (Test-Path $gitDir)) {
    Write-Warning "NIGHTMARE_GIT_PULL=1 but $Root has no .git; skipping pull."
    return
  }
  Write-Host "NIGHTMARE_GIT_PULL=1: git pull --ff-only in $Root"
  & git -C $Root pull --ff-only
  if ($LASTEXITCODE -ne 0) { throw "git pull --ff-only failed (exit $LASTEXITCODE)." }
}

function Invoke-NightmareComposeBuildPull {
  param(
    [Parameter(Mandatory = $true)][string] $DockerExe,
    [Parameter(Mandatory = $true)][string] $ComposeFile,
    [switch] $NoCache
  )
  $args = @("compose", "-f", $ComposeFile, "build", "--pull")
  if ($NoCache) { $args += "--no-cache" }
  & $DockerExe @args
  if ($LASTEXITCODE -ne 0) { throw "docker compose build failed (exit $LASTEXITCODE)." }
}

function Invoke-NightmareComposeUpRecreate {
  param(
    [Parameter(Mandatory = $true)][string] $DockerExe,
    [Parameter(Mandatory = $true)][string] $ComposeFile
  )
  $args = @("compose", "-f", $ComposeFile, "up", "-d", "--force-recreate", "--remove-orphans")
  & $DockerExe @args
  if ($LASTEXITCODE -ne 0) { throw "docker compose up failed (exit $LASTEXITCODE)." }
}
