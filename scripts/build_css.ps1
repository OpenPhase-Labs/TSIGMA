<#
.SYNOPSIS
  Compile the TSIGMA Tailwind v4 stylesheet using the vendored Windows
  standalone CLI binary.

.DESCRIPTION
  Developer-side build (Windows). Compiles tsigma/static/css/tailwind.src.css
  into the committed tsigma/static/css/tailwind.css. Deployers never run this -
  they serve the committed tailwind.css as-is (no Node, no Tailwind, no extra
  packages).

  The standalone binary is NOT committed (it is ~125 MB and platform-specific).
  Download the pinned version once into tools/tailwind/tailwindcss.exe:

    Tailwind CSS v4.3.0 (pinned)
    https://github.com/tailwindlabs/tailwindcss/releases/download/v4.3.0/tailwindcss-windows-x64.exe

  Cross-platform / CI builds are a future concern (Linux migration, agency
  pipelines); for now this Windows binary is the build path.

.PARAMETER Check
  Build to a temp file and diff against the committed tailwind.css instead of
  overwriting it. Non-zero exit if the committed CSS is stale. (Currency gate.)
#>
[CmdletBinding()]
param(
  [switch]$Check
)

$ErrorActionPreference = 'Stop'

$repoRoot = Resolve-Path (Join-Path $PSScriptRoot '..')
$binary   = Join-Path $repoRoot 'tools\tailwind\tailwindcss.exe'
$src      = Join-Path $repoRoot 'tsigma\static\css\tailwind.src.css'
$out      = Join-Path $repoRoot 'tsigma\static\css\tailwind.css'
$genScript = Join-Path $PSScriptRoot 'gen_safelist.py'

# Regenerate the safelist (the human-readable utility vocabulary) BEFORE
# compiling, so tailwind.src.css's @source "./safelist.txt" force-emits the
# full vocabulary. Prefer the known-present pinned interpreter; fall back to
# whatever `python` resolves to on PATH.
$pythonExe = 'C:\Python314\python.exe'
if (-not (Test-Path $pythonExe)) {
  $pythonCmd = Get-Command python -ErrorAction SilentlyContinue
  if ($null -eq $pythonCmd) {
    throw "No Python interpreter found (looked for C:\Python314\python.exe and python on PATH) to run gen_safelist.py"
  }
  $pythonExe = $pythonCmd.Source
}
& $pythonExe $genScript
if ($LASTEXITCODE -ne 0) { throw "gen_safelist.py failed (exit $LASTEXITCODE)" }

if (-not (Test-Path $binary)) {
  throw "Tailwind binary not found at $binary`n" +
        "Download Tailwind v4.3.0 (Windows x64) once:`n" +
        "  https://github.com/tailwindlabs/tailwindcss/releases/download/v4.3.0/tailwindcss-windows-x64.exe`n" +
        "and save it as tools\tailwind\tailwindcss.exe"
}

if ($Check) {
  $tmp = [System.IO.Path]::GetTempFileName() + '.css'
  & $binary -i $src -o $tmp --minify
  if (-not (Test-Path $out)) { throw "Committed tailwind.css missing at $out" }
  $built     = Get-Content $tmp -Raw
  $committed = Get-Content $out -Raw
  Remove-Item $tmp -Force
  if ($built -ne $committed) {
    Write-Error "tailwind.css is STALE - rerun scripts\build_css.ps1 and commit the result."
    exit 1
  }
  Write-Host "tailwind.css is up to date."
  exit 0
}

& $binary -i $src -o $out --minify
Write-Host "Built $out"
