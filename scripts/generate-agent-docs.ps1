# Generates compressed agent documentation from human-readable docs.
# Run manually after updating docs: .\scripts\generate-agent-docs.ps1
# Requires: ANTHROPIC_API_KEY environment variable.

param(
    [string]$Threshold = "0.20"
)

$ErrorActionPreference = "Stop"

if (-not $env:ANTHROPIC_API_KEY) {
    Write-Error "ANTHROPIC_API_KEY is not set. Add it to your PowerShell profile or run: `$env:ANTHROPIC_API_KEY = 'your-key'"
    exit 1
}

$ProjectRoot = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$env:PROJECT_ROOT = $ProjectRoot

Write-Host "Generating compressed agent docs (threshold: $Threshold)..."

Push-Location $ProjectRoot
try {
    sbt "docGenerator/run $Threshold"
} finally {
    Pop-Location
}

Write-Host "Agent docs generated. Review changes with: git diff .claude/agent-docs/"
