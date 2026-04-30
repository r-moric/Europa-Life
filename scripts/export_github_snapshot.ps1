param(
    [string]$TargetFolderName
)

$ErrorActionPreference = "Stop"

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$parentDir = Split-Path -Parent $repoRoot
$repoFolderName = Split-Path -Leaf $repoRoot

if (-not $TargetFolderName) {
    $TargetFolderName = "${repoFolderName}_github_ready"
}

$targetRoot = Join-Path $parentDir $TargetFolderName

if (Test-Path -LiteralPath $targetRoot) {
    $resolvedTarget = (Resolve-Path -LiteralPath $targetRoot).Path
    if (-not $resolvedTarget.StartsWith($parentDir, [System.StringComparison]::OrdinalIgnoreCase)) {
        throw "Refusing to remove target outside expected parent directory: $resolvedTarget"
    }
    Remove-Item -LiteralPath $resolvedTarget -Recurse -Force
}

New-Item -ItemType Directory -Path $targetRoot | Out-Null

$itemsToCopy = @(
    ".gitattributes",
    ".gitignore",
    "README.md",
    "pyproject.toml",
    "requirements.txt",
    "assets",
    "configs",
    "docs",
    "prompts",
    "scripts",
    "sql",
    "src"
)

foreach ($item in $itemsToCopy) {
    $sourcePath = Join-Path $repoRoot $item
    if (Test-Path -LiteralPath $sourcePath) {
        Copy-Item -LiteralPath $sourcePath -Destination $targetRoot -Recurse -Force
    }
}

Write-Output "GitHub-ready snapshot created at: $targetRoot"
