$ErrorActionPreference = "Stop"
Set-Location -LiteralPath $PSScriptRoot\..
python .\src\europa_pipeline.py demo-run --config .\configs\workspace_large.json --root .\workspace_large --run-label workspace_large --reset
