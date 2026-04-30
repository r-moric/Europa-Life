$ErrorActionPreference = "Stop"
Set-Location -LiteralPath $PSScriptRoot\..
python .\src\europa_pipeline.py demo-run --config .\configs\overnight_quality.json --root .\workspace_overnight_quality --run-label overnight_quality --reset
