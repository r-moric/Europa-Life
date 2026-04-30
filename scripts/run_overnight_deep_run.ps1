$ErrorActionPreference = "Stop"
Set-Location -LiteralPath $PSScriptRoot\..
python .\src\europa_pipeline.py demo-run --config .\configs\overnight_deep_run.json --root .\workspace_overnight_deep_run --run-label overnight_deep_run --reset
