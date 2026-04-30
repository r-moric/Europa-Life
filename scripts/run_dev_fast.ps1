$ErrorActionPreference = "Stop"
Set-Location -LiteralPath $PSScriptRoot\..
python .\src\europa_pipeline.py demo-run --config .\configs\dev_fast.json --root .\workspace_dev_fast --run-label dev_fast --reset
