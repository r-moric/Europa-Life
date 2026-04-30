$ErrorActionPreference = "Stop"
Set-Location -LiteralPath $PSScriptRoot\..
python .\src\europa_pipeline.py demo-run --config .\configs\evening_3hour_quality.json --root .\workspace_evening_3hour_quality --run-label evening_3hour_quality --reset
