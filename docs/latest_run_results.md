# Latest Run Results

## Executed profile

- profile: `dev_fast`
- execution date: `2026-04-06`
- workspace: `workspace_dev_fast_run02`

## Outcome

The pipeline completed end-to-end successfully:

1. canonical SQLite database initialized and seeded
2. synthetic documents generated
3. NER and entity resolution completed
4. auditor/judge review completed
5. dark-theme DQ report and exports written to disk

## Summary metrics

- documents processed: `16`
- raw mentions: `300`
- resolved mentions: `181`
- unresolved mentions: `119`
- resolved rate: `0.6033`
- unresolved rate: `0.3967`
- exactish rate: `0.2707`
- weak provenance rate: `0.0276`
- component match average: `0.7799`
- supply match average: `0.8667`
- auditor finding count: `12`

## LLM usage

- total LLM calls: `13`
- document generation calls: `1`
- resolver LLM calls: `0`
- auditor calls: `12`
- model used in this run: `mistral-small3.1:latest`

## Key artifacts

- run summary manifest: `workspace_dev_fast_run02/manifests/run_summary_dev_fast_20260406_163937.json`
- DQ report: `workspace_dev_fast_run02/reports/dq_report_20260406_163937.html`
- SQLite database: `workspace_dev_fast_run02/db/europa_masterdata.sqlite`
- ERD asset: `workspace_dev_fast_run02/assets/ERD_MERMAID.md`
- CSV exports: `workspace_dev_fast_run02/exports/`
- logs: `workspace_dev_fast_run02/logs/`

## Readout

This run is strong enough to demonstrate the architecture pattern and artifact discipline. The main quality gap is still entity resolution coverage, especially unresolved mentions and modest exact-match performance. That is acceptable for a truthful showcase because the repo exposes these weaknesses instead of hiding them.
