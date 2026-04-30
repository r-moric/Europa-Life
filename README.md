# Europa Life

Europa Life is a self-contained showcase repo for a truth-first data architecture pattern:

- seed governed canonical master data in SQLite
- generate messy operational documents from that truth
- resolve mentions back to mastered entities
- audit weak evidence and suspicious merges
- publish reports, manifests, logs, and CSV data products

The key story is not "AI extracted text." It is that a data architecture can turn document chaos into decision-grade, traceable data products while preserving uncertainty and review points.

## Why this matters

This repo demonstrates:

- canonical identifiers and normalized entity modeling
- metadata-aware linkage and provenance preservation
- quality and lineage as first-class architecture concerns
- reproducible pipeline runs with saved artifacts
- stakeholder outputs for technical reviewers, stewards, analysts, and reviewers

## Repository layout

```text
configs/
  dev_fast.json
  evening_3hour_quality.json
  workspace_large.json
  overnight_quality.json
  overnight_deep_run.json
docs/
  architecture_note.md
  EuropaLife_CaseStudy_Public.pptx
  europa_life_erd.md
  latest_run_results.md
  evening_3hour_quality_results.md
  workspace_large_results.md
prompts/
  mission_seed/
  generator/
  resolver/
  judge/
scripts/
  export_github_snapshot.ps1
  run_dev_fast.ps1
  run_evening_3hour_quality.ps1
  run_workspace_large.ps1
  run_overnight_quality.ps1
  run_overnight_deep_run.ps1
sql/
  schema.sql
  starter_queries.sql
  llm_usage_queries.sql
src/
  europa_pipeline.py
```

## Profiles

### `dev_fast`

Balanced for repeatable end-to-end validation on local hardware:

- template-heavy document generation
- deterministic resolver baseline with resolver LLM review disabled by default
- local auditor enabled
- moderate package volume

### `overnight_quality`

Designed for slower, higher-quality runs:

- more LLM-authored documents
- resolver LLM review enabled for ambiguous cases
- heavier auditor model
- larger document volume

### `workspace_large`

The large overnight profile used for the most recent full-scale run:

- 48 packages and 288 documents
- high resolver LLM usage
- deep auditor pass
- materially improved assembly truth-vs-discovered performance

## Requirements

- Python 3.10+
- local [Ollama](https://ollama.com/) server

Recommended local models already used by this repo:

- `qwen2.5:7b-instruct`
- `qwen2.5:14b`
- `mistral-small3.1:latest`
- `deepseek-r1:14b-qwen-distill-q4_K_M`

## Quick start

Run the full `dev_fast` pipeline:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.\scripts\run_dev_fast.ps1
```

Run the stepwise commands manually if you want to inspect each stage:

```powershell
python .\src\europa_pipeline.py init-db --root .\workspace_dev_fast --reset
python .\src\europa_pipeline.py seed --config .\configs\dev_fast.json --root .\workspace_dev_fast
python .\src\europa_pipeline.py generate-docs --config .\configs\dev_fast.json --root .\workspace_dev_fast --run-label dev_fast
python .\src\europa_pipeline.py run-ner --config .\configs\dev_fast.json --root .\workspace_dev_fast --docs-run dev_fast
python .\src\europa_pipeline.py run-audit --config .\configs\dev_fast.json --root .\workspace_dev_fast
python .\src\europa_pipeline.py run-dq --config .\configs\dev_fast.json --root .\workspace_dev_fast
```

## Outputs

Each run writes a workspace with:

```text
workspace_.../
  assets/
  db/
  docs/
  exports/
  logs/
  manifests/
  reports/
  sql/
```

Important artifacts:

- SQLite database in `db/`
- generated documents in `docs/`
- DQ report HTML in `reports/`
- CSV exports in `exports/`
- JSON summaries in `manifests/`
- stage logs in `logs/`

Generated `workspace_*` folders are local runtime artifacts and are intentionally gitignored for GitHub publishing.

Generated databases, logs, and document bundles are intentionally excluded from the public repository. The code, schema, prompts, run documentation, ERD, and case-study deck are the public artifacts.

## Current build notes

- The canonical truth layer is seeded deterministically in this build for repeatability.
- Prompt files are externalized for document generation, resolver review, and audit review.
- Fine-tuning is deliberately out of scope for Phase 1.
- The current large-run assembly fix resolves assembly mentions into the `ASSEMBLY` category instead of collapsing them into `PART`.

## Supporting docs

- architecture and stakeholder framing: `docs/architecture_note.md`
- schema ERD: `docs/europa_life_erd.md`
- case-study deck: `docs/EuropaLife_CaseStudy_Public.pptx`
- fast baseline results: `docs/latest_run_results.md`
- higher-volume evening run results: `docs/evening_3hour_quality_results.md`
- large overnight run results: `docs/workspace_large_results.md`

## GitHub publishing

This folder is prepared for GitHub with `.gitignore` rules that exclude generated workspaces, databases, private handoff notes, logs, and local export folders.

If you want a clean upload snapshot without local run folders, use:

```powershell
.\scripts\export_github_snapshot.ps1
```

That creates a clean copy containing only the code, configs, prompts, SQL, and documentation needed for publishing.
