# Workspace Large Results

## Execution window

- profile: `workspace_large`
- root: `workspace_large`
- start: `2026-04-06 21:33:37 -04:00`
- finish: `2026-04-07 05:22:49 -04:00`
- elapsed: about `7 hours 49 minutes 12 seconds`

## Outcome

The run completed end to end:

- canonical database seeded
- 288 documents generated
- NER and entity resolution completed
- 100 auditor cases completed
- report, exports, logs, and manifests saved

## Key metrics

- raw mentions: `11295`
- resolved mentions: `4847`
- unresolved mentions: `6448`
- resolved rate: `0.4291`
- exactish rate: `0.2653`
- weak provenance rate: `0.0177`
- component match average: `0.9018`
- supply match average: `0.9469`
- auditor finding count: `100`

## Assembly improvement

The assembly-focused resolver fix materially improved the truth-vs-discovered result for assemblies:

- assembly truth in docs: `48`
- assembly discovered: `47`
- assembly matched: `47`
- assembly missed truth: `1`
- assembly false positive: `0`
- assembly match rate: `0.979`

This is a large improvement over the earlier run where assembly discovery was effectively zero.

## LLM usage

- total LLM calls: `3326`
- generator calls: `147`
- resolver calls: `3079`
- auditor calls: `100`

Models used:

- `qwen2.5:14b`
- `mistral-small3.1:latest`
- `deepseek-r1:14b-qwen-distill-q4_K_M`

## Readout

This run is strong as a showcase artifact because it combines scale, saved observability, and honest quality reporting. It slightly missed the desired 5:00 AM finish target, but it stayed close and materially improved the assembly category, which was the most visible structural weakness in the earlier report.
