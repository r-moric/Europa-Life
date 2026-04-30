# Evening 3-Hour Quality Run

## Execution window

- profile: `evening_3hour_quality`
- start: `2026-04-06 16:51:47 -04:00`
- finish: `2026-04-06 19:24:12 -04:00`
- elapsed: about `2 hours 32 minutes 25 seconds`

## Workload

- packages: `18`
- documents created: `108`
- components seeded: `260`
- assemblies seeded: `30`
- supplies seeded: `72`

## Results

- raw mentions: `3139`
- resolved mentions: `1513`
- unresolved mentions: `1626`
- resolved rate: `0.482`
- unresolved rate: `0.518`
- exactish rate: `0.2327`
- weak provenance rate: `0.0304`
- component match average: `0.844`
- supply match average: `0.9417`
- auditor finding count: `48`

## LLM usage

- total LLM calls: `1092`
- generator calls: `34`
- resolver calls: `1010`
- auditor calls: `48`

Models used:

- `qwen2.5:14b`
- `mistral-small3.1:latest`
- `deepseek-r1:14b-qwen-distill-q4_K_M`

## Readout

This run successfully stressed the higher-quality profile and produced a much richer evaluation corpus than the `dev_fast` baseline. The main tradeoff is visible in the metrics: the expanded resolver workload increased coverage of possible matches but also produced a large unresolved population, which is useful for an honest showcase because the report clearly exposes the gap.

## Calibration impact

Measured throughput from this run was used to recalibrate the overnight profile upward so the next run more closely targets an April 6 evening start and an April 7 early-morning finish.
