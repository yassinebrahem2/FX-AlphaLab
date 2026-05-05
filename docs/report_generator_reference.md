# Report Generator Reference

This note summarizes the original Python report generator so the backend owner can reconnect real report data later without needing the full standalone generator inside the frontend.

The current FX-AlphaLab frontend report lives in:

```text
frontend/app/reports/[symbol]/page.tsx
frontend/components/report/report-document.tsx
frontend/lib/report-html.ts
frontend/lib/report-paths.ts
```

The frontend should stay UI-only. The Python generator should be treated as backend/reference logic, not frontend runtime code.

## Original Python Flow

```text
decision package JSON
-> validate_decision_package.py
-> input_normalizer.py
-> report_plan.py
-> report_renderer.py
-> HTML report + static assets + manifests
```

The main CLI entry was:

```text
main.py --input sample_inputs/eurusd_sample_report.json --output outputs/eurusd_report.html --schema decision_package_schema.json --mode core
```

## Core Python Files

`main.py`

Entry point. Reads a decision package JSON, validates it against `decision_package_schema.json`, normalizes it, builds the report plan, renders the report, and prints generation/verification status.

`validate_decision_package.py`

JSON Schema validator. Uses `jsonschema.Draft7Validator` and returns readable validation errors such as `$.final_alpha.confidence: ...`.

`input_normalizer.py`

Compatibility layer for incoming agent/alpha payloads. It accepts different field names and normalizes them into one report shape.

Important normalized fields:

```text
report_id
pair
timestamp_utc
generated_by
report_version
final_alpha
agents
agent_weights
top_drivers
what_would_flip_decision
regime_summary
disagreement_matrix
market_timeseries
sentiment_timeseries
source_mix
event_frequency
geopolitical_graph
market_narrative
report_universe
scenario_analysis
evidence_manifest
multimodal_evidence
audit_manifest
```

`report_plan.py`

Main planning layer. Converts normalized data into the final report view model used by the template.

Important responsibilities:

```text
Build decision summary
Format confidence, risk, timestamps, percentages, and scores
Build agent sections and evidence chips
Build cross-agent alignment and contribution rows
Build market snapshot, risk monitor, and invalidation summary
Attach chart outputs
Attach visual manifest
Run input verification
```

`report_renderer.py`

Jinja renderer. Loads `templates/report_template.html`, copies static assets, renders the report, and writes generation manifests.

`verifier.py`

Report input sanity checker. Ensures required fields exist, validates alpha/agent stances and confidence ranges, checks evidence IDs, and warns about missing chart or audit data.

## Supporting Python Modules

`charts/chart_builder.py`

Builds Plotly chart HTML for the report sections.

`retrieval/*.py`

Reference retrievers for semantic, graph, mock, and structured evidence lookup. These are backend/reference concepts, not frontend concerns.

`data_sources/*.py`

Reference adapters for GDELT, Google Trends, Reddit, and StockTwits-style source inputs.

`visuals/*.py`

Reference visual pipeline for graph/image/visual manifest generation. The frontend currently keeps generated/static assets only.

## Backend Integration Contract

The future backend should ideally return a report payload shaped like the normalized output from `input_normalizer.py`, or a frontend-ready version of the plan produced by `report_plan.py`.

Minimum useful payload for the frontend:

```text
symbol/pair
generated timestamp
final alpha direction
confidence
risk level
time horizon
action summary
top drivers
flip/invalidation conditions
agent outputs
agent weights
evidence manifest
market timeseries
sentiment timeseries
geopolitical graph/path data
audit manifest
```

## What Not To Move Into Frontend

Do not move the Python generator into `frontend/`.

Do not convert validation, retrieval, LLM/image generation, or data processing into TSX. Those belong in backend/service code later.

Only the presentation/report UI belongs in TSX.

## Current Frontend State

The current TSX report route preserves the original generated report design by storing the generated report body in `frontend/lib/report-html.ts` and rendering it through `ReportDocument`.

Static visual assets required by the report remain in:

```text
frontend/public/reports/assets
frontend/public/reports/static
```

The one remaining embedded HTML asset is:

```text
frontend/public/reports/assets/graphs/geopolitical_graph.html
```

That file is a graph artifact embedded inside the expert appendix. It can remain static for now, or later be replaced by a React graph component when backend graph data is ready.
