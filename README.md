FX-AlphaLab — Data \& Infrastructure Foundation (Week 4)



This repository contains the data ingestion and infrastructure layer

for the FX-AlphaLab project.



This phase is intentionally infrastructure-first.



⚠️ No trading logic.

⚠️ No forecasting.

⚠️ No intelligence (yet).



Everything here exists to guarantee data correctness, reproducibility, and auditability.



🎯 Scope (Week 4)

✅ Implemented



FX price data ingestion (MetaTrader 5)



Macroeconomic data ingestion (FRED)



Central bank policy document ingestion (RSS)



Strict raw → clean data separation



Schema-first validation



Deterministic identifiers



SQLite database persistence



Idempotent, reproducible pipelines



Manifest-based audit trail



❌ Explicitly Out of Scope



Trading strategies



Signal generation



Forecasting or prediction



NLP / sentiment analysis



Agent intelligence



Live trading



These are intentionally deferred to later phases.



🧱 Core Design Principles



All pipelines follow the same invariants:



Raw ≠ Clean



No invented or inferred data



No interpretation at ingestion time



UTC timestamps everywhere



Deterministic primary keys



Idempotent, append-only pipelines



Schema-first validation



Safe re-runs



Full auditability via manifests



If a feature violates one of these rules, it does not belong in this phase.



📁 Project Structure

MAJOR\_CURRENCIES\_PROJECT\_4DS2/

│

├── data/

│   ├── raw/                  # Immutable source data

│   │   ├── price/

│   │   ├── macro/

│   │   └── central\_bank/

│   │

│   ├── clean/                # Validated, normalized datasets

│   │

│   └── manifests/            # Pipeline run metadata (ignored by git)

│

├── docs/                     # Project documentation

│

├── notebooks/

│   └── technical\_validation.ipynb

│

├── src/

│   ├── agents/               # Reserved for future intelligence layers

│   │   ├── README.md

│   │   └── technical\_agent.py

│   │

│   ├── common/               # Shared utilities (dates, IDs, helpers)

│   │

│   ├── ingestion/            # Source connectors (MT5, FRED, RSS)

│   │

│   └── pipelines/

│       ├── price/

│       ├── macro/

│       └── central\_bank/

│

├── requirements.txt

└── README.md



🔄 Data Pipelines Overview

1️⃣ FX Price Pipeline (MT5)



Source: MetaTrader 5 (demo)



Instruments: EURUSD, GBPUSD, USDJPY, USDCHF



Timeframe: H1 (H4 / D1 extendable)



Output:



Raw CSV



Clean CSV



SQLite (price\_ohlc)



Run manifest



Status: Complete \& verified



2️⃣ Macroeconomic Pipeline (FRED)



Source: Federal Reserve Economic Data



Examples: CPI, GDP, Unemployment



Guarantees:



No forward-looking leakage



Historical truth preserved



Deterministic event\_id



Output:



Raw CSV



Clean CSV



SQLite (macro\_data)



Run manifest



Status: Complete \& verified



3️⃣ Central Bank Document Pipeline



Institutions:



FED



ECB



BOE



BOJ



Ingests metadata only



No parsing, no NLP, no sentiment



Deterministic document\_id



URL ≠ identity



Output:



Raw CSV



Clean CSV



SQLite (central\_bank\_documents)



Run manifest



Status: Complete \& verified



🗄️ Database



Engine: SQLite



Tables:



price\_ohlc



macro\_data



central\_bank\_documents



Design guarantees:



Deterministic primary keys



Append-only inserts



Safe re-runs



No destructive operations



🧪 Verification \& Auditability



Every pipeline run produces:



Validated datasets



Database inserts



A manifest describing:



inputs



row counts



timestamps



schema versions



This enables full time-T reconstruction of:



what data existed



when it was known



how it was ingested



🏁 Current Status



Week 4 — COMPLETE



This repository is a stable foundation for:



macro-aware research



temporal alignment



derived features



future intelligence layers

