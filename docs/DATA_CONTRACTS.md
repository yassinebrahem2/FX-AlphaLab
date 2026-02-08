\# Data Contracts (Week 4)



\## Price (OHLC)

Source: MT5  

Granularity: H1  

Schema: src/pipelines/price/schema.py



\## Macro

Source: FRED  

Indicators: CPI, GDP, Unemployment  

Schema: src/pipelines/macro/schema.py



\## Central Bank Documents

Source: Official RSS feeds  

Content: raw documents only  

Schema: src/pipelines/central\_bank/schema.py



All datasets:

\- UTC timestamps

\- Raw + clean separation

\- Schema validation enforced



