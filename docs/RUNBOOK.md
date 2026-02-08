\# Runbook — FX-AlphaLab (Week 4)



\## Full refresh

1\. Activate environment

2\. Run price pipeline

3\. Run macro pipeline

4\. Run central bank pipeline



\## Outputs

\- CSVs in data/raw and data/clean

\- SQLite DB: data/fx\_data.db

\- Manifests in data/manifests



\## Troubleshooting

\- Missing API key → check environment variables

\- Empty datasets → check source availability

\- Schema errors → inspect validation messages



