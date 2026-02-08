"""
Central Bank Document Schema (RAW)
STEP 5 – Policy Source Data
"""

CB_SCHEMA_RAW = {
    # ------------------------------------------------------------------
    # Identity & provenance
    # ------------------------------------------------------------------
    "document_id": str,                 # deterministic unique ID
    "institution": str,                 # FED, ECB, BOE, BOJ
    "document_type": str,               # statement, minutes, speech, report

    # ------------------------------------------------------------------
    # Publication metadata
    # ------------------------------------------------------------------
    "title": str,
    "publication_datetime_utc": "datetime64[ns]",
    "url": str,

    # ------------------------------------------------------------------
    # Raw content
    # ------------------------------------------------------------------
    "raw_text": str,                    # unprocessed document text

    # ------------------------------------------------------------------
    # Ingestion metadata
    # ------------------------------------------------------------------
    "source": str,                      # official website name
    "ingested_at_utc": "datetime64[ns]",
}
