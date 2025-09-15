"""
etl/raw_io.py
-------------
Manages the RAW layer of the IPEDS ETL.

Responsibilities
----------------
- Ensure ipeds_raw.<endpoint>_raw tables exist (no CREATE SCHEMA here).
- Store one row per *page* of API results per (endpoint, year).
- Compute a stable hash of payloads to avoid unnecessary rewrites.
- UPSERT rows when data changes (idempotent loads).
- Provide utilities called by notebooks (e.g., 10_load_endpoint.ipynb).

Table shape
-----------
    ipeds_raw.<endpoint>_raw (
        year         INT           NOT NULL,
        page_number  INT           NOT NULL,
        source_url   TEXT          NOT NULL,
        source_hash  TEXT          NOT NULL,
        ingested_at  TIMESTAMPTZ   NOT NULL DEFAULT now(),
        record_count INT           NOT NULL,
        payload      JSONB         NOT NULL,   -- JSONB array of records
        PRIMARY KEY (year, page_number),
        CHECK (jsonb_typeof(payload) = 'array')
    );

Helpful indexes:
- source_hash (detect duplicates quickly)
- year (filtering)

Notes
-----
- "source_url" can be synthetic if the API client doesn’t expose the exact page URL,
  but should still be traceable.
- This module calls the API via etl.http.fetch_endpoint_data() and writes results here.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any, List, Dict

from sqlalchemy import text, bindparam
from sqlalchemy.dialects.postgresql import JSONB

from etl.config import settings
from etl.db import get_sqlalchemy_engine
from etl.http import fetch_endpoint_data


# -----------------------------------------------------------------------------
# Internal helper: stable JSON hash
# -----------------------------------------------------------------------------
def _stable_json_hash(obj: Any) -> str:
    """
    Compute a deterministic digest for JSON-like objects.

    - json.dumps(sort_keys=True, compact separators) → stable bytes
    - sha1 for speed/shortness (sha256 also fine)
    """
    normalized: bytes = json.dumps(
        obj,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    return hashlib.sha1(normalized).hexdigest()


# -----------------------------------------------------------------------------
# Table creation
# -----------------------------------------------------------------------------
def ensure_raw_table(endpoint: str) -> None:
    """
    Ensure ipeds_raw.{endpoint}_raw exists and is duplicate-safe.

    Assumptions
    -----------
    - The schema `ipeds_raw` ALREADY exists (created by sql/00_schemas.sql).
    - The ETL role may not have DB-level CREATE permissions, so we do not create schemas here.
    """
    schema = "ipeds_raw"
    table = f'{schema}."{endpoint}_raw"'  # quote to avoid keyword/char issues

    engine = get_sqlalchemy_engine(echo=getattr(settings, "LOG_SQL", False))

    # Pre-check: schema must exist or we raise a clear error (use SQLAlchemy binds, not driver binds)
    with engine.connect() as cxn:
        exists = cxn.execute(
            text("select exists (select 1 from information_schema.schemata where schema_name = :s)"),
            {"s": schema},
        ).scalar_one()
    if not exists:
        raise RuntimeError(
            "Schema ipeds_raw is missing. Run sql/00_schemas.sql as an admin to create base schemas."
        )

    # Create table + indexes (idempotent)
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {table} (
        year         INT           NOT NULL,
        page_number  INT           NOT NULL,
        source_url   TEXT          NOT NULL,
        source_hash  TEXT          NOT NULL,
        ingested_at  TIMESTAMPTZ   NOT NULL DEFAULT now(),
        record_count INT           NOT NULL,
        payload      JSONB         NOT NULL,
        PRIMARY KEY (year, page_number),
        CHECK (jsonb_typeof(payload) = 'array')
    );
    """
    idx_hash = f'CREATE INDEX IF NOT EXISTS "{endpoint}_raw_hash_idx" ON {table}(source_hash);'
    idx_year = f'CREATE INDEX IF NOT EXISTS "{endpoint}_raw_year_idx" ON {table}(year);'

    with engine.begin() as conn:
        conn.execute(text(create_sql))
        conn.execute(text(idx_hash))
        conn.execute(text(idx_year))


# -----------------------------------------------------------------------------
# Insert / UPSERT
# -----------------------------------------------------------------------------
def insert_raw_payloads(
    endpoint: str,
    year: int,
    endpoint_path: str,
    page_size: int | None = None,
) -> int:
    """
    Fetch API data for (endpoint, year), chunk into pages, and upsert into RAW.

    Parameters
    ----------
    endpoint : str
        Short key for the endpoint (used to build table name).
    year : int
        IPEDS survey year.
    endpoint_path : str
        API path template (e.g., "ipeds/directory/{year}/" or "ipeds/directory/").
    page_size : int | None
        Override chunk size. Defaults to settings.RAW_PAGE_SIZE (or 500).

    Returns
    -------
    int : number of page rows inserted/updated
    """
    # 1) Ensure table exists
    ensure_raw_table(endpoint)

    # 2) Fetch all records for the year via the HTTP client
    all_records = fetch_endpoint_data(endpoint_path, year) or []
    if not isinstance(all_records, list):
        raise TypeError(f"Expected list from fetch_endpoint_data, got {type(all_records)}")

    # 3) Chunk into pages
    ps = page_size or getattr(settings, "RAW_PAGE_SIZE", 500)
    chunks: List[List[Dict[str, Any]]] = [all_records[i:i + ps] for i in range(0, len(all_records), ps)]

    if not chunks:
        print(f"[OK] {endpoint} {year}: no records found, nothing inserted.")
        return 0

    # 4) Build rows for UPSERT
    rows_to_write: List[Dict[str, Any]] = []
    for page_number, chunk in enumerate(chunks, start=1):
        rows_to_write.append({
            "year": year,
            "page_number": page_number,
            "source_url": f"{settings.URBAN_BASE_URL.rstrip('/')}{endpoint_path if endpoint_path.startswith('/') else '/' + endpoint_path}?year={year}&page={page_number}&pagesize={ps}",
            "source_hash": _stable_json_hash(chunk),
            "ingested_at": datetime.now(timezone.utc),
            "record_count": len(chunk),
            "payload": chunk,
        })

    # 5) UPSERT rows (only update when the hash changes)
    upsert_sql = text(f"""
        INSERT INTO ipeds_raw."{endpoint}_raw"
            (year, page_number, source_url, source_hash, ingested_at, record_count, payload)
        VALUES
            (:year, :page_number, :source_url, :source_hash, :ingested_at, :record_count, :payload)
        ON CONFLICT (year, page_number) DO UPDATE
        SET payload      = EXCLUDED.payload,
            source_url   = EXCLUDED.source_url,
            source_hash  = EXCLUDED.source_hash,
            record_count = EXCLUDED.record_count,
            ingested_at  = now()
        WHERE ipeds_raw."{endpoint}_raw".source_hash IS DISTINCT FROM EXCLUDED.source_hash;
    """).bindparams(bindparam("payload", type_=JSONB))

    engine = get_sqlalchemy_engine(echo=getattr(settings, "LOG_SQL", False))
    with engine.begin() as conn:
        conn.execute(upsert_sql, rows_to_write)

    print(f"[OK] {endpoint} {year}: inserted/updated {len(chunks)} page(s) into ipeds_raw.{endpoint}_raw")
    return len(chunks)
