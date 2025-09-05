from __future__ import annotations

import hashlib
import json
from datetime import datetime
from sqlalchemy import text
from typing import Any

from etl.config import settings
from etl.db import get_sqlalchemy_engine
from etl.http import fetch_endpoint_data


def _hash_payload(obj) -> str:
    """
    Produce a *stable* hash for a JSON-like Python object (dict/list of dicts).

    Purpose
    -------
    - Detect whether a "page" of API results has changed since last load.
    - Stability matters more than cryptographic strength here: given the same
      logical JSON content (regardless of key order or whitespace), this returns
      the same hash so our UPSERT can skip unnecessary rewrites.

    Notes
    -----
    - This is NOT for security. SHA-1 is chosen for speed/shortness and is fine
      for change detection. If you prefer, swap to SHA-256 with minimal impact.
    - Inputs must be JSON-serializable (dict/list/str/int/float/bool/None).
      Our ETL passes lists of dicts from the API, which satisfies this.
    """
    # Normalize the object into a canonical JSON string:
    #   - sort_keys=True removes key-order differences across runs/platforms
    #   - separators=(",", ":") strips spaces to avoid whitespace diffs
    #   - ensure_ascii default is True; UTF-8 encode below guarantees bytes
    normalized = json.dumps(
        obj,
        sort_keys=True,            # order-insensitive: {"a":1,"b":2} == {"b":2,"a":1}
        separators=(",", ":"),     # compact: no spaces -> stable byte stream
    ).encode("utf-8")              # hashlib operates on bytes

    # Compute a fast, deterministic digest of the normalized bytes.
    # For stronger (but larger/slower) digests, use hashlib.sha256(normalized).
    return hashlib.sha1(normalized).hexdigest()


def ensure_raw_table(endpoint: str) -> None:
    """
    Ensure ipeds_raw.{endpoint}_raw exists and is duplicate-safe.

    Structure:
      - one row per (year, page_number)
      - unique constraint on (year, page_number) to prevent dup pages
      - source_hash for change detection (only update when payload changes)
    """
    table = f"ipeds_raw.{endpoint}_raw"

    # 1) Create the table (idempotent)
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {table} (
        year         INTEGER  NOT NULL,
        page_number  INTEGER  NOT NULL,
        source_url   TEXT     NOT NULL,
        source_hash  TEXT     NOT NULL,
        ingested_at  TIMESTAMP NOT NULL DEFAULT NOW(),
        payload      JSONB    NOT NULL
    );
    """

    # 2) Create helpful indexes (idempotent)
    idx_unique = f"""
    CREATE UNIQUE INDEX IF NOT EXISTS {endpoint}_raw_unq
    ON {table} (year, page_number);
    """
    idx_hash = f"""
    CREATE INDEX IF NOT EXISTS {endpoint}_raw_hash_idx
    ON {table} (source_hash);
    """
    idx_year = f"""
    CREATE INDEX IF NOT EXISTS {endpoint}_raw_year_idx
    ON {table} (year);
    """

    # 3) Execute in one transaction
    engine = get_sqlalchemy_engine(echo=settings.LOG_SQL)
    with engine.begin() as conn:
        conn.execute(text(create_sql))
        conn.execute(text(idx_unique))
        conn.execute(text(idx_hash))
        conn.execute(text(idx_year))


def insert_raw_payloads(endpoint: str, year: int, endpoint_path: str) -> None:
    """
    Pulls API results for a given endpoint/year and inserts them into ipeds_raw.

    Each page of results is saved as one row in the raw table.
    """
    ensure_raw_table(endpoint)

    engine = get_sqlalchemy_engine(echo=settings.LOG_SQL)

    # Get all results for the year (already paginated/followed in http.py)
    all_records = fetch_endpoint_data(endpoint_path, year)

    # Store 1 row per "page" in raw. We simulate pages for storage; adjust size as you like.
    page_size = 500
    chunks = [all_records[i:i + page_size] for i in range(0, len(all_records), page_size)]

    with engine.begin() as conn:
        rows_to_write = []
        for page_number, chunk in enumerate(chunks, start=1):
            rows_to_write.append({
                "year": year,
                "page_number": page_number,
                # Synthetic but traceable; OK that real API used 'next' instead
                "source_url": f"{settings.URBAN_BASE_URL.rstrip('/')}{endpoint_path}?year={year}&page={page_number}",
                "source_hash": _hash_payload(chunk),
                "ingested_at": datetime.utcnow(),
                "payload": chunk,  # pass Python obj; psycopg2 serializes to JSONB
            })

        upsert_sql = text(f"""
            INSERT INTO ipeds_raw.{endpoint}_raw
                (year, page_number, source_url, source_hash, ingested_at, payload)
            VALUES
                (:year, :page_number, :source_url, :source_hash, :ingested_at, :payload)
            ON CONFLICT (year, page_number) DO UPDATE
            SET payload     = EXCLUDED.payload,
                source_url  = EXCLUDED.source_url,
                source_hash = EXCLUDED.source_hash,
                ingested_at = NOW()
            WHERE ipeds_raw.{endpoint}_raw.source_hash IS DISTINCT FROM EXCLUDED.source_hash;
        """)

        conn.execute(upsert_sql, rows_to_write)

    print(f"[OK] Inserted {len(chunks)} page(s) into ipeds_raw.{endpoint}_raw for year {year}")
