"""
etl/raw_io.py
-------------
Handles raw data ingestion into ipeds_raw.{endpoint}_raw tables.

This layer stores the exact JSON payloads returned from the Urban Institute API,
plus metadata about the source (URL, page, hash, etc.) for auditing and reproducibility.

Each row represents one page of results, not individual records.

Table schema (per endpoint) looks like:

    ipeds_raw.directory_raw
    ------------------------
    - year         INTEGER
    - page_number  INTEGER
    - source_url   TEXT
    - source_hash  TEXT        -- MD5 or SHA1 hash of the payload
    - ingested_at  TIMESTAMP   -- when this record was inserted
    - payload      JSONB       -- the exact API response

This is append-only by default.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime
from sqlalchemy import text
from sqlalchemy.exc import ProgrammingError
from typing import Any

from etl.config import settings
from etl.db import get_sqlalchemy_engine
from etl.http import fetch_endpoint_data

# -----------------------------------------------------------------------------
# Create raw table for an endpoint (if not already created)
# -----------------------------------------------------------------------------
def ensure_raw_table(endpoint: str) -> None:
    """
    Creates a raw table if it doesn't exist, with the standard structure.

    Table will be named: ipeds_raw.{endpoint}_raw

    Parameters
    ----------
    endpoint : str
        The IPEDS dataset (e.g., "directory", "admissions").

    Returns
    -------
    None
    """
    sql = f"""
    CREATE TABLE IF NOT EXISTS ipeds_raw.{endpoint}_raw (
        year INTEGER NOT NULL,
        page_number INTEGER NOT NULL,
        source_url TEXT NOT NULL,
        source_hash TEXT NOT NULL,
        ingested_at TIMESTAMP NOT NULL DEFAULT NOW(),
        payload JSONB NOT NULL
    );
    """

    engine = get_sqlalchemy_engine(echo=settings.LOG_SQL)
    with engine.begin() as conn:
        conn.execute(text(sql))


# -----------------------------------------------------------------------------
# Insert raw page payloads + metadata into the raw table
# -----------------------------------------------------------------------------
def insert_raw_payloads(endpoint: str, year: int, endpoint_path: str) -> None:
    """
    Pulls API results for a given endpoint/year and inserts them into ipeds_raw.

    Each page of results is saved as one row in the raw table.

    Parameters
    ----------
    endpoint : str
        Short name like "directory".
    year : int
        Target year to fetch.
    endpoint_path : str
        API path fragment like "/directory/".

    Returns
    -------
    None
    """
    ensure_raw_table(endpoint)

    engine = get_sqlalchemy_engine(echo=settings.LOG_SQL)

    # Get all paginated results (flattened list of records)
    all_records = fetch_endpoint_data(endpoint_path, year)

    # We simulate pagination here to store 1 row per "page" in the raw table
    # even though fetch_endpoint_data returned everything flattened.
    # If you want to store 1 row per *record*, modify this logic.
    page_size = 500  # arbitrary, for splitting into chunks
    chunks = [all_records[i:i + page_size] for i in range(0, len(all_records), page_size)]

    with engine.begin() as conn:
        for page_number, chunk in enumerate(chunks, start=1):
            payload_json = json.dumps(chunk, sort_keys=True)

            source_url = f"{settings.URBAN_BASE_URL.rstrip('/')}{endpoint_path}?year={year}&page={page_number}"
            source_hash = hashlib.sha1(payload_json.encode("utf-8")).hexdigest()

            insert_sql = text(f"""
                INSERT INTO ipeds_raw.{endpoint}_raw (
                    year,
                    page_number,
                    source_url,
                    source_hash,
                    ingested_at,
                    payload
                )
                VALUES (
                    :year,
                    :page_number,
                    :source_url,
                    :source_hash,
                    :ingested_at,
                    :payload
                )
            """)

            conn.execute(
                insert_sql,
                {
                    "year": year,
                    "page_number": page_number,
                    "source_url": source_url,
                    "source_hash": source_hash,
                    "ingested_at": datetime.utcnow(),
                    "payload": payload_json,
                }
            )

    print(f"[OK] Inserted {len(chunks)} page(s) into ipeds_raw.{endpoint}_raw for year {year}")