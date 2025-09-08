"""
etl/core_io.py
--------------
Loads clean, typed rows into ipeds_core.{endpoint} from ipeds_raw.{endpoint}_raw.

Design:
- Table DDL comes from etl/registry.py (authoritative column list + PK).
- Row shaping comes from the endpoint's mapper (e.g., etl.mappers.directory.map_directory_row).
- Idempotent UPSERT on the declared primary key.
"""

from __future__ import annotations
from typing import Iterable, List, Dict, Any, Optional

import json
from sqlalchemy import text

from etl.db import get_sqlalchemy_engine
from etl.registry import get_endpoint_config
# mappers are referenced via the registry; we don't import a specific mapper here


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def _ensure_core_table(endpoint: str) -> None:
    """
    Create ipeds_core.{endpoint} if missing, using registry schema & PK.

    The registry provides:
      - schema: {column_name: sql_type, ...}
      - primary_key: ["col1", "col2"]
    """
    cfg = get_endpoint_config(endpoint)
    cols = cfg["schema"]
    pk = cfg["primary_key"]
    table = f"ipeds_core.{endpoint}"

    # 1) Build column DDL like: "unitid INTEGER NOT NULL, year INTEGER NOT NULL, inst_name TEXT, ..."
    col_defs = ",\n    ".join([f"{name} {sqltype}" for name, sqltype in cols.items()])
    pk_clause = f",\n    PRIMARY KEY ({', '.join(pk)})" if pk else ""

    ddl = f"""
    CREATE SCHEMA IF NOT EXISTS ipeds_core;

    CREATE TABLE IF NOT EXISTS {table} (
        {col_defs}
        {pk_clause}
    );
    """

    engine = get_sqlalchemy_engine()
    with engine.begin() as cx:
        cx.execute(text(ddl))


def _iter_raw_records(endpoint: str, years: Optional[Iterable[int]] = None) -> Iterable[Dict[str, Any]]:
    """
    Yield individual JSON records from ipeds_raw.{endpoint}_raw for the given years.

    Notes:
    - Each raw row is one "page" (payload JSON array). We expand it into per-record dicts.
    - Handles payload stored as JSONB (already a Python list) or TEXT (string -> json.loads).
    """
    table = f"ipeds_raw.{endpoint}_raw"
    engine = get_sqlalchemy_engine()

    if years:
        placeholders = ", ".join([str(int(y)) for y in years])
        sql = f"SELECT year, page_number, payload FROM {table} WHERE year IN ({placeholders}) ORDER BY year, page_number"
    else:
        sql = f"SELECT year, page_number, payload FROM {table} ORDER BY year, page_number"

    with engine.connect() as cx:
        for yr, page, payload in cx.execute(text(sql)):
            # payload could be a Python list (JSONB) or a JSON string
            if isinstance(payload, str):
                page_items = json.loads(payload)
            else:
                page_items = payload

            if not isinstance(page_items, list):
                # Be defensive: the API should have returned a list of objects.
                continue

            for obj in page_items:
                # Attach the 'year' from the raw row as a guard (some payloads omit it).
                if "year" not in obj:
                    obj["year"] = yr
                yield obj


def _build_upsert_sql(endpoint: str) -> str:
    """
    Build parameterized INSERT ... ON CONFLICT DO UPDATE statement from registry.
    """
    cfg = get_endpoint_config(endpoint)
    cols = list(cfg["schema"].keys())            # insertion order follows registry schema
    pk = cfg["primary_key"]
    non_pk_cols = [c for c in cols if c not in pk]

    col_list = ", ".join(cols)
    param_list = ", ".join([f":{c}" for c in cols])
    conflict_cols = ", ".join(pk)
    set_list = ", ".join([f"{c} = EXCLUDED.{c}" for c in non_pk_cols])

    sql = f"""
    INSERT INTO ipeds_core.{endpoint} ({col_list})
    VALUES ({param_list})
    ON CONFLICT ({conflict_cols}) DO UPDATE
    SET {set_list};
    """
    return sql


# -----------------------------------------------------------------------------
# Public API
# -----------------------------------------------------------------------------
def load_core_from_raw(endpoint: str, years: Optional[Iterable[int]] = None, batch_size: int = 1000) -> int:
    """
    Read ipeds_raw.{endpoint}_raw, map/clean records, and UPSERT into ipeds_core.{endpoint}.

    Parameters
    ----------
    endpoint : str
        e.g., "directory"
    years : Optional[Iterable[int]]
        If provided, restrict to these years; otherwise load all years present in raw.
    batch_size : int
        How many mapped rows to send per execute() call.

    Returns
    -------
    int : number of records processed (mapped rows, not pages)
    """
    cfg = get_endpoint_config(endpoint)
    mapper = cfg["mapper"]

    _ensure_core_table(endpoint)
    upsert_sql = text(_build_upsert_sql(endpoint))

    engine = get_sqlalchemy_engine()
    rows_buffer: List[Dict[str, Any]] = []
    processed = 0

    def flush():
        nonlocal rows_buffer, processed
        if not rows_buffer:
            return
        with engine.begin() as cx:
            cx.execute(upsert_sql, rows_buffer)
        processed += len(rows_buffer)
        rows_buffer = []

    # Iterate raw records and map them into core-shaped dicts
    for raw in _iter_raw_records(endpoint, years=years):
        mapped = mapper(raw)  # returns dict with keys matching registry schema
        rows_buffer.append(mapped)
        if len(rows_buffer) >= batch_size:
            flush()

    flush()  # final partial batch
    print(f"[OK] Upserted {processed} record(s) into ipeds_core.{endpoint}")
    return processed
