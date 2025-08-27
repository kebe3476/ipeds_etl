# IPEDS ETL Architecture

This document covers the database layout (schemas, roles, views), maintenance practices (indexes, partitions, refresh), lineage tables, and the Python ETL package internals.

---

## 1) Database Layout (`ipeds_db/`)
```
ipeds_db/
├─ schemas/
│ ├─ ipeds_raw/ # raw API payloads (JSON + metadata, append-only)
│ ├─ ipeds_core/ # cleaned & typed tables (analysis-ready, stable keys)
│ ├─ ipeds_dim/ # small lookup/dimension tables for readable labels
│ ├─ ipeds_vw/ # BI-friendly views (joins, denormalized shapes)
│ └─ public/ # default Postgres schema (not used for IPEDS)
│
├─ roles/
│ ├─ ipeds_loader # ETL role: write to raw/core
│ ├─ ipeds_reader # analyst role: read-only core + views
│ └─ bi_user # BI role: read-only views only
│
├─ maintenance/
│ ├─ housekeeping # vacuum/analyze, reindex guidance
│ ├─ partitions # optional year-based partitioning for large endpoints
│ └─ refresh_jobs # schedules for refreshing materialized views
│
└─ lineage/
├─ load_log # per-run counts, timestamps, endpoint/year
└─ source_trace # source_url, source_hash, ingested_at, load_ts
```

### Schemas

**`ipeds_raw/` — Inbox for data**  
- Stores API responses exactly as received (JSON + metadata like `ingested_at`, `source_url`, `source_hash`, `page_cursor`).  
- One table per endpoint (e.g., `directory_raw`, `admissions_raw`).  
- Why: auditability, reproducibility, easy reprocessing.

**`ipeds_core/` — Clean, typed, deduped**  
- Structured tables ready for analysis and joins.  
- Common keys:  
  - Institution-year endpoints → `(unitid, year)`  
  - Program endpoints (Completions) → `(unitid, year, cipcode, award_level)`  
- IPEDS special values (-1/-2/-3) are cast to `NULL` (or mapped via `ipeds_dim`).

**`ipeds_dim/` — Lookups**  
- Tiny tables translating codes → human labels (e.g., sector, region, locale, CIP).  
- Keeps dashboards readable and consistent.

**`ipeds_vw/` — BI-friendly views**  
- Pre-joined shapes for analysts and BI. Examples:  
  - `institutions_latest` — latest Directory per institution.  
  - `admissions_enriched` — Admissions + Directory (name, state, sector).  
  - `completions_by_cip` — Program detail with CIP titles.  
  - `yearly_kpis` — accept rate, yield, completions, etc.  
- Heavier views may be **materialized** with scheduled refresh after ETL.

---

## 2) Roles (least privilege)

- `ipeds_loader` → writes to raw/core; used only by ETL.  
- `ipeds_reader` → reads core & views; for power users/analysts.  
- `bi_user` → reads views only; safest for BI connectors.

> These are Postgres roles (permissions), **not** separate schemas or databases.

---

## 3) Maintenance & Performance

**Housekeeping**  
- Routine `VACUUM (ANALYZE)`; occasional `REINDEX` on high-churn tables.

**Indexes**  
- Always index **primary keys** and common filters. Typical helpful indexes:  
  - `(unitid, year)`  
  - `state`, `sector`, `institution_level`, `year`  
  - For Completions: `(unitid, year, cipcode, award_level)` and filtered queries on `cipcode`.

**Partitions (optional)**  
- For very large endpoints (e.g., Completions), use **year-based partitioning** on `year`.  
- Benefits: faster loads & queries, easier pruning.

**Materialized Views & Refresh**  
- Materialize heavy views hit by BI.  
- Schedule `REFRESH MATERIALIZED VIEW CONCURRENTLY` after ETL completes.

---

## 4) Lineage & Quality Tracking

- **`load_log`** (per ETL run)  
  - Fields: `run_id`, `endpoint`, `year_range`, `rows_inserted`, `rows_updated`, `started_at`, `finished_at`, `status`, `notes`.

- **`source_trace`** (row-level provenance)  
  - Fields: `core_table`, `core_pk` (composite), `source_url`, `source_hash`, `ingested_at`, `load_ts`.

Purpose: trust, auditability, and troubleshooting (what changed, when, why, and from where).

---

## 5) ETL Package Layout (`ipeds-etl/`)
```
ipeds-etl/
├─ notebooks/
│ ├─ 00_env_check.ipynb # sanity: connect to Postgres, check schemas
│ ├─ 10_load_endpoint.ipynb # run any endpoint & year range
│ └─ 90_ad_hoc_exploration.ipynb # scratch queries once data lands
│
├─ etl/
│ ├─ init.py
│ ├─ config.py # BASE_URL, DATABASE_URL, timeouts, rate-limit
│ ├─ http.py # requests session, retries, pagination
│ ├─ db.py # SQLAlchemy engine, run_sql, run_many
│ ├─ raw_io.py # create raw tables, insert JSON payloads
│ ├─ core_io.py # create core tables, UPSERT
│ ├─ casting.py # handle -1/-2/-3 → NULL
│ ├─ registry.py # endpoint catalog (path, schema, PK, mapper)
│ ├─ mappers/
│ │ ├─ directory.py # JSON → typed dict for Directory
│ │ └─ admissions.py # plus others (finance, completions, etc.)
│ └─ runner.py # load_endpoint_years(endpoint, start, end)
│
├─ sql/
│ ├─ 00_schemas.sql # CREATE SCHEMA ipeds_raw/core/vw
│ ├─ 20_core_indexes.sql # optional extra indexes
│ └─ 90_views_examples.sql # example BI views & materializations
│
├─ config/
│ ├─ .env.example # DATABASE_URL=...
│ └─ endpoints.yaml # (optional) batch run plan
│
├─ tests/
│ ├─ test_casting.py # -1/-2/-3 handling
│ ├─ test_registry.py # schema/PK sanity
│ └─ test_end_to_end_small.py # smoke test on tiny slice
```

**Module notes**
- `config.py` — API base (Urban Institute IPEDS), DB URL, rate limits.  
- `http.py` — retries, backoff, pagination.  
- `raw_io.py` — upserts raw JSON + metadata to `ipeds_raw`.  
- `core_io.py` — builds typed tables, performs idempotent UPSERTs.  
- `casting.py` — centralizes value cleaning; avoids `-1/-2/-3` leaking to BI.  
- `registry.py` — single source of truth for endpoints (schema + PK + mapper).  
- `mappers/` — endpoint-specific field mapping.  
- `runner.py` — convenience orchestration per endpoint/year range.

---

## 6) Data Flow (mental model)

Urban API → ipeds_raw (JSON + metadata)
→ mapper + casting
→ ipeds_core (typed rows with PKs)
→ ipeds_vw (views/materializations for BI)
→ Power BI / SQL analysts

---

## 7) Roles in Practice (example policy)

- Grant `ipeds_loader` `INSERT/UPDATE` on `ipeds_raw/*` and `ipeds_core/*`; `SELECT` on `ipeds_dim/*`.  
- Grant `ipeds_reader` `SELECT` on `ipeds_core/*`, `ipeds_vw/*`, and `ipeds_dim/*`.  
- Grant `bi_user` `SELECT` on `ipeds_vw/*` (and needed dims).

---

## 8) Testing

- **Unit**: casting edge cases; registry shape/PKs.  
- **Integration**: a tiny slice from one endpoint (e.g., 1–2 years of Directory/Admissions).  
- **Contract**: alert if API fields drift from registry definitions.

---

## 9) Notes on Special IPEDS Values

- `-1` Missing/Not reported → `NULL`  
- `-2` Not applicable → `NULL`  
- `-3` Suppressed → `NULL` (plus optional flag column if you need to track suppression)

Keep handling centralized in `casting.py` for consistency.

---

## 10) Citation / Acknowledgements

This project uses the **Urban Institute’s Education Data API**. See their acknowledgements:  
https://educationdata.urban.org/documentation/#acknowledgements

_Disclaimer: Not affiliated with the Urban Institute. Transformations and any errors are the author’s own._