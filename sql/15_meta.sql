/*
------------------------------------------------------------------------------
  IPEDS ETL Lineage & Metadata
  ----------------------------
  Purpose:
    - Create a dedicated schema (ipeds_meta) for tracking ETL runs
    - Tables record:
        * When each ETL job ran, how many rows it inserted/updated
        * Which API endpoint/year was loaded
        * Where the data came from (source URL + hash)
    - Grant correct privileges (loader can write; analysts/BI can read)
    - Future-proof so new meta tables inherit these same privileges
------------------------------------------------------------------------------
*/

-- ============================================================================
-- 1. Create schema for metadata
--    This keeps lineage/audit info separate from raw/core data.
-- ============================================================================
CREATE SCHEMA IF NOT EXISTS ipeds_meta;

-- ============================================================================
-- 2. ETL run log
--    - One row per ETL run (like a job history)
--    - Tracks endpoint, years, row counts, start/finish times
-- ============================================================================
CREATE TABLE IF NOT EXISTS ipeds_meta.load_log (
  load_id        BIGSERIAL PRIMARY KEY,   -- unique run ID
  endpoint       TEXT NOT NULL,           -- e.g. 'directory', 'admissions'
  year_start     INT,                     -- first year loaded
  year_end       INT,                     -- last year loaded
  rows_inserted  BIGINT DEFAULT 0,        -- how many new rows added
  rows_updated   BIGINT DEFAULT 0,        -- how many rows updated
  started_at     TIMESTAMPTZ DEFAULT now(), -- timestamp when job started
  finished_at    TIMESTAMPTZ                 -- timestamp when job finished
);

-- ============================================================================
-- 3. Source trace table
--    - One row per data batch
--    - Records where the data came from (API URL + hash) and when
-- ============================================================================
CREATE TABLE IF NOT EXISTS ipeds_meta.source_trace (
  trace_id     BIGSERIAL PRIMARY KEY,     -- unique trace ID
  endpoint     TEXT NOT NULL,             -- dataset name
  year         INT,                       -- year of the data
  source_url   TEXT NOT NULL,             -- API URL pulled
  source_hash  TEXT NOT NULL,             -- checksum/fingerprint of raw data
  ingested_at  TIMESTAMPTZ DEFAULT now()  -- when the data was pulled
);

-- ============================================================================
-- 4. Grants (who can use it)
--    - Loader: can INSERT + SELECT (write logs as ETL runs)
--    - Reader + BI: can only SELECT (read audit history)
-- ============================================================================
GRANT USAGE ON SCHEMA ipeds_meta TO ipeds_reader, bi_user, ipeds_loader;

GRANT SELECT, INSERT ON ALL TABLES IN SCHEMA ipeds_meta TO ipeds_loader;
GRANT SELECT ON ALL TABLES IN SCHEMA ipeds_meta TO ipeds_reader, bi_user;

-- ============================================================================
-- 5. Default privileges (future-proofing)
--    Any new tables created in ipeds_meta automatically inherit same rules
-- ============================================================================
ALTER DEFAULT PRIVILEGES IN SCHEMA ipeds_meta 
  GRANT SELECT, INSERT ON TABLES TO ipeds_loader;

ALTER DEFAULT PRIVILEGES IN SCHEMA ipeds_meta 
  GRANT SELECT ON TABLES TO ipeds_reader, bi_user;

/*
------------------------------------------------------------------------------
  End of Script
  - ipeds_loader → writes lineage (insert rows into load_log/source_trace)
  - ipeds_reader → can query lineage (read-only)
  - bi_user      → can query lineage (read-only)
  This gives you a permanent “black box recorder” of ETL activity.
------------------------------------------------------------------------------
*/