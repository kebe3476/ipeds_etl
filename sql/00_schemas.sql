/*
------------------------------------------------------------------------------
  IPEDS ETL - Schema Setup
  ------------------------
  Purpose:
    - Create the base schemas used in the ETL pipeline
    - Each schema has a clear responsibility:
        * ipeds_raw   → API payloads as-is (JSON, append-only)
        * ipeds_core  → cleaned/typed tables (analysis-ready)
        * ipeds_dim   → small lookup tables (labels for codes)
        * ipeds_vw    → BI-friendly views (joins, denormalized shapes)
    - Public schema is not used for IPEDS data
------------------------------------------------------------------------------
*/

-- ============================================================================
-- 1. Create schemas if they don't exist
-- ============================================================================
CREATE SCHEMA IF NOT EXISTS ipeds_raw;
CREATE SCHEMA IF NOT EXISTS ipeds_core;
CREATE SCHEMA IF NOT EXISTS ipeds_dim;
CREATE SCHEMA IF NOT EXISTS ipeds_vw;

-- (Optional: explicitly note public is off-limits)
COMMENT ON SCHEMA public IS 'Default Postgres schema (not used for IPEDS data).';

-- ============================================================================
-- 2. Add comments for clarity
-- ============================================================================
COMMENT ON SCHEMA ipeds_raw  IS 'Raw API payloads (JSON + metadata, append-only)';
COMMENT ON SCHEMA ipeds_core IS 'Cleaned & typed tables (analysis-ready, stable keys)';
COMMENT ON SCHEMA ipeds_dim  IS 'Lookup/dimension tables for readable labels';
COMMENT ON SCHEMA ipeds_vw   IS 'BI-friendly views (joins, denormalized shapes)';

-- ============================================================================
-- End of Script
-- Run this first (before roles, indexes, or views)
-- ============================================================================