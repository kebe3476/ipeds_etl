/*
------------------------------------------------------------------------------
  IPEDS ETL Database Roles & Grants
  ---------------------------------
  Purpose:
    - Create least-privilege roles for ETL, analysts, and BI tools.
    - Ensure schemas (raw/core/dim/views) have the correct permissions.
    - Future-proof privileges so new tables automatically inherit correct grants.
-------------------------------------------------------------------------------
*/

-- ============================================================================
-- 1. Create roles (login roles with passwords)
--    - ipeds_loader → ETL scripts, can write to raw/core
--    - ipeds_reader → Analysts, read-only core + views
--    - bi_user      → BI connectors (Power BI, Tableau), read-only views + dims
-- ============================================================================
-- TODO: Change passwords to something else.
CREATE ROLE ipeds_loader LOGIN PASSWORD 'password';
CREATE ROLE ipeds_reader LOGIN PASSWORD 'password';
CREATE ROLE bi_user      LOGIN PASSWORD 'password';

-- ============================================================================
-- 2. Lock down defaults
--    By default, Postgres gives everyone rights on the "public" schema.
--    We revoke this so only explicit grants are allowed.
-- ============================================================================
REVOKE ALL ON SCHEMA public FROM PUBLIC;

-- ============================================================================
-- 3. Grant schema usage (everyone can at least "see" the schemas)
--    Note: USAGE does NOT mean SELECT/INSERT — it just means you can reference
--    objects inside the schema once specific rights are granted.
-- ============================================================================
GRANT USAGE ON SCHEMA ipeds_raw, ipeds_core, ipeds_dim, ipeds_vw 
  TO ipeds_reader, bi_user, ipeds_loader;

-- ipeds_meta: lineage/audit logs
-- Loader must be able to create tables here (e.g., ingest_log)
GRANT USAGE, CREATE ON SCHEMA ipeds_meta TO ipeds_loader;
ALTER SCHEMA ipeds_meta OWNER TO ipeds_loader;

-- Reader + BI can see ipeds_meta but not create
GRANT USAGE ON SCHEMA ipeds_meta TO ipeds_reader, bi_user;

-- ============================================================================
-- 4. Table-level privileges
--    Assign specific abilities per role and schema
-- ============================================================================
-- Loader: full read/write to raw + core
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA ipeds_raw  TO ipeds_loader;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA ipeds_core TO ipeds_loader;

-- Reader: read-only on core + dim + views
GRANT SELECT ON ALL TABLES IN SCHEMA ipeds_core, ipeds_dim, ipeds_vw TO ipeds_reader;

-- BI user: read-only on views + dim (safe for dashboards)
GRANT SELECT ON ALL TABLES IN SCHEMA ipeds_core, ipeds_dim, ipeds_vw TO bi_user;

-- ============================================================================
-- 5. Default privileges (future-proofing)
--    - When new tables are created in these schemas, they will automatically 
--      inherit the right privileges without rerunning GRANT manually.
-- ============================================================================
-- Loader defaults
ALTER DEFAULT PRIVILEGES IN SCHEMA ipeds_raw  
  GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO ipeds_loader;

ALTER DEFAULT PRIVILEGES IN SCHEMA ipeds_core 
  GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO ipeds_loader;

-- Reader defaults
ALTER DEFAULT PRIVILEGES IN SCHEMA ipeds_core, ipeds_dim, ipeds_vw 
  GRANT SELECT ON TABLES TO ipeds_reader;

-- BI defaults
ALTER DEFAULT PRIVILEGES IN SCHEMA ipeds_core, ipeds_dim, ipeds_vw 
  GRANT SELECT ON TABLES TO bi_user;

/*
------------------------------------------------------------------------------
  End of Script
  - After running this:
      * ipeds_loader can insert/update/delete into raw & core schemas.
      * ipeds_reader can query (read-only) from core + dim + views.
      * bi_user      → can query (read-only) from core + dim + views.
  - This ensures "least privilege" and prevents accidental overwrites or misuse.
------------------------------------------------------------------------------
*/