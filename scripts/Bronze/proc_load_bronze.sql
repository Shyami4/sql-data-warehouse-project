/*
===============================================================================
Bronze Layer Data Load (SQL-Only with Timestamps)
===============================================================================
Script Purpose:
  - Loads Bronze tables from CSVs stored in Unity Catalog Volumes.
  - Records a START and END timestamp for each table load (no T-SQL variables).
  - Produces final summaries: row counts and per-table load duration (seconds).

Environment:
  - Catalog: datawarehouse
  - Schema : bronze
  - Source folders (Volumes):
      /Volumes/datawarehouse/bronze/landing/crm/
      /Volumes/datawarehouse/bronze/landing/erp/

Notes:
  - PATTERN uses **regex** (not glob). (?i) makes it case-insensitive.
  - COPY_OPTIONS('force'='true') reloads files even if already ingested.

WARNING:
  - TRUNCATE TABLE is used before each load (full reload behavior).

Author: Bronze load with SQL timestamps
===============================================================================
*/

-- Set the correct catalog and schema
USE CATALOG datawarehouse;
USE SCHEMA bronze;

-- ---------------------------------------------------------------------------
-- 0) Namespace & Audit table
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS load_audit_sql (
  batch_id    STRING,
  table_name  STRING,
  phase       STRING,        -- 'start' | 'end'
  ts          TIMESTAMP
) USING DELTA;

- -----------------------------------------------------------------------------
-- TEMP VIEW: __run
-- Purpose:
--   Databricks SQL doesn’t support T-SQL-style variables (DECLARE/SET), so we
--   create a one-row TEMP VIEW that holds a single UUID called batch_id.
--   Every START/END audit INSERT for each table selects this same batch_id from
--   __run, ensuring all rows for this execution are grouped under one id.
--
-- How it’s used:
--   INSERT INTO load_audit_sql
--   SELECT batch_id, '<table_name>', 'start'|'end', current_timestamp() FROM __run;
--   ...
--   In the summary query at the end, we join/filter by this batch_id to compute
--   per-table durations and show results for *this* run only.
--
-- Scope & lifecycle:
--   - TEMP VIEWs are session-scoped and ephemeral: they exist only in the
--     current SQL session. If you open a new session or run blocks separately,
--     recreate __run first.
--   - The audit table (load_audit_sql) is persistent Delta; __run is not.
--   - Concurrent runs in separate sessions are safe: each session generates its
--     own batch_id.
--
-- Why not a table?
--   A TEMP VIEW avoids leaving behind extra artifacts and guarantees that a
--   new UUID is generated for each execution without manual cleanup.
-- -----------------------------------------------------------------------------
-- Create a single-run id for this script execution
CREATE OR REPLACE TEMP VIEW __run AS
SELECT uuid() AS batch_id;

-- ---------------------------------------------------------------------------
-- 1) CRM loads
-- ---------------------------------------------------------------------------
-- ====== CRM: cust_info ======
INSERT INTO load_audit_sql
SELECT batch_id, 'crm_cust_info', 'start', current_timestamp() FROM __run;

TRUNCATE TABLE crm_cust_info;
COPY INTO crm_cust_info
FROM '/Volumes/datawarehouse/bronze/landing/crm/'
FILEFORMAT = CSV
PATTERN = '*cust_info*.csv'  -- Simplified pattern
FORMAT_OPTIONS (
    'header' = 'true',
    'multiline' = 'false'
)
COPY_OPTIONS (
    'force' = 'true'
);

INSERT INTO load_audit_sql
SELECT batch_id, 'crm_cust_info', 'end', current_timestamp() FROM __run;

-- ====== CRM: Product Info ======
INSERT INTO load_audit_sql
SELECT batch_id, 'crm_prd_info', 'start', current_timestamp() FROM __run;

TRUNCATE TABLE crm_prd_info;
COPY INTO crm_prd_info
FROM '/Volumes/datawarehouse/bronze/landing/crm/'
FILEFORMAT = CSV
PATTERN = 'prd_info*.csv'  -- Simplified pattern
FORMAT_OPTIONS (
    'header' = 'true',
    'multiline' = 'false'
)
COPY_OPTIONS (
    'force' = 'true'
);

INSERT INTO load_audit_sql
SELECT batch_id, 'crm_prd_info', 'end', current_timestamp() FROM __run;

-- ====== CRM: Sales Details ======
INSERT INTO load_audit_sql
SELECT batch_id, 'crm_sales_details', 'start', current_timestamp() FROM __run;

TRUNCATE TABLE crm_sales_details;
COPY INTO crm_sales_details
FROM '/Volumes/datawarehouse/bronze/landing/crm/'
FILEFORMAT = CSV
PATTERN = 'sales_details*.csv'  -- Simplified pattern
FORMAT_OPTIONS (
    'header' = 'true',
    'multiline' = 'false'
)
COPY_OPTIONS (
    'force' = 'true'
);

INSERT INTO load_audit_sql
SELECT batch_id, 'crm_sales_details', 'end', current_timestamp() FROM __run;

-- ---------------------------------------------------------------------------
-- 2) ERP loads
-- ---------------------------------------------------------------------------
-- ====== ERP: Location A101 ======
INSERT INTO load_audit_sql
SELECT batch_id, 'erp_loc_a101', 'start', current_timestamp() FROM __run;

TRUNCATE TABLE erp_loc_a101;
COPY INTO erp_loc_a101
FROM '/Volumes/datawarehouse/bronze/landing/erp/'
FILEFORMAT = CSV
PATTERN = 'LOC_A101*.csv'  -- Simplified pattern
FORMAT_OPTIONS (
    'header' = 'true',
    'multiline' = 'false'
)
COPY_OPTIONS (
    'force' = 'true'
);

INSERT INTO load_audit_sql
SELECT batch_id, 'erp_loc_a101', 'end', current_timestamp() FROM __run;

-- ====== ERP: Customer AZ12 ======
INSERT INTO load_audit_sql
SELECT batch_id, 'erp_cust_az12', 'start', current_timestamp() FROM __run;

TRUNCATE TABLE erp_cust_az12;
COPY INTO erp_cust_az12
FROM '/Volumes/datawarehouse/bronze/landing/erp/'
FILEFORMAT = CSV
PATTERN = 'CUST_AZ12*.csv'  -- Simplified pattern
FORMAT_OPTIONS (
    'header' = 'true',
    'multiline' = 'false'
)
COPY_OPTIONS (
    'force' = 'true'
);

INSERT INTO load_audit_sql
SELECT batch_id, 'erp_cust_az12', 'end', current_timestamp() FROM __run;

-- ====== ERP: Product Category G1V2 ======
INSERT INTO load_audit_sql
SELECT batch_id, 'erp_px_cat_g1v2', 'start', current_timestamp() FROM __run;

TRUNCATE TABLE erp_px_cat_g1v2;
COPY INTO erp_px_cat_g1v2
FROM '/Volumes/datawarehouse/bronze/landing/erp/'
FILEFORMAT = CSV
PATTERN = 'PX_CAT_G1V2*.csv'  -- Simplified pattern
FORMAT_OPTIONS (
    'header' = 'true',
    'multiline' = 'false'
)
COPY_OPTIONS (
    'force' = 'true'
);

INSERT INTO load_audit_sql
SELECT batch_id, 'erp_px_cat_g1v2', 'end', current_timestamp() FROM __run;
-- ---------------------------------------------------------------------------
-- 3) Verification: row counts (current state)
-- ---------------------------------------------------------------------------
SELECT 'crm_cust_info'   AS table_name, COUNT(*) AS row_count FROM crm_cust_info
UNION ALL SELECT 'crm_prd_info',      COUNT(*) FROM crm_prd_info
UNION ALL SELECT 'crm_sales_details', COUNT(*) FROM crm_sales_details
UNION ALL SELECT 'erp_loc_a101',      COUNT(*) FROM erp_loc_a101
UNION ALL SELECT 'erp_cust_az12',     COUNT(*) FROM erp_cust_az12
UNION ALL SELECT 'erp_px_cat_g1v2',   COUNT(*) FROM erp_px_cat_g1v2
ORDER BY table_name;

-- ---------------------------------------------------------------------------
-- 4) Run-time summary for THIS batch (durations + rows)
-- ---------------------------------------------------------------------------
WITH run_id AS (
  SELECT batch_id FROM __run              -- <-- was "FROM batch" by mistake
),
durs AS (
  SELECT
    a.table_name,
    MIN(CASE WHEN a.phase = 'start' THEN a.ts END) AS start_ts,
    MAX(CASE WHEN a.phase = 'end'   THEN a.ts END) AS end_ts
  FROM load_audit_sql a
  JOIN run_id r ON a.batch_id = r.batch_id
  GROUP BY a.table_name
),
counts AS (
  SELECT 'crm_cust_info'   AS table_name, COUNT(*) AS rows_after FROM crm_cust_info
  UNION ALL SELECT 'crm_prd_info',      COUNT(*) FROM crm_prd_info
  UNION ALL SELECT 'crm_sales_details', COUNT(*) FROM crm_sales_details
  UNION ALL SELECT 'erp_loc_a101',      COUNT(*) FROM erp_loc_a101
  UNION ALL SELECT 'erp_cust_az12',     COUNT(*) FROM erp_cust_az12
  UNION ALL SELECT 'erp_px_cat_g1v2',   COUNT(*) FROM erp_px_cat_g1v2
)
SELECT
  d.table_name,
  d.start_ts,
  d.end_ts,
  CAST((unix_timestamp(d.end_ts) - unix_timestamp(d.start_ts)) AS INT) AS duration_sec,
  c.rows_after
FROM durs d
LEFT JOIN counts c USING (table_name)
ORDER BY d.table_name;