/*
===============================================================================
File:           scripts/11_bronze_streaming_create.sql
Title:          Create Streaming Bronze Tables (Databricks SQL / Delta)
Catalog/Schema: datawarehouse.bronze

Purpose:
  - Define the Bronze layer as **Streaming Tables** that continuously ingest
    CSV files dropped in Unity Catalog Volumes.
  - Keeps Bronze raw and schema-on-read (all fields as STRING by default).
  - Adds lightweight lineage columns for debugging (`_ingest_ts`, `_source_file`).

What this script does:
  1) Sets the working catalog/schema.
  2) (Safeguard) Drops any pre-existing *batch* tables with the same names.
  3) Creates/refreshes six STREAMING TABLES reading from Volumes via
     `FROM STREAM read_files(...)`.

Scheduling & orchestration:
  - You can **either**:
      • add `SCHEDULE EVERY …` to each streaming table (self-refresh), or
      • orchestrate with a Databricks **Workflow** that issues
        `ALTER STREAMING TABLE <name> REFRESH;`
    Do **not** do both.

Assumptions:
  - The Unity Catalog **catalog** `datawarehouse` and **schema** `bronze` exist.
  - Files are present at:
      /Volumes/datawarehouse/bronze/landing/crm/  (cust_info, prd_info, sales_details)
      /Volumes/datawarehouse/bronze/landing/erp/  (CUST_AZ12, LOC_A101, PX_CAT_G1V2)
  - File names/patterns match those used below.

Notes:
  - `read_files` options here keep Bronze raw:
      format => 'CSV', header => true, inferSchema => false (→ STRING columns).
  - Databricks may add `_rescued_data` for malformed rows; you can inspect it
    in Bronze or ignore it and fix in Silver.
  - If you previously created *non-streaming* tables with these names,
    the DROP statements below allow the streaming creates to succeed.

Change Log:
  - 2025-09-19: Initial streaming Bronze script.
===============================================================================
*/

USE CATALOG datawarehouse;
USE SCHEMA bronze;

/* ------------------------------------------------------------------------- */
/* Safeguard: drop any existing non-streaming tables with the same names     */
/* ------------------------------------------------------------------------- */
-- DROP TABLE IF EXISTS crm_cust_info;
-- DROP TABLE IF EXISTS crm_prd_info;
-- DROP TABLE IF EXISTS crm_sales_details;
-- DROP TABLE IF EXISTS erp_loc_a101;
-- DROP TABLE IF EXISTS erp_cust_az12;
-- DROP TABLE IF EXISTS erp_px_cat_g1v2;

/* ------------------------------------------------------------------------- */
/* CRM: cust_info                                                            */
/* ------------------------------------------------------------------------- */
CREATE OR REFRESH STREAMING TABLE crm_cust_info
-- SCHEDULE EVERY 1 HOUR
AS
SELECT
  *,
  current_timestamp()         AS _ingest_ts,
  _metadata.file_name         AS _source_file
FROM STREAM read_files(
  '/Volumes/datawarehouse/bronze/landing/crm/cust_info*.csv',
  format       => 'CSV',
  header       => true,
  multiline    => false
 -- inferSchema  => false        -- keep Bronze raw: all STRING
);

/* ------------------------------------------------------------------------- */
/* CRM: prd_info                                                             */
/* ------------------------------------------------------------------------- */
CREATE OR REFRESH STREAMING TABLE crm_prd_info
-- SCHEDULE EVERY 1 HOUR
AS
SELECT
  *,
  current_timestamp()         AS _ingest_ts,
  _metadata.file_name         AS _source_file
FROM STREAM read_files(
  '/Volumes/datawarehouse/bronze/landing/crm/prd_info*.csv',
  format       => 'CSV',
  header       => true,
  multiline    => false
--  inferSchema  => false
);

/* ------------------------------------------------------------------------- */
/* CRM: sales_details                                                        */
/* ------------------------------------------------------------------------- */
CREATE OR REFRESH STREAMING TABLE crm_sales_details
-- SCHEDULE EVERY 1 HOUR
AS
SELECT
  *,
  current_timestamp()         AS _ingest_ts,
  _metadata.file_name         AS _source_file
FROM STREAM read_files(
  '/Volumes/datawarehouse/bronze/landing/crm/sales_details*.csv',
  format       => 'CSV',
  header       => true,
  multiline    => false
--  inferSchema  => false
);

/* ------------------------------------------------------------------------- */
/* ERP: LOC_A101                                                             */
/* ------------------------------------------------------------------------- */
CREATE OR REFRESH STREAMING TABLE erp_loc_a101
-- SCHEDULE EVERY 1 HOUR
AS
SELECT
  *,
  current_timestamp()         AS _ingest_ts,
  _metadata.file_name         AS _source_file
FROM STREAM read_files(
  '/Volumes/datawarehouse/bronze/landing/erp/LOC_A101*.csv',
  format       => 'CSV',
  header       => true,
  multiline    => false
--  inferSchema  => false
);

/* ------------------------------------------------------------------------- */
/* ERP: CUST_AZ12                                                            */
/* ------------------------------------------------------------------------- */
CREATE OR REFRESH STREAMING TABLE erp_cust_az12
-- SCHEDULE EVERY 1 HOUR
AS
SELECT
  *,
  current_timestamp()         AS _ingest_ts,
  _metadata.file_name         AS _source_file
FROM STREAM read_files(
  '/Volumes/datawarehouse/bronze/landing/erp/CUST_AZ12*.csv',
  format       => 'CSV',
  header       => true,
  multiline    => false
--  inferSchema  => false
);

/* ------------------------------------------------------------------------- */
/* ERP: PX_CAT_G1V2                                                          */
/* ------------------------------------------------------------------------- */
CREATE OR REFRESH STREAMING TABLE erp_px_cat_g1v2
-- SCHEDULE EVERY 1 HOUR
AS
SELECT
  *,
  current_timestamp()         AS _ingest_ts,
  _metadata.file_name         AS _source_file
FROM STREAM read_files(
  '/Volumes/datawarehouse/bronze/landing/erp/PX_CAT_G1V2*.csv',
  format       => 'CSV',
  header       => true,
  multiline    => false
--  inferSchema  => false
);

