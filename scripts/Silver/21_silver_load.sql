/*
===============================================================================
Silver Load: Bronze -> Silver (Databricks / Delta Lake)
===============================================================================
Script Purpose:
  Executes the ETL transformations to populate the Silver layer from Bronze.
  Mirrors your original stored-proc style (truncate + insert with transforms).

What it does:
  - crm_cust_info  : trim, normalize values, dedupe to latest per cst_id
  - crm_prd_info   : parse cat_id/prd_key, normalize line; compute SCD end date
  - crm_sales_det  : parse YYYYMMDD ints, repair price/sales
  - erp_cust_az12  : strip 'NAS' prefix, future-birthdate → NULL, normalize GEN
  - erp_loc_a101   : strip '-' in CID, expand country codes, handle blanks
  - erp_px_cat_g1v2: pass-through to silver
  - Adds dwh_create_date = current_timestamp() in each insert

Notes:
  - Uses Databricks SQL functions like to_date(string,'yyyyMMdd').
  - Window LEAD used for SCD end date (batch-friendly).
  - At end: quick row-count check.
===============================================================================
*/

USE CATALOG datawarehouse;
USE SCHEMA silver;

/* ----------------------------------------------------------
   CRM: crm_cust_info (latest row per cst_id, trim/normalize)
----------------------------------------------------------- */
TRUNCATE TABLE crm_cust_info;

INSERT INTO crm_cust_info (
  cst_id, cst_key, cst_firstname, cst_lastname,
  cst_marital_status, cst_gndr, cst_create_date, dwh_create_date
)
WITH src AS (
  SELECT
    TRY_CAST(cst_id AS INT)                                AS cst_id,
    cst_key,
    TRIM(cst_firstname)                                    AS cst_firstname,
    TRIM(cst_lastname)                                     AS cst_lastname,
    CASE UPPER(TRIM(cst_marital_status))
      WHEN 'S' THEN 'Single'
      WHEN 'M' THEN 'Married'
      ELSE 'n/a'
    END                                                    AS std_marital_status,
    CASE UPPER(TRIM(cst_gndr))
      WHEN 'F' THEN 'Female'
      WHEN 'M' THEN 'Male'
      ELSE 'n/a'
    END                                                    AS std_gndr,
    TO_DATE(cst_create_date)                               AS cst_create_date,
    COALESCE(TO_TIMESTAMP(cst_create_date), TIMESTAMP '1900-01-01 00:00:00') AS sort_ts
  FROM datawarehouse.bronze.crm_cust_info
  WHERE cst_id IS NOT NULL
),
dedup AS (
  SELECT
    cst_id,
    MAX_BY(
      NAMED_STRUCT(
        'cst_key',            cst_key,
        'cst_firstname',      cst_firstname,
        'cst_lastname',       cst_lastname,
        'cst_marital_status', std_marital_status,
        'cst_gndr',           std_gndr,
        'cst_create_date',    cst_create_date
      ),
      sort_ts
    ) AS rec
  FROM src
  GROUP BY cst_id
)
SELECT
  cst_id,
  rec.cst_key,
  rec.cst_firstname,
  rec.cst_lastname,
  rec.cst_marital_status,
  rec.cst_gndr,
  rec.cst_create_date,
  CURRENT_TIMESTAMP()
FROM dedup;

/* ----------------------------------------------------------
   CRM: crm_prd_info (SCD end date from next start - 1)
----------------------------------------------------------- */
TRUNCATE TABLE crm_prd_info;

INSERT INTO crm_prd_info (
  prd_id, cat_id, prd_key, prd_nm, prd_cost,
  prd_line, prd_start_dt, prd_end_dt, dwh_create_date
)
WITH base AS (
  SELECT
    TRY_CAST(prd_id AS INT)                                AS prd_id,
    REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_')            AS cat_id,
    SUBSTRING(prd_key, 7)                                  AS prd_key,
    prd_nm,
    TRY_CAST(prd_cost AS INT)                              AS prd_cost,
    CASE UPPER(TRIM(prd_line))
      WHEN 'M' THEN 'Mountain'
      WHEN 'R' THEN 'Road'
      WHEN 'S' THEN 'Other Sales'
      WHEN 'T' THEN 'Touring'
      ELSE 'n/a'
    END                                                    AS prd_line,
    TO_DATE(prd_start_dt)                                  AS prd_start_dt
  FROM datawarehouse.bronze.crm_prd_info
),
scd AS (
  SELECT
    *,
    DATE_SUB(
      LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt),
      1
    ) AS prd_end_dt
  FROM base
)
SELECT
  prd_id, cat_id, prd_key, prd_nm, prd_cost,
  prd_line, prd_start_dt, prd_end_dt,
  CURRENT_TIMESTAMP()
FROM scd;

/* ----------------------------------------------------------
   CRM: crm_sales_details (date parse + price/sales repair)
----------------------------------------------------------- */
TRUNCATE TABLE crm_sales_details;

INSERT INTO crm_sales_details (
  sls_ord_num, sls_prd_key, sls_cust_id,
  sls_order_dt, sls_ship_dt, sls_due_dt,
  sls_sales, sls_quantity, sls_price,
  dwh_create_date
)
WITH parsed AS (
  SELECT
    sls_ord_num,
    sls_prd_key,
    TRY_CAST(sls_cust_id AS INT)                                           AS sls_cust_id,

    -- Safely parse YYYYMMDD ints to DATE (0/invalid -> NULL)
    CASE WHEN sls_order_dt = 0 OR LENGTH(CAST(sls_order_dt AS STRING)) != 8
         THEN NULL
         ELSE TO_DATE(CAST(sls_order_dt AS STRING), 'yyyyMMdd')
    END                                                                    AS sls_order_dt,
    CASE WHEN sls_ship_dt = 0 OR LENGTH(CAST(sls_ship_dt AS STRING)) != 8
         THEN NULL
         ELSE TO_DATE(CAST(sls_ship_dt AS STRING), 'yyyyMMdd')
    END                                                                    AS sls_ship_dt,
    CASE WHEN sls_due_dt = 0 OR LENGTH(CAST(sls_due_dt AS STRING)) != 8
         THEN NULL
         ELSE TO_DATE(CAST(sls_due_dt AS STRING), 'yyyyMMdd')
    END                                                                    AS sls_due_dt,

    TRY_CAST(sls_quantity AS INT)                                          AS sls_quantity,
    TRY_CAST(sls_price    AS INT)                                          AS sls_price,
    TRY_CAST(sls_sales    AS INT)                                          AS sls_sales
  FROM datawarehouse.bronze.crm_sales_details
),
repaired AS (
  SELECT
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_quantity,

    -- price candidate: use given price; if missing/<=0, derive from sales/qty
    ABS(
      COALESCE(
        NULLIF(sls_price, 0),
        CASE WHEN sls_quantity IS NOT NULL AND sls_quantity <> 0
             THEN CAST(sls_sales AS DOUBLE) / sls_quantity
             ELSE NULL
        END
      )
    ) AS price_calc
  FROM parsed
),
finalized AS (
  SELECT
    p.sls_ord_num,
    p.sls_prd_key,
    p.sls_cust_id,
    p.sls_order_dt,
    p.sls_ship_dt,
    p.sls_due_dt,
    p.sls_quantity,
    TRY_CAST(ROUND(price_calc) AS INT)                                AS sls_price_fixed,
    -- if original sales invalid or inconsistent, recompute from price * qty
    CASE
      WHEN c.sls_sales IS NULL OR c.sls_sales <= 0
        OR c.sls_sales <> p.sls_quantity * TRY_CAST(ROUND(price_calc) AS INT)
      THEN p.sls_quantity * TRY_CAST(ROUND(price_calc) AS INT)
      ELSE c.sls_sales
    END AS sls_sales_fixed
  FROM repaired p
  JOIN parsed   c
    ON c.sls_ord_num = p.sls_ord_num
)
SELECT
  sls_ord_num,
  sls_prd_key,
  sls_cust_id,
  sls_order_dt,
  sls_ship_dt,
  sls_due_dt,
  sls_sales_fixed   AS sls_sales,
  sls_quantity,
  sls_price_fixed   AS sls_price,
  CURRENT_TIMESTAMP() AS dwh_create_date
FROM finalized;

/* ----------------------------------------------------------
   ERP: erp_cust_az12 (strip NAS, future DOB -> NULL, normalize GEN)
----------------------------------------------------------- */
TRUNCATE TABLE erp_cust_az12;

INSERT INTO erp_cust_az12 (cid, bdate, gen, dwh_create_date)
SELECT
  CASE WHEN CID LIKE 'NAS%' THEN SUBSTRING(CID, 4) ELSE CID END          AS cid,
  CASE WHEN BDATE > CURRENT_DATE THEN NULL ELSE BDATE END                 AS bdate,
  CASE UPPER(TRIM(GEN))
    WHEN 'F' THEN 'Female'
    WHEN 'M' THEN 'Male'
    ELSE 'n/a'
  END                                                                     AS gen,
  CURRENT_TIMESTAMP()
FROM datawarehouse.bronze.erp_cust_az12;

/* ----------------------------------------------------------
   ERP: erp_loc_a101 (normalize CID, expand country)
----------------------------------------------------------- */
TRUNCATE TABLE erp_loc_a101;

INSERT INTO erp_loc_a101 (cid, cntry, dwh_create_date)
SELECT
  REPLACE(CID, '-', '')                                                   AS cid,
  CASE
    WHEN TRIM(CNTRY) = 'DE'           THEN 'Germany'
    WHEN TRIM(CNTRY) IN ('US','USA')  THEN 'United States'
    WHEN TRIM(CNTRY) = '' OR CNTRY IS NULL THEN 'n/a'
    ELSE TRIM(CNTRY)
  END                                                                     AS cntry,
  CURRENT_TIMESTAMP()
FROM datawarehouse.bronze.erp_loc_a101;

/* ----------------------------------------------------------
   ERP: erp_px_cat_g1v2 (pass-through)
----------------------------------------------------------- */
TRUNCATE TABLE erp_px_cat_g1v2;

INSERT INTO erp_px_cat_g1v2 (id, cat, subcat, maintenance, dwh_create_date)
SELECT
  ID, CAT, SUBCAT, MAINTENANCE, CURRENT_TIMESTAMP()
FROM datawarehouse.bronze.erp_px_cat_g1v2;

/* ----------------------------------------------------------
   Quick verification: row counts
----------------------------------------------------------- */
SELECT 'silver.crm_cust_info'   AS table_name, COUNT(*) AS rows_after FROM silver.crm_cust_info
UNION ALL
SELECT 'silver.crm_prd_info'    AS table_name, COUNT(*) FROM silver.crm_prd_info
UNION ALL
SELECT 'silver.crm_sales_details',                COUNT(*) FROM silver.crm_sales_details
UNION ALL
SELECT 'silver.erp_cust_az12',                    COUNT(*) FROM silver.erp_cust_az12
UNION ALL
SELECT 'silver.erp_loc_a101',                     COUNT(*) FROM silver.erp_loc_a101
UNION ALL
SELECT 'silver.erp_px_cat_g1v2',                  COUNT(*) FROM silver.erp_px_cat_g1v2;
