/*
===============================================================================
DDL Script: Create Bronze Tables (Databricks / Delta Lake)
===============================================================================
Script Purpose:
    Drops existing tables (if any) and recreates Bronze tables as Delta tables
    under catalog `dataWarehouse`, schema `bronze`.

Notes:
    - Databricks uses `DROP TABLE IF EXISTS` (no OBJECT_ID / GO).
    - Use STRING instead of NVARCHAR; DATETIME -> TIMESTAMP.
===============================================================================
*/
-- Target namespace
USE CATALOG dataWarehouse;
USE SCHEMA bronze;

-- crm_cust_info
DROP TABLE IF EXISTS crm_cust_info;
CREATE TABLE crm_cust_info (
  cst_id             STRING,
  cst_key            STRING,
  cst_firstname      STRING,
  cst_lastname       STRING,
  cst_marital_status STRING,
  cst_gndr           STRING,
  cst_create_date    STRING
) 
USING DELTA;

-- crm_prd_info
DROP TABLE IF EXISTS crm_prd_info;
CREATE TABLE crm_prd_info (
  prd_id       STRING,
  prd_key      STRING,
  prd_nm       STRING,
  prd_cost     STRING,
  prd_line     STRING,
  prd_start_dt STRING,
  prd_end_dt   STRING
) USING DELTA;

-- crm_sales_details
DROP TABLE IF EXISTS crm_sales_details;
CREATE TABLE crm_sales_details (
  sls_ord_num  STRING,
  sls_prd_key  STRING,
  sls_cust_id  STRING,
  sls_order_dt STRING,
  sls_ship_dt  STRING,
  sls_due_dt   STRING,
  sls_sales    STRING,
  sls_quantity STRING,
  sls_price    STRING
)
USING DELTA;

-- erp_loc_a101
DROP TABLE IF EXISTS erp_loc_a101;
CREATE TABLE erp_loc_a101 (
    cid    STRING,
    cntry  STRING
)
USING DELTA;

-- erp_cust_az12
DROP TABLE IF EXISTS erp_cust_az12;
CREATE TABLE erp_cust_az12 (
    cid    STRING,
    bdate  STRING,
    gen    STRING
)
USING DELTA;

-- erp_px_cat_g1v2
DROP TABLE IF EXISTS erp_px_cat_g1v2;
CREATE TABLE erp_px_cat_g1v2 (
    id           STRING,
    cat          STRING,
    subcat       STRING,
    maintenance  STRING
)
USING DELTA;