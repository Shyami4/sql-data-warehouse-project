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
    cst_id              INT,
    cst_key             STRING,
    cst_firstname       STRING,
    cst_lastname        STRING,
    cst_marital_status  STRING,
    cst_gndr            STRING,
    cst_create_date     DATE
)
USING DELTA;

-- crm_prd_info
DROP TABLE IF EXISTS crm_prd_info;
CREATE TABLE crm_prd_info (
    prd_id       INT,
    prd_key      STRING,
    prd_nm       STRING,
    prd_cost     INT,          -- consider DECIMAL(18,2) if currency
    prd_line     STRING,
    prd_start_dt TIMESTAMP,
    prd_end_dt   TIMESTAMP
)
USING DELTA;

-- crm_sales_details
DROP TABLE IF EXISTS crm_sales_details;
CREATE TABLE crm_sales_details (
    sls_ord_num  STRING,
    sls_prd_key  STRING,
    sls_cust_id  INT,
    sls_order_dt INT,          -- consider DATE if your CSV has dates
    sls_ship_dt  INT,          -- consider DATE
    sls_due_dt   INT,          -- consider DATE
    sls_sales    INT,          -- consider DECIMAL(18,2)
    sls_quantity INT,
    sls_price    INT           -- consider DECIMAL(18,2)
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
    bdate  DATE,
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