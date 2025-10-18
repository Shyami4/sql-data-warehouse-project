/*
===============================================================================
DDL Script: Create Silver Tables (Databricks / Delta Lake)
===============================================================================
Script Purpose:
  Drops existing tables (if any) and recreates Silver tables as Delta tables
  under catalog `datawarehouse`, schema `silver`.

Notes:
  - Uses Delta tables (Databricks).
  - No DEFAULT on columns (Option A). We’ll populate dwh_create_date in inserts.
  - Types: STRING (instead of NVARCHAR), TIMESTAMP for audit columns.

Run order:
  1) USE CATALOG / SCHEMA
  2) DROP & CREATE tables
===============================================================================
*/

USE CATALOG datawarehouse;
USE SCHEMA silver;

-- crm_cust_info
DROP TABLE IF EXISTS crm_cust_info;
CREATE TABLE crm_cust_info (
  cst_id             INT,
  cst_key            STRING,
  cst_firstname      STRING,
  cst_lastname       STRING,
  cst_marital_status STRING,
  cst_gndr           STRING,
  cst_create_date    DATE,
  dwh_create_date    TIMESTAMP
);

-- crm_prd_info
DROP TABLE IF EXISTS crm_prd_info;
CREATE TABLE crm_prd_info (
  prd_id          INT,
  cat_id          STRING,
  prd_key         STRING,
  prd_nm          STRING,
  prd_cost        INT,
  prd_line        STRING,
  prd_start_dt    DATE,
  prd_end_dt      DATE,
  dwh_create_date TIMESTAMP
);

-- crm_sales_details
DROP TABLE IF EXISTS crm_sales_details;
CREATE TABLE crm_sales_details (
  sls_ord_num     STRING,
  sls_prd_key     STRING,
  sls_cust_id     INT,
  sls_order_dt    DATE,
  sls_ship_dt     DATE,
  sls_due_dt      DATE,
  sls_sales       INT,
  sls_quantity    INT,
  sls_price       INT,
  dwh_create_date TIMESTAMP
);

-- erp_loc_a101
DROP TABLE IF EXISTS erp_loc_a101;
CREATE TABLE erp_loc_a101 (
  cid             STRING,
  cntry           STRING,
  dwh_create_date TIMESTAMP
);

-- erp_cust_az12
DROP TABLE IF EXISTS erp_cust_az12;
CREATE TABLE erp_cust_az12 (
  cid             STRING,
  bdate           DATE,
  gen             STRING,
  dwh_create_date TIMESTAMP
);

-- erp_px_cat_g1v2
DROP TABLE IF EXISTS erp_px_cat_g1v2;
CREATE TABLE erp_px_cat_g1v2 (
  id              STRING,
  cat             STRING,
  subcat          STRING,
  maintenance     STRING,
  dwh_create_date TIMESTAMP
);
