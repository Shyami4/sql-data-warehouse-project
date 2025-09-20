/*
=============================================================
Create Database and Schemas
=============================================================
Script Purpose:
    This script creates a new catalog named 'dataWarehouse' after checking if it already exists. 
    If the catalog exists, it is dropped and recreated. Additionally, the script sets up three schemas 
    within the catalog: 'bronze', 'silver', and 'gold'.
    
    Finally, it creates a managed Volume named 'landing' under the 'bronze' schema
    for staging raw files (path: /Volumes/dataWarehouse/bronze/landing).
	
WARNING:
    Running this script will drop the entire 'dataWarehouse' catalog if it exists. 
    All data in the catalog will be permanently deleted. Proceed with caution 
    and ensure you have proper backups before running this script.
*/

-- Drop and recreate the 'dataWarehouse' catalog
DROP CATALOG IF EXISTS datawarehouse CASCADE;

-- Create the 'dataWarehouse' database
CREATE CATALOG datawarehouse;
USE CATALOG datawarehouse;

-- Create Schemas
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- Creates /Volumes/datawarehouse/bronze/landing
USE CATALOG datawarehouse;
CREATE VOLUME IF NOT EXISTS bronze.landing;