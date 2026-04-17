/*
=========================================================
Create Database and Schemas
=========================================================
Script Purpose:
       This script creates a new database named 'Datawarehouse' after checking if it already exists.
       If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas
       within the database: 'bronze', 'silver', and 'gold'.
WARNING:
    Running thls scrlpt will drop the entire 'Datawarehouse' database if it exists.
    A11 data in the database will be permanently deleted. Proceed with caution
    and ensure you have proper backups before running this script.
*/

USE MASTER;
GO

-- Drop & Recreate 'The Data Warehouse' Database

IF EXISTS ( SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
DROP DATABASE DataWarehouse;
END;
GO

-- Create 'Data Warehouse' Database

CREATE DATABASE DataWarehouse;
Go

USE DataWarehouse;
Go

-- Creating Schemas

CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
