/*
=============================================================
Create Database and Schemas
=============================================================
Script Purpose: 
This script creates a new database named 'DataWarehouse'. If the database already exists, it is dropped and then recreated. 
The script also sets up three schemas within the database: 'bronze', 'silver', and 'gold'.

WARNING: 
Running this script will drop the existing 'DataWarehouse' database, permanently deleting all its data. Ensure you have proper backups before proceeding.
*/

USE master;
GO

-- Drop and recreate the 'DataWarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse;
END;
GO

-- Creating a database 'DataWareHouse'
CREATE DATABASE DataWareHouse;

-- Shifting to the DataWareHouse database
USE DataWareHouse;

-- Creating the schema for the bronze, silver, and gold layers
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO
