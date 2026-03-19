/* 
==================================================================================
Create Database and Schema
==================================================================================

Script Purpose:
	This script creates a new Database named Datawarehouse after checking it already exists. 
	If the database exists, it is dropped and recreated. Additionally the script sets up three 
	schemas	within the database : 'bronze', 'silver', 'gold'.

Warning !
	Running this script will drop the entire warehouse if it exists.
	All data in the database will be permanently deleted. Proceed with caution and ensure that 
	you have necessary backups before proceeding with this script. 

*/

--  Create Database : 'DataWarehouse'
USE master;
GO

-- Drop the DataWarehouse database if exist.

IF EXISTS (SELECT 1 FROM sys.databases WHERE name= 'DataWarehouse')
BEGIN 
	ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWarehouse;
END;
GO

-- Create Database 

CREATE DATABASE DataWarehouse;
GO
USE DataWarehouse ;
GO 
CREATE SCHEMA bronze;	
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO
