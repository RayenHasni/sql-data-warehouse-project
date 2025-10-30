/*
=========================================
Create Database and Schemas
=========================================
Script purpose : 
  This script creates a new database named 'datawarehouse' and set up three schemas
  whitin the database: "Bronze", "Silver" and "Gold".
*/

use master;
go
  

-- Create the 'datawarehouse' database  
create database DataWarehouse;
go


-- Create schemas
create schema bronze;
go
  
create schema silver;
go
  
create schema gold;
go
