/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None

Usage Example:
    EXEC bronze.load_data;
===============================================================================
*/

CREATE OR ALTER Procedure bronze.load_data as

BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME
	BEGIN TRY

		PRINT '********************************';
		PRINT 'Loading Bronze Layer';
		PRINT '********************************';
		SET @batch_start_time = GETDATE();

		PRINT '----------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '----------------------------';

		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.crm_cust_info
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\SQL\Data\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		TRUNCATE TABLE bronze.crm_prd_info
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\SQL\Data\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		TRUNCATE TABLE bronze.crm_sales_details
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\SQL\Data\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT ''
		PRINT '>> Loading CRM Data Duaration: ' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + ' s'

		PRINT '----------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '----------------------------';

		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.erp_loc_a101
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\SQL\Data\source_erp\loc_a101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		TRUNCATE TABLE bronze.erp_cust_az12
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\SQL\Data\source_erp\cust_az12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		TRUNCATE TABLE bronze.erp_px_cat_g1v2
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\SQL\Data\source_erp\px_cat_g1v2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		SET @end_time = GETDATE();
		PRINT ''
		PRINT '>> Loading ERP Data Duaration: ' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + ' s'

	END TRY
	BEGIN CATCH
		PRINT 'ERROR DURING LOADING BRONZE LAYER';
		PRINT 'ERROR: ' + ERROR_MESSAGE();
	END CATCH

	SET @batch_end_time = GETDATE();
	PRINT ''
	PRINT '***********************'
	PRINT '>> Loading Bronze Layer Duaration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) as NVARCHAR) + ' s'
	PRINT '***********************'
END
