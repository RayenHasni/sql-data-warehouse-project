/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.

Parameters:
    None

Usage Example:
    EXEC silver.load_data;
===============================================================================
*/
CREATE or ALTER PROCEDURE silver.load_data AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME
	BEGIN TRY
		PRINT '********************************';
		PRINT 'Loading Silver Layer';
		PRINT '********************************';
		SET @batch_start_time = GETDATE();

		PRINT '----------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '----------------------------';

		SET @start_time = GETDATE();
		PRINT 'Inserting Data Into: silver.crm_cust_info'
		TRUNCATE TABLE silver.crm_cust_info
		INSERT INTO silver.crm_cust_info (cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date)
		SELECT 
			cst_id,
			cst_key,
			trim(cst_firstname) as cst_firstname,
			trim(cst_lastname) as cst_lastname,
			CASE WHEN TRIM(cst_marital_status) = 'M' THEN 'Married'
				 WHEN TRIM(cst_marital_status) = 'S' THEN 'Single'
				 ELSE 'n/a'
			END AS cst_marital_status,
			CASE WHEN TRIM(cst_gndr) = 'M' THEN 'Male'
				 WHEN TRIM(cst_gndr) = 'F' THEN 'Female'
				 ELSE 'n/a'
			END AS cst_gndr,
			cst_create_date 
		FROM 
			(SELECT *, ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
			FROM bronze.crm_cust_info
			WHERE cst_id IS NOT NULL )t

			WHERE flag_last = 1

		PRINT 'Inserting Data Into: silver.crm_prd_info'
		TRUNCATE TABLE silver.crm_prd_info
		INSERT INTO silver.crm_prd_info (
			prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt)
		SELECT
			prd_id,
			REPLACE(SUBSTRING(prd_key, 1,5), '-', '_') as cat_id,
			SUBSTRING(prd_key, 7, LEN(prd_key)) as prd_key,
			prd_nm,
			ISNULL(prd_cost, 0) as prd_cost,
			CASE WHEN TRIM(prd_line) = 'M' THEN 'Mountain'
				WHEN TRIM(prd_line) = 'R' THEN 'Road'
				WHEN TRIM(prd_line) = 'S' THEN 'Other Sales'
				WHEN TRIM(prd_line) = 'T' THEN 'Touring'
				ELSE 'n/a'
			END AS prd_line,
			CAST(prd_start_dt as DATE) as prd_start_dt,
			CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)- 1 as DATE) as prd_end_dt
		FROM bronze.crm_prd_info

		PRINT 'Inserting Data Into: silver.crm_sales_details'
		TRUNCATE TABLE silver.crm_sales_details
		INSERT INTO silver.crm_sales_details (
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_sales,
			sls_quantity,
			sls_price)
		SELECT 
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			CASE WHEN sls_order_dt = 0 or LEN(sls_order_dt) != 8 THEN NULL
				 ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
			END AS sls_order_dt,
			CASE WHEN sls_ship_dt = 0 or LEN(sls_ship_dt) != 8 THEN NULL
				 ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
			END AS sls_ship_dt,
			CASE WHEN sls_due_dt = 0 or LEN(sls_due_dt) != 8 THEN NULL
				 ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
			END AS sls_due_dt,
	
			CASE WHEN sls_sales is null or sls_sales <= 0 or sls_sales != sls_quantity * ABS(sls_price) THEN sls_quantity * ABS(sls_price)
				 ELSE sls_sales
			END as sls_sales,
			sls_quantity,
			CASE WHEN sls_price is null or sls_price <= 0 THEN sls_sales / NULLIF(sls_quantity, 0)
				 ELSE sls_price
			END AS sls_price
		from bronze.crm_sales_details
		SET @end_time = GETDATE()

		PRINT ''
		PRINT '>> Loading CRM Data Duaration: ' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + ' s'
		
		PRINT '----------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '----------------------------';

		SET @start_time = GETDATE();
		PRINT 'Inserting Data Into: silver.erp_cust_az12'
		TRUNCATE TABLE silver.erp_cust_az12
		INSERT INTO silver.erp_cust_az12 (cid, bdate, gen)
		SELECT
			CASE WHEN TRIM(cid) LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
				 ELSE cid
			END AS cid,
			CASE WHEN bdate >= GETDATE() THEN NULL
				 ELSE bdate
			END AS bdate,
			CASE WHEN TRIM(gen) IN ('F', 'FEMALE') THEN 'Female'
				 WHEN TRIM(gen) IN ('M', 'MALE') THEN 'Male'
				 ELSE 'n/a'
			END AS gen
		FROM bronze.erp_cust_az12

		PRINT 'Inserting Data Into: silver.erp_loc_a101'
		TRUNCATE TABLE silver.erp_loc_a101
		INSERT INTO silver.erp_loc_a101 (cid, cntry)
		SELECT 
			REPLACE(cid, '-', '') AS cid,
			CASE WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
				 WHEN TRIM(cntry) = 'DE' THEN 'Germany'
				 WHEN cntry IS NULL or TRIM(cntry) = '' THEN 'n/a'
				 ELSE TRIM(cntry)
			END as cntry
		FROM bronze.erp_loc_a101

		PRINT 'Inserting Data Into: silver.erp_px_cat_g1v2'
		TRUNCATE TABLE silver.erp_px_cat_g1v2
		INSERT INTO silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
		SELECT
			id,
			cat,
			subcat,
			maintenance
		FROM bronze.erp_px_cat_g1v2

		SET @end_time = GETDATE()
		PRINT ''
		PRINT '>> Loading ERP Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR)+ ' s'
	END TRY
	BEGIN CATCH
		PRINT 'ERROR DURING LOADING SILVER LAYER';
		PRINT 'ERROR: ' + ERROR_MESSAGE();
	END CATCH

	SET @batch_end_time = GETDATE();
	PRINT ''
	PRINT '***********************'
	PRINT '>> Loading Silver Layer Duaration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) as NVARCHAR) + ' s'
	PRINT '***********************'
END
