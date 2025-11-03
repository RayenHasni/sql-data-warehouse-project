/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy, 
    and standardization across the 'silver' layer. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks after data loading Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/

-- ====================================================================
-- Checking 'silver.crm_cust_info'
-- ====================================================================

-- 1. Check for nulls or duplicates in primary key
-- Expectation : No Result
SELECT cst_id, count(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING count(*) > 1 OR cst_id is NULL

-- 2. Check for unwanted spaces
-- Expectation : No Result
SELECT cst_firstname 
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)

-- 3. Data Standardization
SELECT DISTINCT cst_gndr 
FROM silver.crm_cust_info

SELECT * FROM silver.crm_cust_info

-- ====================================================================
-- Checking 'silver.crm_prd_info'
-- ====================================================================
--  1. Check for NULLs or Duplicates in Primary Key
-- Expectation: No Results
SELECT 
    prd_id,
    COUNT(*) 
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1

-- 2. Check for Nulls or Negative Numbers
-- Expectation : No Result
SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 or prd_cost is null

-- 3. Data Standardization
SELECT DISTINCT prd_line 
FROM silver.crm_prd_info

-- 4. Check for Wrong Date Order
SELECT * FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt

-- ====================================================================
-- Checking 'silver.crm_sales_details'
-- ====================================================================
--  1. Check for Invalid Dates
-- Expectation: No Invalid Dates
SELECT sls_order_dt
FROM silver.crm_sales_details
WHERE sls_order_dt <= 0
or len(sls_order_dt) != 8
or sls_order_dt < '19000101'

-- 2. Check for Invalid Date Orders (Order Date > Shipping/Due Dates)
-- Expectation: No Results
SELECT *
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt or sls_order_dt > sls_due_dt

-- Check Data Consistency: Sales = Quantity * Price
-- Expectation: No Results
SELECT
	sls_sales,
	sls_quantity,
	sls_price
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
or sls_sales is null or sls_quantity is null or sls_price is null
or sls_sales < 0 or sls_quantity < 0 or sls_price < 0
ORDER BY sls_sales

-- ====================================================================
-- Checking 'silver.erp_loc_a101'
-- ====================================================================
--  1. Data Standardization and Consistency
SELECT DISTINCT cntry 
FROM silver.erp_loc_a101


-- ====================================================================
-- Checking 'silver.erp_cust_az12'
-- ====================================================================
--  1. Check for Out Of Range Dates
-- Expectation: No Result
SELECT bdate
FROM silver.erp_cust_az12
WHERE bdate > GETDATE()

-- 2. Data Standardization and Consistency
SELECT DISTINCT gen 
FROM silver.erp_cust_az12


-- ====================================================================
-- Checking 'silver.erp_px_cat_g1v2'
-- ====================================================================
--  1. Check for unwanted spaces
-- Expectations : No result
SELECT * 
FROM silver.erp_px_cat_g1v2 
WHERE cat != trim(cat) or subcat != TRIM(subcat) or maintenance != TRIM(maintenance)

-- 2. Data Standardization and Consistency
SELECT DISTINCT cat, subcat, maintenance
FROM silver.erp_px_cat_g1v2 
