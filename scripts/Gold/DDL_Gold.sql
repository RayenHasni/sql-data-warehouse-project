/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/
IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO
CREATE VIEW gold.dim_customers AS
SELECT 
	ROW_NUMBER() OVER (ORDER BY cst_id) as customer_key,
	ci.cst_id as customer_id,
	ci.cst_key as customer_number,
	ci.cst_firstname as first_name,
	ci.cst_lastname as last_name,
	cl.cntry as country,
	CASE WHEN cst_gndr != 'n/a' THEN cst_gndr -- CRM is the master for gender info.
		 ELSE COALESCE(ca.gen, 'n/a') -- Fallback to ERP data
	END AS gender,
	ci.cst_marital_status as marital_status,
	ca.bdate as birthdate,
	ci.cst_create_date as create_date
	
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 cl
ON ci.cst_key = cl.cid

-----------------------------------------
IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
    DROP VIEW gold.dim_products;
GO
CREATE VIEW gold.dim_products AS
SELECT
	ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) as product_key,
	pn.prd_id       AS product_id,
    pn.prd_key      AS product_number,
    pn.prd_nm       AS product_name,
    pn.cat_id       AS category_id,
    pc.cat          AS category,
    pc.subcat       AS subcategory,
    pc.maintenance  AS maintenance,
    pn.prd_cost     AS cost,
    pn.prd_line     AS product_line,
    pn.prd_start_dt AS start_date

FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON pn.cat_id = pc.id
WHERE prd_end_dt IS NULL --Filter out all historical data

-----------------------------------------
IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;
GO
CREATE VIEW gold.fact_sales AS
SELECT 
	sd.sls_ord_num as order_number,
	pr.product_key, --Surrogate Key from product_dim
	cu.customer_key, --Surrogate Key from customer_dim
	sd.sls_order_dt as order_date,
	sd.sls_ship_dt as shipping_date,
	sd.sls_due_dt as due_date,
	sd.sls_sales as sales_amount,
	sd.sls_quantity as quantity,
	sd.sls_price as price

FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cu
ON sd.sls_cust_id = cu.customer_id
