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

-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================
IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO

-- creating a virtual table: gold.dim_customers, for gold layer
CREATE VIEW gold.dim_customers AS
-- joining the tables that have customer info
SELECT
ROW_NUMBER() OVER (ORDER BY cust_i.cst_id) AS customer_key,
cust_i.cst_id AS customer_id,
cust_i.cst_key AS customer_number,
cust_i.cst_firstname AS first_name,
cust_i.cst_lastname AS last_name,
-- aggregating two gender columns into one: new_gen
CASE WHEN cust_i.cst_gndr != 'n/a' THEN cust_i.cst_gndr
	 ELSE COALESCE(cust_a.gen, 'n/a')
END AS gender,
cust_l.cntry AS country,
cust_i.cst_marital_status AS marital_status,
cust_a.bdate AS birthday,
cust_i.cst_create_date AS create_date

FROM silver.crm_cust_info AS cust_i

LEFT JOIN silver.erp_cust_az12 AS cust_a
ON cust_i.cst_key = cust_a.cid -- left join with silver.erp_cust_az12

LEFT JOIN silver.erp_loc_a101 AS cust_l
ON cust_i.cst_key = cust_l.cid -- left join with silver.erp_loc_a101
GO

-- =============================================================================
-- Create Dimension: gold.dim_products
-- =============================================================================
IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
    DROP VIEW gold.dim_products;
GO
	
-- creating a virtual table: gold.dim_products, for gold layer
CREATE VIEW gold.dim_products AS
-- joining the tables that have product info
SELECT
ROW_NUMBER() OVER (ORDER BY prod_i.prd_start_dt, prod_i.prd_key) AS product_key, 
prod_i.prd_id AS product_id,
prod_i.prd_key AS product_number,
prod_i.prd_nm AS product_name,
prod_i.cat_id AS category_id,
prod_c.cat AS category,
prod_c.subcat AS sub_category,
prod_i.prd_cost AS cost,
prod_i.prd_line AS product_line,
prod_c.maintenance,
prod_i.prd_start_dt AS start_date
FROM silver.crm_prd_info AS prod_i
LEFT JOIN silver.erp_px_cat_g1v2 AS prod_c -- left join with silver.crm_prd_info
ON prod_i.cat_id = prod_c.id
WHERE prd_end_dt IS NULL -- filtering out historical data
GO

-- =============================================================================
-- Create Dimension: gold.fact_sales
-- =============================================================================
IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;
GO

-- creating a virtual table: gold.fact_sales, for gold layer
CREATE VIEW gold.fact_sales AS
-- joining the tables that have sales info
SELECT
prod.product_key,
cust.customer_key,
sls.sls_ord_num AS order_number,
sls.sls_order_dt AS order_date,
sls.sls_ship_dt AS shipping_date,
sls.sls_due_dt AS due_date,
sls.sls_sales AS sales_amount,
sls.sls_quantity AS quantity,
sls.sls_price AS price
FROM silver.crm_sales_details AS sls
LEFT JOIN gold.dim_customers AS cust -- left join with gold.dim_customers
ON sls.sls_cust_id = cust.customer_id

LEFT JOIN gold.dim_products AS prod -- left join with gold.dim_products
ON sls.sls_prd_key = prod.product_number
GO
