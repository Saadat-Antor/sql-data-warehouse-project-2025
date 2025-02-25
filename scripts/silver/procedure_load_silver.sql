-- =============== silver.crm_cust_info ===============
-- inserting data into the table
INSERT INTO silver.crm_cust_info(
	cst_id,
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_marital_status,
	cst_gndr,
	cst_create_date)

SELECT
cst_id,
cst_key,
-- removing unwanted spaces in cst_firstname and cst_lastname
TRIM(cst_firstname) AS cst_firstname,
TRIM(cst_lastname) AS cst_lastname,
-- incresing verbocity on cst_marital_status, ensuring standardization
CASE WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
	 WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
	 ELSE 'n/a' 
END cst_marital_status,
-- incresing verbocity on cst_gndr, ensuring standardization
CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
	 WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
	 ELSE 'n/a'
END cst_gndr,
cst_create_date
FROM (SELECT
*,
-- using ROW_NUMBER() to rank the duplicates
ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
FROM bronze.crm_cust_info
WHERE cst_id IS NOT NULL -- handling NULL values
)t
WHERE flag_last = 1; -- keeping records that ranks first

-- =============== silver.crm_prd_info ===============
-- inserting the cleaned data into the table
INSERT INTO silver.crm_prd_info(
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
-- splitting columns and creating cat_id and prd_key
REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
prd_nm,
-- replacing NULLs with 0s in prd_cost
ISNULL(prd_cost, 0) AS prd_cost,
-- incresing verbocity on prd_line, ensuring standardization
CASE UPPER(TRIM(prd_line))
	 WHEN 'M' THEN 'Mountain'
	 WHEN 'R' THEN 'Road'
	 WHEN 'S' THEN 'Other sales'
	 WHEN 'T' THEN 'Touring'
END prd_line,
-- data type casting and handling date discrepencies
CAST(prd_start_dt AS DATE) AS prd_start_dt,
CAST(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS DATE) AS prd_end_dt
FROM bronze.crm_prd_info

SELECT * FROM silver.crm_prd_info
