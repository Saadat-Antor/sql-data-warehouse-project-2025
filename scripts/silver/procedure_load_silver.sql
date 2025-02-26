-- creating a procedure to load data to all the table
CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	BEGIN TRY
		DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
		SET @batch_start_time = GETDATE();
		PRINT '^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^';
		PRINT 'Loading the Silver Layer';
		PRINT '^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^';

		PRINT '+++++++++++++++++++++++++++++++++++++++++';
		PRINT 'Loading CRM Tables';
		PRINT '+++++++++++++++++++++++++++++++++++++++++';
		-- =============== silver.crm_cust_info ===============
		SET @start_time = GETDATE();
		PRINT('Truncating table: silver.crm_cust_info')
		TRUNCATE TABLE silver.crm_cust_info;
		PRINT('Loading data into table:silver.crm_cust_info')
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
		END AS cst_marital_status,
		-- incresing verbocity on cst_gndr, ensuring standardization
		CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
			 WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
			 ELSE 'n/a'
		END AS cst_gndr,
		cst_create_date
		FROM (SELECT
		*,
		-- using ROW_NUMBER() to rank the duplicates
		ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
		FROM bronze.crm_cust_info
		WHERE cst_id IS NOT NULL -- handling NULL values
		)t
		WHERE flag_last = 1; -- keeping records that ranks first
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time)	AS NVARCHAR) + ' seconds';


		-- =============== silver.crm_prd_info ===============
		SET @start_time = GETDATE();
		PRINT('Truncating table: silver.crm_prd_info')
		TRUNCATE TABLE silver.crm_prd_info;
		PRINT('Loading data into table:silver.crm_prd_info')
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
		END AS prd_line,
		-- data type casting and handling date discrepencies
		CAST(prd_start_dt AS DATE) AS prd_start_dt,
		CAST(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS DATE) AS prd_end_dt
		FROM bronze.crm_prd_info
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time)	AS NVARCHAR) + ' seconds';


		-- =============== silver.crm_sales_details ===============
		SET @start_time = GETDATE();
		PRINT('Truncating table: silver.crm_sales_details')
		TRUNCATE TABLE silver.crm_sales_details;
		PRINT('Loading data into table: silver.crm_sales_details')
		-- inserting data into the table
		INSERT INTO silver.crm_sales_details(
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
		-- checking sls_order_dt and double-casting data types
		CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
			 ELSE CAST(CAST(sls_order_dt AS NVARCHAR) AS DATE)
		END AS sls_order_dt,
		-- checking sls_ship_dt and double-casting data types
		CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
			 ELSE CAST(CAST(sls_ship_dt AS NVARCHAR) AS DATE)
		END AS sls_ship_dt,
		-- checking sls_due_dt and double-casting data types
		CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
			 ELSE CAST(CAST(sls_due_dt AS NVARCHAR) AS DATE)
		END AS sls_due_dt,
		CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
				THEN sls_quantity * ABS(sls_price)
			 ELSE sls_sales
		END AS sls_sales,
		sls_quantity,
		CASE WHEN sls_price IS NULL OR sls_price <= 0
				THEN sls_sales/NULLIF(sls_quantity,0)
			 ELSE sls_price
		END AS sls_price
		FROM bronze.crm_sales_details
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time)	AS NVARCHAR) + ' seconds';


		-- =============== silver.erp_cust_az12 ===============
		SET @start_time = GETDATE();
		PRINT('Truncating table: silver.erp_cust_az12')
		TRUNCATE TABLE silver.erp_cust_az12;
		PRINT('Loading data into table: silver.erp_cust_az12')
		-- inserting into the table
		INSERT INTO silver.erp_cust_az12 (
			cid,
			bdate,
			gen)
		SELECT
		-- removing extra letters from cid for joining purposes
		CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
			 ELSE cid
		END AS cid,
		-- making birth dates to NULL if dates are later than the current time
		CASE WHEN bdate > GETDATE() THEN NULL
			 ELSE bdate
		END AS bdate,
		-- increasing verbocity of gen, thus ensuring data standardization
		CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
			 WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
			 ELSE 'n/a'
		END AS gen
		FROM bronze.erp_cust_az12
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time)	AS NVARCHAR) + ' seconds';


		-- =============== silver.erp_loc_a101 ===============
		SET @start_time = GETDATE();
		PRINT('Truncating table: silver.erp_loc_a101')
		TRUNCATE TABLE silver.erp_loc_a101;
		PRINT('Loading data into table: silver.erp_loc_a101')
		-- inserting into the table
		INSERT INTO silver.erp_loc_a101 (
			cid,
			cntry)
		SELECT
		-- omitting the '-' from cid to match the cust_key primary key
		REPLACE(cid,'-','') AS cid,
		-- increasing verbocity and handle missing country name in cntry, ensuring data standardization
		CASE WHEN UPPER(TRIM(cntry)) = 'DE' THEN 'Germany'
			 WHEN UPPER(TRIM(cntry)) IN ('US', 'USA') THEN 'United States'
			 WHEN UPPER(TRIM(cntry)) = '' OR cntry IS NULL THEN 'n/a'
			 ELSE TRIM(cntry)
		END AS cntry
		FROM bronze.erp_loc_a101
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time)	AS NVARCHAR) + ' seconds';


		-- =============== silver.erp_px_cat_g1v2 ===============
		SET @start_time = GETDATE();
		PRINT('Truncating table: silver.erp_px_cat_g1v2')
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		PRINT('Loading data into table: silver.erp_px_cat_g1v2')
		-- inserting into the table
		INSERT INTO silver.erp_px_cat_g1v2(
			id,
			cat,
			subcat,
			maintenance)
		-- data quality is good, thus no transformation is required 
		SELECT
		id,
		cat,
		subcat,
		maintenance
		FROM bronze.erp_px_cat_g1v2
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time)	AS NVARCHAR) + ' seconds';
		PRINT('===============================')
		SET @batch_end_time = GETDATE();
		PRINT '>> Total Load Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time)	AS NVARCHAR) + ' seconds';
	END TRY
	BEGIN CATCH
		PRINT '******************************************';
		PRINT 'AN ERROR HAS OCCURED DURING THE LOADING OF THE SILVER LAYER'
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '******************************************'
	END CATCH
END
