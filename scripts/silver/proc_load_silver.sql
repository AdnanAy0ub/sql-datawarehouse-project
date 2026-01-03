/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/

Create or alter procedure silver.load_silver as
begin
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; 
		BEGIN TRY
			SET @batch_start_time = GETDATE();
			PRINT '================================================';
			PRINT 'Loading Silver Layer';
			PRINT '================================================';

			PRINT '------------------------------------------------';
			PRINT 'Loading CRM Tables';
			PRINT '------------------------------------------------';

	
		-- Clean and load crm_cust_info --
		 SET @start_time = GETDATE();
		Print'>> Truncating table: silver.crm_cust_info';
		truncate table silver.crm_cust_info;
		Print '>> inserting data into: silver.crm_cust_info';
		insert into silver.crm_cust_info (
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_marital_status,
			cst_gndr,
			cst_create_date)

		select cst_id,
		cst_key,
		Trim(cst_firstname) as cst_firstname,
		Trim(cst_lastname) as cst_lastname,
		case when Upper(trim(cst_marital_status)) = 'M' then 'Married'
			 when Upper(trim(cst_marital_status)) = 'S' then 'Single'
			 else 'n/a'
		end cst_marital_status,
		case when lower(trim(cst_gndr)) ='f' then 'Female'
			 when lower(trim(cst_gndr)) = 'm' then 'Male'
			 else 'n/a'
		end as cst_gndr,
		cst_create_date
		from (
			select 
			*,
			row_number () over (partition by cst_id order by cst_create_date desc) as flag_last
			from bronze.crm_cust_info 
			where cst_id is not null
		)t where flag_last =1;
	SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';


		-- Clean and load crm_prd_info --
	 SET @start_time = GETDATE();
		Print'>> Truncating table: silver.crm_prd_info';
		truncate table silver.crm_prd_info;
		Print '>> inserting data into: silver.crm_prd_info';
		insert into silver.crm_prd_info (
			prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt
		)
		select  
		prd_id,
		replace(substring(prd_key,1,5),'-','_') as cat_id,
		substring(prd_key,7,len(prd_key)) as prd_key,
		prd_nm,
		Coalesce(prd_cost,0) as prd_cost,
		case Upper(trim(prd_line)) 
			 when 'M' then 'Mountain'
			 when 'R' then 'Road'
			 when 'S' then 'Other Sales'
			 when 'T' then 'Touring'
			 else 'n/a'
		End as prd_line, 
		prd_start_dt,
		dateadd(day,-1,lead(prd_start_dt) over (partition by prd_key order by prd_start_dt)
		)as prd_end_dt -- Calculate end date as one day before the next starting date.

		from bronze.crm_prd_info;
	 SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';


		-- Clean and load crm.sales_details -- 
	 SET @start_time = GETDATE();
		Print'>> Truncating table: silver.crm_sales_details';
		truncate table silver.crm_sales_details;
		Print '>> inserting data into: silver.crm_sales_details';
		INSERT INTO silver.crm_sales_details (
					sls_ord_num,
					sls_prd_key,
					sls_cust_id,
					sls_order_dt,
					sls_ship_dt,
					sls_due_dt,
					sls_sales,
					sls_quantity,
					sls_price
				)
				SELECT 
					sls_ord_num,
					sls_prd_key,
					sls_cust_id,
					CASE 
						WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
						ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
					END AS sls_order_dt,
					CASE 
						WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
						ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
					END AS sls_ship_dt,
					CASE 
						WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
						ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
					END AS sls_due_dt,
					CASE 
						WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) 
							THEN sls_quantity * ABS(sls_price)
						ELSE sls_sales
					END AS sls_sales, -- Recalculate sales if original value is missing or incorrect
					sls_quantity,
					CASE 
						WHEN sls_price IS NULL OR sls_price <= 0 
							THEN sls_sales / NULLIF(sls_quantity, 0)
						ELSE sls_price  -- Derive price if original value is invalid
					END AS sls_price
				FROM bronze.crm_sales_details;
		SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

		PRINT '------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '------------------------------------------------';


		-- Clean and load erp_cust_az12 --
	 SET @start_time = GETDATE();
		Print'>> Truncating table: silver.erp_cust_az12';
		truncate table silver.erp_cust_az12;
		Print '>> inserting data into: silver.erp_cust_az12';
		 INSERT INTO silver.erp_cust_az12 (
					cid,
					bdate,
					gen
				)
				SELECT
					CASE
						WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) -- Remove 'NAS' prefix if present
						ELSE cid
					END AS cid, 
					CASE
						WHEN bdate > GETDATE() THEN NULL
						ELSE bdate
					END AS bdate, -- Set future birthdates to NULL
					CASE
						WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
						WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
						ELSE 'n/a'
					END AS gen -- Normalize gender values and handle unknown cases
				FROM bronze.erp_cust_az12;
		 SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';


		-- Clean and load erp-loc_a101 --
	 SET @start_time = GETDATE();
		Print'>> Truncating table: silver.erp_loc_a101';
		truncate table silver.erp_loc_a101;
		Print '>> inserting data into: silver.erp_loc_a101';
		INSERT INTO silver.erp_loc_a101 (
					cid,
					cntry
				)
				SELECT
					REPLACE(cid, '-', '') AS cid, 
					CASE
						WHEN TRIM(cntry) = 'DE' THEN 'Germany'
						WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
						WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
						ELSE TRIM(cntry)
					END AS cntry -- Normalize and Handle missing or blank country codes
				FROM bronze.erp_loc_a101;
		 SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';


		-- Clean and load erp_px_cat_g1v2 --
	 SET @start_time = GETDATE();

		Print'>> Truncating table: silver.erp_px_cat_g1v2';
		truncate table silver.erp_px_cat_g1v2;
		Print '>> inserting data into: silver.erp_px_cat_g1v2';
		INSERT INTO silver.erp_px_cat_g1v2 (
					id,
					cat,
					subcat,
					maintenance
			)
			SELECT
					id,
					cat,
					subcat,
					maintenance
			FROM bronze.erp_px_cat_g1v2;
		 SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';
	SET @batch_end_time = GETDATE();
		PRINT '=========================================='
		PRINT 'Loading Silver Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '=========================================='
		
	END TRY
	BEGIN CATCH
		PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
	END CATCH
END 

Exec silver.load_silver; -- execute thee stored procedure --
