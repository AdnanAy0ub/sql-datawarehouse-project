/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data (This refreshes the table with updated data from the files).
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/
create or alter procedure bronze.load_bronze as
begin
	declare @start_time DATETIME, @end_time DATETIME, @start_batch DATETIME, @end_batch DATETIME;
	begin try
		set @start_batch = Getdate();
		print '===================================================================================';
		print 'Loading Bronze Layer';
		print '===================================================================================';


		print '-----------------------------------------------------------------------------------';
		print ' CRM Tables Loading';
		print '-----------------------------------------------------------------------------------';
		
		set @start_time = Getdate();
		print '>>Truncating table bronze.crm_cust_info';
		truncate table bronze.crm_cust_info; -- helps in refreshing the table i.e load new updated data from the file--

		print '>>inserting data into:bronze.crm_cust_info';
		Bulk insert bronze.crm_cust_info
		from 'A:\Projects\DWH barra\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		with (
			Firstrow = 2,
			fieldterminator = ',',
			TABLOCK
		);
		set @end_time = Getdate();
		print '>> Load Duration:' + cast(Datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';
		print '----------------------------------------------------------';
		set @start_time = Getdate();
		print '>>Truncating table bronze.crm_prd_info';
		truncate table bronze.crm_prd_info;

		print '>>inserting data into:bronze.crm_prd_info';
		Bulk insert bronze.crm_prd_info
		from 'A:\Projects\DWH barra\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		with (
			Firstrow = 2,
			fieldterminator = ',',
			TABLOCK
		);
		set @end_time = Getdate();
		print '>> Load Duration:' + cast(Datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';
		print '----------------------------------------------------------';

		set @start_time = Getdate();
		print '>>Truncating table bronze.crm_sales_details';
		truncate table bronze.crm_sales_details;

		print '>>inserting data into:bronze.crm_sales_details';
		Bulk insert bronze.crm_sales_details
		from 'A:\Projects\DWH barra\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		with (
			Firstrow = 2,
			fieldterminator = ',',
			TABLOCK
		);
		set @end_time = Getdate();
		print '>> Load Duration:' + cast(Datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';



		print '-----------------------------------------------------------------------------------';
		print ' ERP Tables Loading';
		print '-----------------------------------------------------------------------------------'; 
		print '>>Truncating table bronze.erp_cust_az12';

		set @start_time = Getdate();
		truncate table bronze.erp_cust_az12;

		print '>>inserting data into:bronze.erp_cust_az12';
		Bulk insert bronze.erp_cust_az12
		from 'A:\Projects\DWH barra\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		with (
			Firstrow = 2,
			fieldterminator = ',',
			TABLOCK
		);
		set @end_time = Getdate();
		print '>> Load Duration:' + cast(Datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';
		print '----------------------------------------------------------';

		set @start_time = Getdate();
		print '>>Truncating table bronze.erp_loc_a101';
	
		truncate table bronze.erp_loc_a101;
		print '>>inserting data into:bronze.erp_loc_a101';
		Bulk insert bronze.erp_loc_a101
		from 'A:\Projects\DWH barra\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		with (
			Firstrow = 2,
			fieldterminator = ',',
			TABLOCK
		);
		set @end_time = Getdate();
		print '>> Load Duration:' + cast(Datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';
		print '----------------------------------------------------------';

		set @start_time = Getdate();
		print '>>Truncating table bronze.erp_px_cat_g1v2';
	
		truncate table bronze.erp_px_cat_g1v2;
		print '>>inserting data into:bronze.erp_px_cat_g1v2';
		Bulk insert bronze.erp_px_cat_g1v2
		from 'A:\Projects\DWH barra\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		with (
			Firstrow = 2,
			fieldterminator = ',',
			TABLOCK
		);
		set @end_time = Getdate();
		print '>> Load Duration:' + cast(Datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';
		print '----------------------------------------------------------';

		set @end_batch= Getdate();
		print'~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~';
		print 'Bronze layer loading completed';
		print 'Bronze Layer loaded in ' + Cast(datediff(second, @start_batch, @end_batch) as nvarchar) + 'seconds';
	end try
	begin catch
		Print'xxxxx Error while loading Bronze Layer xxxxx';
		print'Error message' + Error_message();
	end catch
End;

Exec bronze.load_bronze; -- Execute the stored procedure in order to check for reliability--
