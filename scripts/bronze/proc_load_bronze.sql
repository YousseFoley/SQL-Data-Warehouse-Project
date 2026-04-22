/*
TRUNCATE: Quickly delete all rows from table, resetting it to an empty state
With Clause is for specifications
TABLOCK: Locks the file while loading it into DB - Enhances the performance -
Quality check: SELECT statement checks that the data has not shifted and is in the correct columns
This is common with CSV files due to the structure of the table or the delemiter
================================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
================================================================================

Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files.
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from CSV files to bronze tables.

Parameters:
    None.
    This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;

================================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
	SET @batch_start_time = GETDATE();
		PRINT'=====================================';
		PRINT'Loading Bronze Layer';
		PRINT'=====================================';

		PRINT'-------------------------------------';
		PRINT'Loading CRM Tables';
		PRINT'-------------------------------------';

		SET @start_time = GETDATE();
		PRINT'>> Truncating Table: bronze.crm_cust_info ';
		TRUNCATE TABLE bronze.crm_cust_info;

		PRINT'>> Inserting Data Into: bronze.crm_cust_info ';
		BULK INSERT bronze.crm_cust_info
		FROM 'D:\YF\DATA ANALYST CORE\SELF TRACK\Data Warehouse\Extracted Zip\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK 
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' Seconds';
		PRINT '---------------------------------------';

		SELECT COUNT(*)
		FROM bronze.crm_cust_info

		SET @start_time = GETDATE();
		PRINT'>> Truncating Table: bronze.crm_prd_info ';
		TRUNCATE TABLE bronze.crm_prd_info;

		PRINT'>> Inserting Data Into: bronze.crm_prd_info ';
		BULK INSERT bronze.crm_prd_info
		FROM 'D:\YF\DATA ANALYST CORE\SELF TRACK\Data Warehouse\Extracted Zip\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' Seconds';
		PRINT '---------------------------------------';

		SELECT COUNT(*)
		FROM bronze.crm_prd_info

		SET @start_time = GETDATE();
		PRINT'>> Truncating Table: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;

		PRINT'>> Inserting Data Into: bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'D:\YF\DATA ANALYST CORE\SELF TRACK\Data Warehouse\Extracted Zip\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' Seconds';
		PRINT '---------------------------------------';

		SELECT COUNT(*)
		FROM bronze.crm_sales_details

		PRINT'-------------------------------------';
		PRINT'Loading ERP Tables';
		PRINT'-------------------------------------';

		SET @start_time = GETDATE();
		PRINT'>> Truncating Table: bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;

		PRINT'>> Inserting Data Into: bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'D:\YF\DATA ANALYST CORE\SELF TRACK\Data Warehouse\Extracted Zip\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' Seconds';
		PRINT '---------------------------------------';

		SELECT COUNT(*)
		FROM bronze.erp_cust_az12

		SET @start_time = GETDATE();
		PRINT'>> Truncating Table: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;

		PRINT'>> Inserting Data Into: bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'D:\YF\DATA ANALYST CORE\SELF TRACK\Data Warehouse\Extracted Zip\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' Seconds';
		PRINT '---------------------------------------';
		SELECT COUNT(*)
		FROM bronze.erp_loc_a101

		SET @start_time = GETDATE();
		PRINT'>> Truncating Table: bronze.erp_px_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;

		PRINT'>> Inserting Data Into Table: bronze.erp_px_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'D:\YF\DATA ANALYST CORE\SELF TRACK\Data Warehouse\Extracted Zip\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' Seconds';
		PRINT '>>---------------------------------';

		SET @batch_end_time = GETDATE();
		PRINT '===================================';
		PRINT ' Loading Bronze Layer Is Complete';
		PRINT 'Total Load Duration: ' + CAST(DATEDIFF(SECOND,@batch_start_time,@batch_end_time) AS NVARCHAR) + 'second';
		PRINT '===================================';

		SELECT COUNT(*)
		FROM bronze.erp_px_cat_g1v2
	END TRY
	BEGIN CATCH
		PRINT '=================================================';
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
		PRINT 'ERROR Message' + ERROR_MESSAGE();
		PRINT 'ERROR Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT'==================================================';
	END CATCH
END
