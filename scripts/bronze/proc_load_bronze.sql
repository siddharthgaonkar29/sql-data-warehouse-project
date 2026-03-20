/*
=====================================================================================================
Stored Procedure for Data Ingestion to Bronze Layer
=====================================================================================================
Purpose of the script :
	The purpose of the script is to provide and standard code that will first truncate the avaialable rows in the table.
	Later it will Bulk insert all the data from the CSV file present in each of the sources 'CRM' and 'ERP'. 
	The script also has the functionality to time the Data load for the batch. Also it takes care for error handling. 
	This procedure does not accept any parameter nor does it return any value. 

	Eg.usage : EXEC bronze.load_bronze
*/


CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN 
	
	BEGIN TRY
		PRINT '============================================================='
		PRINT 'LOADING BRONZE LAYER'
		PRINT '============================================================='

		PRINT '>>>>>>  LOADING FROM SOURCE CRM <<<<<<<<'
		
		DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;

		SET @batch_start_time = SYSDATETIME();
		
		SET @start_time = SYSDATETIME();
		
		TRUNCATE TABLE bronze.crm_cust_info;

		BULK INSERT bronze.crm_cust_info
		FROM 'D:\Data With Baraa\First Data Warehouse Project\Data\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
			);


		TRUNCATE TABLE bronze.crm_prd_info;

		BULK INSERT bronze.crm_prd_info
		FROM 'D:\Data With Baraa\First Data Warehouse Project\Data\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
			);

		TRUNCATE TABLE bronze.crm_sales_details;

		BULK INSERT bronze.crm_sales_details
		FROM 'D:\Data With Baraa\First Data Warehouse Project\Data\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
			);
		SET @end_time = SYSDATETIME();
		

		PRINT'CRM TABLE LOAD DURATION = '+CAST(DATEDIFF(second,@end_time, @start_time) AS VARCHAR) +' seconds'

		PRINT '>>>>>>  LOADING FROM SOURCE ERP <<<<<<<<'

		SET @start_time = SYSDATETIME();

		TRUNCATE TABLE bronze.erp_cust_az12;
		BULK INSERT bronze.erp_cust_az12
		FROM 'D:\Data With Baraa\First Data Warehouse Project\Data\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
			);

		TRUNCATE TABLE bronze.erp_loc_a101;
		BULK INSERT bronze.erp_loc_a101
		FROM 'D:\Data With Baraa\First Data Warehouse Project\Data\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
			);

		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'D:\Data With Baraa\First Data Warehouse Project\Data\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
			);
		SET @end_time = SYSDATETIME();

		PRINT'ERP TABLE LOAD DURATION = '+CAST(DATEDIFF(second,@end_time, @start_time) AS VARCHAR)+' seconds'

		SET @batch_end_time = SYSDATETIME();
		PRINT'BRONZE LOAD DURATION = '+ CAST(DATEDIFF(second,@batch_end_time, @batch_start_time) AS VARCHAR)+' seconds'

	END TRY
	
	BEGIN CATCH
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Number' + CAST (ERROR_NUMBER() AS VARCHAR);
		PRINT 'Error State' + CAST (ERROR_STATE() AS VARCHAR);
	END CATCH
END
