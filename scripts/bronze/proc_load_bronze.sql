/*
================================================================================================================
STORED PROCEDURE : Load Bronze Layer(Source -> Bronze)
================================================================================================================
Script Purpose:
      This stored procedure loads data into the 'bronze' Schema from external CSV files 
       It performs the following actions:
         -Truncate the bronze tables before loading data 
          -use the 'BULKINSERT' command  to load data from csv files to bronze tables 

PARAMETERS:
        none
        this stored procedure does not accept any parameters or return any values 

USAGE EXAMPLE:
          exec bronze.load_bronze
===============================================================================================================================================

*/

create or alter procedure bronze.load_bronze as 
begin 
    declare @start_time datetime,@end_time datetime,@batch_start_time datetime,@batch_end_time datetime;
	begin try
	    set @batch_start_time=getdate();
		print'==============================================';
		print 'Loading Bronze Layer';
		print'==============================================';

		print'==============================================';
		print 'Loading CRM Tables';
		print'==============================================';



		set @start_time=getdate();
		
		print '>>>Truncating Table:bronze.crm_cust_info';
		truncate table bronze.crm_cust_info
		print '>>>Inserting Data into:bronze.crm_cust_info';
		bulk insert bronze.crm_cust_info 
		from 'C:\Users\kanis\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		with(
		firstrow=2,
		fieldterminator=',',
		tablock);
		set @end_time=getdate();
		print 'Loading Time '+ cast(datediff(second,@start_time,@end_time) as nvarchar) + ' Seconds';
		print '>>>>>>>>>>>>>>>>>><<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<';
		--if we again run this bulk statement it again inserts rows so we get duplicates rows 
		--thats why we follow truncate and insert method first we nned to trunc the table then only insert values into it 


		select * from bronze.crm_cust_info
		select count(*) from bronze.crm_cust_info

		--=============================================================================
		--=============================================================================
		--==============================================================================
		set @start_time=getdate();
		print '>>>Truncating Table:bronze.crm_prd_info';
		truncate table bronze.crm_prd_info
		print '>>>Inserting Data into:bronze.crm_prd_info ';
		bulk insert bronze.crm_prd_info 
		from 'C:\Users\kanis\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		with(
		firstrow=2,
		fieldterminator=',',
		tablock);
		set @end_time=getdate();
		print 'Loading Time '+ cast(datediff(second,@start_time,@end_time) as nvarchar) + ' Seconds';
		print '>>>>>>>>>>>>>>>>>><<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<';

		select * from bronze.crm_prd_info
		select count(*) from bronze.crm_prd_info

		--=============================================================================
		--=============================================================================
		--==============================================================================
		set @start_time=getdate();
		print '>>>Truncating Table:bronze.crm_sales_details';
		truncate table bronze.crm_sales_details
		print '>>>Inserting Data into:bronze.crm_sales_details ';
		bulk insert bronze.crm_sales_details
		from 'C:\Users\kanis\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		with(
		firstrow=2,
		fieldterminator=',',
		tablock);
		set @end_time=getdate();
		print 'Loading Time '+ cast(datediff(second,@start_time,@end_time) as nvarchar) + ' Seconds';
		print '>>>>>>>>>>>>>>>>>><<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<';

		select * from bronze.crm_sales_details
		select count(*) from bronze.crm_sales_details


		--=============================================================================
		--=============================================================================
		--==============================================================================

		print'==============================================';
		print 'Loading ERP Tables';
		print'==============================================';
		set @start_time=getdate();
		print '>>>Truncating Table:bronze.erp_cust_az12';
		truncate table bronze.erp_cust_az12
		print '>>>Inserting Data into:bronze.erp_cust_az12 ';
		bulk insert bronze.erp_cust_az12
		from 'C:\Users\kanis\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		with(
		firstrow=2,
		fieldterminator=',',
		tablock);
		set @end_time=getdate();
		print 'Loading Time '+ cast(datediff(second,@start_time,@end_time) as nvarchar) + ' Seconds';
		print '>>>>>>>>>>>>>>>>>><<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<';

		select * from bronze.erp_cust_az12
		select count(*) from bronze.erp_cust_az12


		--=============================================================================
		--=============================================================================
		--==============================================================================
		set @start_time=getdate();
		print '>>>Truncating Table:bronze.erp_loc_a101';
		truncate table bronze.erp_loc_a101
		print '>>>Inserting Data into:bronze.erp_loc_a101 ';
		bulk insert bronze.erp_loc_a101
		from 'C:\Users\kanis\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		with(
		firstrow=2,
		fieldterminator=',',
		tablock);
		set @end_time=getdate();
		print 'Loading Time '+ cast(datediff(second,@start_time,@end_time) as nvarchar) + ' Seconds';
		print '>>>>>>>>>>>>>>>>>><<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<';

		select * from bronze.erp_loc_a101
		select count(*) from bronze.erp_loc_a101




		--=============================================================================
		--=============================================================================
		--==============================================================================
		set @start_time=getdate();
		print '>>>Truncating Table:bronze.erp_px_cat_g1v2';
		truncate table bronze.erp_px_cat_g1v2
		print '>>>Inserting Data into:bronze.erp_px_cat_g1v2 ';
		bulk insert bronze.erp_px_cat_g1v2
		from 'C:\Users\kanis\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		with(
		firstrow=2,
		fieldterminator=',',
		tablock);
		set @end_time=getdate();
		print 'Loading Time '+ cast(datediff(second,@start_time,@end_time) as nvarchar) + ' Seconds';
		print '>>>>>>>>>>>>>>>>>><<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<';

		select * from bronze.erp_px_cat_g1v2
		select count(*) from bronze.erp_px_cat_g1v2

		set @batch_end_time=getdate();

		--we create a stored procedure for inserting these values 
		print '[[[[[[[[[[[[[[[[[[Loading Bronze Layer is completed]]]]]]]]]]]]]]]]]]]'
		print 'the entire duration for bronze layer is'+cast(datediff(second,@batch_start_time,@batch_end_time) as nvarchar)+' seconds';
		print '[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]'
	end try
	begin catch
	print '============================================';
	print 'Error Occured During Bronze Layer Loading';
	print 'Error Message '+error_message();
	print 'Error Message '+cast(error_number() as nvarchar);
	print 'Error Message '+cast(error_state() as nvarchar);
	print '============================================';

	end catch
end

exec bronze.load_bronze 
