import logging
from api_extract import fetch_api_store_data, save_raw_data
from snowflake_loader import load_to_snowflake, load_sales_to_snowflake
from run_proc import run_transformation
from generate_sales import generate_sales_data
from create_views import create_reporting_views
from validate_db_load import validate_record_count, validate_star_schema

logging.info("-----PIPELINE COMMENCING...-----")	
def run_pipeline():

###--------------------- STEP 1: EXTRACTION ---------------------###



# EXRACT DATA FROM FAKESTORE API AND SAVE TO A LOCAL CSV FILE
	products_df = fetch_api_store_data("products")
	print(products_df.shape)
	if not products_df.empty:
		logging.info(products_df.head())

	save_raw_data(products_df, "products_raw.csv")
	
###--------------------- STEP 2: LOAD ---------------------###

# LOAD THE CSV FILE TO SNOWFLAKE
	load_to_snowflake("products_raw.csv", "STG_PRODUCTS_RAW", "api_landing_zone")
	

###--------------------- STEP 3: TRANSFORMATION ---------------------###

# RUN MERGE PROCEDURE TO MOVE DATA FROM STAGING TO CORE TABLE
	run_transformation()

# GENERATE SYNTHETIC SALES DATA AND SAVE TO CSV
	sales_df = generate_sales_data(5000)
	save_raw_data(sales_df, "sales_raw.csv")

# UPLOAD THE SALES DATA TO SNOWFLAKE
	load_sales_to_snowflake("sales_raw.csv", "STG_SALES_RAW", "api_landing_zone", "FACT_SALES")

# CREATE REPORTING VIEWS IN SNOWFLAKE FOR ANALYTICS
	create_reporting_views()

###--------------------- STEP 4: VALIDATION ---------------------###

# VALIDATE THE DATA LOAD BY COUNTING RECORDS IN THE TARGET SNOWFLAKE TABLE
	validate_record_count("STG_PRODUCTS_RAW")

# VALIDATE THE SALES DATA LOAD BY COUNTING RECORDS IN THE TARGET SNOWFLAKE TABLE
	validate_record_count("STG_SALES_RAW")		

# VALIDATE THE STAR SCHEMA STRUCTURE BY RUNNING A SAMPLE QUERY THAT JOINS FACT AND DIMENSION TABLES
	validate_star_schema()



if __name__ == "__main__":
	run_pipeline()
	logging.info("-----PIPELINE EXECUTION COMPLETED SUCCESSFULLY-----!")