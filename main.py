import logging

from api_extract import fetch_api_store_data, save_raw_data
from snowflake_loader import load_to_snowflake, load_sales_to_snowflake
from validate_db_load import validate_record_count, validate_star_schema
from run_proc import run_transformation
from generate_sales import generate_sales_data
from create_views import create_reporting_views

def run_pipeline():

# Exract data from FakeStore API and save to a local CSV file
	products_df = fetch_api_store_data("products")
	print(products_df.shape)
	if not products_df.empty:
		logging.info(products_df.head())

	save_raw_data(products_df, "products_raw.csv")

# Load the CSV file to Snowflake
	load_to_snowflake("products_raw.csv", "STG_PRODUCTS_RAW", "api_landing_zone")
	
# Validate the data load by counting records in the target Snowflake table
	validate_record_count("STG_PRODUCTS_RAW")

# Run Merge procedure to move data from staging to final table
	run_transformation()

# Generate synthetic sales data and save to CSV
	sales_df = generate_sales_data(5000)
	save_raw_data(sales_df, "sales_raw.csv")

# LOAD the sales data to Snowflake
	load_sales_to_snowflake("sales_raw.csv", "STG_SALES_RAW", "api_landing_zone", "FACT_SALES")

# Create reporting views in Snowflake for analytics
	create_reporting_views()

# Validate the sales data load by counting records in the target Snowflake table
	validate_record_count("STG_SALES_RAW")		

# Validate the star schema structure by running a sample query that joins fact and dimension tables
	validate_star_schema()
	
if __name__ == "__main__":
	run_pipeline()
