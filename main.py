import logging

from api_extract import fetch_api_store_data, save_raw_data
from snowflake_loader import load_to_snowflake
from validate_db_load import validate_record_count

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

if __name__ == "__main__":
	run_pipeline()
