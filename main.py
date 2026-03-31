import logging

from api_extract import fetch_api_store_data, save_raw_data
from snowflake_loader import load_to_snowflake
from validate_db_load import validate_record_count

def run_pipeline():

	products_df = fetch_api_store_data("products")
	print(products_df.shape)
	if not products_df.empty:
		logging.info(products_df.head())

	save_raw_data(products_df, "products_raw.csv")

	# upload_to_snowflake(products_df, "products_table")
	
	load_to_snowflake("products_raw.csv", "STG_PRODUCTS_RAW", "api_landing_zone")
	validate_record_count("STG_PRODUCTS_RAW")

if __name__ == "__main__":
	run_pipeline()
