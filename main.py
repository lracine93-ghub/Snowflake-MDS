import logging

from api_extract import fetch_api_store_data, save_raw_data
from snowflake_loader import load_to_snowflake
# from snowflake_loader import load_to_snowflake

def run_pipeline():

	products_df = fetch_api_store_data("products")
	print(products_df.shape)
	if not products_df.empty:
		logging.info(products_df.head())
	# upload_to_snowflake(products_df, "products_table")
	save_raw_data(products_df, "products_raw.csv")
	
	# load_to_snowflake("products_raw.csv", "products_table", "products_stage")
if __name__ == "__main__":
	run_pipeline()
