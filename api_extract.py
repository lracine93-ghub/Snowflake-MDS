import pandas as pd
import requests
import logging
from config import config

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def fetch_api_store_data(endpoint: str) -> pd.DataFrame:
	"""Fetch data from an FakeStore API endpoint and return it as a DataFrame."""
	url = f"https://fakestoreapi.com/{endpoint}"
	try:
		response = requests.get(url, timeout = 10)
		response.raise_for_status()  # Raise an error for HTTP errors
		data = response.json()
		logging.info(f"Data fetched {len(data)} items successfully from {endpoint}")
		return pd.DataFrame(data)
	except requests.RequestException as e:
		logging.error(f"Error fetching API data from {endpoint}: {e}")
		return pd.DataFrame()

def save_raw_data(df: pd.DataFrame, filename: str):
	'''Save the raw DataFrame to a CSV file in the data directory.'''
	if df.empty:
		logging.warning(f"No data to save for {filename}")
		return
	filepath = config.DATA_DIR / filename
	df.to_csv(filepath, index=False)
	logging.info(f"Data saved to {filepath}")

def fetch_taxi_data(taxi_type = "yellow", year = 2024, month=1):  

	# Ensure month is 2-digit
	month_str = str(month).zfill(2)
	
	# Construct the URL
	url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi_type}_tripdata_{year}-{month_str}.parquet"
	logging.info(f"Fetching data from {url}")

	# Read the Parquet file into a DataFrame
	try:
		df = pd.read_parquet(url)
		logging.info(f"Data fetched successfully for {taxi_type} taxi, {year}-{month_str}")
	except Exception as e:
		logging.error(f"Error fetching data from {url}: {e}")
		return pd.DataFrame()
	return df

