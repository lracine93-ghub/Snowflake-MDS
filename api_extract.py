import os

import pandas as pd
import requests
import logging
import time
from config import config, header
from curl_cffi import requests as curl_requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
import snowflake.connector
 

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def fetch_api_store_data(endpoint: str, max_retries=5, initial_delay=1) -> pd.DataFrame:
	"""Fetch data from FakeStore API with retry/backoff to reduce 403/429 risk."""
	url = f"https://fakestoreapi.com/{endpoint}"
	delay = initial_delay

	for attempt in range(1, max_retries + 1):
		try:
			logging.info(f"Attempt {attempt}/{max_retries}: GET {url}")
			response = curl_requests.get(url, headers=header(), timeout=15, impersonate="chrome")
			response.raise_for_status()

			data = response.json()
			logging.info(f"Data fetched {len(data)} items successfully from {endpoint}")
			return pd.DataFrame(data)

		except requests.HTTPError as e:
			status = e.response.status_code if e.response is not None else None
			logging.warning(f"HTTP {status} on attempt {attempt}: {e}")

			if status is None or status in (403, 429, 500, 502, 503, 504, 523, 524):
				if attempt == max_retries:
					logging.error("Max retries reached. Returning empty DataFrame.")
					break
				logging.info(f"Sleeping {delay}s before retry")
				time.sleep(delay)
				delay *= 2
				continue
			else:
				logging.error("Non-retryable HTTP error. Aborting.")
				break

		except (requests.ConnectionError, requests.Timeout) as e:
			logging.warning(f"Network error on attempt {attempt}: {e}")
			if attempt == max_retries:
				logging.error("Max retries reached for network error.")
				break
			logging.info(f"Sleeping {delay}s before retry")
			time.sleep(delay)
			delay *= 2
			continue

		except Exception as e:
			logging.error(f"Unexpected error: {e}")
			break

	return pd.DataFrame()




def save_raw_data(df: pd.DataFrame, filename: str):
	'''Save the raw DataFrame to a CSV file in the data directory.'''
	if df.empty:
		logging.warning(f"No data to save for {filename}")
		return
	filepath = config.DATA_DIR / filename
	df.to_csv(filepath, index=False)
	logging.info(f"Data saved to {filepath}")


def upload_to_snowflake_db(df: pd.DataFrame, table_name: str):
	'''Upload a DataFrame to Snowflake using the Snowflake Connector.'''
	if df.empty:
		logging.warning(f"No data to upload for {table_name}")
		return

	conn = snowflake.connector.connect(
		user=config.SNOW_USER,
		password=config.SNOW_PASS,
		account=config.SNOW_ACCOUNT,
		warehouse=config.SNOW_WAREHOUSE,
		database=config.SNOW_DATABASE,
		schema=config.SNOW_SCHEMA,
		role=config.SNOW_ROLE
	)

	cursor = conn.cursor()
	try:
# Stage the file in Snowflake
		file_abs_path = os.path.abspath(config.DATA_DIR / 'products_raw.csv')
		cursor.execute(f"PUT file://{file_abs_path} @%{table_name}")

# Move data from stage to Snowflake table
		cursor.execute(f"COPY INTO {table_name} FROM @%{table_name} FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '\"' SKIP_HEADER = 1) ON_ERROR = 'CONTINUE'")
		conn.commit()
		logging.info(f"Data uploaded to Snowflake table {table_name}")
	except Exception as e:
		logging.error(f"Error uploading to Snowflake: {e}")
	finally:
		cursor.close()
		conn.close()