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
 

# CONFIGURE LOGGING
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# FETCH DATA FROM FAKESTORE API WITH RETRY/BACKOFF AND ANTI-BOT DETECTION TO REDUCE 403/429/523/524 RISK.
def fetch_api_store_data(endpoint: str, max_retries=5, initial_delay=1) -> pd.DataFrame:

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

# SAVE THE RAW DATA TO A CSV FILE IN THE (LOCAL) DATA DIRECTORY
def save_raw_data(df: pd.DataFrame, filename: str):
	'''Save the raw DataFrame to a CSV file in the data directory.'''
	if df.empty:
		logging.warning(f"No data to save for {filename}")
		return
	filepath = config.DATA_DIR / filename
	df.to_csv(filepath, index=False)
	logging.info(f"Data saved to {filepath}")


