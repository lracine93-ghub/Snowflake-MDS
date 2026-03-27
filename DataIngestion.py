import pandas as pd

def fetch_taxi_data(taxi_type = "yellow", year = 2024, month=1): 

	# Ensure month is 2-digit
	month_str = str(month).zfill(2)
	
	# Construct the URL
	url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi_type}_tripdata_{year}-{month_str}.parquet"
	print(f"Fetching data from {url}")

	# Read the Parquet file into a DataFrame
	try:
		df = pd.read_parquet(url)
		print(f"Data fetched successfully for {taxi_type} taxi, {year}-{month_str}")
	except Exception as e:
		print(f"Error fetching data from {url}: {e}")
		return pd.DataFrame()
	return df

if __name__ == "__main__":
	# Example usage
	taxi_type = "yellow"  # or "green"
	year = 2024
	month = 1
	df = fetch_taxi_data(taxi_type, year, month)
	print(df.head())

	