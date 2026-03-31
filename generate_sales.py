import os
import logging
import pandas as pd
from config import Config   
from datetime import datetime as dt, timedelta
import random
import numpy as np 

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def generate_sales_data(num_records=1000):
    logging.info(f"Generating {num_records} sales records.")
    
    file_name = "products_raw.csv"

    local_path = Config.DATA_DIR / file_name
    # Load product data to get product IDs and prices
    products_df = pd.read_csv(local_path)
    product_ids = products_df['id'].tolist()
    product_prices = dict(zip(products_df['id'], products_df['price']))

    # Generate random sales data
    sales_data = []
    for i in range(num_records):
        product_id = random.choice(product_ids)
        quantity = random.randint(1, 10)
        total_price = quantity * product_prices[product_id]
        sale_date = dt.now() - timedelta(days=random.randint(0, 180))  # Sales from the last 6 months
        
        sales_data.append({
            "sale_id": f"S{1000 + i}",
            "product_id": product_id,
            "quantity": quantity,
            "total_price": total_price,
            "sale_date": sale_date.strftime("%Y-%m-%d %H:%M:%S")
        })

    sales_df = pd.DataFrame(sales_data)
    logging.info(f"Generated sales data with shape: {sales_df.shape}")

    output_file = Config.DATA_DIR / "sales_raw.csv"
    sales_df.to_csv(output_file, index=False)
    logging.info(f"Mock Sales data ({num_records}) saved to {output_file}")
    
    return sales_df