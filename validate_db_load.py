import logging
from config import Config
from snowflake_loader import get_snowflake_connection

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')     

def validate_record_count(table_name: str):
    """Validate that the data was loaded correctly by counting rows in the target table"""
    conn = get_snowflake_connection()
    
    try:
        cur = conn.cursor()

        query = f"SELECT COUNT(*) from {table_name}"
        cur.execute(query) 

        result = cur.fetchone()

        row_count = result[0] if result else 0

        if row_count > 0: 
            logging.info(f"Data load into {table_name} validation completed successfully.")
        else:
            logging.warning(f"No data found in table {table_name}. Validation failed.")
    except Exception as e:
        logging.error(f"Data load into {table_name} validation failed: {e}")
    finally:
        cur.close()
        conn.close()    
    
    if __name__ == "__main__":
        validate_record_count("STG_PRODUCTS_RAW")