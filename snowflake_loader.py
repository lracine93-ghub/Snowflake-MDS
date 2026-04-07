import logging
import os
import snowflake.connector
from config import Config


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_snowflake_connection():
    """Establish a connection to Snoflake using credentials from the config"""
    try: 
        conn = snowflake.connector.connect(
            user=Config.SNOW_USER,
            password=Config.SNOW_PASS,
            account=Config.SNOW_ACCOUNT,
            warehouse=Config.SNOW_WAREHOUSE,
            database=Config.SNOW_DATABASE,
            schema=Config.SNOW_SCHEMA,
            role=Config.SNOW_ROLE,
            passphrase=Config.SNOW_PKEY_PASSPHRASE
        )
        logging.info("Successfully connected to Snowflake")
        return conn
    except Exception as e:
        logging.error(f"Error connecting to Snowflake: {e}")
        raise 

def load_to_snowflake(file_name: str, table_name: str, stage_name: str):
    local_path = Config.DATA_DIR / file_name

    if not local_path.exists():
        logging.error(f"File {local_path} does not exist.")
        return

    conn = get_snowflake_connection()
    try:
        cur = conn.cursor()

        # PUT file to Snowflake stage

        logging.info(f"Staging {file_name} to @{stage_name}...")
        cur.execute(f"PUT file://{os.path.abspath(local_path)} @{stage_name} OVERWRITE=TRUE")
        
        # LOAD data from stage to Snowflake table
        logging.info(f"Loading data from @{stage_name} to {table_name}...")
        copy_sql = f"""
        COPY INTO {table_name}
        FROM @{stage_name}/{file_name}.gz
        FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = ',' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
        ON_ERROR = 'CONTINUE'"""
        
        cur.execute(copy_sql)

        logging.info(f"Data loaded successfully into {table_name}")
    
    except Exception as e: 
        logging.error(f"Error occurred while staging file: {e}")
        raise
    finally:
        cur.close()
        conn.close()

def load_sales_to_snowflake(file_name: str, table_name: str, stage_name: str, core_name: str):
    local_path = Config.DATA_DIR / file_name

    if not local_path.exists():
        logging.error(f"File {local_path} does not exist.")
        return

    conn = get_snowflake_connection()
    try:
        cur = conn.cursor()

        # PUT file to Snowflake stage

        logging.info(f"Staging {file_name} to @{stage_name}...")
        cur.execute(f"PUT file://{os.path.abspath(local_path)} @{stage_name} OVERWRITE=TRUE")
        
        # LOAD data from stage to Snowflake table
        logging.info(f"Loading data from @{stage_name} to {table_name}...")
        copy_sql = f"""
        COPY INTO SALES_ANALYTICS.STAGING.{table_name}
        FROM @{stage_name}/{file_name}.gz
        FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = ',' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
        ON_ERROR = 'CONTINUE'"""
        
        cur.execute(copy_sql)

        logging.info(f"Loading sales transaction data into {core_name}")

        insert_sql = f"""
            INSERT INTO SALES_ANALYTICS.CORE.{core_name} 
            (sales_id, product_id, qty, unit_price, total_amt, transaction_date , cust_id)
            SELECT 
                sales_id, 
                product_id, 
                qty,
                unit_price, 
                total_amt, 
                transaction_date :: TIMESTAMP_NTZ, 
                cust_id
            FROM SALES_ANALYTICS.STAGING.{table_name}
"""

        cur.execute(insert_sql)
        logging.info(f"Data loaded successfully into {core_name}")  
    
    except Exception as e: 
        logging.error(f"Error occurred while staging file: {e}")
        raise
    finally:
        cur.close()
        conn.close()