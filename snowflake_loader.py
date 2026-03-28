import logging
import os
import snowflake.connector
from config import config


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_snowflake_connection():
    """Establish a connection to Snoflake using credentials from the config"""
    try: 
        conn = snowflake.connector.connect(
            user=config.SNOW_USER,
            password=config.SNOW_PASS,
            account=config.SNOW_ACCOUNT,
            warehouse=config.SNOW_WAREHOUSE,
            database=config.SNOW_DATABASE,
            schema=config.SNOW_SCHEMA,
            role=config.SNOW_ROLE
        )
        logging.info("Successfully connected to Snowflake")
        return conn
    except Exception as e:
        logging.error(f"Error connecting to Snowflake: {e}")
        raise 

def load_to_snowflake(file_name: str, table_name: str, stage_name: str):
    local_path = config.DATA_DIR / file_name

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
        FROM @{stage_name}/{file_name}  
        FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1)
        ON_ERROR = 'CONTINUE'"""
        cur.execute(copy_sql)

        logging.info(f"Data loaded successfully into {table_name}")
    
    except Exception as e: 
        logging.error(f"Error occurred while staging file: {e}")
        raise
    finally:
        cur.close()
        conn.close()

    