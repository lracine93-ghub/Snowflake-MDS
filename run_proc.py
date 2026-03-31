import logging
from config import Config
from snowflake_loader import get_snowflake_connection
import snowflake_loader

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def run_transformation():
    """Run a stored procedure in Snowflake"""
    conn = get_snowflake_connection()
    
    try:
        cur = conn.cursor()
        logging.info(f"Running Data Transformation procedure SALES_ANALYTICS.CORE.PR_MERGE_PRODUCTS()...")
        cur.execute(f"CALL SALES_ANALYTICS.CORE.PR_MERGE_PRODUCTS()")
        logging.info(f"Procedure SALES_ANALYTICS.CORE.PR_MERGE_PRODUCTS() executed successfully.")
    except Exception as e:
        logging.error(f"Error executing procedure SALES_ANALYTICS.CORE.PR_MERGE_PRODUCTS(): {e}")
    finally:
        cur.close()
        conn.close()