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


def validate_star_schema():
    logging.info("Validating star schema structure in Snowflake...")

    query = """
    SELECT 
        p.category,
        COUNT(f.sales_id) AS total_orders,
        SUM(f.qty) AS total_items_sold,
        ROUND(SUM(f.total_amt), 2) AS total_revenue
    FROM SALES_ANALYTICS.CORE.FACT_SALES f
    JOIN SALES_ANALYTICS.CORE.DIM_PRODUCTS p 
    ON f.product_id = p.product_id
    GROUP BY p.category
    ORDER BY total_revenue DESC
    LIMIT 5;"""

    conn = get_snowflake_connection()
    try:
        cur = conn.cursor()
        cur.execute(query)
        results = cur.fetchall()

        logging.info("Star schema validation query executed successfully. Sample results:")
        for row in results:  # Print first 5 rows of the result
            logging.info(row)
    except Exception as e:
        logging.error(f"Star schema validation failed: {e}")
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    validate_record_count("STG_PRODUCTS_RAW")