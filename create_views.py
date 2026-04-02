import logging
from config import Config
import snowflake.connector  

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def create_reporting_views():
    """Establish a connection to Snowflake using credentials from the config"""
   
    conn = snowflake.connector.connect(
        user=Config.SNOW_USER,
        password=Config.SNOW_PASS,
        account=Config.SNOW_ACCOUNT,
        warehouse=Config.SNOW_WAREHOUSE,
        database=Config.SNOW_DATABASE,
        schema=Config.SNOW_SCHEMA,
        role=Config.SNOW_ROLE
    )
    cursor = conn.cursor()

    try:
        logging.info("Creating reporting views in Snowflake...")
        # CREATE SCHEMA FOR ANALYTICS 
        cursor.execute("""CREATE SCHEMA IF NOT EXISTS SALES_ANALYTICS.ANALYTICS;""")

        logging.info("Creating view for Top 3 Products by Category...")

        view_top_products = """ 
        Create or Replace View sales_analytics.Analytics.vw_Sales_By_Prod_Ctgry as 
        WITH rankedSales AS (
            SELECT 
                dp.CATEGORY,
                DP.TITLE AS PRODUCT_NAME,
                SUM(fs.QTY) AS TOTAL_UNITS_SOLD,
                DENSE_RANK() OVER (PARTITION BY dp.CATEGORY ORDER BY SUM(fs.QTY) DESC) AS SALES_RANK
            FROM SALES_ANALYTICS.CORE.FACT_SALES AS fs
            JOIN SALES_ANALYTICS.CORE.DIM_PRODUCTS AS dp
                ON fs.PRODUCT_ID = dp.PRODUCT_ID
            GROUP BY  dp.CATEGORY, dp.TITLE
        )
        SELECT  CATEGORY, TOTAL_UNITS_SOLD, PRODUCT_NAME, SALES_RANK
        FROM rankedSales
        WHERE SALES_RANK <= 3
        ; """
        
        cursor.execute(view_top_products)

        logging.info("Creating view for Month over Month Sales Revenue...")

        view_mom_sales = """
        Create or Replace View sales_analytics.Analytics.vw_Month_to_Month_Revenue as 
        WITH MonthRevenue AS (
            SELECT 
                date_trunc('Month', transaction_date) as Sales_Month,
                round(sum(qty),2) as Current_Month_Revenue
            FROM SALES_ANALYTICS.CORE.FACT_SALES 
            group by 1
            )
            select Sales_Month,
                    Current_Month_Revenue,
                    lag(Current_Month_Revenue, 1) over (order by Sales_Month) as Previous_Month_Revenue,
                    round(
                            (Current_Month_Revenue - lag(Current_Month_Revenue, 1) over (order by Sales_month)) 
                        / nullif(lag(Current_Month_Revenue, 1) over (order by Sales_month), 0) * 100, 2
                        ) as mom_grwth_pct
        from MonthRevenue;"""

        cursor.execute(view_mom_sales)

        logging.info("Successfully connected to Snowflake")
        return conn
    except Exception as e:
        logging.error(f"Error connecting to Snowflake: {e}")
        raise   