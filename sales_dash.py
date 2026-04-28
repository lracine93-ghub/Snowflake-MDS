import logging
import os

from config import Config, get_snowflake_connection
import streamlit as st
import pandas as pd
import snowflake.connector
from cryptography.hazmat.primitives import serialization
from snowflake_loader import load_offline_data

# SET UP PAGE CONFIGURATION 
st.set_page_config(
    page_title="Sales Analytics Dashboard", 
    page_icon="📊",
    layout="wide"
)


st.title("Cloud-Native Sales Analytics Dashboard")
st.markdown("""Visualize real-time data pipeline ingestions and modeled database views.""")

@st.cache_resource # CACHE THE SNOWFLAKE CONNECTION TO REUSE ACROSS INTERACTIONS
def get_cached_conn():
    try:
        return get_snowflake_connection()
    except Exception:
        return None

# --- DASHBOARD EXECUTION ---
conn = get_cached_conn()

# Visual Indicator for your Portfolio
if conn:
    st.sidebar.success("🟢 Connected to Live Snowflake Warehouse")
else:
    st.sidebar.warning("🟡 Offline Mode: Using Cached Sample Data")

    # conn = get_snowflake_connection()
try:
     # SIDEBAR FILTER
    st.sidebar.header("Dashboard Filters")

    # FIX 1 & 3: USE THE NATIVE CONNECTION DIRECTLY WITH PANDAS!
    # category_df = pd.read_sql("SELECT DISTINCT CATEGORY FROM SALES_ANALYTICS.CORE.DIM_PRODUCTS;", con=conn)
    
    category_df = load_offline_data(
        query="SELECT DISTINCT CATEGORY FROM SALES_ANALYTICS.CORE.DIM_PRODUCTS;",
        fallback_csv="product_cat.csv",
        params = (),  # NO PARAMETERS FOR THIS QUERY
        conn=conn
    )   
    
    # CAPITALIZING THE COLUMN KEY BECAUSE SNOWFLAKE RETURNS IT UPPERCASE
    selected_category = st.sidebar.selectbox("Select a Product Category", category_df['CATEGORY'].tolist())

    # MAIN METRIC CARDS
    st.subheader(f"Top 3 Performing Products by Category: {selected_category}")

    # QUERY TO FETCH TOP 3 PRODUCTS FOR THE SELECTED CATEGORY
    top_products_query = """
        SELECT PRODUCT_NAME, TOTAL_UNITS_SOLD
        FROM SALES_ANALYTICS.ANALYTICS.vw_Sales_By_Prod_Ctgry
        WHERE CATEGORY = %s
        ORDER BY SALES_RANK ASC;
    """
    top_products_df = load_offline_data(
        query=top_products_query,
        params=(selected_category,),
        fallback_csv="vw_sales_by_prod_ctgry.csv",
        conn=conn
    )   

    top_products_df.columns = [col.upper() for col in top_products_df.columns]  # ENSURE COLUMNS ARE UPPERCASE TO MATCH EXPECTED NAMES

    if not conn and not top_products_df.empty:
        top_products_df = top_products_df[top_products_df['CATEGORY'] == selected_category]  # RENAME COLUMNS TO MATCH EXPECTED NAMES

    if top_products_df is not None and not top_products_df.empty:
        cols = st.columns(min(len(top_products_df), 3))
        
        top_products_df = top_products_df.head(3)  # GET TOP N PRODUCTS BASED ON AVAILABLE DATA
     
        for i, row in enumerate(top_products_df.itertuples(index=False)):
            with cols[i]:
                st.metric(
                    label=f'Rank #{i+1}', 
                    value=f"{row[1]} Units Sold", 
                    delta=row[0]
                )     
    else:
        st.info("No sales data available for the selected category yet.")

    st.markdown("---")  

    # MONTH-OVER-MONTH SALES REVENUE GROWTH
    st.subheader("Month-over-Month Sales Revenue Growth")  
    st.markdown("This line chart shows the percentage growth in sales revenue compared to the previous month. Hover over the points to see exact values.")

    # FIX 2: SWAPPING TO STANDARD SNOWFLAKE UPPERCASE COLUMNS
    mom_query = """
        SELECT SALES_MONTH, MOM_GRWTH_PCT 
        FROM SALES_ANALYTICS.ANALYTICS.vw_Month_to_Month_Revenue 
        ORDER BY SALES_MONTH ASC;
    """

    mom_df = load_offline_data(
        query=mom_query,
        params=(),
        fallback_csv="vw_month_to_month_revenue.csv",
        conn=conn   
    )

    if not mom_df.empty:
        # FORMAT THE DATE PROPERLY
        mom_df['SALES_MONTH'] = pd.to_datetime(mom_df['SALES_MONTH']).dt.strftime('%b %Y')
        
        # WE SET THE INDEX TO THE MONTH (X-AXIS)
        chart_data = mom_df.set_index('SALES_MONTH')

        # WE PLOT THE MOM_GRWTH_PCT (Y-AXIS)
        st.line_chart(chart_data, y="MOM_GRWTH_PCT", height=400)
    else:
        st.info("No sales revenue data available yet.")
        
 
except Exception as e:
    logging.error(f"Error connecting to Snowflake: {e}")
    st.error(f"Connection error logged.")