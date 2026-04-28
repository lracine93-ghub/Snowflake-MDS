import logging
import os

from config import Config, get_snowflake_connection
import streamlit as st
import pandas as pd
import snowflake.connector
from cryptography.hazmat.primitives import serialization

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
    return get_snowflake_connection()

conn = get_cached_conn()

if conn is not None:
    logging.info("Successfully connected to Snowflake for dashboard.")
else:
    logging.error("Failed to connect to Snowflake for dashboard.")   
 
try:
     # SIDEBAR FILTER
    st.sidebar.header("Dashboard Filters")

    # FIX 1 & 3: USE THE NATIVE CONNECTION DIRECTLY WITH PANDAS!
    category_df = pd.read_sql("SELECT DISTINCT CATEGORY FROM SALES_ANALYTICS.CORE.DIM_PRODUCTS;", con=conn)
    
    # CAPITALIZING THE COLUMN KEY BECAUSE SNOWFLAKE RETURNS IT UPPERCASE
    selected_category = st.sidebar.selectbox("Select a Product Category", category_df['CATEGORY'].tolist())

    # MAIN METRIC CARDS
    st.subheader(f"Top 3 Performing Products by Category: {selected_category}")

    # QUERY TO FETCH TOP 3 PRODUCTS FOR THE SELECTED CATEGORY
    top_products_query = """
        SELECT PRODUCT_NAME, TOTAL_UNITS_SOLD
        FROM SALES_ANALYTICS.ANALYTICS.vw_Sales_By_Prod_Ctgry
        WHERE Category = %s
        ORDER BY SALES_RANK ASC;
    """
    
    cur = conn.cursor()
    cur.execute(top_products_query, (selected_category,))
    results = cur.fetchall()

    if results:
        cols = st.columns(3)
        for i, row in enumerate(results):
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
    mom_df = pd.read_sql("""
        SELECT SALES_MONTH, MOM_GRWTH_PCT 
        FROM SALES_ANALYTICS.ANALYTICS.vw_Month_to_Month_Revenue 
        ORDER BY SALES_MONTH ASC;
    """, con=conn)
    
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