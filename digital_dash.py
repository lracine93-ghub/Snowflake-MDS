import logging
import os
from config import Config   
import streamlit as st
import pandas as pd
import snowflake.connector

# Set up page configuration 
st.set_page_config(
    page_title="Sales Analytics Dashboard", 
    page_icon="📊",
    layout="wide"
)

st.title("Cloud-Native Sales Analytics Dashboard")
st.markdown("""Visualize real-time data pipeline ingestions and modeled database views.""")

@st.cache_resource # Cache the Snowflake connection to reuse across interactions
def get_snowflake_connection():
    return snowflake.connector.connect(
        user=Config.SNOW_USER,
        password=Config.SNOW_PASS,
        account=Config.SNOW_ACCOUNT,
        warehouse=Config.SNOW_WAREHOUSE,
        database=Config.SNOW_DATABASE,
        schema=Config.SNOW_SCHEMA,
        role=Config.SNOW_ROLE
    )

try:
    conn = get_snowflake_connection()

    # Sidebar Filter
    st.sidebar.header("Dashboard Filters")

    # FIX 1 & 3: Use the native connection directly with Pandas!
    category_df = pd.read_sql("SELECT DISTINCT CATEGORY FROM SALES_ANALYTICS.CORE.DIM_PRODUCTS;", con=conn)
    
    # Capitalizing the column key because Snowflake returns it uppercase
    selected_category = st.sidebar.selectbox("Select a Product Category", category_df['CATEGORY'].tolist())

    # Main Metric Cards
    st.subheader(f"Top 3 Performing Products by Category: {selected_category}")

    # Query to fetch top 3 products for the selected category
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

    # Month-over-Month Sales Revenue Growth
    st.subheader("Month-over-Month Sales Revenue Growth")  
    st.markdown("This line chart shows the percentage growth in sales revenue compared to the previous month. Hover over the points to see exact values.")

    # FIX 2: Swapping to standard Snowflake uppercase columns
    mom_df = pd.read_sql("""
        SELECT SALES_MONTH, MOM_GRWTH_PCT 
        FROM SALES_ANALYTICS.ANALYTICS.vw_Month_to_Month_Revenue 
        ORDER BY SALES_MONTH ASC;
    """, con=conn)
    
    if not mom_df.empty:
        # Format the date properly
        mom_df['SALES_MONTH'] = pd.to_datetime(mom_df['SALES_MONTH']).dt.strftime('%b %Y')
        
        # We set the INDEX to the month (X-axis)
        chart_data = mom_df.set_index('SALES_MONTH')

        # We plot the MOM_GRWTH_PCT (Y-axis)
        st.line_chart(chart_data, y="MOM_GRWTH_PCT", height=400)
    else:
        st.info("No sales revenue data available yet.")
        
except Exception as e:
    logging.error(f"Error connecting to Snowflake: {e}")
    st.error(f"Connection error logged.")