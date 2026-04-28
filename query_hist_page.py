import logging
import os

from config import Config, get_snowflake_connection
import streamlit as st
import pandas as pd
import snowflake.connector
from cryptography.hazmat.primitives import serialization

# SET UP PAGE CONFIGURATION 
st.set_page_config(
    page_title="Query History Dashboard", 
    page_icon="📊",
    layout="wide"
)

st.title("Query History Dashboard")
st.markdown("""Visualize Average Query Run Time by Schema & Query Type""")

@st.cache_resource # CACHE THE SNOWFLAKE CONNECTION TO REUSE ACROSS INTERACTIONS
def get_cached_conn():
    return get_snowflake_connection()

conn = get_cached_conn()

if conn:
    st.sidebar.success("🟢 Connected to Live Snowflake Warehouse")
else:
    st.sidebar.warning("🟡 Offline Mode: Using Cached Sample Data")
     
try:
     # QUERY HISTORY DASHBOARD
    st.subheader("🛠️ Pipeline Audit: Query Performance")
    st.markdown("Monitoring execution patterns for the `INGESTOR_ROLE` to ensure least-privilege compliance.")
    qh_df = pd.read_sql("""SELECT 
                                SCHEMA_NAME,
                                QUERY_TYPE,
                                COUNT(*) AS TOTAL_QUERIES,
                                GREATEST(0.000,AVG(TOTAL_ELAPSED_TIME) / 1000) AS AVERAGE_QUERY_RUN_TIME_SEC 
                            FROM TABLE(SNOWFLAKE.INFORMATION_SCHEMA.QUERY_HISTORY()) 
                            WHERE ROLE_NAME = 'INGESTOR_ROLE'
                            AND SCHEMA_NAME IS NOT NULL
                            GROUP BY 1,2
                            ORDER BY TOTAL_QUERIES DESC 
                        ;""", con=conn)
    st.subheader("Recent Query History")
    st.dataframe(qh_df, width = 'stretch')

    st.bar_chart( data = qh_df, x='SCHEMA_NAME',  y='TOTAL_QUERIES', height=400, stack = False, color = 'QUERY_TYPE')
  #   st.bar_chart( data = qh_df, x='QUERY_TYPE',  y=['QUERY_TOTAL', 'LONGEST_QUERY_RUN_TIME'], height="content", stack = False, color = ['#1f77b4', '#ff7f0e'])
    
except Exception as e:
    logging.error(f"Error connecting to Snowflake: {e}")
    st.error(f"Connection error logged.")