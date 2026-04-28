import logging
import os

from config import Config, get_snowflake_connection
import streamlit as st
import pandas as pd
import snowflake.connector
from cryptography.hazmat.primitives import serialization


st.navigation([st.Page("sales_dash.py"),st.Page("query_hist_page.py")]).run()

