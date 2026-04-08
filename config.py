import os
import logging
from pathlib import Path

from dotenv import load_dotenv
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
import snowflake.connector

# LOCATE THE DIRECTORY OF THIS FILE TO FIND THE .ENV
env_path = Path('.') / '.env'
load_dotenv(dotenv_path=env_path)

class Config:
    """Centralized configuration management for the ETL pipeline."""
    
    # SNOWFLAKE SETTINGS
    SNOW_USER = os.getenv('SNOW_USER')
    SNOW_PASS = os.getenv('SNOW_PASS')
    SNOW_ACCOUNT = os.getenv('SNOW_ACCOUNT')
    SNOW_WAREHOUSE = os.getenv('SNOW_WAREHOUSE')
    SNOW_DATABASE = os.getenv('SNOW_DATABASE')
    SNOW_SCHEMA = os.getenv('SNOW_SCHEMA')
    SNOW_ROLE = os.getenv('SNOW_ROLE')

    # SNOWFLAKE PRIVATE KEY SETTINGS
    SNOW_PKEY_PATH = os.getenv('SNOW_PRIVATE_KEY_PATH')
    SNOW_PKEY_PASSPHRASE = os.getenv('SNOW_PKEY_PASSPHRASE')
    
    # API SETTINGS
    API_BASE_URL = os.getenv('SALES_API_URL')
    
    # LOCAL PATHS
    BASE_DIR = Path(__file__).resolve().parent
    DATA_DIR = BASE_DIR / "data"
    
    # CREATE DATA DIRECTORY IF IT DOESN'T EXIST
    DATA_DIR.mkdir(exist_ok=True)

# INSTANTIATE CONFIG FOR USE IN OTHER FILES
config = Config()

def get_snowflake_connection():
    """Establish a connection to Snowflake using credentials from the config and Key Pair Authentication."""
    try:
        with open(Config.SNOW_PKEY_PATH, "rb") as key_file:
            private_key = serialization.load_pem_private_key(
                key_file.read(),
                password=Config.SNOW_PKEY_PASSPHRASE.encode(),
            )
        private_key_der = private_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        )
        conn = snowflake.connector.connect(
            user=Config.SNOW_USER,
            account=Config.SNOW_ACCOUNT,
            warehouse=Config.SNOW_WAREHOUSE,
            database=Config.SNOW_DATABASE,
            schema=Config.SNOW_SCHEMA,
            role=Config.SNOW_ROLE,
            # PRIVATE_KEY_CONTENT=CONFIG.SNOW_PKEY_PATH,
            # PRIVATE_KEY_PASSPHRASE=CONFIG.SNOW_PKEY_PASSPHRASE,
            private_key=private_key_der
        )
        return conn
    except Exception as e:
        logging.error(f"Error connecting to Snowflake: {e}")
        return None

def header():
    """Return a standard header for API requests."""
    return {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }