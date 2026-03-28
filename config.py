import os
from pathlib import Path
from dotenv import load_dotenv

# Locate the directory of this file to find the .env
env_path = Path('.') / '.env'
load_dotenv(dotenv_path=env_path)

class Config:
    """Centralized configuration management for the ETL pipeline."""
    
    # Snowflake Settings
    SNOW_USER = os.getenv('SNOW_USER')
    SNOW_PASS = os.getenv('SNOW_PASS')
    SNOW_ACCOUNT = os.getenv('SNOW_ACCOUNT')
    SNOW_WAREHOUSE = os.getenv('SNOW_WAREHOUSE')
    SNOW_DATABASE = os.getenv('SNOW_DATABASE')
    SNOW_SCHEMA = os.getenv('SNOW_SCHEMA')
    SNOW_ROLE = os.getenv('SNOW_ROLE')
    
    # API Settings
    API_BASE_URL = os.getenv('SALES_API_URL')
    
    # Local Paths
    BASE_DIR = Path(__file__).resolve().parent
    DATA_DIR = BASE_DIR / "data"
    
    # Create data directory if it doesn't exist
    DATA_DIR.mkdir(exist_ok=True)

# Instantiate config for use in other files
config = Config()