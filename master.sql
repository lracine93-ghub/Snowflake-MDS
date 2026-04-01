-- ==========================================
-- 1. INFRASTRUCTURE SETUP
-- ==========================================
USE ROLE ACCOUNTADMIN;

-- Create a small warehouse to save free credits
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH 
    WITH WAREHOUSE_SIZE = 'XSMALL' 
    AUTO_SUSPEND = 60 
    AUTO_RESUME = TRUE;

-- Create dedicated pipeline role
CREATE ROLE IF NOT EXISTS ingestor_role;
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE ingestor_role;

-- ==========================================
-- 2. DATABASE & SCHEMA SETUP
-- ==========================================
CREATE DATABASE IF NOT EXISTS SALES_ANALYTICS;

-- Staging schema for raw landing
CREATE SCHEMA IF NOT EXISTS SALES_ANALYTICS.STAGING;
-- Core schema for modeled Star Schema data
CREATE SCHEMA IF NOT EXISTS SALES_ANALYTICS.CORE;

-- Give ingestor_role access to the database
GRANT USAGE ON DATABASE SALES_ANALYTICS TO ROLE ingestor_role;
GRANT USAGE, CREATE TABLE ON SCHEMA SALES_ANALYTICS.STAGING TO ROLE ingestor_role;
GRANT USAGE, CREATE TABLE ON SCHEMA SALES_ANALYTICS.CORE TO ROLE ingestor_role;

-- ==========================================
-- 3. STAGING AREA SETUP
-- ==========================================
USE SCHEMA SALES_ANALYTICS.STAGING;

-- Create the Internal Stage
CREATE OR REPLACE STAGE api_landing_zone
    FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
    COMMENT = 'Landing zone for raw API data from Python ingestion';

GRANT READ, WRITE ON STAGE api_landing_zone TO ROLE ingestor_role;

-- Create the raw target table for your products
CREATE OR REPLACE TABLE STG_PRODUCTS_RAW (
    id INT,
    title STRING,
    price FLOAT,
    description STRING,
    category STRING,
    image STRING,
    rating OBJECT -- Handles nested JSON data smoothly
);


CREATE OR REPLACE TABLE DIM_PRODUCTS
    product_id INT PRIMARY KEY,
    title  STRING,
    price FLOAT,
    description STRING,
    rating:rate::FLOAT AS rating_score,
    rating:count::INT AS rating_count-- Handles nested JSON data smoothly
);



