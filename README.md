# ☁️ Cloud-Native Retail Analytics Pipeline
An end-to-end ELT (Extract, Load, Transform) data pipeline that orchestrates automated ingestion of semi-structured API data and mock sales streams into a modeled Snowflake Star Schema. This project demonstrates advanced data engineering concepts including bulk loading, state-of-the-art bot-detection bypass, SQL window functions, and real-time BI visualization.

---

## 🏗️ Architecture & Tech Stack
* **Extraction:** Python script using `curl_cffi` to impersonate browser TLS fingerprints, cleanly bypassing Cloudflare edge bot    detection to fetch products from a public mock REST API.
* **Generation:** Python script generating 5,000+ localized, relationally-sound transactional records to simulate a high-traffic retail environment.
* **Orchestration & Load:** Master `main.py` pipeline utilizing the native Snowflake Connector to compress and bulk-stream local CSV files into Snowflake internal stages and execute `COPY INTO` commands.
* **Transformation:** Multi-layered database modeling in Snowflake (Staging -> Core Star Schema -> Analytics Reporting Views) utilizing complex analytical calculations such as `DENSE_RANK()` and `LAG()`.
* **Presentation:** A full-stack, real-time dashboard built in Python using **Streamlit** to visualize KPIs and month-over-month growth directly from the database.

---

## 🚀 Key Features & Senior Concepts
To elevate this project beyond a standard beginner tutorial, specific architectural choices were implemented to handle scale, security, and edge-case failures:

**Cloud-First ELT vs. Legacy ETL**: Bypassed row-by-row SQLAlchemy processing and intermediate transformations to bulk-stream flat files directly into compressed Snowflake internal stages, drastically reducing latency.

**Idempotency via `MERGE`**: Implemented SQL `MERGE` statements and deduplication logic to ensure that running the pipeline multiple times never creates duplicate records.

**Anti-Bot Mitigation & Resilience**: Leveraged `curl_cffi` for TLS fingerprint impersonation to bypass aggressive Cloudflare edge blocks, and designed an automated retry loop with exponential backoff for network drops.

**Automated Data Validation**: Built isolated automated check scripts to run counts and verify successful API payload landings before signaling a successful pipeline run.

**Zero-Trust Credential Security**: Isolated all database credentials and warehouse keys via .env masking, ensuring zero sensitive data ever leaked into public source control history.

## 🛠️ Tech Stack

* **Language:** Python 3.x
* **Data Warehousing:** Snowflake
* **Libraries:** Pandas, curl_cffi, snowflake-connector-python, Streamlit
* **Security:** Dotenv (Environment variable isolation)

## 📂 Repository Structure
A modular approach was taken to ensure high maintainability, isolated testing, and clean environment separation.

* `main.py` - The master orchestrator file that executes the full pipeline end-to-end.
* `config.py` - Centralized configuration using environment variables (`.env`).
* `api_extract.py` - Connects to the REST API and extracts raw data to local CSVs.
* `snowflake_loader.py` - Manages Snowflake `PUT` and `COPY INTO` commands to move local files to cloud staging.
* `run_proc.py` - Manages Snowflake `MERGE` protocol to ensure idempotency
* `generate_sales.py` - Python script generating 5,000+ randomized, relationally-sound sales transactions.
* `validate_db_load.py` - Automated post-load verification checking row counts to ensure data integrity.
* `create_views.py` - Manages creation of views for reporting and visualization
* `digital_dash.py` - Real-time dashboard using **Streamlit** to present KPI and Month over Month Revenue from database 

---

## 📆 8-Week Agile Roadmap
* **Sprint 1 (Weeks 1-2):** Local Python extraction, mock data generators, and modular environment setup. **(Completed)**
* **Sprint 2 (Weeks 3-4):** Activation of Snowflake trial, schema design, file staging, and table creation. **(Completed)**
* **Sprint 3 (Weeks 5-6):** Advanced SQL transformations, processing semi-structured JSON, and establishing 
                            continuous `MERGE` protocols. **(Completed)**
* **Sprint 4 (Weeks 7-8):** BI Connection, performance tuning, and final documentation.

---

## 🚀 How to Run Locally

1. **Clone the repository:**
   # bash
   git clone https://github.com/lracine93-ghub/Snowflake-MDS
   cd Snowflake-MDS

2.  # bash
    python -m venv venv
    # On Windows
    venv\Scripts\activate
    # On Mac/Linux
    source venv/bin/activate

3. **Install Dependencies**
    # bash
    pip install -r requirements.txt

4. **Configure environment variables**
    Create a .env file in the root directory and add your Snowflake and API credentials:
    
    SNOW_USER={your_username}

    SNOW_PASS={your_password}

    SNOW_ACCOUNT={your_account_locator}

    SNOW_WAREHOUSE=COMPUTE_WH

    SNOW_DATABASE=SALES_ANALYTICS

    SNOW_SCHEMA=CORE
    
    SNOW_ROLE=ACCOUNTADMIN

5. **Execute the Pipeline 🚀**
     # bash
    python main.py
