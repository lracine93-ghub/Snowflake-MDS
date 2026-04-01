# ☁️ Cloud-Native Retail Analytics Pipeline
An end-to-end ELT (Extract, Load, Transform) data pipeline moving data from a live REST API into a modeled Snowflake Cloud Data Warehouse.

## 📌 Project Overview
This project demonstrates a production-grade data engineering workflow designed for a 2-month delivery timeline. It solves the challenge of capturing rapidly changing product and transactional data, centralizing it in a high-performance cloud data warehouse, and modeling it for advanced business intelligence.

---

## 🏗️ Architecture & Tech Stack
* **Extraction:** Python (Requests, Pandas)
* **Storage & Orchestration:** Local file system staging moving to Cloud
* **Cloud Data Warehouse:** Snowflake (Internal Stages, Streams, & Stored Procedures)
* **Data Modeling:** Star Schema (Fact & Dimension tables)

---

## 📂 Repository Structure
A modular approach was taken to ensure high maintainability, isolated testing, and clean environment separation.

* `config.py` - Centralized configuration using environment variables (`.env`).
* `api_extract.py` - Connects to the REST API and extracts raw data to local CSVs.
* `generate_sales.py` - Python script generating 5,000+ randomized, relationally-sound sales transactions.
* `snowflake_loader.py` - Manages Snowflake `PUT` and `COPY INTO` commands to move local files to cloud staging.
* `validate_db_load.py` - Automated post-load verification checking row counts to ensure data integrity.
* `main.py` - The master orchestrator file that executes the full pipeline end-to-end.
* `run_proc.py` - Manages Snowflake `MERGE` protocol to ensure idempotency

---

## 📆 8-Week Agile Roadmap
* **Sprint 1 (Weeks 1-2):** Local Python extraction, mock data generators, and modular environment setup. **(Completed)**
* **Sprint 2 (Weeks 3-4):** Activation of Snowflake trial, schema design, file staging, and table creation. **(Completed)**
* **Sprint 3 (Weeks 5-6):** Advanced SQL transformations, processing semi-structured JSON, and establishing 
                            continuous `MERGE` protocols. **(Completed)**
* **Sprint 4 (Weeks 7-8):** BI Connection, performance tuning, and final documentation.

---

## 🧠 "Senior" Engineering Decisions Made Here
To elevate this project beyond a standard beginner tutorial, specific architectural choices were implemented:
1. **Cloud-First ELT vs. Legacy ETL:** Bypassed intermediate relational databases to load flat files directly into Snowflake stages, reducing latency.
2. **Idempotency via MERGE:** Implemented SQL `MERGE` statements in Snowflake to prevent duplicate records on continuous pipeline runs.
3. **Automated Validation:** Built a specific script to run counts and verify successful API payload landings before signaling task success.
4. **Credential Security:** Leveraged `.env` masking to ensure sensitive cloud warehouse keys never hit public source control.

---

## 🚀 How to Run Locally

1. **Clone the repository:**
   ```bash
   git clone https://github.com/lracine93-ghub/Snowflake-MDS
   cd Snowflake-MDS
2. **Install Dependencies**
    ```bash
    pip install -r requirements.txt

3. **Configure environment variables**
    Create a .env file in the root directory and add your Snowflake and API credentials (see config.py for required keys).

4. **Execute the Pipeline 🚀**
    ```bash
    python main.py
